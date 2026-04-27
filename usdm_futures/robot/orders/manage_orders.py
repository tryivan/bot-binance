import asyncio
import logging
from typing import Any, Callable, Dict, List, Literal, Optional, Set, Tuple, cast
from .order_utils import OrderUtils

import ccxt.async_support as ccxt


class ManageOrders:
    """
    Gerencia ordens de trading na exchange (async).

    Responsabilidades:
    - Configurar alavancagem
    - Buscar posições e ordens
    - Criar e cancelar ordens
    - Normalizar estado de posição
    """

    # SL/TP em Binance Futures com stopPrice são roteados pelo CCXT para o endpoint
    # /fapi/v1/algoOrder (CONDITIONAL). O id retornado é um algoId, distinto do
    # espaço de orderId — por isso fetch_order precisa de params={"stop": True}
    # para bater no endpoint algo certo. Latência residual é coberta pelo retry.
    _SL_CONFIRM_ATTEMPTS = 3
    _SL_CONFIRM_DELAY = 0.5
    _DEAD_ORDER_STATUSES = {"canceled", "expired", "rejected"}
    # Margem acima do timeout do CCXT (30s) — defesa secundária contra
    # conexões aceitas mas nunca respondidas (incidente testnet 19/04).
    _FETCH_POSITIONS_TIMEOUT = 35.0
    # Loop externo de normalize_position_state: cobre falso negativo
    # transiente em _fetch_open_orders (I6) e rejeição -2021 de recriação
    # quando preço está momentaneamente no stopPrice.
    _NORMALIZE_MAX_ATTEMPTS = 3
    _NORMALIZE_RETRY_DELAY = 1.5

    def __init__(
        self,
        exchange: ccxt.Exchange,
        symbol: str,
        leverage: int,
        amount: float,
        stop_loss_percent: float,
        take_profit_percent: float,
        chase_percent: float,
        offset_percent: float,
        fill_timeout: int,
        max_retries: int,
        logger: logging.Logger,
        error_handler: Callable[[Exception], Exception],
        utils: OrderUtils,
        order_type: str = "market",
    ) -> None:
        self._exchange = exchange
        self._log = logger
        self._error_handler = error_handler
        self._utils = utils

        self._symbol = symbol
        self._leverage = leverage
        self._amount = amount
        self._order_type = order_type

        self._percent_sl = stop_loss_percent
        self._percent_tp = take_profit_percent

        self._max_retries = max_retries
        self._chase_percent = chase_percent
        self._offset_percent = offset_percent
        self._entry_fill_timeout = fill_timeout

    async def initialize(self) -> None:
        """Limpa ordens residuais e configura margem ISOLATED e alavancagem.

        A limpeza inicial via cancel_all_orders é pré-requisito de
        set_margin_mode/set_leverage: Binance rejeita com -4067 ("Position
        side cannot be changed if there exists open orders") se existirem
        ordens no par (cenário comum após crash/reboot com órfãs).

        Margem isolada é forçada para todo par: contém o risco de uma operação
        ruim ao valor da margem daquela posição, sem derramar para o saldo
        livre. Idempotência: CCXT 4.x silencia -4046 por padrão e devolve
        {"code": -4046, ...} no retorno; cobrimos também o caso em que
        MarginModeAlreadySet for levantada (throwMarginModeAlreadySet=True).
        """
        await self.cancel_all_orders()

        try:
            response = await self._exchange.set_margin_mode("ISOLATED", self._symbol)
        except ccxt.MarginModeAlreadySet:
            self._log.debug(f"[{self._symbol}] Margem já configurada como ISOLATED")
        except Exception as exc:
            domain_error = self._error_handler(exc)
            raise domain_error from exc
        else:
            if isinstance(response, dict) and response.get("code") == -4046:
                self._log.debug(f"[{self._symbol}] Margem já configurada como ISOLATED")
            else:
                self._log.info(f"[{self._symbol}] Margem configurada como ISOLATED")

        try:
            await self._exchange.set_leverage(self._leverage, self._symbol)
        except Exception as exc:
            domain_error = self._error_handler(exc)
            raise domain_error from exc
        self._log.info(f"[{self._symbol}] Alavancagem configurada: {self._leverage}x")

    # -------------------------------------------------------------------------
    # Busca de dados na exchange
    # -------------------------------------------------------------------------

    async def _get_current_price(self) -> Optional[float]:
        """Recupera o preço de mercado atual."""
        try:
            ticker = await self._exchange.fetch_ticker(self._symbol)
            return ticker["last"]
        except Exception as e:
            domain_error = self._error_handler(e)
            self._log.error(f"[{self._symbol}] Erro ao buscar preço: {domain_error}")
            return None

    async def _fetch_positions(self) -> List[Dict[str, Any]]:
        """Busca todas as posições para o símbolo."""
        return await asyncio.wait_for(
            self._exchange.fetch_positions([self._symbol]),
            timeout=self._FETCH_POSITIONS_TIMEOUT,
        ) or []

    async def _fetch_open_orders(self) -> List[Dict[str, Any]]:
        """Busca todas as ordens abertas do par (regulares + condicionais).

        Binance expõe ordens condicionais (STOP_MARKET, TAKE_PROFIT_MARKET)
        em endpoint separado /fapi/v1/algoOrder; CCXT acessa via
        params={"stop": True}. Validado em scripts/verify_algo_order_fetch.py
        (PR 1.8): bastam 2 variantes — default para regulares, stop=True
        para qualquer condicional.

        Comportamento em erro (I6):
        - Se apenas uma variante falhar: WARNING e retorna o parcial.
        - Se AMBAS falharem: propaga. Retornar lista vazia seria
          indistinguível de "sem ordens" e dispararia recriação de SL/TP
          sobre proteção já existente (causa-raiz do incidente C13).
        """
        orders: List[Dict[str, Any]] = []
        seen_ids: Set[str] = set()

        def _collect(result: Optional[List[Dict[str, Any]]]) -> None:
            if not result:
                return
            for order in result:
                order_id = str(order.get("id")) if order.get("id") is not None else None
                if order_id and order_id in seen_ids:
                    continue
                if order_id:
                    seen_ids.add(order_id)
                orders.append(order)

        param_variants: List[Optional[Dict[str, Any]]] = [None, {"stop": True}]
        last_failure: Optional[Exception] = None
        failure_count = 0

        for params in param_variants:
            try:
                if params:
                    response = await self._exchange.fetch_open_orders(
                        self._symbol, None, None, params
                    )
                else:
                    response = await self._exchange.fetch_open_orders(self._symbol)
                _collect(response)
            except Exception as exc:
                failure_count += 1
                last_failure = exc
                domain_error = self._error_handler(exc)
                self._log.warning(
                    f"[{self._symbol}] fetch_open_orders variant "
                    f"{params or 'default'} falhou: {domain_error}"
                )

        if failure_count == len(param_variants) and last_failure is not None:
            raise self._error_handler(last_failure) from last_failure

        return orders

    async def has_active_position(self) -> Optional[bool]:
        """Verifica se existe posição ativa para o símbolo.

        Returns:
            True se posição ativa.
            False se não há posição.
            None se houve erro ao verificar.
        """
        try:
            positions = await self._fetch_positions()
        except Exception as e:
            domain_error = self._error_handler(e)
            self._log.error(f"[{self._symbol}] Erro ao verificar posição: {domain_error}")
            return None

        active = next((p for p in positions if self._utils.extract_size(p) > 0), None)

        return active is not None

    # -------------------------------------------------------------------------
    # Detecção de ordens de proteção
    # -------------------------------------------------------------------------

    async def _detect_protection_orders(
        self, position_side: str, entry_price: float
    ) -> Tuple[bool, bool]:
        """Detecta ordens de SL e TP existentes para uma posição."""
        has_sl = False
        has_tp = False
        open_orders = await self._fetch_open_orders()
        closing_side = (
            self._utils.SHORT_SIDE
            if position_side == self._utils.LONG_SIDE
            else self._utils.LONG_SIDE
        )

        for order in open_orders:
            if not self._utils.is_protection_order(order, closing_side):
                continue

            stop_price = self._utils.get_stop_price(order)

            if self._utils.is_take_profit_type(order):
                has_tp = True
                continue
            if self._utils.is_stop_loss_type(order):
                has_sl = True
                continue

            if stop_price is not None:
                is_sl, is_tp = self._utils.classify_by_price(
                    position_side, entry_price, stop_price
                )
                has_sl = has_sl or is_sl
                has_tp = has_tp or is_tp

        return has_sl, has_tp

    # -------------------------------------------------------------------------
    # Cancelamento de ordens
    # -------------------------------------------------------------------------

    async def _cancel_orders_individually(
        self, orders: Optional[List[Dict[str, Any]]] = None
    ) -> bool:
        """Cancela todas as ordens abertas individualmente."""
        if orders is None:
            orders = await self._fetch_open_orders()
        success = True

        for order in orders:
            try:
                try:
                    await self._exchange.cancel_order(order["id"], self._symbol)
                except Exception:
                    await self._exchange.cancel_order(
                        order["id"], self._symbol, {"stop": True}
                    )
            except Exception as exc:
                success = False
                domain_error = self._error_handler(exc)
                if "Unknown order" in str(exc):
                    self._log.debug(
                        f"[{self._symbol}] Ordem já cancelada: {order.get('id')}"
                    )
                else:
                    self._log.warning(
                        f"[{self._symbol}] Falha ao cancelar ordem {order.get('id')}: {domain_error}"
                    )
        if not orders:
            self._log.info(f"[{self._symbol}] Nenhuma ordem pendente para cancelar.")

        return success

    async def _cancel_non_protection_orders(self) -> None:
        """Cancela apenas ordens que NÃO são reduceOnly (preserva SL/TP)."""
        orders = await self._fetch_open_orders()
        non_protection = [
            o for o in orders
            if not self._utils.is_truthy_flag(
                o.get("reduceOnly") or o.get("info", {}).get("reduceOnly")
            )
        ]
        if non_protection:
            await self._cancel_orders_individually(non_protection)
        else:
            self._log.info(
                f"[{self._symbol}] Nenhuma ordem não-proteção para cancelar."
            )

    async def cancel_all_orders(self) -> None:
        """Cancela todas as ordens abertas do par (incluindo reduceOnly).

        Dois callers:
        1. initialize(): limpa resíduos antes de set_margin_mode/set_leverage
           para evitar -4067 ("Position side cannot be changed if there
           exists open orders") em caso de órfãs de execução anterior.
        2. _handle_monitoring: chamado na transição True→False, quando
           qualquer ordem reduceOnly restante é definitivamente órfã (SL/TP
           que não disparou), diferente do caso de PR 1.4 onde fetch_positions
           vazio pode ser engano temporário da Binance.

        Best-effort: engole erros internos do bulk cancel e cai em fallback
        individual. Não levanta exceções — falha parcial resulta em órfãs
        residuais mas não quebra o fluxo do bot.
        """
        try:
            await self._exchange.cancel_all_orders(self._symbol)
            self._log.info(f"[{self._symbol}] cancel_all_orders executado.")
        except Exception as exc:
            domain_error = self._error_handler(exc)
            self._log.debug(
                f"[{self._symbol}] cancel_all_orders falhou, cancelamento individual: {domain_error}"
            )

        try:
            await self._cancel_orders_individually()
        except Exception as exc:
            domain_error = self._error_handler(exc)
            self._log.warning(
                f"[{self._symbol}] Cancelamento individual falhou totalmente: "
                f"{domain_error}. Órfãs podem permanecer."
            )

    # -------------------------------------------------------------------------
    # Criação de ordens
    # -------------------------------------------------------------------------

    async def _create_protection_order(
        self,
        side: Literal["buy", "sell"],
        entry_price: float,
        order_type: str,
        percent: float,
        is_stop_loss: bool,
        amount: float,
    ) -> Optional[Dict]:
        """Cria uma ordem de proteção (Stop Loss ou Take Profit)."""
        protection_price = self._utils.calculate_protection_price(
            side, entry_price, percent, is_stop_loss
        )
        order_name = "Stop Loss" if is_stop_loss else "Take Profit"
        opposite_side = (
            self._utils.SHORT_SIDE
            if side == self._utils.LONG_SIDE
            else self._utils.LONG_SIDE
        )

        try:
            order = await self._exchange.create_order(
                symbol=self._symbol,
                type=cast(Any, order_type),
                side=opposite_side,
                amount=float(self._utils.format_amount(amount)),
                params={"stopPrice": protection_price, "reduceOnly": True},
            )
            self._log.info(f"[{self._symbol}] {order_name} criado: {protection_price}")
            return order
        except Exception as exc:
            domain_error = self._error_handler(exc)
            self._log.error(
                f"[{self._symbol}] Falha ao criar {order_name} em {protection_price}: {domain_error}"
            )
            return None

    async def _send_market_order(
        self, side: Literal["buy", "sell"], amount: float, attempt: int, max_retries: int
    ) -> Optional[Dict]:
        """Envia uma ordem market."""
        order = await self._exchange.create_order(
            symbol=self._symbol,
            type="market",
            side=side,
            amount=float(self._utils.format_amount(amount)),
        )

        if not order or "id" not in order:
            return None

        filled_qty = float(order.get("filled") or 0)
        if filled_qty > 0:
            return order

        refreshed = await self._exchange.fetch_order(order["id"], self._symbol)
        filled_qty = float(refreshed.get("filled") or 0)
        if filled_qty > 0:
            return refreshed

        self._log.warning(
            f"[{self._symbol}] Ordem market não preenchida. Tentativa {attempt}/{max_retries}."
        )
        return None

    async def _send_limit_order(
        self,
        side: Literal["buy", "sell"],
        amount: float,
        current_price: float,
        attempt: int,
        max_retries: int,
    ) -> Optional[Dict]:
        """Envia uma ordem limit com timeout."""
        entry_price = self._utils.calculate_entry_price(
            side, current_price, self._offset_percent
        )

        order = await self._exchange.create_order(
            symbol=self._symbol,
            type="limit",
            side=side,
            amount=float(self._utils.format_amount(amount)),
            price=entry_price,
        )

        if not order or "id" not in order:
            return None

        await asyncio.sleep(self._entry_fill_timeout)

        refreshed = await self._exchange.fetch_order(order["id"], self._symbol)
        filled_qty = float(refreshed.get("filled") or 0)

        if filled_qty > 0:
            return refreshed

        self._log.info(
            f"[{self._symbol}] Ordem {order['id']} não executada. Cancelando..."
        )
        try:
            await self._exchange.cancel_order(order["id"], self._symbol)
        except Exception as exc:
            self._log.warning(
                f"[{self._symbol}] Falha ao cancelar ordem {order['id']}: {exc}. Verificando fill..."
            )
            refreshed = await self._exchange.fetch_order(order["id"], self._symbol)
            filled_qty = float(refreshed.get("filled") or 0)
            if filled_qty > 0:
                return refreshed
        return None

    async def _send_order(
        self, side: Literal["buy", "sell"], amount: float
    ) -> Optional[Dict]:
        """Envia ordem de entrada com retry e limite de perseguição de preço."""
        max_retries = self._max_retries if self._max_retries > 0 else 1

        initial_price = await self._get_current_price()
        if not initial_price or initial_price <= 0:
            self._log.error(f"[{self._symbol}] Preço inicial inválido. Abortando ordem.")
            return None

        for attempt in range(1, max_retries + 1):
            current_price = await self._get_current_price()
            if not current_price or current_price <= 0:
                self._log.warning(f"[{self._symbol}] Preço atual inválido. Abortando.")
                break

            price_deviation = abs(current_price - initial_price)

            chase_limit = initial_price * (self._chase_percent / 100)
            if price_deviation > chase_limit:
                move_pct = (price_deviation / initial_price) * 100
                self._log.warning(
                    f"[{self._symbol}] Preço se moveu {move_pct:.3f}% "
                    f"(absoluto: {price_deviation:.8g}). "
                    f"Limite: {self._chase_percent}%. Abortando."
                )
                break

            if self._order_type == "market":
                result = await self._send_market_order(side, amount, attempt, max_retries)
            else:
                result = await self._send_limit_order(
                    side, amount, current_price, attempt, max_retries
                )

            if result:
                return result

        return None

    async def _recreate_missing_protection(
        self, side: str, entry_price: float, has_sl: bool, has_tp: bool, amount: float
    ) -> Tuple[bool, bool]:
        """Recria ordens de proteção faltantes e confirma cada criação.

        Returns:
            Tupla (sl_ok, tp_ok). True se a proteção está presente e confirmada
            na exchange via fetch_order no endpoint algo; False se criação ou
            confirmação falhou.
        """
        typed_side = cast(Literal["buy", "sell"], side)
        sl_ok = has_sl
        tp_ok = has_tp

        if not has_sl:
            sl_order = await self._create_protection_order(
                typed_side,
                entry_price,
                "stop_market",
                self._percent_sl,
                is_stop_loss=True,
                amount=amount,
            )
            if sl_order and await self._confirm_protection_order(sl_order):
                sl_ok = True
            else:
                self._log.warning(
                    f"[{self._symbol}] Stop Loss não confirmado após recriação."
                )

        if not has_tp:
            tp_order = await self._create_protection_order(
                typed_side,
                entry_price,
                "take_profit_market",
                self._percent_tp,
                is_stop_loss=False,
                amount=amount,
            )
            if tp_order and await self._confirm_protection_order(tp_order):
                tp_ok = True
            else:
                self._log.warning(
                    f"[{self._symbol}] Take Profit não confirmado após recriação."
                )

        return sl_ok, tp_ok

    async def _confirm_protection_order(self, order: Optional[Dict]) -> bool:
        """Confirma que uma ordem de proteção (SL/TP) está viva na exchange.

        SL/TP são ordens condicionais (algo orders); o id retornado por create_order
        é um algoId. fetch_order precisa de params={"stop": True} para consultar o
        endpoint /fapi/v1/algoOrder. Sem esse param, CCXT roteia para /fapi/v1/order
        e Binance responde -2013 "Order does not exist" (falso negativo).

        Faz N tentativas com delay curto para cobrir eventual consistency residual.
        """
        if not order or not order.get("id"):
            return False

        order_id = order["id"]
        for attempt in range(1, self._SL_CONFIRM_ATTEMPTS + 1):
            try:
                fetched = await self._exchange.fetch_order(
                    order_id, self._symbol, {"stop": True}
                )
                status = str((fetched or {}).get("status") or "").lower()
                if fetched and status not in self._DEAD_ORDER_STATUSES:
                    return True
                self._log.debug(
                    f"[{self._symbol}] Confirmação SL tentativa {attempt}: status='{status}'"
                )
            except Exception as exc:
                domain_error = self._error_handler(exc)
                self._log.debug(
                    f"[{self._symbol}] Confirmação SL tentativa {attempt} falhou: {domain_error}"
                )

            if attempt < self._SL_CONFIRM_ATTEMPTS:
                await asyncio.sleep(self._SL_CONFIRM_DELAY)

        return False

    async def _emergency_close_position(
        self, side: Literal["buy", "sell"], amount: float
    ) -> None:
        """Fecha posição a mercado em emergência (SL falhou após entrada)."""
        opposite_side = (
            self._utils.SHORT_SIDE
            if side == self._utils.LONG_SIDE
            else self._utils.LONG_SIDE
        )
        try:
            await self._exchange.create_order(
                symbol=self._symbol,
                type="market",
                side=opposite_side,
                amount=float(self._utils.format_amount(amount)),
                params={"reduceOnly": True},
            )
            self._log.critical(
                f"[{self._symbol}] Posição fechada a mercado (emergência). SL não confirmado."
            )
        except Exception as exc:
            domain_error = self._error_handler(exc)
            self._log.critical(
                f"[{self._symbol}] FALHA AO FECHAR POSIÇÃO DE EMERGÊNCIA: {domain_error}"
            )

    # -------------------------------------------------------------------------
    # Interface pública
    # -------------------------------------------------------------------------

    async def send_protection_orders(
        self,
        side: Literal["buy", "sell"],
        entry_price: float,
        amount: Optional[float] = None,
    ) -> Tuple[Optional[Dict], Optional[Dict]]:
        """Cria ordens de Stop Loss e Take Profit para uma posição."""
        effective_amount = amount if amount is not None else self._amount
        sl_order = await self._create_protection_order(
            side,
            entry_price,
            "stop_market",
            self._percent_sl,
            is_stop_loss=True,
            amount=effective_amount,
        )
        tp_order = await self._create_protection_order(
            side,
            entry_price,
            "take_profit_market",
            self._percent_tp,
            is_stop_loss=False,
            amount=effective_amount,
        )
        return sl_order, tp_order

    async def open_order(self, side: Literal["buy", "sell"]) -> Dict:
        """Abre uma posição com ordem de entrada e ordens de proteção."""
        if side not in self._utils.VALID_SIDES:
            self._log.error(f"[{self._symbol}] Side inválido: {side}")
            raise ValueError(f"Side inválido: {side}")

        result: Dict[str, Any] = {
            "success": False,
            "order": None,
            "entry_price": None,
            "sl_order": None,
            "tp_order": None,
        }

        order_result = await self._send_order(side, self._amount)
        if not order_result or not order_result.get("id"):
            self._log.warning(
                f"[{self._symbol}] Ordem aparentemente não preenchida. Verificando posição..."
            )
            has_position = await self.has_active_position()
            if has_position is not True:
                self._log.warning(f"[{self._symbol}] Sem posição confirmada. Abortando.")
                return result

            self._log.warning(
                f"[{self._symbol}] Posição detectada apesar de ordem sem fill. "
                f"Prosseguindo com proteção."
            )
            positions = await self._fetch_positions()
            active = next(
                (p for p in positions if self._utils.extract_size(p) > 0), None
            )
            if not active:
                return result

            entry_price = self._utils.extract_entry_price(active)
            filled_qty = self._utils.extract_size(active)
            order_result = {
                "id": "reconciled",
                "filled": filled_qty,
                "status": "filled",
                "average": entry_price,
                "price": entry_price,
            }

        result["order"] = order_result

        filled_qty = float(order_result.get("filled") or 0)
        status = str(order_result.get("status") or "").lower()
        is_filled = filled_qty > 0 or status in {"closed", "filled"}

        if not is_filled:
            self._log.warning(
                f"[{self._symbol}] Ordem criada mas não preenchida. Verificando posição..."
            )
            has_position = await self.has_active_position()
            if has_position is not True:
                self._log.warning(
                    f"[{self._symbol}] Sem posição confirmada. "
                    f"[Id: {order_result.get('id')}] [Status: {status}]"
                )
                return result

            self._log.warning(
                f"[{self._symbol}] Posição detectada apesar de filled=0. "
                f"Prosseguindo com proteção."
            )
            positions = await self._fetch_positions()
            active = next(
                (p for p in positions if self._utils.extract_size(p) > 0), None
            )
            if not active:
                return result

            entry_price_val = self._utils.extract_entry_price(active)
            filled_qty = self._utils.extract_size(active)
            order_result.update({
                "filled": filled_qty,
                "status": "filled",
                "average": entry_price_val,
            })
            result["order"] = order_result

        entry_price = order_result.get("average") or order_result.get("price")

        self._log.info(
            f"[{self._symbol}] Ordem preenchida! "
            f"[Lado: {side}] [Preço: {entry_price}] [Id: {order_result['id']}]"
        )

        if entry_price is None:
            entry_price = await self._get_current_price()

        if entry_price is None:
            self._log.warning(
                f"[{self._symbol}] Preço de entrada indisponível. Não é possível criar SL/TP."
            )
            return result

        amount_for_protection = filled_qty if filled_qty > 0 else self._amount

        sl_order = await self._create_protection_order(
            side,
            float(entry_price),
            "stop_market",
            self._percent_sl,
            is_stop_loss=True,
            amount=amount_for_protection,
        )

        sl_confirmed = await self._confirm_protection_order(sl_order)

        if not sl_confirmed:
            self._log.critical(
                f"[{self._symbol}] SL não confirmado. Fechando posição de emergência."
            )
            await self._emergency_close_position(side, amount_for_protection)
            result["sl_order"] = sl_order
            return result

        tp_order = await self._create_protection_order(
            side,
            float(entry_price),
            "take_profit_market",
            self._percent_tp,
            is_stop_loss=False,
            amount=amount_for_protection,
        )

        if not tp_order:
            self._log.warning(
                f"[{self._symbol}] TP falhou, mas SL está ativo. Posição mantida."
            )

        result.update(
            {
                "success": True,
                "entry_price": float(entry_price),
                "sl_order": sl_order,
                "tp_order": tp_order,
            }
        )

        return result

    async def normalize_position_state(self) -> Optional[bool]:
        """Verifica e normaliza o estado da posição com retry e confirmação.

        Fluxo: detecta proteção → se incompleta, recria + confirma via endpoint
        algo (fetch_order com params={"stop": True}) → repete até N tentativas.
        Nunca fecha a posição a mercado: falha terminal retorna None para o
        StateChief decidir (vai para ERROR; humano investiga).

        Returns:
            True se posição ativa com SL/TP confirmados na exchange.
            False se não há posição (ordens não-proteção canceladas).
            None se não foi possível estabelecer/verificar proteção após retries.
        """
        try:
            positions = await self._fetch_positions()
        except Exception as exc:
            domain_error = self._error_handler(exc)
            self._log.error(
                f"[{self._symbol}] Não foi possível verificar posição: {domain_error}"
            )
            return None

        active_position = next(
            (p for p in positions if self._utils.extract_size(p) > 0), None
        )

        if not active_position:
            self._log.info(
                f"[{self._symbol}] Nenhuma posição ativa. Cancelando ordens pendentes."
            )
            try:
                await self._cancel_non_protection_orders()
            except Exception as exc:
                domain_error = self._error_handler(exc)
                self._log.warning(
                    f"[{self._symbol}] Falha ao cancelar ordens não-proteção: "
                    f"{domain_error}. Prosseguindo — posição não está exposta."
                )
            return False

        entry_price = self._utils.extract_entry_price(active_position)
        side = self._utils.derive_side(active_position)

        if entry_price is None or side is None:
            self._log.warning(
                f"[{self._symbol}] Posição sem dados suficientes. Cancelando ordens."
            )
            try:
                await self._cancel_non_protection_orders()
            except Exception as exc:
                domain_error = self._error_handler(exc)
                self._log.warning(
                    f"[{self._symbol}] Falha ao cancelar ordens não-proteção: "
                    f"{domain_error}. Prosseguindo."
                )
            return False

        position_size = self._utils.extract_size(active_position)
        amount_for_protection = position_size if position_size > 0 else self._amount

        for attempt in range(1, self._NORMALIZE_MAX_ATTEMPTS + 1):
            try:
                has_sl, has_tp = await self._detect_protection_orders(
                    side, entry_price
                )
            except Exception as exc:
                domain_error = self._error_handler(exc)
                self._log.warning(
                    f"[{self._symbol}] Detecção de proteção falhou "
                    f"(tentativa {attempt}/{self._NORMALIZE_MAX_ATTEMPTS}): {domain_error}"
                )
                # Não recriar sem conhecer o estado real — recriação cega
                # geraria duplicatas sobre SL/TP vivos (I6/C13). Dorme e
                # tenta nova detecção na próxima iteração.
                if attempt < self._NORMALIZE_MAX_ATTEMPTS:
                    await asyncio.sleep(self._NORMALIZE_RETRY_DELAY)
                continue

            if has_sl and has_tp:
                self._log.info(
                    f"[{self._symbol}] Posição ativa. SL/TP configurados. ✓"
                )
                return True

            self._log.warning(
                f"[{self._symbol}] Proteção incompleta (SL: {has_sl}, TP: {has_tp}). "
                f"Recriando... (tentativa {attempt}/{self._NORMALIZE_MAX_ATTEMPTS})"
            )

            sl_ok, tp_ok = await self._recreate_missing_protection(
                side, entry_price, has_sl, has_tp, amount=amount_for_protection
            )

            if sl_ok and tp_ok:
                self._log.info(
                    f"[{self._symbol}] Posição ativa. "
                    f"SL/TP confirmados após recriação. ✓"
                )
                return True

            if attempt < self._NORMALIZE_MAX_ATTEMPTS:
                await asyncio.sleep(self._NORMALIZE_RETRY_DELAY)

        self._log.critical(
            f"[{self._symbol}] Proteção não pôde ser estabelecida após "
            f"{self._NORMALIZE_MAX_ATTEMPTS} tentativas. Sinalizando erro."
        )
        return None
