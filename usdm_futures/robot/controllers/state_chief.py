import asyncio
import logging
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

from typing import Callable, Literal, Optional

import pandas as pd

from usdm_futures.robot.orders.manage_orders import ManageOrders
from usdm_futures.robot.orders.order_utils import OrderUtils
from usdm_futures.robot.pipeline.fetch_data.csv_storage import CsvRepository
from usdm_futures.robot.pipeline.fetch_data.data_pipeline import DataPipeline
from usdm_futures.robot.pipeline.fetch_data.source import ExchangeFetchData
from usdm_futures.robot.pipeline.fetch_data.transform import (
    DataTransformer,
    DataValidator,
)
from usdm_futures.robot.strategies import Strategy
from usdm_futures.shared.connection.client import Exchange as ExchangeClient
from usdm_futures.shared.error.exceptions import (
    AuthenticationError,
    BadRequestError,
)
from usdm_futures.shared.logging.logger import (
    clear_status_line,
    get_logger,
    update_status_line,
)
from usdm_futures.shared.reports.trade_reporter import TradeReporter
from usdm_futures.shared.utils.helpers import mark_toml_as_invalid
from usdm_futures.shared.utils.market_hours import MarketHoursChecker
from usdm_futures.shared.utils.paths import ProjectPaths
from usdm_futures.shared.validator.secrets import LoadKey
from usdm_futures.shared.validator.symbol_config import SymbolConfig, load_symbol_config


# Número máximo de falhas consecutivas em has_active_position durante
# MONITORING antes de abandonar e ir para ERROR. Com sleep de 30s entre
# tentativas: 10 = 5 minutos de tolerância a instabilidade de rede.
MAX_MONITORING_FAILURES = 10

# Cadência de heartbeat no arquivo de log durante MONITORING. Uma linha de
# INFO é emitida a cada N verificações bem-sucedidas. Com sleep de 30s entre
# checagens: 20 = ~10 minutos entre heartbeats no arquivo. A linha dinâmica
# no stdout (via update_status_line) é atualizada em TODA verificação.
MONITORING_HEARTBEAT_EVERY = 20


class StateChief:
    """Máquina de estados para um único par de trading.

    Gerencia o ciclo completo: verificação de janela, limpeza,
    análise, abertura de posição e monitoramento.
    """

    class State(Enum):
        CHECK_WINDOW = "check_window"
        GET_PAIR = "get_pair"
        EXCHANGE = "exchange"
        MANAGE_ORDERS = "manage_orders"
        CLEAN_ORDERS_ORPHANS = "clean_orders_orphans"
        FETCH_DATA = "fetch_data"
        APPLY_STRATEGY = "apply_strategy"
        CHECK_SIGNAL = "check_signal"
        OPENING_POSITION = "opening_position"
        MONITORING = "monitoring"
        STANDBY = "standby"
        ERROR = "error"
        STOPPED = "stopped"

    class StandbyReason(Enum):
        MARKET_CLOSED = "market_closed"
        WAIT_NEXT_CANDLE = "wait_next_candle"

    def __init__(
        self,
        hours_checker: MarketHoursChecker,
        toml_path: str,
        keys: LoadKey,
        error_handler: Callable[[Exception], Exception],
        logger: logging.Logger,
        strategy: Strategy,
    ) -> None:
        ...
        self._toml_path = toml_path
        self._state = StateChief.State.CHECK_WINDOW
        self._log = logger
        self._hours_checker = hours_checker
        self._keys = keys
        self._error_handler = error_handler
        self._strategy = strategy
        self._standby_reason: StateChief.StandbyReason | None = None
        self._standby_next_state: StateChief.State | None = None
        self._symbol: str | None = None

        self._conn: Optional[ExchangeClient] = None
        self._orders: Optional[ManageOrders] = None
        self._symbol_config: Optional[SymbolConfig] = None
        self._cleanup_retries: int = 0
        self._data_pipeline = None
        self._dataframe: Optional[pd.DataFrame] = None
        self._signal_side: Literal["buy", "sell"] | None = None
        self._monitoring_logged: bool = False
        self._error_retries: int = 0
        self._monitoring_failures: int = 0
        self._monitoring_check_count: int = 0
        self._monitoring_started_at: datetime | None = None

        # Contexto da posição aberta para registro pós-encerramento (PR 1.12).
        # Preenchido em _handle_opening_position quando a entrada confirma com
        # success=True, consumido em _handle_monitoring na transição True→False.
        self._trade_reporter: Optional[TradeReporter] = None
        self._open_side: Literal["buy", "sell"] | None = None
        self._opened_at: datetime | None = None
        self._open_entry_price: float | None = None
        self._open_sl_price: float | None = None
        self._open_tp_price: float | None = None
        self._open_sl_order_id: str | None = None
        self._open_tp_order_id: str | None = None

    async def run(self) -> None:
        """Loop principal do par."""
        self._log.info(f"StateChief iniciado. Estratégia: {self._strategy.name}")

        while self._state != StateChief.State.STOPPED:
            try:
                if self._state == StateChief.State.CHECK_WINDOW:
                    await self._handle_check_window()

                elif self._state == StateChief.State.GET_PAIR:
                    await self._handle_get_pair()

                elif self._state == StateChief.State.EXCHANGE:
                    await self._handle_exchange()

                elif self._state == StateChief.State.MANAGE_ORDERS:
                    await self._handle_manage_orders()

                elif self._state == StateChief.State.CLEAN_ORDERS_ORPHANS:
                    await self._handle_clean_orders_orphans()

                elif self._state == StateChief.State.FETCH_DATA:
                    await self._handle_fetch_data()

                elif self._state == StateChief.State.APPLY_STRATEGY:
                    await self._handle_apply_strategy()

                elif self._state == StateChief.State.CHECK_SIGNAL:
                    await self._handle_check_signal()

                elif self._state == StateChief.State.OPENING_POSITION:
                    await self._handle_opening_position()

                elif self._state == StateChief.State.MONITORING:
                    await self._handle_monitoring()

                elif self._state == StateChief.State.STANDBY:
                    await self._handle_standby()

                elif self._state == StateChief.State.ERROR:
                    await self._handle_error()

            except Exception as e:
                clear_status_line()
                self._log.error(f"Erro: {e}", exc_info=True)
                self._state = StateChief.State.ERROR

    async def _handle_check_window(self) -> None:
        """Verifica se o mercado está dentro da janela operacional."""
        if self._hours_checker.is_market_open():
            self._log.info("Mercado aberto. Get Symbol Stock.")
            self._state = StateChief.State.GET_PAIR
        else:
            self._standby_reason = StateChief.StandbyReason.MARKET_CLOSED
            self._state = StateChief.State.STANDBY

    async def _handle_get_pair(self) -> None:
        """Carrega o par do TOML recebido."""
        if self._symbol is not None:
            if self._conn is not None:
                self._state = StateChief.State.MANAGE_ORDERS
            else:
                self._state = StateChief.State.EXCHANGE
            return

        try:
            self._symbol_config = load_symbol_config(self._toml_path)
            self._symbol = self._symbol_config.symbol

            self._log.info(f"Par selecionado: {self._symbol}")

            if self._conn is not None:
                self._state = StateChief.State.MANAGE_ORDERS
            else:
                self._state = StateChief.State.EXCHANGE

        except Exception as e:
            self._log.error(f"Erro ao carregar arquivo TOML {self._toml_path}: {e}")
            self._state = StateChief.State.ERROR

    async def _handle_exchange(self) -> None:
        """Cria a conexão com a exchange."""
        if self._conn is None:
            self._conn = ExchangeClient(
                exchange_name=self._keys.exchange,
                api_key=self._keys.binance_api_key,
                api_secret=self._keys.binance_api_secret,
                market_type=self._keys.market_type,
                sandbox=self._keys.sandbox,
                logger=get_logger("exchange"),
                error_handler=self._error_handler,
            )

        try:
            await self._conn.connect()
            self._state = StateChief.State.MANAGE_ORDERS

        except ConnectionError as e:
            self._log.error(f"Falha na conexão: {e}")
            self._state = StateChief.State.ERROR

    async def _handle_manage_orders(self) -> None:
        """Cria a instância de ManageOrders para o par."""
        if self._orders is not None:
            self._error_retries = 0
            self._state = StateChief.State.CLEAN_ORDERS_ORPHANS
            return

        if self._symbol_config is None:
            self._log.error("Symbol Config não carregado. ERROR STATE.")
            self._state = StateChief.State.ERROR
            return

        if self._conn is None or self._symbol is None:
            self._log.error("Dependências não inicializadas. ERROR STATE.")
            self._state = StateChief.State.ERROR
            return

        self._log.info(f"[{self._symbol}] Criando ManageOrders...")

        exchange = await self._conn.connect()
        utils = OrderUtils(exchange=exchange, symbol=self._symbol_config.symbol)

        self._orders = ManageOrders(
            exchange=exchange,
            symbol=self._symbol_config.symbol,
            leverage=self._symbol_config.leverage,
            amount=self._symbol_config.amount,
            stop_loss_percent=self._symbol_config.stop_loss_percent,
            take_profit_percent=self._symbol_config.take_profit_percent,
            chase_percent=self._symbol_config.chase_percent,
            offset_percent=self._symbol_config.offset_percent,
            fill_timeout=self._symbol_config.fill_timeout,
            max_retries=self._symbol_config.max_retries,
            logger=get_logger("manage_orders"),
            error_handler=self._error_handler,
            utils=utils,
            order_type=self._symbol_config.order_type,
        )

        try:
            await self._orders.initialize()
        except (BadRequestError, AuthenticationError) as exc:
            self._log.critical(
                f"[{self._symbol}] Configuração inválida para esta conta: {exc}. "
                f"TOML: {self._toml_path}. STOPPED."
            )
            try:
                invalid_path = mark_toml_as_invalid(self._toml_path)
                self._log.critical(
                    f"[{self._symbol}] TOML renomeado para: {invalid_path}"
                )
            except Exception as rename_exc:
                self._log.error(
                    f"[{self._symbol}] Falha ao renomear TOML: {rename_exc}"
                )
            self._state = StateChief.State.STOPPED
            return
        except Exception as exc:
            self._log.warning(
                f"[{self._symbol}] Falha transitória em initialize: {exc}. ERROR STATE."
            )
            self._orders = None
            self._state = StateChief.State.ERROR
            return

        if self._trade_reporter is None:
            self._trade_reporter = TradeReporter(
                exchange=exchange,
                logger=get_logger("trade_reporter"),
                csv_path=Path(ProjectPaths.TRADES_CSV.value),
            )

        self._log.info(f"[{self._symbol}] ManageOrders criado.")
        self._error_retries = 0
        self._state = StateChief.State.CLEAN_ORDERS_ORPHANS

    async def _handle_clean_orders_orphans(self) -> None:
        """Verifica posições e ordens órfãs do par e normaliza."""
        if self._orders is None:
            self._state = StateChief.State.MANAGE_ORDERS
            return

        self._log.info(f"[{self._symbol}] Verificando posições e ordens órfãs...")

        has_position = await self._orders.has_active_position()

        if has_position is None:
            self._cleanup_retries += 1
            self._log.warning(
                f"[{self._symbol}] Falha ao verificar posição. "
                f"Tentativa {self._cleanup_retries}/3. Reiniciando fluxo..."
            )
            if self._cleanup_retries >= 3:
                self._log.error(f"[{self._symbol}] Falha após 3 tentativas. ERROR STATE.")
                self._state = StateChief.State.ERROR
            else:
                self._state = StateChief.State.CHECK_WINDOW
            return

        self._cleanup_retries = 0

        if has_position:
            self._log.info(
                f"[{self._symbol}] Posição ativa encontrada. Verificando proteção..."
            )
            normalized = await self._orders.normalize_position_state()
            if normalized is None:
                self._log.critical(
                    f"[{self._symbol}] Proteção não confirmada após retries. "
                    f"Posição permanece viva. ERROR STATE."
                )
                self._state = StateChief.State.ERROR
                return
            self._log.info(f"[{self._symbol}] Posição protegida. MONITORING STATE.")
            self._error_retries = 0
            self._state = StateChief.State.MONITORING
        else:
            normalized = await self._orders.normalize_position_state()
            if normalized is None:
                self._log.critical(
                    f"[{self._symbol}] Falha ao consultar/normalizar estado. ERROR STATE."
                )
                self._state = StateChief.State.ERROR
                return
            self._error_retries = 0
            self._state = StateChief.State.FETCH_DATA

    async def _handle_fetch_data(self) -> None:
        """Baixa e atualiza os candles do par."""
        if self._conn is None or self._symbol is None or self._symbol_config is None:
            self._log.error("Dependências não inicializadas. Indo para ERROR.")
            self._state = StateChief.State.ERROR
            return

        if self._data_pipeline is None:
            self._log.info(f"[{self._symbol}] Criando DataPipeline...")

            exchange = await self._conn.connect()

            source = ExchangeFetchData(
                exchange=exchange,
                logger=get_logger("exchange_fetch_data"),
                symbol=self._symbol,
                timeframe=self._symbol_config.timeframe,
                error_handler=self._error_handler,
            )

            csv_repo = CsvRepository(
                symbol=self._symbol,
                timeframe=self._symbol_config.timeframe,
                logger=get_logger("csvstorage"),
                data_dir=ProjectPaths.BINANCE_DATA_DIR.value,
            )

            validator = DataValidator(
                logger=get_logger("datavalidator"), timeframe_ms=source.timeframe_ms
            )

            transformer = DataTransformer(logger=get_logger("datatransform"))

            self._data_pipeline = DataPipeline(
                data_source=source,
                csv_repo=csv_repo,
                validator=validator,
                transformer=transformer,
                logger=get_logger("datapipeline"),
                candle_limit=self._symbol_config.candle_limit,
                max_rows=self._symbol_config.max_rows,
                batch_limit=self._symbol_config.batch_limit,
                fetch_retry_attempts=self._symbol_config.fetch_retry_attempts,
                fetch_retry_delay=self._symbol_config.fetch_retry_delay,
            )

        self._dataframe = await self._data_pipeline.run()

        self._log.info(f"[{self._symbol}] Dataset atualizado")
        self._error_retries = 0
        self._state = StateChief.State.APPLY_STRATEGY

    async def _handle_apply_strategy(self) -> None:
        """Calcula indicadores técnicos e alimenta o dataset."""
        if self._dataframe is None or self._dataframe.empty:
            self._standby_reason = StateChief.StandbyReason.WAIT_NEXT_CANDLE
            self._standby_next_state = StateChief.State.FETCH_DATA
            self._state = StateChief.State.STANDBY
            return

        self._dataframe = self._strategy.apply_indicators(self._dataframe)

        self._log.info(f"[{self._symbol}] Indicadores calculados.")
        self._state = StateChief.State.CHECK_SIGNAL

    async def _handle_check_signal(self) -> None:
        """Consulta a estratégia para decidir entrada no último candle."""
        if self._dataframe is None:
            self._standby_reason = StateChief.StandbyReason.WAIT_NEXT_CANDLE
            self._standby_next_state = StateChief.State.FETCH_DATA
            self._state = StateChief.State.STANDBY
            return

        signal = self._strategy.check_signal(self._dataframe)

        if signal == "buy":
            self._log.info(f"[{self._symbol}] Sinal de COMPRA.")
            self._signal_side = "buy"
            self._state = StateChief.State.OPENING_POSITION
            return

        if signal == "sell":
            self._log.info(f"[{self._symbol}] Sinal de VENDA.")
            self._signal_side = "sell"
            self._state = StateChief.State.OPENING_POSITION
            return

        self._log.info(f"[{self._symbol}] Sem sinal.")
        self._standby_reason = StateChief.StandbyReason.WAIT_NEXT_CANDLE
        self._standby_next_state = StateChief.State.FETCH_DATA
        self._state = StateChief.State.STANDBY

    async def _handle_opening_position(self) -> None:
        """Abre uma posição baseada no sinal detectado."""
        if self._orders is None or self._signal_side is None:
            self._log.error(
                f"[{self._symbol}] Dependências não inicializadas. ERROR STATE."
            )
            self._state = StateChief.State.ERROR
            return

        has_position = await self._orders.has_active_position()

        if has_position is True:
            self._log.warning(
                f"[{self._symbol}] Posição já ativa. Cancelando entrada. MONITORING STATE."
            )
            self._signal_side = None
            self._state = StateChief.State.MONITORING
            return

        if has_position is None:
            self._log.warning(
                f"[{self._symbol}] Falha ao verificar posição. Cancelando entrada. STANDBY STATE."
            )
            self._signal_side = None
            self._standby_reason = StateChief.StandbyReason.WAIT_NEXT_CANDLE
            self._standby_next_state = StateChief.State.FETCH_DATA
            self._state = StateChief.State.STANDBY
            return

        self._log.info(
            f"[{self._symbol}] Abrindo posição: {self._signal_side.upper()}..."
        )

        opened_at = datetime.now(timezone.utc)
        opening_side = self._signal_side
        result = await self._orders.open_order(side=opening_side)

        if result["success"]:
            self._log.info(
                f"[{self._symbol}] Posição aberta! Preço: {result['entry_price']:.8g}"
            )
            self._capture_open_context(opening_side, opened_at, result)
            self._signal_side = None
            self._error_retries = 0
            self._state = StateChief.State.MONITORING
        else:
            self._log.warning(f"[{self._symbol}] Falha ao abrir posição.")
            self._signal_side = None
            self._standby_reason = StateChief.StandbyReason.WAIT_NEXT_CANDLE
            self._standby_next_state = StateChief.State.FETCH_DATA
            self._state = StateChief.State.STANDBY

    async def _handle_monitoring(self) -> None:
        """Monitora a posição aberta até ser encerrada."""

        if self._orders is None:
            self._log.error(
                f"[{self._symbol}] ManageOrders não inicializado. ERROR STATE."
            )
            self._state = StateChief.State.ERROR
            return

        if self._monitoring_started_at is None:
            self._monitoring_started_at = datetime.now()

        if not self._monitoring_logged:
            self._log.info(f"[{self._symbol}] Monitorando posição...")
            self._monitoring_logged = True

        has_position = await self._orders.has_active_position()

        if has_position is None:
            self._monitoring_failures += 1
            update_status_line(
                f"[{self._symbol}] Monitorando — erro na verificação "
                f"({self._monitoring_failures}/{MAX_MONITORING_FAILURES})"
            )
            self._log.warning(
                f"[{self._symbol}] Falha ao verificar posição. "
                f"Tentativa {self._monitoring_failures}/{MAX_MONITORING_FAILURES}."
            )
            if self._monitoring_failures >= MAX_MONITORING_FAILURES:
                self._log.critical(
                    f"[{self._symbol}] Falhas consecutivas esgotadas em MONITORING. "
                    f"Indo para ERROR."
                )
                self._monitoring_failures = 0
                self._monitoring_check_count = 0
                self._monitoring_started_at = None
                clear_status_line()
                self._state = StateChief.State.ERROR
                return
            await asyncio.sleep(30)
            return

        self._monitoring_failures = 0

        if has_position:
            self._monitoring_check_count += 1

            now = datetime.now()
            active_seconds = int((now - self._monitoring_started_at).total_seconds())
            active_min = active_seconds // 60
            update_status_line(
                f"[{self._symbol}] Monitorando — {now.strftime('%H:%M:%S')} ✓ | "
                f"verificações: {self._monitoring_check_count} | "
                f"ativa há: {active_min}min"
            )

            if self._monitoring_check_count % MONITORING_HEARTBEAT_EVERY == 0:
                minutes_window = (MONITORING_HEARTBEAT_EVERY * 30) // 60
                self._log.info(
                    f"[{self._symbol}] Monitorando (heartbeat): "
                    f"{MONITORING_HEARTBEAT_EVERY} verificações ok nos últimos "
                    f"{minutes_window}min."
                )

            await asyncio.sleep(30)
            return

        self._log.info(f"[{self._symbol}] Posição encerrada. Indo para limpeza.")
        clear_status_line()
        await self._record_closed_trade()

        try:
            await self._orders.cancel_all_orders()
        except Exception as exc:
            domain_error = self._error_handler(exc)
            self._log.warning(
                f"[{self._symbol}] Falha ao limpar ordens órfãs após fechamento: "
                f"{domain_error}. Prosseguindo."
            )

        self._monitoring_logged = False
        self._monitoring_check_count = 0
        self._monitoring_started_at = None
        self._error_retries = 0
        self._state = StateChief.State.CLEAN_ORDERS_ORPHANS

    async def _handle_standby(self) -> None:
        """Aguarda conforme o motivo do standby."""
        if self._standby_reason == StateChief.StandbyReason.MARKET_CLOSED:
            seconds = self._hours_checker.seconds_until_next_open()

            if seconds <= 0:
                self._log.info("Mercado aberto. Saindo de standby.")
                self._state = StateChief.State.CHECK_WINDOW
                self._standby_reason = None
                self._standby_next_state = None
                return

            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            self._log.info(
                f"Entrando em standby. Mercado fechado — "
                f"aguardando {hours}h {minutes}min até a próxima abertura."
            )
            await asyncio.sleep(seconds)
            self._log.info("Standby concluído. Voltando para CHECK_WINDOW.")
            self._state = StateChief.State.CHECK_WINDOW

        elif self._standby_reason == StateChief.StandbyReason.WAIT_NEXT_CANDLE:
            if self._data_pipeline is None:
                self._state = StateChief.State.FETCH_DATA
                return

            seconds = self._data_pipeline.seconds_until_next_candle() + 5

            self._log.info(f"[{self._symbol}] Aguardando próximo candle...")
            await asyncio.sleep(seconds)
            self._state = self._standby_next_state or StateChief.State.FETCH_DATA

        self._standby_reason = None
        self._standby_next_state = None

    def _capture_open_context(
        self,
        side: Literal["buy", "sell"],
        opened_at: datetime,
        result: dict,
    ) -> None:
        """Guarda contexto da posição aberta para registro pós-encerramento."""
        self._open_side = side
        self._opened_at = opened_at
        entry_price = result.get("entry_price")
        self._open_entry_price = float(entry_price) if entry_price is not None else None

        sl_order = result.get("sl_order") or {}
        tp_order = result.get("tp_order") or {}

        self._open_sl_price = self._extract_stop_price(sl_order)
        self._open_tp_price = self._extract_stop_price(tp_order)
        self._open_sl_order_id = self._extract_order_id(sl_order)
        self._open_tp_order_id = self._extract_order_id(tp_order)

    @staticmethod
    def _extract_stop_price(order: dict) -> Optional[float]:
        raw = order.get("stopPrice")
        if raw is None:
            raw = (order.get("info") or {}).get("stopPrice")
        try:
            return float(raw) if raw is not None else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _extract_order_id(order: dict) -> Optional[str]:
        oid = order.get("id")
        return str(oid) if oid is not None else None

    async def _record_closed_trade(self) -> None:
        """Registra a operação encerrada via TradeReporter e limpa o contexto."""
        if (
            self._trade_reporter is None
            or self._symbol is None
            or self._open_side is None
            or self._opened_at is None
            or self._open_entry_price is None
            or self._symbol_config is None
        ):
            self._reset_open_context()
            return

        try:
            await self._trade_reporter.record_closed_trade(
                symbol=self._symbol,
                side=self._open_side,
                opened_at=self._opened_at,
                entry_price=self._open_entry_price,
                sl_price=self._open_sl_price,
                tp_price=self._open_tp_price,
                leverage=self._symbol_config.leverage,
                sl_order_id=self._open_sl_order_id,
                tp_order_id=self._open_tp_order_id,
            )
        except Exception as exc:
            self._log.warning(
                f"[{self._symbol}] Erro inesperado no registro da operação: {exc}"
            )

        self._reset_open_context()

    def _reset_open_context(self) -> None:
        self._open_side = None
        self._opened_at = None
        self._open_entry_price = None
        self._open_sl_price = None
        self._open_tp_price = None
        self._open_sl_order_id = None
        self._open_tp_order_id = None

    async def _handle_error(self) -> None:
        """Tenta recuperar de erros com retry."""
        self._error_retries += 1

        if self._error_retries >= 3:
            self._log.critical(
                f"[{self._symbol}] Falha após {self._error_retries} tentativas. Encerrando."
            )
            self._state = StateChief.State.STOPPED
            return

        self._log.warning(
            f"[{self._symbol}] Erro detectado. "
            f"Tentativa {self._error_retries}/3. Reiniciando em 60s..."
        )
        await asyncio.sleep(60)
        self._state = StateChief.State.CHECK_WINDOW
