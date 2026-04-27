from decimal import Decimal
from typing import Any, Dict, Literal, Optional, Tuple

import ccxt.async_support as ccxt


class OrderUtils:
    """
    Utilitários puros para ordens de trading.

    Responsabilidades:
    - Cálculos de preços (entrada, SL, TP)
    - Formatação de valores para precisão da exchange
    - Extração de dados de posição
    - Detecção e classificação de ordens de proteção
    """

    LONG_SIDE: Literal["buy"] = "buy"
    SHORT_SIDE: Literal["sell"] = "sell"
    VALID_SIDES: Tuple[Literal["buy"], Literal["sell"]] = ("buy", "sell")

    def __init__(self, exchange: ccxt.Exchange, symbol: str) -> None:
        self._exchange = exchange
        self._symbol = symbol

    # -------------------------------------------------------------------------
    # Cálculos de preço e formatação
    # -------------------------------------------------------------------------

    def format_amount(self, amount: float) -> str:
        """Formata a quantidade para a precisão da exchange."""
        precision_amount = self._exchange.amount_to_precision(self._symbol, amount)
        return (
            precision_amount
            if precision_amount is not None
            else str(Decimal(str(amount)))
        )

    def calculate_entry_price(
        self, side: str, current_price: float, offset_percent: float
    ) -> str:
        """Calcula o preço de entrada com offset aplicado."""
        if current_price <= 0:
            raise ValueError(
                f"current_price deve ser positivo, recebido: {current_price}"
            )

        price = Decimal(str(current_price))
        offset = Decimal(str(offset_percent)) / 100

        if side == self.LONG_SIDE:
            result = price * (1 - offset)
        else:
            result = price * (1 + offset)

        precision_price = self._exchange.price_to_precision(self._symbol, float(result))
        return precision_price if precision_price is not None else str(result)

    def calculate_protection_price(
        self, side: str, entry_price: float, percent: float, is_stop_loss: bool
    ) -> str:
        """Calcula o preço de Stop Loss ou Take Profit."""
        if entry_price <= 0:
            raise ValueError(f"entry_price deve ser positivo, recebido: {entry_price}")

        is_long = side == self.LONG_SIDE
        should_subtract = (is_long and is_stop_loss) or (not is_long and not is_stop_loss)

        price = Decimal(str(entry_price))
        pct = Decimal(str(percent)) / 100

        if should_subtract:
            result = price * (1 - pct)
        else:
            result = price * (1 + pct)

        precision_price = self._exchange.price_to_precision(self._symbol, float(result))
        return precision_price if precision_price is not None else str(result)

    # -------------------------------------------------------------------------
    # Extração de dados de posição
    # -------------------------------------------------------------------------

    def extract_entry_price(self, position: Dict[str, Any]) -> Optional[float]:
        """Extrai o preço de entrada da posição."""
        info = position.get("info", {})
        candidates = [
            position.get("entryPrice"),
            info.get("entryPrice"),
            info.get("avgEntryPrice"),
        ]
        raw_price = next((v for v in candidates if v is not None), None)

        if raw_price is None:
            return None

        try:
            price = float(raw_price)
            return price if price > 0 else None
        except (TypeError, ValueError):
            return None

    def extract_size(self, position: Dict[str, Any]) -> float:
        """Extrai o tamanho da posição (valor absoluto)."""
        contracts = position.get("contracts") or position.get("info", {}).get(
            "positionAmt"
        )

        try:
            return abs(float(contracts or 0))
        except (TypeError, ValueError):
            return 0.0

    def derive_side(self, position: Dict[str, Any]) -> Optional[str]:
        """Determina o lado da posição (buy/sell)."""
        side = position.get("side") or position.get("info", {}).get("positionSide")

        if side:
            side = str(side).lower()
            if side in ("long", "buy"):
                return self.LONG_SIDE
            if side in ("short", "sell"):
                return self.SHORT_SIDE

        contracts = position.get("contracts") or position.get("info", {}).get(
            "positionAmt"
        )

        try:
            value = float(contracts)
            if value > 0:
                return self.LONG_SIDE
            if value < 0:
                return self.SHORT_SIDE
        except (TypeError, ValueError):
            pass

        return None

    # -------------------------------------------------------------------------
    # Detecção e classificação de ordens de proteção
    # -------------------------------------------------------------------------

    @staticmethod
    def is_truthy_flag(value: Any) -> bool:
        """Retorna True para valores que representam verdadeiro, inclusive strings."""
        if value is None:
            return False
        if isinstance(value, bool):
            return value
        return str(value).lower() not in ("false", "0", "")

    def is_protection_order(self, order: Dict[str, Any], closing_side: str) -> bool:
        """Verifica se a ordem é uma ordem de proteção válida."""
        reduce_only_raw = order.get("reduceOnly")
        if reduce_only_raw is None:
            reduce_only_raw = order.get("info", {}).get("reduceOnly")

        if not self.is_truthy_flag(reduce_only_raw):
            return False

        order_side = (
            order.get("side") or order.get("info", {}).get("side") or ""
        ).lower()
        if order_side and order_side not in self.VALID_SIDES:
            if "sell" in order_side:
                order_side = self.SHORT_SIDE
            elif "buy" in order_side:
                order_side = self.LONG_SIDE

        return not order_side or order_side == closing_side

    @staticmethod
    def is_take_profit_type(order: Dict[str, Any]) -> bool:
        """Retorna True se o tipo da ordem indica Take Profit."""
        info = order.get("info", {})
        candidates = [order.get("type"), info.get("type"), info.get("origType")]
        return any("take_profit" in str(t).lower() for t in candidates if t)

    @staticmethod
    def is_stop_loss_type(order: Dict[str, Any]) -> bool:
        """Retorna True se o tipo da ordem indica Stop Loss (não Take Profit)."""
        info = order.get("info", {})
        candidates = [order.get("type"), info.get("type"), info.get("origType")]
        return any(
            "stop" in str(t).lower() and "take_profit" not in str(t).lower()
            for t in candidates
            if t
        )

    @staticmethod
    def get_stop_price(order: Dict[str, Any]) -> Optional[float]:
        """Extrai o preço de stop da ordem."""
        raw_stop = order.get("stopPrice") or order.get("info", {}).get("stopPrice")
        if raw_stop is None:
            raw_stop = (
                order.get("price")
                if order.get("type") in {"take_profit", "TAKE_PROFIT"}
                else None
            )

        if raw_stop is not None:
            try:
                return float(raw_stop)
            except (TypeError, ValueError):
                pass
        return None

    @staticmethod
    def classify_by_price(
        position_side: str, entry_price: float, stop_price: float
    ) -> Tuple[bool, bool]:
        """Classifica a ordem como SL ou TP baseado no preço."""
        is_sl = False
        is_tp = False

        if position_side == "buy":
            if stop_price < entry_price:
                is_sl = True
            elif stop_price > entry_price:
                is_tp = True
        else:
            if stop_price > entry_price:
                is_sl = True
            elif stop_price < entry_price:
                is_tp = True

        return is_sl, is_tp
