import logging
from typing import Literal

import pandas as pd

from usdm_futures.shared.indicators.t_analisys import Indicators


class DoubleMeanStrategy:
    """Estratégia de cruzamento EMA / SMA sobre uma coluna de preço.

    Sinal de compra: EMA cruza SMA para cima entre iloc[-2] e iloc[-1].
    Sinal de venda: EMA cruza SMA para baixo entre iloc[-2] e iloc[-1].
    """

    name = "double_mean"

    def __init__(
        self,
        indicators: Indicators,
        logger: logging.Logger,
        sma_length: int = 40,
        ema_length: int = 7,
        source_column: str = "close",
    ) -> None:
        self._indicators = indicators
        self._log = logger
        self._sma_length = sma_length
        self._ema_length = ema_length
        self._source_column = source_column
        self._sma_col = f"sma_{sma_length}"
        self._ema_col = f"ema_{ema_length}"

    def apply_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        return self._indicators.double_mean(
            df=df,
            col=self._source_column,
            type_1="sma",
            length_1=self._sma_length,
            type_2="ema",
            length_2=self._ema_length,
        )

    def check_signal(self, df: pd.DataFrame) -> Literal["buy", "sell"] | None:
        if len(df) < 2:
            return None

        prev_ema = df[self._ema_col].iloc[-2]
        prev_sma = df[self._sma_col].iloc[-2]
        curr_ema = df[self._ema_col].iloc[-1]
        curr_sma = df[self._sma_col].iloc[-1]

        if prev_ema < prev_sma and curr_ema > curr_sma:
            return "buy"

        if prev_ema > prev_sma and curr_ema < curr_sma:
            return "sell"

        return None
