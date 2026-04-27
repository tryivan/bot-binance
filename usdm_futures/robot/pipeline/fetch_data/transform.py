import logging
from typing import Any, List

import pandas as pd


class DataTransformer:
    """Transforma dados OHLCV de lista para DataFrame."""

    DEFAULT_TIMEZONE: str = "America/Sao_Paulo"

    def __init__(self, logger: logging.Logger, timezone: str = DEFAULT_TIMEZONE) -> None:
        self._log = logger
        self._timezone = timezone

    def to_dataframe(self, candles: List[List[Any]]) -> pd.DataFrame:
        """Converte lista de candles para DataFrame com timezone.

        Args:
            candles: Lista de candles [[timestamp, o, h, l, c, v], ...]

        Returns:
            DataFrame com colunas [open, high, low, close, volume]
            e índice datetime.
        """
        if not candles:
            self._log.warning("Lista de candles vazia")
            return pd.DataFrame()

        df = pd.DataFrame(
            candles, columns=["timestamp", "open", "high", "low", "close", "volume"]
        )

        df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        df.drop(columns=["timestamp"], inplace=True)
        df["datetime"] = df["datetime"].dt.tz_convert(self._timezone)
        df.set_index("datetime", inplace=True)

        return df


class DataValidator:
    """Valida dados OHLCV antes da transformação."""

    def __init__(self, logger: logging.Logger, timeframe_ms: int) -> None:
        self._log = logger
        self._timeframe_ms = timeframe_ms

    def validate(self, candles: List[List[Any]]) -> None:
        """Valida se não há gaps nos timestamps entre candles.

        O último candle é ignorado pois pode estar incompleto
        (candle ainda aberto na exchange).

        Args:
            candles: Lista de candles [[timestamp, o, h, l, c, v], ...]

        Raises:
            ValueError: Se encontrar gap nos dados.
        """
        if len(candles) <= 2:
            return

        candles_to_check = candles[:-1]

        for i in range(1, len(candles_to_check)):
            timestamp_atual = candles_to_check[i][0]
            timestamp_anterior = candles_to_check[i - 1][0]
            diferenca = timestamp_atual - timestamp_anterior

            if diferenca != self._timeframe_ms:
                self._log.critical(
                    f"Gap detectado: {timestamp_anterior} → {timestamp_atual} "
                    f"(diferença: {diferenca}ms, esperado: {self._timeframe_ms}ms)"
                )
                raise ValueError(f"Gap encontrado nos dados entre índices {i - 1} e {i}")
