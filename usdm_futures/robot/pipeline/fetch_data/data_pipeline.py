import logging
from typing import Any, List

import pandas as pd
import asyncio

from .source import ExchangeFetchData
from .csv_storage import CsvRepository
from .transform import DataValidator, DataTransformer
from usdm_futures.shared.error.exceptions import EmptyOHLCVError


class DataPipeline:
    """Orquestra o pipeline de dados OHLCV."""

    def __init__(
        self,
        data_source: ExchangeFetchData,
        csv_repo: CsvRepository,
        validator: DataValidator,
        transformer: DataTransformer,
        logger: logging.Logger,
        candle_limit: int,
        max_rows: int,
        batch_limit: int,
        fetch_retry_attempts: int,
        fetch_retry_delay: int,
    ) -> None:
        """Inicializa o orquestrador do pipeline de dados.

        Args:
            data_source: Instância para download de candles.
            csv_repo: Instância para persistência CSV.
            validator: Instância para validação de dados.
            transformer: Instância para transformação de dados.
            logger: Logger configurado.
            max_rows: Máximo de linhas no dataset.
            fetch_retry_attempts: Tentativas de fetch quando resposta vem vazia.
            fetch_retry_delay: Segundos entre tentativas de fetch.
        """
        self._log = logger
        self._data_source = data_source
        self._csv_repo = csv_repo
        self._validator = validator
        self._transformer = transformer
        self._max_rows = max_rows
        self._candle_limit = candle_limit
        self._batch_limit = batch_limit
        self._fetch_retry_attempts = fetch_retry_attempts
        self._fetch_retry_delay = fetch_retry_delay

    def load(self) -> pd.DataFrame:
        """Carrega o dataset do CSV."""
        return self._csv_repo.load()

    def save(self, df: pd.DataFrame) -> None:
        """Salva o DataFrame no CSV."""
        self._csv_repo.save(df)

    async def download(self) -> List[List[Any]]:
        """Download inicial de candles."""
        return await self._data_source.download_by_limit(limit=self._candle_limit)

    async def update(self, existing_df: pd.DataFrame) -> List[List[Any]]:
        """Baixa apenas candles novos em batches."""
        last_datetime = existing_df.index[-1]
        last_utc = last_datetime.tz_convert("UTC")
        last_timestamp = int(last_utc.timestamp() * 1000)
        since = last_timestamp + 1
        all_candles: List[List[Any]] = []

        while True:
            candles = await self._data_source.download_since(
                since=since, limit=self._batch_limit
            )

            if not candles:
                break

            all_candles.extend(candles)

            if len(candles) < self._batch_limit:
                break

            since = int(candles[-1][0]) + self._data_source.timeframe_ms

        return all_candles

    def validate(self, candles: List[List[Any]]) -> None:
        """Valida gaps nos timestamps."""
        self._validator.validate(candles)

    def transform(self, candles: List[List[Any]]) -> pd.DataFrame:
        """Transforma lista de candles em DataFrame."""
        return self._transformer.to_dataframe(candles)

    def append(self, existing_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
        """Combina DataFrames removendo duplicatas."""
        combined = pd.concat([existing_df, new_df])
        combined = combined[~combined.index.duplicated(keep="last")]
        combined.sort_index(inplace=True)
        return combined

    def trim(self, df: pd.DataFrame) -> pd.DataFrame:
        """Mantém apenas as últimas max_rows linhas."""
        if self._max_rows and len(df) > self._max_rows:
            return df.iloc[-self._max_rows :]
        return df

    def seconds_until_next_candle(self) -> int:
        """Calcula segundos até o próximo candle fechar."""
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)
        timeframe_seconds = self._data_source.timeframe_ms // 1000
        current_timestamp = int(now.timestamp())
        seconds_into_candle = current_timestamp % timeframe_seconds
        remaining = timeframe_seconds - seconds_into_candle

        return remaining

    async def run(self) -> pd.DataFrame:
        """Orquestra o pipeline completo de dados.

        Se o fetch retornar vazio, tenta novamente até
        ``fetch_retry_attempts`` vezes com ``fetch_retry_delay`` segundos
        entre tentativas. Se todas falharem, levanta ``EmptyOHLCVError``.
        """
        existing_df = self.load()
        is_initial = existing_df.empty

        candles: List[List[Any]] = []
        for attempt in range(1, self._fetch_retry_attempts + 1):
            if is_initial:
                candles = await self.download()
            else:
                candles = await self.update(existing_df)

            if candles:
                break

            if attempt < self._fetch_retry_attempts:
                level = logging.INFO if attempt < 3 else logging.WARNING
                self._log.log(
                    level,
                    f"Candle indisponível (tentativa {attempt}/"
                    f"{self._fetch_retry_attempts}). "
                    f"Aguardando {self._fetch_retry_delay}s...",
                )
                await asyncio.sleep(self._fetch_retry_delay)
        else:
            raise EmptyOHLCVError(
                f"Nenhum candle obtido após {self._fetch_retry_attempts} tentativas"
            )

        self.validate(candles)

        new_df = self.transform(candles)

        if not existing_df.empty:
            new_df = self.append(existing_df, new_df)

        new_df = self.trim(new_df)

        self.save(new_df)

        return new_df
