import os
import logging
from pathlib import Path
from typing import Set

import pandas as pd


class CsvRepository:
    """Gerencia a persistência dos dados em disco (CSV)."""

    DEFAULT_TIMEZONE: str = "America/Sao_Paulo"
    REQUIRED_COLUMNS: Set[str] = {"open", "high", "low", "close", "volume"}

    def __init__(
        self,
        symbol: str,
        timeframe: str,
        logger: logging.Logger,
        data_dir: str = "data",
        timezone: str = DEFAULT_TIMEZONE,
    ) -> None:
        """Inicializa o repositório CSV para um par.

        Args:
            symbol: Símbolo do par (ex: 'BTC/USDT:USDT').
            timeframe: Timeframe dos candles (ex: '1h').
            logger: Logger configurado.
            data_dir: Diretório para salvar os CSVs.
            timezone: Timezone para conversão de datas.
        """
        self._log = logger
        self._timezone = timezone
        safe_symbol = symbol.replace("/", "").replace(":", "")
        filename = f"{safe_symbol}_{timeframe}_dataset.csv"
        self._filepath = Path(data_dir) / filename

    def load(self) -> pd.DataFrame:
        """Carrega dados do CSV com tratamento de timezone."""
        if not self._filepath.exists():
            return pd.DataFrame()

        try:
            df = pd.read_csv(self._filepath)
            df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
            df["datetime"] = df["datetime"].dt.tz_convert(self._timezone)
            df.set_index("datetime", inplace=True)
            df.sort_index(inplace=True)
            return df

        except Exception as e:
            self._log.error(f"Erro ao ler arquivo {self._filepath}: {e}", exc_info=True)
            return pd.DataFrame()

    def save(self, df: pd.DataFrame) -> None:
        """Salva dados no CSV."""
        if df.empty:
            self._log.warning("DataFrame vazio, nada para salvar.")
            return

        if not self.REQUIRED_COLUMNS.issubset(df.columns):
            missing = self.REQUIRED_COLUMNS - set(df.columns)
            self._log.error(f"Colunas ausentes no DataFrame: {missing}")
            return

        try:
            os.makedirs(self._filepath.parent, exist_ok=True)
            df.to_csv(self._filepath, index=True, date_format="%Y-%m-%dT%H:%M:%S%z")
            # self._log.info(f"Dados salvos em {self._filepath}")

        except Exception as e:
            self._log.error(
                f"Erro ao salvar arquivo {self._filepath}: {e}", exc_info=True
            )
