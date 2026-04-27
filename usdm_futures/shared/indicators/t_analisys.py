import logging
from typing import Optional

import pandas as pd
import pandas_ta_classic as ta


class Indicators:
    """Calcula indicadores técnicos sobre o dataset OHLCV."""

    def __init__(self, logger: logging.Logger) -> None:
        """Inicializa o calculador de indicadores.

        Args:
            logger: Logger configurado.
        """
        self._log = logger

    def sma(self, series: pd.Series, length: int) -> Optional[pd.Series]:
        """Calcula a Média Móvel Simples (SMA).

        Args:
            series: Série de preços (ex: close).
            length: Período da média.

        Returns:
            Series com os valores da SMA ou None se falhar.
        """
        result = ta.sma(series, length=length)
        if result is None:
            self._log.warning(f"SMA({length}) retornou None.")
            return None
        return result

    def ema(self, series: pd.Series, length: int) -> Optional[pd.Series]:
        """Calcula a Média Móvel Exponencial (EMA).

        Args:
            series: Série de preços (ex: close).
            length: Período da média.

        Returns:
            Series com os valores da EMA ou None se falhar.
        """
        result = ta.ema(series, length=length)
        if result is None:
            self._log.warning(f"EMA({length}) retornou None.")
            return None
        return result

    # -------------------------------------------------------------------------
    # Estratégias
    # -------------------------------------------------------------------------
    def double_mean(
        self,
        df: pd.DataFrame,
        col: str,
        type_1: str,
        length_1: int,
        type_2: str,
        length_2: int,
    ) -> pd.DataFrame:
        """Aplica duas médias móveis ao dataset.

        Args:
            df: DataFrame com os dados.
            col: Coluna de referência (ex: 'close').
            type_1: Tipo da primeira média ('sma' ou 'ema').
            length_1: Período da primeira média.
            type_2: Tipo da segunda média ('sma' ou 'ema').
            length_2: Período da segunda média.

        Returns:
            DataFrame com as colunas das médias adicionadas.
        """
        methods = {"sma": self.sma, "ema": self.ema}

        calc_1 = methods.get(type_1)
        calc_2 = methods.get(type_2)

        if calc_1 is None:
            self._log.warning(f"Tipo de média desconhecido: {type_1}")
            return df

        if calc_2 is None:
            self._log.warning(f"Tipo de média desconhecido: {type_2}")
            return df

        result_1 = calc_1(df[col], length=length_1)
        result_2 = calc_2(df[col], length=length_2)

        if result_1 is not None:
            df[f"{type_1}_{length_1}"] = result_1

        if result_2 is not None:
            df[f"{type_2}_{length_2}"] = result_2

        return df
