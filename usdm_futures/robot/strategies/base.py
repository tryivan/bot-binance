from typing import Literal, Protocol, runtime_checkable

import pandas as pd


@runtime_checkable
class Strategy(Protocol):
    """Interface para estratégias de trading plugáveis.

    Uma estratégia é responsável por:
    1. Calcular os indicadores que ela consome (apply_indicators).
    2. Decidir se há sinal de entrada no último candle (check_signal).

    Implementações devem ser puras (sem I/O) e síncronas.
    """

    name: str

    def apply_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Adiciona colunas de indicadores ao dataframe e devolve-o."""
        ...

    def check_signal(self, df: pd.DataFrame) -> Literal["buy", "sell"] | None:
        """Retorna 'buy'/'sell' se há sinal no último candle, senão None."""
        ...
