import tomllib
from pathlib import Path
from typing import Literal
from pydantic import BaseModel


class ScreeningParams(BaseModel):
    """Parâmetros de configuração do screening."""

    # --- Screening ---
    max_concurrent: int
    min_volume: float
    min_atr: float
    max_adx: float
    atr_divisor: int
    tp_multiplier: float
    max_leverage: int
    atr_period: int
    adx_period: int
    timeframe: str
    ohlcv_limit: int

    # --- TOML Defaults ---
    chase_percent: float
    offset_percent: float
    fill_timeout: int
    max_retries: int
    order_type: Literal["market", "limit"]
    candle_limit: int
    max_rows: int
    batch_limit: int
    fetch_retry_attempts: int
    fetch_retry_delay: int


def load_screening_params(filepath: str) -> ScreeningParams:
    """Carrega os parâmetros de screening do arquivo TOML.

    Args:
        filepath: Caminho do arquivo TOML.

    Returns:
        ScreeningParams validado.

    Raises:
        FileNotFoundError: Se o arquivo não existir.
        ValidationError: Se os dados forem inválidos.
    """
    path = Path(filepath)

    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")

    with open(path, "rb") as f:
        data = tomllib.load(f)

    merged = {}
    merged.update(data.get("screening", {}))
    merged.update(data.get("toml_defaults", {}))

    return ScreeningParams(**merged)
