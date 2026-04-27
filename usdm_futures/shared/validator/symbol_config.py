import tomllib
from pathlib import Path
from typing import Literal
from pydantic import BaseModel


class SymbolConfig(BaseModel):
    """Configuração operacional de um par de trading.

    Carrega e valida os parâmetros do arquivo TOML do par.
    """

    symbol: str  # Par de moedas (ex: BTC/USDT:USDT)

    # --- Data ---
    batch_limit: int  # Tamanho do lote para download em batches
    candle_limit: int  # Quantidade de candles no download inicial
    max_rows: int  # Máximo de linhas no dataset CSV
    timeframe: str  # Intervalo dos candles

    # --- Fetch retry ---
    fetch_retry_attempts: int  # Máximo de tentativas quando fetch retorna vazio
    fetch_retry_delay: int  # Segundos entre tentativas

    # --- Ordem ---
    order_type: Literal["market", "limit"]  # Tipo de ordem
    amount: float  # Quantidade a operar
    chase_percent: float  # Limite de perseguição de preço em %
    offset_percent: float  # Offset de entrada em %
    fill_timeout: int  # Tempo de espera para preenchimento (segundos)
    max_retries: int  # Tentativas em caso de falha

    # --- Proteção ---
    stop_loss_percent: float  # Stop Loss em %
    take_profit_percent: float  # Take Profit em %

    # --- Risco ---
    leverage: int  # Alavancagem


def load_symbol_config(filepath: str) -> SymbolConfig:
    """Carrega a configuração operacional de um par do arquivo TOML.

    Args:
        filepath: Caminho do arquivo TOML do par.

    Returns:
        SymbolConfig validado.

    Raises:
        FileNotFoundError: Se o arquivo não existir.
        ValueError: Se o TOML não contiver dados de símbolos.
        ValidationError: Se os dados forem inválidos.
    """
    path = Path(filepath)

    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")

    with open(path, "rb") as f:
        data = tomllib.load(f)

    symbols = data.get("symbols")
    if not symbols:
        raise ValueError(f"Nenhum símbolo encontrado em '{path}'.")

    return SymbolConfig(**symbols[0])
