def format_binance_symbol(symbol: str) -> str:
    """Formata o símbolo para o padrão Binance Futures.

    Args:
        symbol: Símbolo da moeda (ex: 'BTC', 'ETH').

    Returns:
        Símbolo formatado (ex: 'BTC/USDT:USDT').
    """
    return f"{symbol}/USDT:USDT"


def mark_toml_as_used(filepath: str) -> str:
    """Renomeia o arquivo TOML adicionando sufixo '_used'.

    Args:
        filepath: Caminho do arquivo TOML (ex: 'adausdt.toml').

    Returns:
        Novo caminho do arquivo renomeado.
    """
    from pathlib import Path

    path = Path(filepath)
    new_path = path.with_stem(f"{path.stem}_used")
    path.rename(new_path)

    return str(new_path)


def mark_toml_as_invalid(filepath: str) -> str:
    """Renomeia o arquivo TOML adicionando sufixo '.invalid'.

    Usado quando o TOML tem configuração incompatível com a conta (símbolo
    inexistente, leverage acima do máximo do par, credencial rejeitada). O
    sufixo permite que o operador identifique o arquivo problemático por
    listagem do diretório sem precisar inspecionar logs.

    Args:
        filepath: Caminho do arquivo TOML (ex: 'geniususdt.toml').

    Returns:
        Novo caminho do arquivo renomeado (ex: 'geniususdt.toml.invalid').
    """
    from pathlib import Path

    path = Path(filepath)
    new_path = path.with_name(f"{path.name}.invalid")
    path.rename(new_path)

    return str(new_path)
