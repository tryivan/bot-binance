import asyncio
from typing import Optional

import ccxt.async_support as ccxt


class ExchangeError(Exception):
    """Erro base para operações na exchange."""

    def __init__(self, message: str, original: Optional[Exception] = None) -> None:
        self.original = original
        super().__init__(message)


class NetworkError(ExchangeError):
    """Falha de rede ao comunicar com a exchange."""


class AuthenticationError(ExchangeError):
    """Chave de API inválida ou sem permissão."""


class RateLimitError(ExchangeError):
    """Limite de requisições excedido."""


class BadRequestError(ExchangeError):
    """Parâmetros inválidos enviados à exchange."""


class OrderNotFoundError(ExchangeError):
    """Ordem não encontrada na exchange."""


class InsufficientFundsError(ExchangeError):
    """Saldo insuficiente para a operação."""


class UnexpectedExchangeError(ExchangeError):
    """Erro inesperado da exchange."""


class EmptyOHLCVError(ExchangeError):
    """Exchange respondeu sem candles novos após esgotar todas as tentativas."""


class HedgeModeError(ExchangeError):
    """Conta está em Hedge mode; bot só suporta One-way mode."""


class ApiPermissionError(ExchangeError):
    """Escopo da API key incompatível com operação segura do bot."""


# Mapeamento: exceção CCXT → exceção do domínio
_CCXT_MAP = {
    asyncio.TimeoutError: NetworkError,
    ccxt.NetworkError: NetworkError,
    ccxt.AuthenticationError: AuthenticationError,
    ccxt.RateLimitExceeded: RateLimitError,
    ccxt.BadRequest: BadRequestError,
    ccxt.OrderNotFound: OrderNotFoundError,
    ccxt.InsufficientFunds: InsufficientFundsError,
    ccxt.ExchangeError: UnexpectedExchangeError,
}


def handle_exchange_error(exc: Exception) -> ExchangeError:
    """Converte uma exceção CCXT para uma exceção do domínio.

    Args:
        exc: Exceção original do CCXT.

    Returns:
        Exceção do domínio correspondente.
    """
    for ccxt_type, domain_type in _CCXT_MAP.items():
        if isinstance(exc, ccxt_type):
            return domain_type(str(exc), original=exc)

    return UnexpectedExchangeError(str(exc), original=exc)
