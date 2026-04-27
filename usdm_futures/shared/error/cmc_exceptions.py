from typing import Optional

from requests.exceptions import RequestException, Timeout, HTTPError, ConnectionError


class CoinMarketCapError(Exception):
    """Erro base para operações com a CoinMarketCap API."""

    def __init__(self, message: str, original: Optional[Exception] = None) -> None:
        self.original = original
        super().__init__(message)


class CmcTimeoutError(CoinMarketCapError):
    """Timeout ao conectar com a CoinMarketCap API."""


class CmcHttpError(CoinMarketCapError):
    """Erro HTTP na resposta da CoinMarketCap API."""


class CmcConnectionError(CoinMarketCapError):
    """Falha de rede ao conectar com a CoinMarketCap API."""


class CmcResponseError(CoinMarketCapError):
    """Resposta inválida da CoinMarketCap API."""


class CmcUnexpectedError(CoinMarketCapError):
    """Erro inesperado na CoinMarketCap API."""


_CMC_MAP = {
    Timeout: CmcTimeoutError,
    HTTPError: CmcHttpError,
    ConnectionError: CmcConnectionError,
    RequestException: CmcUnexpectedError,
}


def handle_cmc_error(exc: Exception) -> CoinMarketCapError:
    """Converte uma exceção requests para uma exceção do domínio CMC.

    Args:
        exc: Exceção original.

    Returns:
        Exceção do domínio correspondente.
    """
    for req_type, domain_type in _CMC_MAP.items():
        if isinstance(exc, req_type):
            return domain_type(str(exc), original=exc)

    return CmcUnexpectedError(str(exc), original=exc)
