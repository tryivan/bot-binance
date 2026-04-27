import asyncio
import logging

from typing import Any, Callable, List, Optional
import ccxt.async_support as ccxt
from datetime import datetime
from zoneinfo import ZoneInfo


class ExchangeFetchData:
    """Gerencia o download assíncrono de dados OHLCV da exchange."""

    def __init__(
        self,
        exchange: ccxt.Exchange,
        logger: logging.Logger,
        symbol: str,
        timeframe: str,
        error_handler: Callable[[Exception], Exception],
    ) -> None:
        self._log = logger
        self._exchange = exchange
        self._symbol = symbol
        self._timeframe = timeframe
        self._error_handler = error_handler
        self._timeframe_ms = self._resolve_timeframe_ms()

    def _resolve_timeframe_ms(self) -> int:
        """Converte o timeframe em milissegundos.

        Returns:
            Timeframe em milissegundos.

        Raises:
            Exception: Se o timeframe for inválido.
        """
        try:
            seconds = self._exchange.parse_timeframe(self._timeframe)
            return int(seconds * 1000)
        except Exception as e:
            self._log.critical(
                f"Erro crítico ao parsear timeframe '{self._timeframe}': {e}"
            )
            raise

    async def _download_with_retry(
        self, since: Optional[int], limit: int, max_retries: int, retry_delay: int
    ) -> List[List[Any]]:
        """Executa download de candles com retry em caso de falha.

        Args:
            since: Timestamp inicial em milissegundos (None para download por limite).
            limit: Quantidade máxima de candles.
            max_retries: Número máximo de tentativas.
            retry_delay: Intervalo entre tentativas em segundos.

        Returns:
            Lista de candles OHLCV.
        """
        if max_retries <= 0:
            self._log.warning("max_retries <= 0: nenhuma tentativa será realizada.")
            return []

        retry_count = 0

        while retry_count < max_retries:
            try:
                candles = await self._exchange.fetch_ohlcv(
                    symbol=self._symbol,
                    timeframe=self._timeframe,
                    since=since,
                    limit=limit,
                )
                self._log.info(f"Download concluído: {len(candles)} candles obtidos")
                return candles

            except Exception as e:
                domain_error = self._error_handler(e)
                retry_count += 1

                if retry_count >= max_retries:
                    self._log.critical(
                        f"Falha crítica após {max_retries} tentativas: {domain_error}"
                    )
                    raise domain_error from e

                self._log.warning(
                    f"Tentativa {retry_count}/{max_retries} falhou: {domain_error}. "
                    f"Aguardando {retry_delay}s..."
                )
                await asyncio.sleep(retry_delay)

        return []

    async def download_by_limit(
        self, limit: int, max_retries: int = 3, retry_delay: int = 3
    ) -> List[List[Any]]:
        """Download de candles com base na quantidade."""
        return await self._download_with_retry(
            since=None, limit=limit, max_retries=max_retries, retry_delay=retry_delay
        )

    async def download_since(
        self, since: int, limit: int = 1000, max_retries: int = 3, retry_delay: int = 3
    ) -> List[List[Any]]:
        """Download de candles a partir de um timestamp."""

        next_candle_dt = datetime.fromtimestamp(
            (since + self._timeframe_ms - 1) / 1000, tz=ZoneInfo("America/Sao_Paulo")
        )
        self._log.info(
            f"[{self._symbol}] Buscando candle de {next_candle_dt.strftime('%H:%M:%S')}..."
        )

        return await self._download_with_retry(
            since=since, limit=limit, max_retries=max_retries, retry_delay=retry_delay
        )

    @property
    def timeframe_ms(self) -> int:
        """Retorna o timeframe em milissegundos."""
        return self._timeframe_ms
