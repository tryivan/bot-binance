import asyncio
import pandas as pd
from typing import Any, Callable, Dict, List, Optional, Tuple
import ccxt.async_support as ccxt
from usdm_futures.shared.validator.screening_params import ScreeningParams


class CryptoScreening:
    """Pipeline de screening e seleção de pares de criptomoedas.

    Filtra pares de futuros perpétuos USDT da Binance por volume,
    volatilidade (ATR) e tendência (ADX), calcula parâmetros
    operacionais e exporta TOMLs individuais por par.
    """

    def __init__(
        self,
        exchange: ccxt.Exchange,
        error_handler: Callable[[Exception], Exception],
        params: ScreeningParams,
    ) -> None:
        """Inicializa o screening de pares.

        Args:
            exchange: Instância CCXT autenticada.
            error_handler: Função para converter exceções da exchange.
            max_concurrent: Máximo de downloads simultâneos.
        """
        self._exchange = exchange
        self._error_handler = error_handler
        self._params = params
        self._semaphore = asyncio.Semaphore(params.max_concurrent)

    # -------------------------------------------------------------------------
    # Utilitários
    # -------------------------------------------------------------------------
    @staticmethod
    def clean_symbol(symbol: str) -> str:
        """Remove barra e sufixo do symbol da exchange.

        Args:
            symbol: Símbolo no formato exchange (ex: 'BTC/USDT:USDT').

        Returns:
            Símbolo limpo (ex: 'BTCUSDT').
        """
        return symbol.split(":")[0].replace("/", "")

    @staticmethod
    def _quote_volume_series(candles: List[List[Any]]) -> pd.Series:
        """Calcula o volume em USDT (close * volume) para cada candle.

        Args:
            candles: Lista de candles com coluna ATR adicionada.

        Returns:
            Series com volume em USDT.
        """
        columns = ["timestamp", "open", "high", "low", "close", "volume", "atr"]
        df = pd.DataFrame(candles, columns=columns)
        return df["close"] * df["volume"]

    # -------------------------------------------------------------------------
    # Coleta de dados (async)
    # -------------------------------------------------------------------------
    async def active_pairs(self) -> List[str]:
        """Retorna pares ativos de futuros perpétuos lineares USDT.

        Returns:
            Lista de símbolos ordenados.
        """
        try:
            markets = await self._exchange.load_markets()
        except Exception as exc:
            domain_error = self._error_handler(exc)
            print(f"Falha ao carregar mercados: {domain_error}")
            raise domain_error from exc

        pairs: List[str] = []

        for symbol, market in markets.items():
            # Para cada par (chave, valor) dentro do dicionário markets, separe a chave em symbol e o valor em market.
            if not market.get("contract") or not market.get("active"):
                # Se qualquer um dos dois for False ou None → descarta o par → próxima iteração
                continue
            if market.get("quote") != "USDT":
                # Se o quote for diferente de USDT → descarta o par → próxima iteração
                continue
            if not market.get("swap"):
                # Se "swap" for False ou None → descarta o par → próxima iteração
                continue
            if not market.get("linear"):
                # Se "linear" for False ou None → descarta o par → próxima iteração
                continue
            pairs.append(symbol)

        print(f"{len(pairs)} pares ativos encontrados.")
        return sorted(pairs)

    async def quote_volume(self, pairs: List[str]) -> Dict[str, Optional[float]]:
        """Busca o volume em quote (USDT) das últimas 24h para cada par.

        Args:
            pairs: Lista de símbolos.

        Returns:
            Dicionário {símbolo_limpo: volume_24h}.
        """
        try:
            tickers = await self._exchange.fetch_tickers(pairs)
        except Exception as exc:
            domain_error = self._error_handler(exc)
            print(f"Falha ao buscar tickers: {domain_error}")
            raise domain_error from exc

        return {
            self.clean_symbol(symbol): ticker.get("quoteVolume")
            for symbol, ticker in tickers.items()
        }

    async def _fetch_one_ohlcv(
        self, symbol: str, timeframe: str, limit: int
    ) -> Tuple[str, List[List[Any]]]:
        """Baixa candles de um único par, respeitando o semáforo.

        Args:
            symbol: Símbolo do par.
            timeframe: Timeframe dos candles.
            limit: Quantidade máxima de candles.

        Returns:
            Tupla (símbolo, lista de candles).
        """
        async with self._semaphore:
            try:
                data = await self._exchange.fetch_ohlcv(
                    symbol, timeframe=timeframe, limit=limit
                )
                return symbol, data
            except Exception as exc:
                domain_error = self._error_handler(exc)
                print(f"Erro ao baixar {symbol}: {domain_error}")
                return symbol, []

    async def fetch_ohlcv(
        self, pairs: List[str], timeframe: str = "1h", limit: int = 1000
    ) -> Dict[str, List[List[Any]]]:
        """Baixa candles OHLCV para cada par em paralelo.

        Args:
            pairs: Lista de símbolos.
            timeframe: Timeframe dos candles.
            limit: Quantidade máxima de candles por par.

        Returns:
            Dicionário {símbolo: lista de candles}.
        """
        tasks = [self._fetch_one_ohlcv(symbol, timeframe, limit) for symbol in pairs]
        results = await asyncio.gather(*tasks)

        ohlcv = {symbol: data for symbol, data in results if data}
        print(f"OHLCV baixado para {len(ohlcv)}/{len(pairs)} pares.")
        return ohlcv

    # -------------------------------------------------------------------------
    # Filtros
    # -------------------------------------------------------------------------
    def filter_by_volume(
        self, pairs_dict: Dict[str, Optional[float]]
    ) -> Dict[str, float]:
        """Filtra pares cujo volume em USDT é maior ou igual ao mínimo.

        Args:
            pairs_dict: Dicionário {símbolo: volume}.
            min_volume: Volume mínimo em USDT.

        Returns:
            Dicionário filtrado.
        """
        filtered = {
            symbol: volume
            for symbol, volume in pairs_dict.items()
            if volume and volume >= self._params.min_volume
        }
        print(
            f"Filtro volume: {len(filtered)}/{len(pairs_dict)} pares (min: {self._params.min_volume:,.0f} USDT)."
        )
        return filtered
