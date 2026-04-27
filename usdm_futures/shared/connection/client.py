import ccxt.async_support as ccxt
import logging
from typing import Optional, Callable


class Exchange:
    """Gerencia a conexão assíncrona com a exchange via CCXT.

    Cria e autentica a instância da exchange configurada, suporta modos
    sandbox e produção, testa a conectividade na inicialização e garante o
    fechamento correto da sessão HTTP. Trata exceções específicas do CCXT
    para auxiliar no diagnóstico de erros.
    """

    def __init__(
        self,
        exchange_name: str,
        api_key: str,
        api_secret: str,
        market_type: str,
        sandbox: bool,
        logger: logging.Logger,
        error_handler: Callable[[Exception], Exception],
    ) -> None:
        """Inicializa o gerenciador de conexão com a exchange.

        Args:
            exchange_name: Nome da exchange no CCXT (ex: 'binance').
            api_key: Chave de API.
            api_secret: Secret da API.
            market_type: Tipo de mercado (ex: 'future').
            sandbox: Se True, usa modo testnet.
            logger: Logger configurado.
            error_handler: Função para converter exceções da exchange.

        Raises:
            AttributeError: Se a exchange não existir no CCXT.
        """
        self._exchange_name = exchange_name
        self._api_key = api_key
        self._api_secret = api_secret
        self._market_type = market_type
        self._sandbox = sandbox
        self._log = logger
        self._error_handler = error_handler
        self._exchange: Optional[ccxt.Exchange] = None

        try:
            self._exchange_class = getattr(ccxt, exchange_name)
        except AttributeError as e:
            self._log.error(f"Exchange '{exchange_name}' não encontrada no CCXT: {e}")
            raise

    def _create_instance(self) -> ccxt.Exchange:
        """Cria e retorna uma instância autenticada da exchange.

        Returns:
            Instância CCXT configurada.

        Raises:
            Exception: Se a criação falhar.
        """
        try:
            exchange: ccxt.Exchange = self._exchange_class(
                {
                    "apiKey": self._api_key,
                    "secret": self._api_secret,
                    "enableRateLimit": True,
                    "timeout": 30000,
                    "options": {
                        "defaultType": self._market_type,
                        "adjustForTimeDifference": True,
                        "recvWindow": 5000,
                    },
                }
            )
            if self._sandbox:
                exchange.enable_demo_trading(True)

            return exchange

        except Exception as e:
            self._log.error(f"Erro ao criar instância da exchange: {e}")
            raise

    async def _test_connection(self, exchange: ccxt.Exchange) -> bool:
        """Testa a conexão com a exchange usando fetch_balance."""
        try:
            balance = await exchange.fetch_balance()
        except Exception as e:
            domain_error = self._error_handler(e)
            self._log.error(f"Falha ao testar conexão: {domain_error}")
            return False

        mode = "TESTNET" if self._sandbox else "REAL"
        self._log.info(
            f"Conexão testada com sucesso! "
            f"[ {self._exchange_name.upper()}, {self._market_type} | {mode} ]"
        )

        try:
            usdt_free = balance["USDT"]["free"]
            self._log.info(f"USDT disponível: {usdt_free}")
        except (KeyError, TypeError):
            self._log.debug("Saldo USDT não disponível para este mercado.")

        if not await self._check_position_mode(exchange):
            return False

        # if not await self._check_api_permissions(exchange):
        #     return False

        return True

    async def _check_position_mode(self, exchange: ccxt.Exchange) -> bool:
        """Valida que a conta está em One-way mode.

        Returns:
            True se One-way mode confirmado, ou se o endpoint não suportar
            a verificação (degrade gracioso). False se Hedge mode detectado
            ou erro de comunicação.
        """
        try:
            result = await exchange.fetch_position_mode(params={"subType": "linear"})
        except ccxt.NotSupported as exc:
            self._log.warning(
                f"Verificação de position mode não suportada nesta exchange: {exc}. "
                "Prosseguindo — confirme manualmente que a conta está em One-way mode."
            )
            return True
        except Exception as exc:
            domain_error = self._error_handler(exc)
            self._log.error(f"Falha ao verificar position mode: {domain_error}")
            return False

        if result.get("hedged"):
            self._log.critical(
                "Conta está em HEDGE MODE. Este bot só opera em ONE-WAY MODE. "
                "Corrija em: Binance Futures → Preferences → Position Mode → One-way. "
                "Abortando."
            )
            return False

        self._log.info("Position mode validado: One-way.")
        return True

    async def _check_api_permissions(self, exchange: ccxt.Exchange) -> bool:
        """Valida o escopo da API key antes de autorizar operações.

        Bloqueia se a key tiver saque habilitado, leitura desabilitada, ou
        futuros desabilitado. Apenas avisa (warning) se spot/margin estiver
        habilitado — o usuário pode ter razão legítima para manter.

        Returns:
            True se escopo é seguro, ou se o endpoint não suportar a
            verificação (degrade gracioso — comum em testnet). False se
            escopo inseguro foi detectado ou erro de comunicação.
        """
        fetch_restrictions = getattr(exchange, "sapi_get_account_apirestrictions", None)
        if fetch_restrictions is None:
            self._log.warning(
                "Endpoint sapi_get_account_apirestrictions indisponível nesta "
                "versão do CCXT. Verificação de escopo da API key ignorada — "
                "confirme manualmente na Binance."
            )
            return True

        try:
            restrictions = await fetch_restrictions()
        except ccxt.NotSupported as exc:
            self._log.warning(
                f"Verificação de escopo da API key não suportada: {exc}. "
                "Prosseguindo — comum em testnet. Confirme manualmente."
            )
            return True
        except Exception as exc:
            domain_error = self._error_handler(exc)
            self._log.error(f"Falha ao verificar escopo da API key: {domain_error}")
            return False

        if restrictions.get("enableWithdrawals"):
            self._log.critical(
                "API key permite SAQUES. Desabilite 'Enable Withdrawals' na "
                "Binance → API Management. Abortando."
            )
            return False

        if not restrictions.get("enableReading"):
            self._log.critical(
                "API key sem permissão de leitura. Habilite 'Enable Reading' "
                "na Binance → API Management. Abortando."
            )
            return False

        if not restrictions.get("enableFutures"):
            self._log.critical(
                "API key sem permissão de Futures. Habilite 'Enable Futures' "
                "na Binance → API Management. Abortando."
            )
            return False

        if restrictions.get("enableSpotAndMarginTrading"):
            self._log.warning(
                "API key tem Spot/Margin trading habilitado. Não é bloqueante, "
                "mas reduza o escopo se esta key for exclusiva do bot."
            )

        self._log.info("Escopo da API key validado.")
        return True

    async def connect(self) -> ccxt.Exchange:
        """Cria, testa e reutiliza a conexão com a exchange.

        Raises:
            ConnectionError: Se o teste de conexão falhar

        Returns:
            Instância autenticada da exchange
        """
        if self._exchange is None:
            self._exchange = self._create_instance()
            if not await self._test_connection(self._exchange):
                await self._exchange.close()
                self._exchange = None
                raise ConnectionError(
                    "Falha ao conectar com a exchange - verifique os logs."
                )
        return self._exchange

    async def close(self) -> None:
        """Fecha a sessão HTTP assíncrona da exchange."""
        if self._exchange is not None:
            await self._exchange.close()
            self._exchange = None
            self._log.info("Conexão com a exchange encerrada.")
