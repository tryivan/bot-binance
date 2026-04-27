from __future__ import annotations
from pydantic import AliasChoices, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from pathlib import Path


class LoadKey(BaseSettings):
    """Configuração de chaves e credenciais do sistema.

    Carrega chaves de API da Binance (testnet e produção),
    credenciais do Telegram e CoinMarketCap do arquivo .env.
    """

    model_config = SettingsConfigDict(
        env_file=str(Path(__file__).resolve().parent.parent / "env" / ".env"),
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # =========================================================================
    # EXCHANGE
    # =========================================================================
    exchange: str = ""
    market_type: str = "future"
    sandbox: bool

    # Campos internos — mapeados às env vars via validation_alias
    binance_api_key_test: SecretStr = SecretStr("")
    binance_api_secret_test: SecretStr = SecretStr("")

    binance_api_key_prod: SecretStr = Field(
        default=SecretStr(""),
        validation_alias=AliasChoices("binance_api_key", "BINANCE_API_KEY"),
    )
    binance_api_secret_prod: SecretStr = Field(
        default=SecretStr(""),
        validation_alias=AliasChoices("binance_api_secret", "BINANCE_API_SECRET"),
    )

    @property
    def binance_api_key(self) -> str:
        key = self.binance_api_key_test if self.sandbox else self.binance_api_key_prod
        return key.get_secret_value()

    @property
    def binance_api_secret(self) -> str:
        secret = (
            self.binance_api_secret_test if self.sandbox else self.binance_api_secret_prod
        )
        return secret.get_secret_value()

    # =========================================================================
    # TELEGRAM
    # =========================================================================
    telegram_bot_token: SecretStr = SecretStr("")
    telegram_chat_id: str = ""

    # =========================================================================
    # COIN MARKET CAP
    # =========================================================================
    coin_market_cap: SecretStr = SecretStr("")

    # =========================================================================
    # VALIDAÇÕES: Strings
    # =========================================================================
    @field_validator("exchange", "market_type", "telegram_chat_id")
    @classmethod
    def not_empty(cls, v: str, info) -> str:
        if not v or not v.strip():
            raise ValueError(f"{info.field_name} não pode ser vazio.")
        return v.strip()

    @field_validator(
        "binance_api_key_test",
        "binance_api_secret_test",
        "binance_api_key_prod",
        "binance_api_secret_prod",
        "telegram_bot_token",
        "coin_market_cap",
    )
    @classmethod
    def not_empty_if_set(cls, v: SecretStr, info) -> SecretStr:
        value = v.get_secret_value()
        if value and not value.strip():
            raise ValueError(f"{info.field_name} não pode ser somente espaços em branco.")
        return SecretStr(value.strip())

    @model_validator(mode="after")
    def validate_active_api_keys(self) -> LoadKey:
        if self.sandbox:
            if not self.binance_api_key_test.get_secret_value():
                raise ValueError(
                    "sandbox=True mas BINANCE_API_KEY_TEST não está definida."
                )
            if not self.binance_api_secret_test.get_secret_value():
                raise ValueError(
                    "sandbox=True mas BINANCE_API_SECRET_TEST não está definida."
                )
        else:
            if not self.binance_api_key_prod.get_secret_value():
                raise ValueError("sandbox=False mas BINANCE_API_KEY não está definida.")
            if not self.binance_api_secret_prod.get_secret_value():
                raise ValueError(
                    "sandbox=False mas BINANCE_API_SECRET não está definida."
                )
        return self
