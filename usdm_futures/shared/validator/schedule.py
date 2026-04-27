from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path


class MarketHoursConfig(BaseSettings):
    """Configuração de horários de operação do mercado.

    Carrega os horários de abertura e fechamento do arquivo schedule.env.
    """

    model_config = SettingsConfigDict(
        env_file=str(Path(__file__).resolve().parent.parent / "env" / "schedule.env"),
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    market_open_day: int
    market_open_hour: int
    market_open_minute: int
    market_close_day: int
    market_close_hour: int
    market_close_minute: int
    timezone: str = "America/Sao_Paulo"

    @field_validator("market_open_day", "market_close_day")
    @classmethod
    def validate_weekday(cls, v: int, info) -> int:
        if not (0 <= v <= 6):
            raise ValueError(f"{info.field_name} deve ser entre 0 e 6, recebido: {v}")
        return v

    @field_validator("market_open_hour", "market_close_hour")
    @classmethod
    def validate_hour(cls, v: int, info) -> int:
        if not (0 <= v <= 23):
            raise ValueError(f"{info.field_name} deve ser entre 0 e 23, recebido: {v}")
        return v

    @field_validator("market_open_minute", "market_close_minute")
    @classmethod
    def validate_minute(cls, v: int, info) -> int:
        if not (0 <= v <= 59):
            raise ValueError(f"{info.field_name} deve ser entre 0 e 59, recebido: {v}")
        return v
