import math
from datetime import datetime, timedelta
from enum import Enum
from zoneinfo import ZoneInfo

from ..validator.schedule import MarketHoursConfig


class MarketState(str, Enum):
    """Status possíveis do mercado."""

    OPEN = "open"
    STANDBY = "standby"


class MarketHoursChecker:
    """Verifica se o mercado está dentro da janela operacional."""

    def __init__(self, config: MarketHoursConfig) -> None:
        self.tz = ZoneInfo(config.timezone)
        self.open_day = config.market_open_day
        self.open_hour = config.market_open_hour
        self.open_minute = config.market_open_minute
        self.close_day = config.market_close_day
        self.close_hour = config.market_close_hour
        self.close_minute = config.market_close_minute

    def get_status(self) -> MarketState:
        """Verifica se o mercado está aberto ou em standby.

        Compara a ocorrência mais recente de abertura e fechamento.
        Se a abertura mais recente é posterior ao fechamento mais recente,
        estamos dentro da janela aberta.
        """
        now = datetime.now(self.tz)
        prev_open = self._previous_occurrence(
            now, self.open_day, self.open_hour, self.open_minute
        )
        prev_close = self._previous_occurrence(
            now, self.close_day, self.close_hour, self.close_minute
        )
        return MarketState.OPEN if prev_open > prev_close else MarketState.STANDBY

    def is_market_open(self) -> bool:
        """Retorna True se o mercado está aberto."""
        return self.get_status() == MarketState.OPEN

    def seconds_until_next_open(self) -> int:
        """Calcula quantos segundos faltam até a próxima abertura.

        Retorna 0 quando o mercado já está aberto. Usa math.ceil para evitar
        truncar fração de segundo para 0 (que causaria busy-wait no standby).
        """
        if self.is_market_open():
            return 0

        now = datetime.now(self.tz)
        next_open = self._next_occurrence(
            now, self.open_day, self.open_hour, self.open_minute
        )
        delta = (next_open - now).total_seconds()
        return max(1, math.ceil(delta))

    @staticmethod
    def _previous_occurrence(
        now: datetime, day: int, hour: int, minute: int
    ) -> datetime:
        """Ocorrência mais recente (<= now) de um (weekday, hour, minute)."""
        target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        days_back = (now.weekday() - day) % 7
        candidate = target - timedelta(days=days_back)
        if candidate > now:
            candidate -= timedelta(days=7)
        return candidate

    @staticmethod
    def _next_occurrence(
        now: datetime, day: int, hour: int, minute: int
    ) -> datetime:
        """Próxima ocorrência (> now) de um (weekday, hour, minute)."""
        target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        days_fwd = (day - now.weekday()) % 7
        candidate = target + timedelta(days=days_fwd)
        if candidate <= now:
            candidate += timedelta(days=7)
        return candidate
