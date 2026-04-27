import asyncio
import sys

from usdm_futures.shared.validator.schedule import MarketHoursConfig
from usdm_futures.shared.utils.market_hours import MarketHoursChecker
from usdm_futures.shared.logging.logger import get_logger
from usdm_futures.robot.controllers.state_chief import StateChief
from usdm_futures.robot.strategies import DoubleMeanStrategy
from usdm_futures.shared.indicators.t_analisys import Indicators

from usdm_futures.shared.validator.secrets import LoadKey
from usdm_futures.shared.error.exceptions import handle_exchange_error


async def main() -> None:
    log = get_logger("statechief")

    keys = LoadKey()  # type: ignore
    config = MarketHoursConfig()  # type: ignore
    checker = MarketHoursChecker(config)

    if len(sys.argv) < 2:
        print("Uso: python main.py <caminho_do_toml>")
        sys.exit(1)

    toml_path = sys.argv[1]

    indicators = Indicators(logger=get_logger("indicators"))
    strategy = DoubleMeanStrategy(
        indicators=indicators,
        logger=get_logger("strategy"),
        sma_length=168,
        ema_length=6,
        source_column="close",
    )

    chief = StateChief(
        hours_checker=checker,
        toml_path=toml_path,
        keys=keys,
        error_handler=handle_exchange_error,
        logger=log,
        strategy=strategy,
    )

    try:
        await chief.run()
    finally:
        if chief._conn is not None:
            await chief._conn.close()


if __name__ == "__main__":
    asyncio.run(main())
