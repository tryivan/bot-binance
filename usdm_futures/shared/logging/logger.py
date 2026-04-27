# logger.py
import logging
import logging.handlers
import os
from logging.config import dictConfig
from pathlib import Path
from typing import Any
import sys

from rich.console import Console
from rich.live import Live
from rich.logging import RichHandler
from rich.text import Text


# =============================================================================
# CONFIGURAÇÃO DE DIRETÓRIOS E ARQUIVOS DE LOG
# =============================================================================
LOG_DIR = Path(__file__).resolve().parents[2] / "log"

try:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
except PermissionError as e:
    raise RuntimeError(f"Sem permissão para criar diretório de logs: {LOG_DIR}") from e
except OSError as e:
    raise RuntimeError(f"Falha ao criar diretório de logs: {e}") from e

IS_CONTAINER = (
    not sys.stdout.isatty() or os.environ.get("CONTAINER", "").lower() == "true"
)

CONSOLE_WIDTH = int(os.environ.get("COLUMNS", 200 if IS_CONTAINER else 120))

LOG_FILE = LOG_DIR / "bot.log"


# =============================================================================
# CONFIGURAÇÃO DE MÓDULOS
# =============================================================================
MODULES = {
    "statechief": {"level": "INFO", "color": "bright_yellow"},
    "exchange": {"level": "INFO", "color": "bright_green"},
    "exchange_fetch_data": {"level": "INFO", "color": "bright_green"},
    "datatransform": {"level": "INFO", "color": "dodger_blue1"},
    "datavalidator": {"level": "INFO", "color": "dodger_blue1"},
    "csvstorage": {"level": "INFO", "color": "dodger_blue1"},
    "manage_orders": {"level": "INFO", "color": "orchid1"},
    "datapipeline": {"level": "INFO", "color": "dodger_blue1"},
    "strategy": {"level": "INFO", "color": "bright_cyan"},
}


# =============================================================================
# CORES POR NÍVEL DE LOG (CUSTOM)
# =============================================================================
LEVEL_COLORS = {
    "DEBUG": "dim",
    "INFO": "cyan",
    "WARNING": "yellow",
    "ERROR": "red",
    "CRITICAL": "bold red",
}


class CustomFormatter(logging.Formatter):
    """Formatter customizado que aplica cores por nível de log."""

    def format(self, record: logging.LogRecord) -> str:
        level_color = LEVEL_COLORS.get(record.levelname, "white")
        record.levelname = f"[{level_color}]{record.levelname}[/{level_color}]"
        return super().format(record)


# =============================================================================
# CONSOLE CUSTOMIZADO
# =============================================================================
console = Console(width=CONSOLE_WIDTH, force_terminal=True, no_color=False)


# =============================================================================
# CONFIGURAÇÃO BASE DO LOGGING
# =============================================================================
LOGGING_CONFIG: dict[str, Any] = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {"file": {"format": "%(asctime)s|%(name)s|%(levelname)s|%(message)s"}},
    "handlers": {
        "file_global": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "file",
            "filename": str(LOG_FILE),
            "maxBytes": 5 * 1024 * 1024,
            "backupCount": 5,
            "encoding": "utf-8",
        }
    },
    "loggers": {},
}


# =============================================================================
# CONFIGURAÇÃO AUTOMÁTICA DOS MÓDULOS
# =============================================================================
for module, props in MODULES.items():
    level = props["level"]

    LOGGING_CONFIG["loggers"][f"{module}"] = {
        "handlers": ["file_global"],
        "level": level,
        "propagate": False,
    }


# =============================================================================
# APLICAR CONFIGURAÇÃO
# =============================================================================
dictConfig(LOGGING_CONFIG)


# =============================================================================
# ADICIONAR RICH HANDLERS
# =============================================================================
for module, props in MODULES.items():
    color = props["color"]

    rich_handler = RichHandler(
        console=console,
        rich_tracebacks=False,
        tracebacks_show_locals=False,
        show_time=True,
        show_level=False,  # desativado para controle manual
        omit_repeated_times=False,
        enable_link_path=False,
        show_path=False,
        markup=True,
    )

    formatter = CustomFormatter(
        f"[{color}][{module.upper()}][/{color}] %(levelname)s %(message)s"
    )

    rich_handler.setFormatter(formatter)

    logger = logging.getLogger(f"{module}")
    logger.addHandler(rich_handler)


# =============================================================================
# FUNÇÃO AUXILIAR
# =============================================================================
def get_logger(name: str) -> logging.Logger:
    """Retorna um logger configurado pelo nome do módulo.

    Args:
        name: Nome do módulo (ex: 'exchange', 'coinmarketcap').

    Returns:
        Logger configurado com Rich handler e arquivo de log.
    """
    return logging.getLogger(name)


# =============================================================================
# LINHA DINÂMICA DE STATUS (heartbeat visual)
# =============================================================================
# Uma única Live por processo (cada par roda em processo próprio). Em ambientes
# sem TTY (IS_CONTAINER=True, ex: systemd/docker) as funções viram no-op para
# não corromper a saída.

_status_line_live: Live | None = None


def update_status_line(text: str) -> None:
    """Atualiza (ou inicia) a linha dinâmica na parte inferior do console.

    Em ambiente sem TTY, é no-op. Usa o mesmo ``console`` global que o
    RichHandler — logs emitidos via logger normal coexistem: Rich renderiza
    a nova linha de log acima da live region e redesenha a linha dinâmica.
    """
    global _status_line_live

    if IS_CONTAINER:
        return

    renderable = Text.from_markup(text)

    if _status_line_live is None:
        _status_line_live = Live(
            renderable, console=console, refresh_per_second=4, transient=False
        )
        _status_line_live.start()
    else:
        _status_line_live.update(renderable)


def clear_status_line() -> None:
    """Para a Live e limpa a linha dinâmica.

    Chamar ao sair de MONITORING (posição encerrada, ERROR, exceção) para não
    deixar resíduo visual antes do próximo log. No-op se a Live nunca iniciou.
    """
    global _status_line_live

    if _status_line_live is not None:
        _status_line_live.stop()
        _status_line_live = None


def get_pair_logger(symbol: str) -> logging.Logger:
    """Cria um logger dedicado para um par de trading.

    Args:
        symbol: Símbolo no formato Binance (ex: 'BTC/USDT:USDT').

    Returns:
        Logger configurado com arquivo dedicado e Rich handler.
    """
    clean_name = symbol.replace("/", "").replace(":", "").lower()
    logger_name = f"{clean_name}"

    logger = logging.getLogger(logger_name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    logger.propagate = False

    # File handler dedicado
    pair_log_file = LOG_DIR / f"{clean_name}.log"
    file_handler = logging.handlers.RotatingFileHandler(
        filename=str(pair_log_file),
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s|%(name)s|%(levelname)s|%(message)s")
    )
    logger.addHandler(file_handler)

    # Rich handler para console
    rich_handler = RichHandler(
        console=console,
        rich_tracebacks=False,
        tracebacks_show_locals=False,
        show_time=True,
        show_level=False,
        omit_repeated_times=False,
        enable_link_path=False,
        show_path=False,
        markup=True,
    )

    formatter = CustomFormatter(
        f"[cyan][{clean_name.upper()}][/cyan] %(levelname)s %(message)s"
    )
    rich_handler.setFormatter(formatter)
    logger.addHandler(rich_handler)

    return logger
