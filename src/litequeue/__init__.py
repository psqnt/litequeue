import logging
from colorlog import ColoredFormatter

logger = logging.getLogger("litequeue")
handler = logging.StreamHandler()
handler.setFormatter(
    ColoredFormatter(
        "%(asctime)s - %(log_color)s%(levelname)s%(reset)s - %(message)s",
        log_colors={
            "DEBUG": "cyan",
            "INFO": "green",
            "ERROR": "red",
        },
    )
)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

from .server import LiteQueue

__all__ = ["LiteQueue", "logger"]
