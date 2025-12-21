import logging
from typing import Optional


def configure_logging(level: int = logging.INFO, filename: str = "log.txt") -> None:
    """
    Configure root logging for the application.

    Logs to `filename` with timestamps; call once at application start.
    """
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.FileHandler(filename)],
        force=True,
    )
