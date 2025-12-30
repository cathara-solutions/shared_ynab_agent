import logging
import sys


def configure_logging(level: int = logging.INFO) -> None:
    """
    Configure root logging for the application.

    Logs to stdout with timestamps; call once at application start.
    """
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )
