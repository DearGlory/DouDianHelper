from __future__ import annotations

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path


class _LevelPrefixFormatter(logging.Formatter):
    LEVEL_PREFIX = {
        logging.DEBUG: "DBG",
        logging.INFO: "INF",
        logging.WARNING: "WRN",
        logging.ERROR: "ERR",
        logging.CRITICAL: "CRT",
    }

    def format(self, record: logging.LogRecord) -> str:
        record.levelshort = self.LEVEL_PREFIX.get(record.levelno, record.levelname[:3])
        return super().format(record)


def _configure_console_encoding() -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")


def setup_logger(level: str = "INFO", log_file: str | None = None) -> logging.Logger:
    _configure_console_encoding()
    logger = logging.getLogger("doudian-helper")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.handlers.clear()
    logger.propagate = False

    console_fmt = _LevelPrefixFormatter(
        fmt="%(asctime)s | %(levelshort)s | %(message)s",
        datefmt="%H:%M:%S",
    )
    file_fmt = _LevelPrefixFormatter(
        fmt="%(asctime)s | %(levelshort)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(console_fmt)
    logger.addHandler(console)

    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        fh = RotatingFileHandler(log_path, maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8", delay=True)
        fh.setFormatter(file_fmt)
        logger.addHandler(fh)

    return logger
