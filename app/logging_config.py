from __future__ import annotations

import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path

APP_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LOG_PATH = APP_ROOT / "logs" / "app.log"


def setup_application_logging() -> logging.Logger:
    log_file = Path(os.getenv("COLSANITAS_LOG_FILE", str(DEFAULT_LOG_PATH))).resolve()
    log_level_name = os.getenv("COLSANITAS_LOG_LEVEL", "INFO").strip().upper()
    log_level = getattr(logging, log_level_name, logging.INFO)

    logger = logging.getLogger("sanitas")
    if logger.handlers:
        return logger

    log_file.parent.mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter("[%(asctime)s] %(message)s", datefmt="%H:%M:%S")

    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)

    file_handler = RotatingFileHandler(
        filename=log_file,
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)

    logger.setLevel(log_level)
    logger.propagate = False
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    logger.info(f"Logging inicializado en {log_file}")
    return logger


def get_application_logger(name: str | None = None) -> logging.Logger:
    base_logger = setup_application_logging()
    if not name:
        return base_logger
    return base_logger.getChild(name)
