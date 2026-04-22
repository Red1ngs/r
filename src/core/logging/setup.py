"""
core/logging/setup.py — центральна точка налаштування логування.

Викликається ОДИН РАЗ першим рядком у main.py:
    from src.core.logging.setup import setup_logging
    setup_logging()

Структура файлів після запуску:
    logs/
      system.log          ← scheduler, monitor, глобальні INFO+
      errors.log          ← ERROR+ з усіх джерел (агрегований)
      scheduler.log       ← окремий файл для src.core.scheduler
      accounts/
        acc_01.log        ← все що стосується акаунта (INFO+)
      tasks/
        acc_01_tasks.log  ← деталі тасків і HTTP (DEBUG+)

Ротація: 10 MB × 5 backup-файлів на кожному handler-і.
"""
from __future__ import annotations

import logging
import logging.handlers
from pathlib import Path

_FMT_FULL  = "%(asctime)s  %(levelname)-8s  [%(name)s]  %(message)s"
_FMT_SHORT = "%(asctime)s  %(levelname)-8s  %(message)s"
_DATE_FMT  = "%Y-%m-%d %H:%M:%S"
_MAX_BYTES    = 10 * 1024 * 1024   # 10 MB
_BACKUP_COUNT = 5


def _make_handler(
    path: Path,
    fmt: str = _FMT_FULL,
    level: int = logging.DEBUG,
) -> logging.handlers.RotatingFileHandler:
    path.parent.mkdir(parents=True, exist_ok=True)
    h = logging.handlers.RotatingFileHandler(
        path,
        maxBytes=_MAX_BYTES,
        backupCount=_BACKUP_COUNT,
        encoding="utf-8",
    )
    h.setLevel(level)
    h.setFormatter(logging.Formatter(fmt, datefmt=_DATE_FMT))
    return h


def setup_logging(
    log_dir: str | Path = "logs",
    console: bool = False,
    console_level: int = logging.WARNING,
) -> None:
    """
    Налаштовує всю систему логування.

    log_dir       : корінь папки з логами (default: "logs/")
    console       : чи виводити у консоль (default: False)
    console_level : рівень консолі якщо увімкнено
    """
    root = Path(log_dir)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.handlers.clear()

    # system.log — загальні повідомлення INFO+
    root_logger.addHandler(
        _make_handler(root / "system.log", fmt=_FMT_FULL, level=logging.INFO)
    )

    # errors.log — ERROR+ з усього (агрегований)
    root_logger.addHandler(
        _make_handler(root / "errors.log", fmt=_FMT_FULL, level=logging.ERROR)
    )

    # Консоль (опційно)
    if console:
        ch = logging.StreamHandler()
        ch.setLevel(console_level)
        ch.setFormatter(logging.Formatter(_FMT_SHORT, datefmt=_DATE_FMT))
        root_logger.addHandler(ch)

    logging.getLogger("src.core.logging").info(
        f"Logging initialized → {root.resolve()}"
    )