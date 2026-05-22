"""
timing.py — конвертація «людського» часу в time.monotonic().
Використовує виключно інтерфейси src.utils.time.
"""
from __future__ import annotations
from src.utils.time import now_ts, monotonic, parse_to_ts, next_timestamp_for_time

# Типи, які приймає будь-яке поле run_at / at
RunAt = str | int | float


def to_monotonic(run_at: RunAt) -> float:
    """
    Перетворює run_at у time.monotonic()-timestamp.

    Логіка:
    wall_target (UTC unix) → delta від поточного wall-часу → додаємо до monotonic.
    Якщо moment вже в минулому — повертає поточний monotonic.
    """
    wall_target = _parse_to_wall_ts(run_at)
    delta = wall_target - now_ts()
    return monotonic() + max(delta, 0.0)


def _parse_to_wall_ts(run_at: RunAt) -> float:
    """Приводить вхідні дані до Unix timestamp (float)."""
    if isinstance(run_at, (int, float)):
        return float(run_at)

    s = run_at.strip()

    # Якщо це формат часу "HH:MM" або "HH:MM:SS" (без дати)
    if len(s) <= 8 and ":" in s and "-" not in s and "T" not in s:
        # Нормалізуємо "1:30" -> "01:30"
        if s.find(":") == 1:
            s = "0" + s
        # Використовуємо логіку "сьогодні/завтра" з utils.time
        return next_timestamp_for_time(s[:5])

    # Для всіх інших випадків (ISO, YYYY-MM-DD) звертаємось до парсера в utils.time
    return parse_to_ts(s)