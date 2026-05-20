"""
timing.py — конвертація «людського» часу в time.monotonic().

Підтримувані формати run_at:
    "14:30"              — сьогодні о 14:30 локального часу; якщо вже минуло → завтра
    "14:30:00"           — те саме з секундами
    "2025-06-01 14:30"   — конкретна локальна дата+час
    "2025-06-01T14:30:00"— ISO 8601 (враховується tzinfo, якщо є)
    1_735_689_600        — unix timestamp (int або float) UTC

Значення без tzinfo тепер трактуються як локальний час машини.
"""
from __future__ import annotations

import datetime
import time

# Типи, які приймає будь-яке поле run_at / at
RunAt = str | int | float


def to_monotonic(run_at: RunAt) -> float:
    """
    Перетворює run_at у time.monotonic()-timestamp.

    wall_target (UTC unix) → delta від поточного wall-часу → додаємо до monotonic.
    Якщо moment вже в минулому — повертає поточний monotonic (запустити негайно).
    """
    wall_target = _parse_wall(run_at)
    delta = wall_target - time.time()
    return time.monotonic() + max(delta, 0.0)


def next_wall_for_time(t: datetime.time) -> float:
    """
    Повертає unix timestamp наступного настання datetime.time (у поточному локальному часі).
    Якщо HH:MM:SS сьогодні вже минув — повертає завтрашнє.
    """
    now_local = datetime.datetime.now().astimezone()
    candidate = datetime.datetime.combine(now_local.date(), t, tzinfo=now_local.tzinfo)
    if candidate <= now_local:
        candidate += datetime.timedelta(days=1)
    return candidate.timestamp()


# ---------------------------------------------------------------------------
# Internal
# ---------------------------------------------------------------------------

def _parse_wall(run_at: RunAt) -> float:
    if isinstance(run_at, (int, float)):
        return float(run_at)

    s = run_at.strip()

    # "HH:MM" або "HH:MM:SS" — локальний час (будь-який tz-naive рядок вважається локальною датою/годиною).
    if len(s) <= 8 and s.count(":") in (1, 2) and "T" not in s and "-" not in s:
        if s.find(":") == 1:
            s = "0" + s 
            
        t = datetime.time.fromisoformat(s)
        return next_wall_for_time(t)

    # ISO / "YYYY-MM-DD HH:MM[:SS]"
    s_normalized = s.replace(" ", "T")
    dt = datetime.datetime.fromisoformat(s_normalized)
    if dt.tzinfo is None:
        # treat as local timezone when no explicit tz is given
        local_tz = datetime.datetime.now().astimezone().tzinfo
        dt = dt.replace(tzinfo=local_tz)
    return dt.timestamp()