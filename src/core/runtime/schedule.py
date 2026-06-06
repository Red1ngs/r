"""
src/core/runtime/schedule.py

Описує контракти тригерів, базову логіку, парсер RunAt та конфігуратор ScheduleDef.
"""
from __future__ import annotations

from src.utils.time import now_ts, next_timestamp_for_time

RunAt = str | int | float


# ── RunAt Parser ─────────────────────────────────────────────────────────────

def parse_run_at(run_at: RunAt) -> float:
    """
    Перетворює різні зручні формати часу (RunAt) в absolute UTC timestamp (float).
    Підтримує:
      - float/int: Unix timestamp (наприклад, 1716321600)
      - str з відносним зсувом від поточного моменту: "+30s", "+15m", "+2h", "+1d"
      - str точного часу: "HH:MM" (найближчий запуск у майбутньому)
      - str абсолютного timestamp: "1716321600"
    """
    if isinstance(run_at, (int, float)):
        return float(run_at)

    val = str(run_at).strip()
    
    # 1. Відносні зсуви на кшталт "+30s", "+15m", "+2h", "+1d"
    if val.startswith("+"):
        try:
            unit = val[-1].lower()
            amount = float(val[1:-1])
            now = now_ts()
            if unit == "s": return now + amount
            if unit == "m": return now + amount * 60
            if unit == "h": return now + amount * 3600
            if unit == "d": return now + amount * 86400
        except Exception:
            pass

    # 2. Точний час у форматі "HH:MM"
    if ":" in val and len(val) <= 5:
        try:
            return next_timestamp_for_time(val)
        except Exception:
            pass

    # 3. Спроба розпарсити як звичайний float
    try:
        return float(val)
    except ValueError:
        raise ValueError(f"Невідомий формат запланованого часу RunAt: {run_at!r}")
