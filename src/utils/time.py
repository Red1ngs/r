from __future__ import annotations
import datetime
import time as _time_module
from typing import Optional

_tz: Optional[datetime.timezone] = None  # None = локальний час машини

def set_timezone(tz: str | datetime.timezone | None) -> None:
    global _tz
    if tz is None:
        _tz = None
        return
    if isinstance(tz, datetime.timezone):
        _tz = tz
        return

    s = tz.strip()
    if s.upper() == "UTC":
        _tz = datetime.timezone.utc
        return
    if s.upper().startswith("UTC"):
        offset_str = s[3:]
        if offset_str:
            _tz = _parse_offset(offset_str)
            return
        _tz = datetime.timezone.utc
        return

    # 1. Пробуємо ZoneInfo (вимагає pip install tzdata на Windows)
    try:
        from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
        try:
            _tz = ZoneInfo(s)  # type: ignore
            return
        except ZoneInfoNotFoundError:
            pass # Йдемо далі до pytz
    except ImportError:
        pass

    # 2. Пробуємо pytz (як запасний варіант)
    try:
        import pytz
        _tz = pytz.timezone(s)  # type: ignore
        return
    except (ImportError, Exception):
        pass

    raise ValueError(f"Не вдалося розпізнати timezone {tz!r}. "
                     f"Порада: встановіть 'tzdata' (pip install tzdata)")
    
def _parse_offset(s: str) -> datetime.timezone:
    s = s.strip()
    sign = -1 if s.startswith("-") else 1
    if s.startswith(("+", "-")): s = s[1:]
    if ":" in s:
        h, m = map(int, s.split(":", 1))
    else:
        h, m = int(s), 0
    return datetime.timezone(datetime.timedelta(hours=sign * h, minutes=sign * m))

def now() -> datetime.datetime:
    """Внутрішня функція для отримання datetime об'єкта."""
    return datetime.datetime.now(_tz) if _tz else datetime.datetime.now().astimezone()

def today() -> str:
    """Поточна дата у форматі 'YYYY-MM-DD' в налаштованій часовій зоні."""
    return now().strftime("%Y-%m-%d")

def now_ts() -> float:
    return _time_module.time()

def monotonic() -> float:
    return _time_module.monotonic()

def next_timestamp_for_time(hh_mm: str) -> float:
    h, m = map(int, hh_mm.split(":"))
    n = now()
    target = n.replace(hour=h, minute=m, second=0, microsecond=0)
    if target <= n:
        target += datetime.timedelta(days=1)
    return target.timestamp()

def next_day_timestamp_for_time(hh_mm: str) -> float:
    """Повертає Unix timestamp завтрашнього дня о заданому часі HH:MM.
    На відміну від next_timestamp_for_time — завжди завтра, навіть якщо час ще не настав сьогодні.
    Використовується щоб не повторити тригер в той самий день після restore_state().
    """
    h, m = map(int, hh_mm.split(":"))
    n = now()
    tomorrow = (n + datetime.timedelta(days=1)).replace(
        hour=h, minute=m, second=0, microsecond=0
    )
    return tomorrow.timestamp()

def parse_to_ts(s: str) -> float:
    """
    Єдине місце поза цим файлом, де розбираються складні рядки дати.
    Перетворює ISO або 'YYYY-MM-DD HH:MM' у Unix Timestamp.
    """
    s_normalized = s.strip().replace(" ", "T")
    dt = datetime.datetime.fromisoformat(s_normalized)
    
    if dt.tzinfo is None:
        # Якщо в рядку немає зони, примусово ставимо налаштовану зону проекту
        project_tz = now().tzinfo
        dt = dt.replace(tzinfo=project_tz)
    
    return dt.timestamp()

def format_ts(ts: float, fmt: str = "%Y-%m-%d %H:%M") -> str:
    """Форматує Unix timestamp у рядок з урахуванням налаштованої timezone."""
    return datetime.datetime.fromtimestamp(ts, tz=_tz or datetime.datetime.now().astimezone().tzinfo).strftime(fmt)

def sleep(seconds: float) -> None:
    _time_module.sleep(seconds)
    
def seconds_until_midnight() -> float:
    """
    Секунд від зараз до 00:00:00 наступного дня в налаштованій часовій зоні.
    Замінює системний локальний час на налаштований у проекті.
    """
    n = now()  # бере налаштовану зону (або локальну, якщо set_timezone не викликався)
    tomorrow = (n + datetime.timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return max(60.0, (tomorrow - n).total_seconds())