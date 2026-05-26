"""
src/core/runtime/schedule.py

Описує контракти тригерів, базову логіку, парсер RunAt та конфігуратор ScheduleDef.
"""
from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING, Callable, Iterable, Optional, Protocol, runtime_checkable,
)

from src.utils.time import now_ts, next_timestamp_for_time, next_day_timestamp_for_time

if TYPE_CHECKING:
    from src.core.account import Account
    from src.core.inventory.model import Inventories
    from src.core.tasks.base import AnyTask

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


# ── Trigger Contracts ──────────────────────────────────────────────────────────

@runtime_checkable
class TriggerProtocol(Protocol):
    """
    Контракт тригера. Scheduler працює виключно через цей інтерфейс.
    """
    name:       str
    account_id: str

    def is_due(self) -> bool: ...
    def is_expired(self, inv: "Inventories") -> bool: ...
    def seconds_until(self) -> float: ...
    def dispatch(self) -> None: ...
    def advance(self, bot: "Account") -> None: ...
    def producer(self, bot: "Account") -> Iterable["AnyTask"]: ...
    
    # Додано метод для примусового переносу запуску
    def reschedule(self, run_at: RunAt) -> None: ...
    

@dataclass
class BaseTrigger:
    """
    Абстрактна реалізація спільної механіки тригера.
    """
    name:       str
    account_id: str
    _next_fire:    float = field(default=0.0, init=False)
    _in_flight:    bool  = field(default=False, init=False)
    _dispatch_ts:  float = field(default=0.0, init=False)

    # Максимальний час перебування в стані in_flight.
    # Якщо pipeline впав і on_cycle_done() так і не викликався —
    # watchdog скине _in_flight щоб тригер не завис назавжди.
    FLIGHT_TIMEOUT: float = field(default=300.0, init=False)

    # ── TriggerProtocol ───────────────────────────────────────────────────────

    @property
    def next_fire(self) -> float:
        """Unix timestamp наступного запуску (read-only для зовнішнього коду)."""
        return self._next_fire

    @property
    def in_flight(self) -> bool:
        """True якщо тригер наразі виконується (задачі ще не завершені)."""
        return self._in_flight

    def is_due(self) -> bool:
        if self._in_flight:
            # Watchdog: якщо тригер завис у in_flight довше ніж FLIGHT_TIMEOUT —
            # скидаємо прапор щоб уникнути вічного заморожування.
            if now_ts() - self._dispatch_ts > self.FLIGHT_TIMEOUT:
                self._in_flight = False
        return not self._in_flight and now_ts() >= self._next_fire

    def is_expired(self, inv: "Inventories") -> bool:
        return False

    def seconds_until(self) -> float:
        if self._in_flight:
            return float("inf")
        return max(0.0, self._next_fire - now_ts())

    def dispatch(self) -> None:
        self._in_flight = True
        self._dispatch_ts = now_ts()
        self._next_fire = float("inf")

    def advance(self, bot: "Account") -> None:
        self._next_fire =  now_ts() + max(0.0, self.next_delay(bot))
        self._in_flight = False

    def reschedule(self, run_at: RunAt) -> None:
        """Дозволяє у будь-який момент примусово змінити запланований час запуску."""
        self._next_fire = parse_run_at(run_at)
        self._in_flight = False  # Розблоковуємо, якщо тригер був заморожений

    @abstractmethod
    def producer(self, bot: "Account") -> Iterable["AnyTask"]: ...

    @abstractmethod
    def next_delay(self, bot: "Account") -> float: ...

    @property
    def is_one_shot(self) -> bool:
        return False

    def __repr__(self) -> str:
        return (
            f"<{type(self).__name__} "
            f"name={self.name!r} "
            f"account={self.account_id!r} "
            f"in_flight={self._in_flight} "
            f"due_in={self.seconds_until():.1f}s>"
        )


# ── Concrete Trigger Implementation ──────────────────────────────────────────

@dataclass
class ScheduleTrigger(BaseTrigger):
    interval:  int
    _producer: Callable[["Account"], Iterable["AnyTask"]]
    at:        Optional[str] = None

    def __post_init__(self) -> None:
        if self.at:
            self._next_fire = parse_run_at(self.at)
        else:
            self._next_fire = now_ts()

    def next_delay(self, bot: "Account") -> float:
        return float(self.interval)

    def producer(self, bot: "Account") -> Iterable["AnyTask"]:
        return self._producer(bot)

    def advance_to_next_day_at(self, at: str) -> None:
        """
        Ставить _next_fire на завтра о заданому часі (HH:MM).
        Завжди у майбутньому — навіть якщо час ще не настав сьогодні.
        Використовується restore_state() коли бонус вже зібрано сьогодні.
        """
        self._next_fire = next_day_timestamp_for_time(at)
        self._in_flight = False


# ── Schedule Configurator (Builder) ───────────────────────────────────────────

@dataclass(frozen=True)
class ScheduleDef:
    interval:  int
    producer:  Callable[["Account"], Iterable["AnyTask"]]
    at:        Optional[str] = None

    def to_trigger(self, account_id: str) -> "ScheduleTrigger":  # ← був TriggerProtocol
        name = f"scheduled_{self.at}" if self.at else f"interval_{self.interval}"
        return ScheduleTrigger(
            name=name,
            account_id=account_id,
            interval=self.interval,
            _producer=self.producer,
            at=self.at,
        )