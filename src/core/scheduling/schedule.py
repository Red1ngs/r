"""
schedule.py — протокол тригерів і базові реалізації.

Ієрархія
────────
    TriggerProtocol — контракт. Scheduler знає тільки його.
    BaseTrigger     — абстрактна реалізація спільної механіки:
                      dispatch / advance / is_due / seconds_until.
                      Не знає про інтервал — тільки про next_delay().
    IntervalTrigger — конкретна реалізація в core:
                      фіксований інтервал, next_delay() = self.interval.
    ScheduleDef     — декларативний builder для IntervalTrigger.

Кастомні тригери в прикладному шарі:
    @dataclass
    class MyTrigger(BaseTrigger):
        def next_delay(self, bot: AccountPull) -> float:
            return ...   # будь-яка логіка

Lifecycle одного циклу (гарантія "один цикл за раз")
──────────────────────────────────────────────────────
    Scheduler: is_due() → True
    Scheduler: dispatch()              ← блокує наступний is_due()
    Scheduler: передає задачі воркеру

    Після завершення action:
    Pipeline/task: advance(bot)        ← рахує наступний fire, знімає блок

    Якщо producer повернув []:
    Scheduler: advance(bot)            ← одразу, без очікування
"""
from __future__ import annotations

import time
from abc import abstractmethod
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING, Callable, Iterable, Optional, Protocol, runtime_checkable,
)

if TYPE_CHECKING:
    from src.core.account import AccountPull
    from src.core.inventory.model import Inventories
    from src.core.tasks.base import AnyTask

RunAt = str | int | float


# ─────────────────────────────────────────────────────────────────────────────
# TriggerProtocol — єдине що знає Scheduler
# ─────────────────────────────────────────────────────────────────────────────

@runtime_checkable
class TriggerProtocol(Protocol):
    """
    Контракт тригера. Scheduler працює виключно через цей інтерфейс.

    name       : ідентифікатор для логів
    account_id : якому воркеру передавати задачі
    """
    name:       str
    account_id: str

    def is_due(self) -> bool: ...
    def is_expired(self, inv: "Inventories") -> bool: ...
    def seconds_until(self) -> float: ...
    def dispatch(self) -> None: ...
    def advance(self, bot: "AccountPull") -> None: ...
    def producer(self, bot: "AccountPull") -> Iterable["AnyTask"]: ...


# ─────────────────────────────────────────────────────────────────────────────
# BaseTrigger — спільна механіка без прив'язки до інтервалу
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class BaseTrigger:
    """
    Абстрактна реалізація спільної механіки тригера.

    Реалізує dispatch / advance / is_due / seconds_until / is_expired.
    Не знає як рахувати інтервал — делегує до next_delay().

    Підклас зобов'язаний реалізувати:
        next_delay(bot) → float   — скільки чекати після advance()
        producer(bot)   → Iterable[AnyTask]

    Підклас може перевизначити:
        is_one_shot     → bool    — True = видалити після першого advance()
        is_expired(inv) → bool    — умова дострокового видалення

    Підклас зазвичай додає:
        _producer: Callable  — щоб producer() міг її викликати
        until: Callable      — щоб is_expired() мала що перевіряти
    """
    name:       str
    account_id: str
    _next_fire: float = field(default=0.0, init=False)
    _in_flight: bool  = field(default=False, init=False)

    # ── TriggerProtocol ───────────────────────────────────────────────────────

    def is_due(self) -> bool:
        return not self._in_flight and time.time() >= self._next_fire

    def is_expired(self, inv: "Inventories") -> bool:
        return False

    def seconds_until(self) -> float:
        if self._in_flight:
            return float("inf")
        return max(0.0, self._next_fire - time.time())

    def dispatch(self) -> None:
        self._in_flight = True
        self._next_fire = float("inf")

    def advance(self, bot: "AccountPull") -> None:
        """
        Рахує наступний _next_fire через next_delay() і знімає блок.
        Не перевизначай — перевизнач next_delay().
        """
        self._next_fire = time.time() + max(0.0, self.next_delay(bot))
        self._in_flight = False

    @abstractmethod
    def producer(self, bot: "AccountPull") -> Iterable["AnyTask"]: ...

    # ── Розширювані точки ─────────────────────────────────────────────────────

    @abstractmethod
    def next_delay(self, bot: "AccountPull") -> float:
        """Скільки секунд чекати після advance(). Реалізується в підкласі."""
        ...

    @property
    def is_one_shot(self) -> bool:
        """True = тригер видаляється після першого спрацювання."""
        return False

    def __repr__(self) -> str:
        return (
            f"<{type(self).__name__} "
            f"name={self.name!r} "
            f"account={self.account_id!r} "
            f"in_flight={self._in_flight} "
            f"due_in={self.seconds_until():.1f}s>"
        )


# ─────────────────────────────────────────────────────────────────────────────
# IntervalTrigger — фіксований інтервал
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class IntervalTrigger(BaseTrigger):
    """
    Тригер з фіксованим інтервалом між спрацюваннями.

    interval  : секунди між спрацюваннями; 0 = one-shot
    _producer : fn(bot) → Iterable[AnyTask]
    until     : якщо until(inv) → True — тригер видаляється
    """
    interval:  float
    _producer: Callable[["AccountPull"], Iterable["AnyTask"]]
    until:     Optional[Callable[["Inventories"], bool]] = None

    def next_delay(self, bot: "AccountPull") -> float:
        return self.interval

    def producer(self, bot: "AccountPull") -> Iterable["AnyTask"]:
        return self._producer(bot)

    def is_expired(self, inv: "Inventories") -> bool:
        return self.until is not None and self.until(inv)

    @property
    def is_one_shot(self) -> bool:
        return self.interval <= 0


# ─────────────────────────────────────────────────────────────────────────────
# ScheduleDef — декларативний builder для IntervalTrigger
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ScheduleDef:
    """
    Декларативний опис повторюваного завдання → IntervalTrigger.

    Приклади:
        ScheduleDef(86400, daily_bonus, at="14:30")
        ScheduleDef(3600,  sync_trades)
        ScheduleDef(3600,  daily_bonus, until=has("is_banned"))
    """
    interval: float
    producer: Callable[["AccountPull"], Iterable["AnyTask"]]
    until:    Optional[Callable[["Inventories"], bool]] = None
    at:       Optional[RunAt]                           = None

    def to_trigger(self, account_id: str) -> IntervalTrigger:
        t = IntervalTrigger(
            name       = getattr(self.producer, "__name__", "schedule"),
            account_id = account_id,
            interval   = self.interval,
            _producer  = self.producer,
            until      = self.until,
        )
        if self.at is not None:
            from src.core.utils.timing import _parse_wall
            t._next_fire = _parse_wall(self.at)
        return t

    def to_entry(self, account_id: str) -> IntervalTrigger:
        """Alias для зворотної сумісності."""
        return self.to_trigger(account_id)


# Aliases — зворотна сумісність
Trigger        = IntervalTrigger
ScheduledEntry = IntervalTrigger