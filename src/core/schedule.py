"""
schedule.py — система тригерів.

Концепція
─────────
Розподіл відповідальностей:
    Task / Pipeline  — ЩО і ЯК виконується.
    Trigger          — КОЛИ виконується (один запис у TriggerTable).
    Scheduler        — опитує TriggerTable, передає задачі воркерам.

Гарантія "один цикл за раз"
────────────────────────────
Тригер має прапор _in_flight. Поки він True — is_due() повертає False,
тобто Scheduler не dispatch-ить новий цикл поки попередній не завершився.

Lifecycle:
    Scheduler бачить is_due() → True
    → викликає trigger.dispatch()          [_in_flight=True, _next_fire=inf]
    → передає задачі воркеру

    Після завершення action:
    → pipeline викликає trigger.advance()  [рахує _next_fire, _in_flight=False]

    Якщо producer повернув порожній список (нічого робити):
    → Scheduler викликає trigger.advance() одразу

advance() викликається після action — тому dynamic_next(bot) бачить актуальний
стан SlotScheduler (mark_done вже виконано) і повертає коректний delay.

Приклади
────────
    # Статичний тригер — запускати кожні 3600 секунд
    ScheduleDef(3600, sync_trades).to_trigger("acc_01")

    # Тригер з динамічним інтервалом (SlotScheduler)
    Trigger(
        name         = "reader_slot",
        account_id   = "acc_01",
        interval     = 0,
        producer     = fetch_and_read,
        dynamic_next = lambda bot: bot.inventory.reader.scheduler.delay_until_next(),
    )
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable, Iterable, Optional

if TYPE_CHECKING:
    from src.core.account_pull import AccountPull
    from src.core.inventory.model import Inventories
    from src.core.task import AnyTask

RunAt = str | int | float


# ─────────────────────────────────────────────────────────────────────────────
# Trigger — один запис у TriggerTable
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Trigger:
    """
    Описує один повторюваний або одноразовий виклик задач.

    name         : ідентифікатор для логів і видалення
    account_id   : якому воркеру передати задачі
    interval     : секунди між спрацюваннями (0 = one-shot)
    producer     : fn(bot) → Iterable[AnyTask]
    until        : якщо until(inv) → True, тригер видаляється
    dynamic_next : якщо задано — наступний час рахується як
                   time.time() + dynamic_next(bot) замість time.time() + interval.
                   Увага: викликається в advance(), тобто ПІСЛЯ завершення action —
                   коли SlotScheduler вже виконав mark_done().
    _next_fire   : unix timestamp наступного спрацювання (0 = негайно)
    _in_flight   : True поки попередній цикл не завершився.
                   Блокує is_due() — Scheduler не dispatch-ить новий цикл.
    """
    name:         str
    account_id:   str
    interval:     float
    producer:     Callable[["AccountPull"], Iterable["AnyTask"]]
    until:        Optional[Callable[["Inventories"], bool]]          = None
    dynamic_next: Optional[Callable[["AccountPull"], float]]         = None
    _next_fire:   float                                              = field(default=0.0, init=False)
    _in_flight:   bool                                               = field(default=False, init=False)

    def is_due(self) -> bool:
        """Повертає True тільки якщо час настав І немає in-flight циклу."""
        if self._in_flight:
            return False
        return time.time() >= self._next_fire

    def is_expired(self, inv: "Inventories") -> bool:
        return self.until is not None and self.until(inv)

    def dispatch(self) -> None:
        """
        Позначає цикл як in-flight.
        Scheduler викликає це одразу перед передачею задач воркеру.
        Блокує is_due() до виклику advance().
        """
        self._in_flight = True
        self._next_fire = float("inf")

    def advance(self, bot: "AccountPull") -> None:
        """
        Рахує наступний _next_fire і знімає in-flight блок.

        Викликається з двох місць:
          1. Scheduler   — якщо producer повернув [] (нічого не запустили)
          2. Pipeline    — після завершення action, через on_cycle_done callback
                           (до цього моменту SlotScheduler вже виконав mark_done)

        Саме тому dynamic_next отримує актуальне значення delay_until_next().
        """
        if self.dynamic_next is not None:
            delay = self.dynamic_next(bot)
        else:
            delay = self.interval
        self._next_fire = time.time() + max(0.0, delay)
        self._in_flight = False

    def seconds_until(self) -> float:
        """Скільки секунд до наступного спрацювання."""
        if self._in_flight:
            return float("inf")
        return max(0.0, self._next_fire - time.time())


# ─────────────────────────────────────────────────────────────────────────────
# ScheduleDef — декларативний опис (builder для Trigger)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ScheduleDef:
    """
    Декларативний опис повторюваного завдання.
    Зворотно сумісний зі старим кодом — to_entry() збережено як alias.

    Приклади:
        ScheduleDef(86400, daily_bonus, at="14:30")
        ScheduleDef(3600,  sync_trades)
        ScheduleDef(3600,  daily_bonus, until=has("is_banned"))
    """
    interval: float
    producer: Callable[["AccountPull"], Iterable["AnyTask"]]
    until:    Optional[Callable[["Inventories"], bool]] = None
    at:       Optional[RunAt]                           = None

    def to_trigger(self, account_id: str) -> Trigger:
        t = Trigger(
            name       = getattr(self.producer, "__name__", "schedule"),
            account_id = account_id,
            interval   = self.interval,
            producer   = self.producer,
            until      = self.until,
        )
        if self.at is not None:
            from src.core.timing import _parse_wall
            t._next_fire = _parse_wall(self.at)
        return t

    def to_entry(self, account_id: str) -> "Trigger":
        return self.to_trigger(account_id)


ScheduledEntry = Trigger