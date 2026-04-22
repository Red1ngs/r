"""
profession.py — декларативний опис поведінки акаунта.

Розподіл відповідальностей
──────────────────────────
    startup   — що запустити ОДИН РАЗ при старті воркера
    pipelines — що запустити ОДИН РАЗ (pipeline-фабрики)
    schedule  — [ScheduleDef] → конвертуються у Trigger при реєстрації
    triggers  — [Trigger] напряму (нова концепція)
    guard     — умова «бот живий»

Ключовий принцип:
    Task і Pipeline несуть ЩО і ЯК.
    Trigger несе КОЛИ (інтервал, dynamic_next, until).
    Profession — декларативний контракт між профілем і фреймворком.

Приклад з triggers:
    from src.core.schedule import Trigger, ScheduleDef

    reader_profession = Profession(
        name    = "reader",
        startup = [build_manga_reader],
        triggers = [
            Trigger(
                name         = "slot_clock",
                account_id   = "",          # заповнюється Scheduler-ом
                interval     = 0,
                producer     = produce_read_task,
                dynamic_next = lambda bot: bot.inventory.reader.scheduler.delay_until_next(),
            ),
        ],
        guard = not_(has("is_banned")),
    )

    # Або через ScheduleDef (зворотна сумісність):
    trader = Profession(
        name     = "trader",
        schedule = [ScheduleDef(3600, sync_trades)],
    )
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable, Iterable, Optional

if TYPE_CHECKING:
    from src.core.account_pull import AccountPull
    from src.core.conditions import Condition
    from src.core.inventory.model import Inventories
    from src.core.schedule import ScheduleDef, Trigger
    from src.core.task import AnyTask

PipelineFn = Callable[["AccountPull"], list["AnyTask"]]


@dataclass
class Profession:
    """
    Повний опис поведінки акаунта.

    name      : ідентифікатор для логів
    startup   : список fn(bot) → Iterable[AnyTask]  — одноразово при старті
    pipelines : список pipeline_fn(bot) → [AnyTask] — одноразово при старті
    schedule  : [ScheduleDef] — зворотна сумісність, конвертується у triggers
    triggers  : [Trigger]     — нова концепція, управляє КОЛИ запускати задачі
    guard     : Condition | None — перевіряється монітором Scheduler-а
    """
    name:      str
    startup:   list[Callable[["AccountPull"], Iterable["AnyTask"]]] = field(default_factory=list)
    pipelines: list[PipelineFn]                                      = field(default_factory=list)
    schedule:  list["ScheduleDef"]                                   = field(default_factory=list)
    triggers:  list["Trigger"]                                       = field(default_factory=list)
    guard:     Optional["Condition"]                                  = None

    def startup_tasks(self, bot: "AccountPull") -> list["AnyTask"]:
        """
        Всі задачі для старту воркера:
          - задачі з startup-продюсерів
          - початкові задачі кожного pipeline
        """
        tasks: list[AnyTask] = []
        for producer in self.startup:
            tasks.extend(producer(bot))
        for pipeline_fn in self.pipelines:
            tasks.extend(pipeline_fn(bot))
        return tasks

    def build_triggers(self, account_id: str) -> list["Trigger"]:
        """
        Повертає всі тригери цієї профессії з прив'язаним account_id.

        Включає:
          - triggers напряму (account_id заповнюється якщо порожній)
          - ScheduleDef.to_trigger() для schedule (зворотна сумісність)
        """
        result: list[Trigger] = []

        # Нові triggers — заповнити account_id якщо не вказано
        for t in self.triggers:
            if not t.account_id:
                t.account_id = account_id
            result.append(t)

        # Старий schedule → Trigger
        for sd in self.schedule:
            result.append(sd.to_trigger(account_id))

        return result

    def check_guard(self, inv: "Inventories") -> bool:
        """False → profession знімається."""
        if self.guard is None:
            return True
        return self.guard(inv)

    def __repr__(self) -> str:
        n_triggers = len(self.triggers) + len(self.schedule)
        return (
            f"<Profession {self.name!r} "
            f"startup={len(self.startup)} "
            f"pipelines={len(self.pipelines)} "
            f"triggers={n_triggers} "
            f"guard={'yes' if self.guard else 'no'}>"
        )