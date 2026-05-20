"""
profession.py — декларативний опис поведінки акаунта.

Profession не знає про деталі тригерів — тільки про TriggerProtocol.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Iterable, Optional

if TYPE_CHECKING:
    from src.core.account import Account
    from src.core.inventory.model import Inventories
    from src.core.scheduling.conditions import Condition
    from src.core.scheduling.schedule import ScheduleDef, TriggerProtocol
    from src.core.tasks.base import AnyTask

PipelineFn = Callable[["Account"], list["AnyTask"]]
ProfessionBuilder = Callable[["Account"], Any]

@dataclass
class Profession:
    """
    Повний опис поведінки акаунта.

    name      : ідентифікатор для логів
    startup   : список fn(bot) → Iterable[AnyTask] — одноразово при старті
    pipelines : список pipeline_fn(bot) → [AnyTask] — одноразово при старті
    schedule  : [ScheduleDef] — зворотна сумісність → конвертується у тригери
    triggers  : [TriggerProtocol] — нова концепція
    guard     : Condition | None
    """
    name:      str
    startup:   list[Callable[["Account"], Iterable["AnyTask"]]] = field(default_factory=lambda: [])
    pipelines: list[PipelineFn]                                      = field(default_factory=lambda: [])
    schedule:  list["ScheduleDef"]                                   = field(default_factory=lambda: [])
    triggers:  list["TriggerProtocol"]                               = field(default_factory=lambda: [])
    guard:     Optional["Condition"]                                  = None

    def startup_tasks(self, bot: "Account") -> list["AnyTask"]:
        tasks: list[AnyTask] = []
        for producer in self.startup:
            tasks.extend(producer(bot))
        for pipeline_fn in self.pipelines:
            tasks.extend(pipeline_fn(bot))
        return tasks

    def build_triggers(self, account_id: str) -> list["TriggerProtocol"]:
        """
        Повертає всі тригери з прив'язаним account_id.

        Включає:
          - triggers напряму (account_id заповнюється якщо порожній)
          - ScheduleDef.to_trigger() для schedule (зворотна сумісність)
        """
        result: list[TriggerProtocol] = []

        for t in self.triggers:
            if not t.account_id:
                t.account_id = account_id
            result.append(t)

        for sd in self.schedule:
            result.append(sd.to_trigger(account_id))

        return result

    def check_guard(self, inv: "Inventories") -> bool:
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
        
        
class ProfessionFactory:
    def __init__(self) -> None:
        self._registry: dict[str, ProfessionBuilder] = {}

    def register(self, name: str, builder: ProfessionBuilder) -> None:
        """Реєструє білдер професії за її ім'ям."""
        self._registry[name] = builder

    def build(self, name: str, bot: "Account") -> tuple[Profession, Any]:
        """
        Будує професію за її назвою.
        Повертає те, що повертає її функція-білдер (напр. tuple(Profession, Stats)).
        """
        builder = self._registry.get(name)
        if not builder:
            raise ValueError(f"Невідома професія: {name!r}")
        return builder(bot)

    def get_builder(self, name: str) -> ProfessionBuilder | None:
        """Отримує безпосередньо функцію-білдер, якщо потрібно викликати її вручну."""
        return self._registry.get(name)

    def names(self) -> list[str]:
        """Повертає список усіх зареєстрованих назв професій."""
        return list(self._registry.keys())

    def __repr__(self) -> str:
        return f"<ProfessionFactory names={list(self._registry)}>"


# Глобальний інстанс (сінглтон) для використання по всьому проєкту
profession_factory = ProfessionFactory()