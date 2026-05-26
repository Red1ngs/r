from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Iterable

from src.core.runtime.schedule import BaseTrigger
from src.core.tasks.base import AnyTask
from src.mangabuff.daily.guards import wait_for_daily

if TYPE_CHECKING:
    from src.core.account import Account


@dataclass
class ReaderTrigger(BaseTrigger):
    """
    Тригер з динамічним next_delay — SlotScheduler знає, коли наступний слот.
    Має вбудовану перевірку статусу збору Daily (streak).
    """
    _producer: Callable[["Account"], Iterable[AnyTask]]

    def next_delay(self, bot: "Account") -> float:
        if wait_for_daily(bot):
            return float("inf")

        # Якщо працюємо окремо або бонус уже зібрано — працюємо за розкладом слотів
        return bot.inventory.reader.slot_scheduler.delay_until_next()  # type: ignore[attr-defined]

    def producer(self, bot: "Account") -> Iterable[AnyTask]:
        return self._producer(bot)