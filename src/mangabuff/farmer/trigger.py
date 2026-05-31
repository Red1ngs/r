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
    Тригер читача з підтримкою двох режимів.

    standard:
      Затримка після кожного читання визначається виключно через
      SlotScheduler.delay_until_next() — який в свою чергу рахує
      now + slot.interval_seconds після mark_done().

      read_interval_s використовується як fallback:
        = 0  → всі слоти готові зараз (перший запуск / рестарт)
        > 0  → до опівночі (всі слоти закриті на сьогодні)

    event:
      Фіксований інтервал event_interval_s, незалежно від слотів.

    Видалено:
      _last_reward / notify_reward — більше не потрібні,
      бо тригер не знає про тип нагороди і не застосовує cooldown.
      Єдине джерело правди для затримки — SlotScheduler.
    """
    _producer: Callable[["Account"], Iterable[AnyTask]]

    def next_delay(self, bot: "Account") -> float:
        if wait_for_daily(bot):
            return float("inf")

        cfg = bot.app_config.reader.reading_mode

        if cfg.is_event:
            return cfg.event_interval_s

        slot_delay = bot.inventory.reader.slot_scheduler.delay_until_next()  # type: ignore[attr-defined]
        return slot_delay if slot_delay > 0 else cfg.read_interval_s

    def producer(self, bot: "Account") -> Iterable[AnyTask]:
        return self._producer(bot)