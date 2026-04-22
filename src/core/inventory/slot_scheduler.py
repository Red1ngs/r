"""
slot_scheduler.py — планувальник читань по слотах.

Концепція: абсолютна часова пряма.

    t=0 (старт):  scroll.next=0,  card.next=0   ← всі стартують одразу
    t=0:          виконується scroll (менший interval → вищий пріоритет)
                  scroll.next = 0 + 3600
    t=0+jitter:   виконується card  (колізія → зсув)
                  card.next   = jitter + 5400
    t=3600:       виконується scroll
                  scroll.next = 3600 + 3600
    ...

Колізія (два слоти готові одночасно):
    Перший виконується одразу.
    Другий зсувається на random(JITTER_MIN, JITTER_MAX).

Добовий скид collected:
    Разом зі slot_schedule в inventory.data зберігається ключ
    "slot_reset_date" — рядок "YYYY-MM-DD" (локальна дата машини).

    При кожному виклику _maybe_daily_reset():
      якщо поточна дата != slot_reset_date →
        усі SlotProgress.collected скидаються до 0,
        slot_reset_date оновлюється,
        schedule скидається (всі слоти стартують з now).

    Скид відбувається ліниво — при першому зверненні після опівночі:
      initialize(), current(), delay_until_next().
    Персистується через InventoryStore автоматично разом з inventory.

Зберігається в ReaderInventory.data:
    "slot_schedule"  : { slot_name: next_fire_wall_timestamp }
    "slot_reset_date": "YYYY-MM-DD"
"""
from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from src.core.inventory.model import ReaderInventory, SlotProgress

log = logging.getLogger(__name__)

JITTER_MIN: float = 1.0
JITTER_MAX: float = 30.0

_SCHEDULE_KEY   = "slot_schedule"
_RESET_DATE_KEY = "slot_reset_date"


@dataclass
class SlotFire:
    """Результат current() — який слот виконувати зараз."""
    slot:  "SlotProgress"
    delay: float   # секунди до виконання (0.0 = прямо зараз)


class SlotScheduler:
    """
    Планувальник слотів для одного ReaderInventory.

    Стан зберігається в inventory.data і персистується через InventoryStore.

    Публічний API:
        initialize()         — викликати один раз при старті
        current()            → готовий слот або None
        mark_done(name)      — після виконання слота
        delay_until_next()   → секунд до наступного слота
        reset()              — примусовий скид (для тестів / ручного керування)
    """

    def __init__(self, inventory: "ReaderInventory"):
        self._inv = inventory

    # ── Публічний API ─────────────────────────────────────────────────────────

    def initialize(self) -> None:
        """
        Викликати один раз при старті (build_manga_reader).

        1. Перевіряє добовий скид (новий день → collected = 0).
        2. Реєструє нові слоти з next_fire = now.
        3. Слоти що вже є в розкладі — не чіпає (зберігаємо прогрес після рестарту).
        """
        self._maybe_daily_reset()

        now      = time.time()
        schedule = self._raw_schedule()
        for slot in self._inv.all_slots():
            if slot.slot_name not in schedule:
                schedule[slot.slot_name] = now
        self._inv.data[_SCHEDULE_KEY] = schedule

        log.debug(f"SlotScheduler initialized: {list(schedule.keys())}")

    def current(self) -> Optional["SlotProgress"]:
        """
        Перший готовий (next_fire_at <= now) незакритий слот.

        Якщо кілька готові одночасно — обираємо з найменшим interval,
        решту зсуваємо на jitter щоб уникнути колізії.

        Повертає None якщо жоден не готовий або всі закриті.
        """
        self._maybe_daily_reset()

        now      = time.time()
        schedule = self._raw_schedule()

        ready = [
            s for s in self._inv.pending_slots()
            if schedule.get(s.slot_name, 0.0) <= now
        ]

        if not ready:
            return None

        winner = ready[0]
        for collider in ready[1:]:
            jitter = random.uniform(JITTER_MIN, JITTER_MAX)
            schedule[collider.slot_name] = now + jitter
            log.debug(
                f"SlotScheduler collision: '{collider.slot_name}' "
                f"deferred {jitter:.1f}s"
            )
        self._inv.data[_SCHEDULE_KEY] = schedule

        return winner

    def mark_done(self, slot_name: str) -> None:
        """
        Викликати після кожного читання (незалежно від нагороди).
        Встановлює next_fire_at = now + interval.
        """
        slot = self._find_slot(slot_name)
        if slot is None:
            return

        schedule            = self._raw_schedule()
        schedule[slot_name] = time.time() + slot.interval
        self._inv.data[_SCHEDULE_KEY] = schedule

        log.debug(
            f"SlotScheduler: '{slot_name}' next fire in {slot.interval:.0f}s"
        )

    def delay_until_next(self) -> float:
        """
        Секунд до наступного готового незакритого слота.

        Варіанти відповіді:
          > 0   — чекати N секунд до наступного слота
          = 0   — є готовий слот прямо зараз → можна читати
          сек до опівночі — всі слоти закриті на сьогодні,
                            наступний цикл починається завтра

        Це значення використовується як Trigger.dynamic_next:
            Trigger(dynamic_next=lambda bot: bot.inventory.reader.scheduler.delay_until_next())
        Тому воно НІКОЛИ не повертає 0.0 якщо немає pending-слотів —
        інакше Trigger спінитиметься вічно.
        """
        self._maybe_daily_reset()

        now           = time.time()
        schedule      = self._raw_schedule()
        pending_names = {s.slot_name for s in self._inv.pending_slots()}

        if not pending_names:
            # Всі слоти закриті — спимо до початку наступного дня
            return self._seconds_until_midnight()

        upcoming = [
            schedule[name]
            for name in pending_names
            if name in schedule
        ]
        if not upcoming:
            return 0.0

        return max(0.0, min(upcoming) - now)

    def _seconds_until_midnight(self) -> float:
        """Секунд від зараз до 00:00:00 наступного дня (локальний час)."""
        import datetime as _dt
        now_local = _dt.datetime.now().astimezone()
        tomorrow  = (now_local + _dt.timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        delta = (tomorrow - now_local).total_seconds()
        # Мінімум 60 секунд — щоб не спінитись у граничних ситуаціях
        return max(60.0, delta)

    def reset(self) -> None:
        """Примусово скидає розклад — всі слоти стартують з now."""
        now      = time.time()
        schedule = {s.slot_name: now for s in self._inv.all_slots()}
        self._inv.data[_SCHEDULE_KEY] = schedule
        log.info("SlotScheduler reset — all slots fire immediately")

    # ── Добовий скид ─────────────────────────────────────────────────────────

    def _maybe_daily_reset(self) -> None:
        """
        Скидає collected у нуль якщо настав новий день.

        Логіка:
          - читаємо "slot_reset_date" з inventory.data
          - порівнюємо з date.today().isoformat()
          - якщо відрізняється → скидаємо collected у всіх SlotProgress,
            оновлюємо дату, скидаємо schedule (щоб слоти стартували з now)
        """
        today     = date.today().isoformat()   # "YYYY-MM-DD"
        last_date = self._inv.data.get(_RESET_DATE_KEY, "")

        if last_date == today:
            return  # той самий день — нічого не робимо

        # ── Новий день ────────────────────────────────────────────────────────
        if last_date:
            log.info(
                f"SlotScheduler: новий день ({last_date} → {today}), "
                f"скидаємо collected"
            )
        else:
            log.debug(
                f"SlotScheduler: перший запуск, ініціалізуємо дату {today}"
            )

        # Скидаємо тільки collected — таймінги слотів не чіпаємо.
        # next_fire_at зберігається як є: якщо слот мав спрацювати
        # о 03:00 — він спрацює о 03:00 наступного дня в штатному режимі.
        raw_slots = self._inv.data.get("slots", {})
        for name, slot_dict in raw_slots.items():
            if slot_dict.get("collected", 0) != 0:
                slot_dict["collected"] = 0
                log.debug(f"SlotScheduler: '{name}' collected → 0")
        self._inv.data["slots"] = raw_slots

        # Зберігаємо нову дату
        self._inv.data[_RESET_DATE_KEY] = today

    # ── Внутрішнє ────────────────────────────────────────────────────────────

    def _raw_schedule(self) -> dict[str, float]:
        return self._inv.data.setdefault(_SCHEDULE_KEY, {})

    def _find_slot(self, name: str) -> Optional["SlotProgress"]:
        for s in self._inv.all_slots():
            if s.slot_name == name:
                return s
        return None