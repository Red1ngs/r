"""
daily/build.py — DailyMonitor.

Архітектурні зміни:
    - Повністю видалено Pipeline, Step, Priority, triggers та BotWorker.
    - Впроваджено DailyMonitor, який керує розкладом та часом очікування.
"""
from __future__ import annotations
import hashlib

import asyncio
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Optional

from src.core.monitoring.monitor import BaseMonitor
from src.utils.time import now

if TYPE_CHECKING:
    from src.core.runtime.scheduler import EventDrivenScheduler

from src.core.logging.loggers import get_account_logger


def get_stable_random_time(base_time: str, account_id: str, max_jitter_minutes: int = 60) -> str:
    """
    Повертає псевдовипадковий час у форматі HH:MM, зміщений відносно base_time.
    Зсув стабільний для конкретного account_id (MD5-хеш).
    """
    try:
        h, m = map(int, base_time.split(":"))
    except ValueError:
        return base_time

    hash_val = int(hashlib.md5(account_id.encode()).hexdigest(), 16)
    jitter   = hash_val % (max_jitter_minutes + 1)

    total_minutes = (h * 60 + m + jitter) % 1440
    new_h, new_m  = divmod(total_minutes, 60)
    return f"{new_h:02d}:{new_m:02d}"


class DailyMonitor(BaseMonitor):
    """
    Монітор, який визначає КОЛИ запускати збір щоденних бонусів.
    
    Він вираховує індивідуальний стабільний час для акаунта і засинає
    точно до моменту запуску. Прокидається достроково у разі отримання
    сигналу примусового збору (daily.force_claim).
    """

    BASE_TIME = "04:30"

    @property
    def monitor_id(self) -> str:
        return "daily"

    def __init__(self) -> None:
        self._account_id: str                               = ""
        self._scheduler:  Optional["EventDrivenScheduler"] = None
        self._wakeup_task: Optional[asyncio.Task[None]]    = None

    async def attach(
        self,
        scheduler:  "EventDrivenScheduler",
        account_id: str,
    ) -> None:
        self._account_id = account_id
        self._scheduler  = scheduler

        # Слухаємо подію примусового збору, щоб прокинутися негайно
        scheduler.subscribe("daily.force_claim", self._on_force_claim)

        await self._schedule_next(delay=0.0)

    async def detach(
        self,
        scheduler:  "EventDrivenScheduler",
        account_id: str,
    ) -> None:
        self._cancel_wakeup()
        self._scheduler = None

    async def _schedule_next(self, delay: Optional[float] = None) -> None:
        self._cancel_wakeup()
        if self._scheduler is None:
            return

        if delay is None:
            delay = self._calculate_delay()

        async def _fire() -> None:
            try:
                await asyncio.sleep(delay)
                await self._send_claim_request()
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                get_account_logger(self._account_id).error(
                    f"[DailyMonitor] Помилка у фоновому циклі: {exc}",
                    exc_info=True
                )

        self._wakeup_task = asyncio.ensure_future(_fire())

    def _cancel_wakeup(self) -> None:
        if self._wakeup_task and not self._wakeup_task.done():
            self._wakeup_task.cancel()
        self._wakeup_task = None

    def _calculate_delay(self) -> float:
        scheduler = self._scheduler
        if scheduler is None:
            return 300.0

        bot = scheduler.get_bot(self._account_id)
        if bot is None:
            return 300.0

        inv = bot.inventory.daily 
        to_day = bot.inventory.personal.to_day

        all_done = (
            inv.last_daily_claimed == to_day
            and inv.last_calendar_claimed == to_day
        )

        scheduled_time = get_stable_random_time(self.BASE_TIME, self._account_id, max_jitter_minutes=60)
        now = datetime.now(timezone.utc)
        h, m = map(int, scheduled_time.split(":"))
        target_today = now.replace(hour=h, minute=m, second=0, microsecond=0)

        if all_done:
            # Оскільки все вже зібрано сьогодні, чекаємо розкладу на наступний день
            target_tomorrow = target_today + timedelta(days=1)
            delay = (target_tomorrow - now).total_seconds()
            get_account_logger(self._account_id).info(
                f"[DailyMonitor] Обидва бонуси на сьогодні вже зібрано. "
                f"Наступний запуск заплановано на завтра о {scheduled_time} UTC (через {int(delay)}с)"
            )
            return max(0.0, delay)

        if now >= target_today:
            # Час збору настав або минув, а бонуси не зібрані — запускаємо негайно
            get_account_logger(self._account_id).info(
                f"[DailyMonitor] Настав час збору бонусів ({scheduled_time} UTC) — запуск негайно"
            )
            return 0.0
        else:
            # Чекаємо сьогоднішнього запланованого часу
            delay = (target_today - now).total_seconds()
            get_account_logger(self._account_id).info(
                f"[DailyMonitor] Очікуємо планового часу збору о {scheduled_time} UTC (через {int(delay)}с)"
            )
            return max(0.0, delay)

    async def _send_claim_request(self) -> None:
        if self._scheduler is None:
            return

        log = get_account_logger(self._account_id)
        log.info("[DailyMonitor] Ініціюємо запит збору бонусів")

        res = await self._scheduler.ask(
            account_id=self._account_id,
            profession_id="daily",
            intent="claim",
            caller="daily_monitor"
        )

        if not res.approved:
            log.warning(f"[DailyMonitor] Спроба збору відхилена професією: {res.reason}")

        # Після спроби збору (успішної чи ні) плануємо наступний крок
        await self._schedule_next()

    async def _on_force_claim(self, payload: dict[str, Any]) -> None:
        if payload.get("account_id") != self._account_id:
            return
        get_account_logger(self._account_id).info(
            "[DailyMonitor] Отримано сигнал force_claim → позачерговий запуск негайно"
        )
        await self._schedule_next(delay=0.0)
