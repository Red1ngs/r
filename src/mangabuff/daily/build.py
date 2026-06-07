"""
daily/build.py — DailyProfession + DailyMonitor.

Архітектурні зміни:
    - Повністю видалено Pipeline, Step, Priority, triggers та BotWorker.
    - Впроваджено DailyMonitor, який керує розкладом та часом очікування.
    - DailyProfession тепер виконує I/O збору бонусів напряму в асинхронному
      обробнику handle_request (інтент "claim") та зберігає стан у БД.
"""
from __future__ import annotations

import hashlib
import asyncio
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Optional

from src.core.runtime.profession import BaseProfession, RequestResult
from src.core.monitoring.monitor import BaseMonitor, monitor_registry
from src.mangabuff.daily.inventory import DailyInventory
from src.mangabuff.daily.stats import DailyRewardStats
from src.utils.time import today

if TYPE_CHECKING:
    from src.core.account import Account
    from src.core.runtime.request_router import RequestContext
    from src.core.runtime.scheduler import EventDrivenScheduler

from src.core.logging.loggers import get_account_logger


# ─────────────────────────────────────────────────────────────────────────────
# Допоміжна функція для рандомізації часу
# ─────────────────────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────────────────────
# DailyMonitor
# ─────────────────────────────────────────────────────────────────────────────

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

        loop = self._scheduler._async_loop
        if loop is None or not loop.is_running():
            return

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

        self._wakeup_task = loop.create_task(_fire())

    def _cancel_wakeup(self) -> None:
        if self._wakeup_task and not self._wakeup_task.done():
            self._wakeup_task.cancel()
        self._wakeup_task = None

    def _calculate_delay(self) -> float:
        bot = self._scheduler.get_bot(self._account_id)
        if bot is None:
            return 300.0

        inv = getattr(bot.inventory, "daily", None)
        if inv is None:
            return 300.0

        to_day = today()
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


# Реєструємо монітор у глобальному реєстрі
monitor_registry.register("daily", DailyMonitor)


# ─────────────────────────────────────────────────────────────────────────────
# DailyProfession
# ─────────────────────────────────────────────────────────────────────────────

class DailyProfession(BaseProfession):
    """Profession «Щоденні бонуси»."""

    def __init__(self) -> None:
        self._account_id:   str                              = ""
        self._stats:        DailyRewardStats                 = DailyRewardStats()
        self._scheduler:    Optional["EventDrivenScheduler"] = None

    @property
    def profession_id(self) -> str:
        return "daily"

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def setup(self, scheduler: "EventDrivenScheduler", account_id: str) -> None:
        self._account_id = account_id
        self._scheduler  = scheduler
        scheduler.subscribe("account.unbanned", self._on_account_unbanned)

    async def restore_state(self, bot: "Account") -> None:
        inv: DailyInventory = bot.inventory.daily  # type: ignore[attr-defined]
        to_day = today()
        all_done = (
            inv.last_daily_claimed == to_day
            and inv.last_calendar_claimed == to_day
        )

        get_account_logger(self._account_id).info(
            f"DailyProfession відновлено: "
            f"daily={inv.last_daily_claimed!r} "
            f"calendar={inv.last_calendar_claimed!r} "
            f"can_claim_calendar={inv.can_claim_calendar} "
            f"all_done={all_done}"
        )

    def check_guard(self, bot: "Account") -> bool:
        return not bool(bot.inventory.personal.data.get("is_banned"))

    # ── handle_request ────────────────────────────────────────────────────────

    async def handle_request(
        self,
        intent: str,
        data:   dict[str, Any],
        ctx:    "RequestContext",
    ) -> RequestResult:
        if intent == "claim":
            return await self._handle_claim(ctx)
        if intent == "get_status":
            return await self._handle_get_status(ctx)
        if intent == "force_claim":
            return await self._handle_force_claim(ctx)
        return RequestResult.deny(f"unknown intent: {intent!r}")

    async def _handle_claim(self, ctx: "RequestContext") -> RequestResult:
        bot = ctx.bot
        inv: DailyInventory = bot.inventory.daily  # type: ignore[attr-defined]
        to_day = today()
        log = get_account_logger(self._account_id)

        if not self.check_guard(bot):
            return RequestResult.deny("account is banned")

        needs_daily    = inv.last_daily_claimed    != to_day
        needs_calendar = inv.last_calendar_claimed != to_day

        if not needs_daily and not needs_calendar:
            log.info("🎁 Всі бонуси на сьогодні вже зібрано")
            if self._scheduler is not None:
                self._scheduler.emit_event(
                    "daily.claimed",
                    {
                        "account_id": bot.account_id,
                        "day": bot.inventory.daily.day,           
                        "last_daily_claimed": bot.inventory.daily.last_daily_claimed,
                    },
                    source=bot.account_id)
                
            return RequestResult.approve(data={"status": "all_claimed"})

        # Парсимо календар, якщо день стріку ще невідомий
        if needs_calendar and not inv.can_claim_calendar:
            log.info("🎁 День стріку невідомий → отримуємо календарний статус")
            try:
                # Виконуємо асинхронний запит у сесії
                day = bot.safe_session.fetch_daily_streak()
                if day is None:
                    log.info("🎁 Календарний бонус зараз недоступний")
                    inv.can_claim_calendar    = False
                    inv.last_calendar_claimed = to_day
                else:
                    log.info(f"🎁 Календар: отримано день {day}")
                    inv.day                = day
                    inv.can_claim_calendar = True
                
                # Зберігаємо проміжний стан парсингу календаря
                bot.repo.inventory.save(self._account_id, bot.inventory)
            except Exception as e:
                log.error(f"❌ Помилка отримання дня стріку: {e}", exc_info=True)
                return RequestResult.deny(f"parse_error: {e}")

        # Повторно перевіряємо статус після оновлення календаря
        needs_calendar = inv.last_calendar_claimed != to_day
        claimed_any = False

        # Збір звичайного щоденного бонусу
        if needs_daily:
            log.info("🎁 Збираємо звичайний бонус…")
            try:
                success, result = bot.safe_session.claim_daily()
                self._stats.daily_results = result
                if success:
                    inv.last_daily_claimed = to_day
                    claimed_any = True
                    log.info(f"✅ Звичайний бонус зібрано: {result}")
                else:
                    log.warning(f"⚠️ Не вдалося зібрати звичайний бонус: {result}")
            except Exception as e:
                log.error(f"❌ Помилка під час збору звичайного бонусу: {e}", exc_info=True)

        # Збір календарного бонусу
        if needs_calendar and inv.can_claim_calendar:
            day = inv.day
            log.info(f"🎁 Збираємо календарний бонус (день {day})…")
            try:
                success, result = bot.safe_session.claim_calendar(day)
                self._stats.calendar_results = result
                if success:
                    inv.last_calendar_claimed = to_day
                    inv.can_claim_calendar    = False
                    claimed_any = True
                    log.info(f"✅ Календарний бонус зібрано: {result}")
                else:
                    log.warning(f"⚠️ Не вдалося зібрати календарний бонус: {result}")
                    # Сервер відповів, але бонус недоступний (напр. 422).
                    # Вважаємо календар «зробленим» на сьогодні, щоб монітор
                    # не смикав сервер знову і знову до наступного дня.
                    inv.last_calendar_claimed = to_day
                    inv.can_claim_calendar    = False
                    bot.repo.inventory.save(self._account_id, bot.inventory)
            except Exception as e:
                log.error(f"❌ Помилка під час збору календарного бонусу: {e}", exc_info=True)

        # Зберігаємо зміни у БД та емітимо події за потреби
        if claimed_any:
            bot.repo.inventory.save(self._account_id, bot.inventory)

        if inv.last_daily_claimed == to_day:
            both_done = inv.last_calendar_claimed == to_day
            payload = {
                "account_id":    bot.account_id,
                "daily_done":    True,
                "calendar_done": inv.last_calendar_claimed == to_day,
                "calendar_day":  inv.day,
            }

            if self._scheduler is not None:
                self._scheduler.emit_event("daily.claimed", payload, source=bot.account_id)
                if both_done:
                    self._scheduler.emit_event("daily.all_claimed", payload, source=bot.account_id)

        return RequestResult.approve(data={
            "status": "processed",
            "daily_claimed": inv.last_daily_claimed == to_day,
            "calendar_claimed": inv.last_calendar_claimed == to_day
        })

    async def _handle_get_status(self, ctx: "RequestContext") -> RequestResult:
        inv: DailyInventory = ctx.bot.inventory.daily  # type: ignore[attr-defined]
        to_day = today()
        return RequestResult.approve(data={
            "last_daily_claimed":    inv.last_daily_claimed,
            "last_calendar_claimed": inv.last_calendar_claimed,
            "daily_done":            inv.last_daily_claimed    == to_day,
            "calendar_done":         inv.last_calendar_claimed == to_day,
            "calendar_day":          inv.day,
            "can_claim_calendar":    inv.can_claim_calendar,
            "stats":                 self._stats.data,
        })

    async def _handle_force_claim(self, ctx: "RequestContext") -> RequestResult:
        """
        Скидає стан збору бонусів та сповіщає монітор про необхідність збору.
        """
        inv: DailyInventory = ctx.bot.inventory.daily  # type: ignore[attr-defined]
        inv.last_daily_claimed    = None  # type: ignore[assignment]
        inv.last_calendar_claimed = None  # type: ignore[assignment]
        inv.can_claim_calendar    = False
        ctx.bot.repo.inventory.save(ctx.account_id, ctx.bot.inventory)

        get_account_logger(ctx.account_id).info(
            "DailyProfession: force_claim → стан скинуто. Емітуємо подію для монітора."
        )

        if self._scheduler is not None:
            self._scheduler.emit_event("daily.force_claim", {"account_id": ctx.account_id}, source=ctx.account_id)

        return RequestResult.approve(data={"status": "reset, monitor notified"})

    # ── Event handlers ────────────────────────────────────────────────────────

    async def _on_account_unbanned(self, payload: dict[str, Any]) -> None:
        if payload.get("account_id") != self._account_id:
            return
        get_account_logger(self._account_id).info("DailyProfession: розбан отримано, сповіщаємо монітор")
        if self._scheduler is not None:
            self._scheduler.emit_event("daily.force_claim", {"account_id": self._account_id}, source=self._account_id)