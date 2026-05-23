"""
daily/build.py — DailyProfession.

Відповідальність:
    Збирає щоденний бонус і календарний streak-бонус.
    Запускається раз на день у псевдовипадковий час відносно 04:30 
    (розподіляється індивідуально для кожного акаунта за його хешем).

State (bot.inventory.daily → DailyInventory):
    last_daily_claimed    : str | None  — "YYYY-MM-DD" UTC
    last_calendar_claimed : str | None  — "YYYY-MM-DD" UTC
    can_claim_calendar    : bool
    day                   : int         — поточний день стріку

Pipeline (один цикл):
    _fetch_bonus_status
      ├─ _NOTHING_TO_DO → все зібрано → on_cycle_done
      ├─ None           → день календаря невідомий → parse → fetch знову
      └─ plan dict      → action → claim_daily / claim_calendar

Events що емітуємо:
    "daily.claimed"     — зібрано хоча б один бонус
    "daily.all_claimed" — зібрано обидва бонуси

handle_request intents:
    "get_status"  → поточний стан бонусів + stats
    "force_claim" → скидає last_*_claimed (спрацює при наступному trigger)

Recovery після restart:
    DailyInventory завантажена з БД в Account.__init__.
    restore_state() лише читає last_*_claimed — IO немає.
"""
from __future__ import annotations

import hashlib
import logging
from typing import TYPE_CHECKING, Any, Callable, Iterable, Optional

from src.core.runtime.profession import BaseProfession, RequestResult
from src.core.runtime.schedule import ScheduleDef, ScheduleTrigger
from src.core.tasks.base import AnyTask, Priority
from src.core.tasks.pipeline import Step, pipeline
from src.mangabuff.daily.inventory import DailyInventory
from src.mangabuff.daily.stats import DailyRewardStats

from src.utils.time import format_ts, now_ts, today

if TYPE_CHECKING:
    from src.core.account import Account
    from src.core.runtime.request_router import RequestContext
    from src.core.runtime.scheduler import EventDrivenScheduler
    from src.core.runtime.schedule import TriggerProtocol

log = logging.getLogger(__name__)

# ── Sentinel ───────────────────────────────────────────────────────────────────
_NOTHING_TO_DO: dict[str, Any] = {}


# ─────────────────────────────────────────────────────────────────────────────
# Допоміжна функція для рандомізації часу
# ─────────────────────────────────────────────────────────────────────────────

def get_stable_random_time(base_time: str, account_id: str, max_jitter_minutes: int = 60) -> str:
    """
    Повертає псевдовипадковий час у форматі HH:MM, зміщений відносно base_time.
    Зсув є абсолютно стабільним для конкретного account_id (використовує MD5-хеш).
    
    Приклад:
        get_stable_random_time("04:30", "acc_01", 60) -> завжди повертатиме, наприклад, "05:14"
        get_stable_random_time("04:30", "acc_02", 60) -> завжди повертатиме, наприклад, "04:47"
    """
    try:
        h, m = map(int, base_time.split(":"))
    except ValueError:
        return base_time  # Відкат до базового значення, якщо формат пошкоджено

    # Отримуємо стабільне число від 0 до max_jitter_minutes на основі хешу ID акаунта
    seed_bytes = account_id.encode("utf-8")
    hash_val = int(hashlib.md5(seed_bytes).hexdigest(), 16)
    jitter = hash_val % (max_jitter_minutes + 1)

    # Розраховуємо новий час з урахуванням переходу через добу
    total_minutes = (h * 60 + m + jitter) % 1440
    new_h, new_m = divmod(total_minutes, 60)
    
    return f"{new_h:02d}:{new_m:02d}"

# ─────────────────────────────────────────────────────────────────────────────
# Pipeline functions
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_bonus_status(bot: "Account") -> Any:
    inv   = bot.inventory.daily  # type: ignore[attr-defined]
    to_day = today()

    needs_daily    = inv.last_daily_claimed    != to_day
    needs_calendar = inv.last_calendar_claimed != to_day

    if not needs_daily and not needs_calendar:
        log.info(f"[{bot.account_id}] 🎁 Всі бонуси на сьогодні вже зібрано")
        return _NOTHING_TO_DO

    if needs_calendar and not inv.can_claim_calendar:
        log.info(f"[{bot.account_id}] 🎁 День стріку невідомий → парсинг")
        return None

    return {"do_daily": needs_daily, "do_calendar": needs_calendar}


def _parse_calendar_day(bot: "Account") -> None:
    inv = bot.inventory.daily  # type: ignore[attr-defined]
    day = bot.session.fetch_daily_streak()

    if day is None:
        log.info(f"[{bot.account_id}] 🎁 Календарний бонус зараз недоступний")
        inv.can_claim_calendar    = False
        inv.last_calendar_claimed = today()
        return

    log.info(f"[{bot.account_id}] 🎁 Календар: день {day}")
    inv.day               = day
    inv.can_claim_calendar = True


# ─────────────────────────────────────────────────────────────────────────────
# DailyProfession
# ─────────────────────────────────────────────────────────────────────────────

class DailyProfession(BaseProfession):
    """
    Profession «Щоденні бонуси».
    """

    INTERVAL = 86_400   # 24 години
    AT_TIME  = "04:30"  # базовий час запуску UTC

    def __init__(self) -> None:
        self._account_id:   str                              = ""
        self._stats:        DailyRewardStats                 = DailyRewardStats()
        self._trigger:      Optional[ScheduleTrigger]        = None   # ← тип змінено
        self._scheduled_at: Optional[str]                    = None   # ← нове поле
        self._scheduler:    Optional["EventDrivenScheduler"] = None

    @property
    def profession_id(self) -> str:
        return "daily_claimer"

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def setup(self, scheduler: "EventDrivenScheduler", account_id: str) -> None:
        self._account_id = account_id
        self._scheduler  = scheduler
        scheduler.subscribe("account.unbanned", self._on_account_unbanned)

    async def restore_state(self, bot: "Account") -> None:

        inv: DailyInventory = bot.inventory.daily  # type: ignore[attr-defined]
        to_day = today()

        daily_done    = inv.last_daily_claimed    == to_day
        calendar_done = inv.last_calendar_claimed == to_day
        all_done      = daily_done and calendar_done

        log.info(
            f"[{self._account_id}] DailyProfession відновлено: "
            f"daily={inv.last_daily_claimed!r} "
            f"calendar={inv.last_calendar_claimed!r} "
            f"all_done={all_done}"
        )

        if self._trigger is None or self._scheduled_at is None:
            return

        if all_done:
            # Бонус вже зібрано — переносимо тригер на завтра.
            # Без цього next_fire в минулому → is_due()=True → негайний повторний запуск.
            self._trigger.advance_to_next_day_at(self._scheduled_at)
            next_dt = format_ts(self._trigger.next_fire)
            log.info(
                f"[{self._account_id}] 🎁 Бонус вже зібрано — "
                f"наступний запуск: {next_dt}"
            )
        elif self._trigger.next_fire < now_ts():
            # Бонус не зібрано, але запланований час вже пройшов (бот упав до збору).
            # Запускаємо негайно на наступному тіку scheduler.
            self._trigger.reschedule("+0s")
            log.info(
                f"[{self._account_id}] 🎁 Пропущений запуск виявлено — запуск негайно"
            )
        # else: час ще не настав — не чіпаємо, тригер спрацює вчасно

    async def teardown(self, scheduler: "EventDrivenScheduler", account_id: str) -> None:
        scheduler._event_bus.unsubscribe("account.unbanned", self._on_account_unbanned)

    # ── Triggers ──────────────────────────────────────────────────────────────

    def build_triggers(self, account_id: str) -> list["TriggerProtocol"]:
        scheduled_time     = get_stable_random_time(self.AT_TIME, account_id, max_jitter_minutes=60)
        self._scheduled_at = scheduled_time  # ← зберігаємо для restore_state

        log.info(
            f"[{account_id}] Створено розклад щоденного збору: "
            f"{scheduled_time} UTC (базовий: {self.AT_TIME}, зміщення стабільне)"
        )

        trigger = ScheduleDef(
            interval=self.INTERVAL,
            producer=self._make_producer(),
            at=scheduled_time,
        ).to_trigger(account_id)   # тепер повертає ScheduleTrigger

        self._trigger = trigger    # тип тепер ScheduleTrigger — advance_to_next_day_at доступний
        return [trigger]

    def check_guard(self, bot: "Account") -> bool:
        return not bool(bot.inventory.personal.data.get("is_banned"))

    # ── handle_request ────────────────────────────────────────────────────────

    async def handle_request(
        self,
        intent: str,
        data:   dict[str, Any],
        ctx:    "RequestContext",
    ) -> RequestResult:
        if intent == "get_status":
            return await self._handle_get_status(ctx)
        if intent == "force_claim":
            return await self._handle_force_claim(ctx)
        return RequestResult.deny(f"unknown intent: {intent!r}")

    async def _handle_get_status(self, ctx: "RequestContext") -> RequestResult:
        inv: DailyInventory = ctx.bot.inventory.daily  # type: ignore[attr-defined]
        to_day = today()
        return RequestResult.approve(data={
            "last_daily_claimed":    inv.last_daily_claimed,
            "last_calendar_claimed": inv.last_calendar_claimed,
            "daily_done":            inv.last_daily_claimed == to_day,
            "calendar_done":         inv.last_calendar_claimed == to_day,
            "calendar_day":          inv.day,
            "can_claim_calendar":    inv.can_claim_calendar,
            "stats":                 self._stats.data,
        })

    async def _handle_force_claim(self, ctx: "RequestContext") -> RequestResult:
        inv: DailyInventory = ctx.bot.inventory.daily  # type: ignore[attr-defined]
        inv.last_daily_claimed    = None  # type: ignore[assignment]
        inv.last_calendar_claimed = None  # type: ignore[assignment]
        inv.can_claim_calendar    = False
        ctx.bot.repo.inventory.save(ctx.account_id, ctx.bot.inventory)

        if self._trigger is not None:
            # Використовуємо публічне API: reschedule скидає _in_flight та ставить час
            self._trigger.reschedule("+0s")

        log.info(f"[{ctx.account_id}] DailyProfession: force_claim → стан скинуто, тригер активовано")
        return RequestResult.approve(data={"status": "reset, will claim on next tick"})

    # ── Event handlers ────────────────────────────────────────────────────────

    async def _on_account_unbanned(self, payload: dict[str, Any]) -> None:
        if payload.get("account_id") != self._account_id:
            return
        log.info(f"[{self._account_id}] DailyProfession: розбан отримано, guard знятий")

    # ── Pipeline ──────────────────────────────────────────────────────────────

    def _make_producer(self) -> Callable[["Account"], Iterable[AnyTask]]:

        def on_cycle_done(bot: "Account") -> None:
            if self._trigger is not None:
                self._trigger.advance(bot)
            else:
                log.warning(
                    f"[{bot.account_id}] DailyProfession: on_cycle_done викликано "
                    f"але self._trigger == None — тригер не зсунуто!"
                )

        return pipeline(
            name   = "daily_claimer",
            fetch  = _fetch_bonus_status,
            parse  = [
                Step(_parse_calendar_day, priority=Priority.NORMAL, max_retries=1),
            ],
            action            = self._make_action(on_cycle_done),
            max_parse_retries = 2,
        )

    def _make_action(
        self,
        on_cycle_done: Callable[["Account"], None],
    ) -> Callable[[Any, "Account"], None]:

        def action(plan: Any, bot: "Account") -> None:
            if plan is _NOTHING_TO_DO:
                on_cycle_done(bot)
                return

            inv: DailyInventory = bot.inventory.daily  # type: ignore[attr-defined]
            to_day       = today()
            claimed_any = False

            if plan.get("do_daily"):
                log.info(f"[{bot.account_id}] 🎁 Збираємо звичайний бонус…")
                success, result = bot.session.claim_daily()
                self._stats.daily_results = result
                if success:
                    inv.last_daily_claimed = to_day
                    claimed_any = True
                    log.info(f"[{bot.account_id}] ✅ Звичайний бонус зібрано: {result}")

            if plan.get("do_calendar"):
                day = inv.day
                log.info(f"[{bot.account_id}] 🎁 Збираємо календарний бонус (день {day})…")
                success, result = bot.session.claim_calendar(day)
                self._stats.calendar_results = result
                if success:
                    inv.last_calendar_claimed = to_day
                    inv.can_claim_calendar    = False
                    claimed_any = True
                    log.info(f"[{bot.account_id}] ✅ Календарний бонус зібрано: {result}")
                else:
                    log.warning(f"[{bot.account_id}] ⚠️ Помилка збору календаря — спробуємо наступного разу")

            if self._scheduler is not None and claimed_any:
                both_done = (
                    inv.last_daily_claimed    == to_day
                    and inv.last_calendar_claimed == to_day
                )
                self._scheduler.emit_event(
                    "daily.all_claimed" if both_done else "daily.claimed",
                    {
                        "account_id":    bot.account_id,
                        "daily_done":    inv.last_daily_claimed == to_day,
                        "calendar_done": inv.last_calendar_claimed == to_day,
                        "calendar_day":  inv.day,
                    },
                    source=bot.account_id,
                )

            on_cycle_done(bot)

        return action