"""
daily/build.py — DailyProfession.

Відповідальність:
    Збирає щоденний бонус і календарний streak-бонус.
    Запускається раз на день у псевдовипадковий час відносно 04:30
    (розподіляється індивідуально для кожного акаунта за його хешем).

State (bot.inventory.daily → DailyInventory):
    last_daily_claimed    : str | None  — "YYYY-MM-DD" UTC
    last_calendar_claimed : str | None  — "YYYY-MM-DD" UTC
    can_claim_calendar    : bool        — день стріку відомий, можна збирати
    day                   : int         — поточний день стріку

    Весь стан живе в DailyInventory.data (dict) і персистується автоматично
    через worker._execute → finally → repo.inventory.save().
    Явний save() в action не потрібен і не використовується.

Pipeline (один цикл):
    _fetch_bonus_status
      ├─ Skip        → все зібрано → on_cycle_done
      ├─ NotReady    → день стріку невідомий → parse(_parse_calendar_day) → fetch знову
      └─ Ready(plan: dict[str, bool]) → action → claim_daily / claim_calendar

Events що емітуємо:
    "daily.claimed"     — зібрано хоча б один бонус
    "daily.all_claimed" — зібрано обидва бонуси

handle_request intents:
    "get_status"  → поточний стан бонусів + stats
    "force_claim" → скидає last_*_claimed (спрацює при наступному trigger)

Recovery після restart:
    DailyInventory завантажується з БД в Account.__init__.
    restore_state() лише читає last_*_claimed — IO немає.
"""
from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Any, Callable, Iterable, Optional

from src.core.runtime.profession import BaseProfession, RequestResult
from src.core.runtime.schedule import ScheduleDef, ScheduleTrigger
from src.core.tasks.base import AnyTask, Priority
from src.core.tasks.pipeline import NotReady, Ready, Skip, Step, pipeline
from src.mangabuff.daily.inventory import DailyInventory
from src.mangabuff.daily.stats import DailyRewardStats
from src.utils.time import format_ts, now_ts, today

if TYPE_CHECKING:
    from src.core.account import Account
    from src.core.runtime.request_router import RequestContext
    from src.core.runtime.schedule import TriggerProtocol
    from src.core.runtime.scheduler import EventDrivenScheduler

from src.core.logging.loggers import get_account_logger


# ─────────────────────────────────────────────────────────────────────────────
# Допоміжна функція для рандомізації часу
# ─────────────────────────────────────────────────────────────────────────────

def get_stable_random_time(base_time: str, account_id: str, max_jitter_minutes: int = 60) -> str:
    """
    Повертає псевдовипадковий час у форматі HH:MM, зміщений відносно base_time.
    Зсув стабільний для конкретного account_id (MD5-хеш).

    Приклад:
        get_stable_random_time("04:30", "acc_01", 60) → завжди "05:14"
        get_stable_random_time("04:30", "acc_02", 60) → завжди "04:47"
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
# Pipeline functions
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_bonus_status(bot: "Account") -> Ready[dict[str, bool]] | NotReady | Skip:
    """
    Fetch-крок pipeline щоденного бонусу.

    Повертає:
        Skip              — обидва бонуси вже зібрано сьогодні.
        NotReady          — потрібен календарний бонус, але день стріку
                            невідомий (can_claim_calendar=False); pipeline
                            запустить _parse_calendar_day → fetch знову.
        Ready(plan: dict) — є що збирати:
                            {"do_daily": bool, "do_calendar": bool}
    """
    inv    = bot.inventory.daily  # type: ignore[attr-defined]
    to_day = today()

    needs_daily    = inv.last_daily_claimed    != to_day
    needs_calendar = inv.last_calendar_claimed != to_day

    if not needs_daily and not needs_calendar:
        get_account_logger(bot.account_id).info("🎁 Всі бонуси на сьогодні вже зібрано")
        return Skip(reason="all claimed")

    if needs_calendar and not inv.can_claim_calendar:
        get_account_logger(bot.account_id).info("🎁 День стріку невідомий → парсинг")
        return NotReady()

    return Ready({"do_daily": needs_daily, "do_calendar": needs_calendar})


def _parse_calendar_day(bot: "Account") -> None:
    """
    Parse-крок: отримує поточний день стріку зі сторінки /balance.

    Якщо бонус недоступний — виставляє last_calendar_claimed = today(),
    щоб наступний fetch побачив needs_calendar=False і не повертав NotReady.
    can_claim_calendar при цьому лишається False і зберігається в БД
    (через worker finally → repo.inventory.save).
    """
    inv = bot.inventory.daily  # type: ignore[attr-defined]
    day = bot.session.fetch_daily_streak()

    if day is None:
        get_account_logger(bot.account_id).info("🎁 Календарний бонус зараз недоступний")
        inv.can_claim_calendar    = False
        inv.last_calendar_claimed = today()
        return

    get_account_logger(bot.account_id).info(f"🎁 Календар: день {day}")
    inv.day                = day
    inv.can_claim_calendar = True


# ─────────────────────────────────────────────────────────────────────────────
# DailyProfession
# ─────────────────────────────────────────────────────────────────────────────

class DailyProfession(BaseProfession):
    """Profession «Щоденні бонуси»."""

    INTERVAL = 86_400   # 24 години
    AT_TIME  = "04:30"  # базовий час запуску UTC

    def __init__(self) -> None:
        self._account_id:   str                              = ""
        self._stats:        DailyRewardStats                 = DailyRewardStats()
        self._trigger:      Optional[ScheduleTrigger]        = None
        self._scheduled_at: Optional[str]                    = None
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
        to_day    = today()
        all_done  = (
            inv.last_daily_claimed    == to_day
            and inv.last_calendar_claimed == to_day
        )

        get_account_logger(self._account_id).info(
            f"DailyProfession відновлено: "
            f"daily={inv.last_daily_claimed!r} "
            f"calendar={inv.last_calendar_claimed!r} "
            f"can_claim_calendar={inv.can_claim_calendar} "
            f"all_done={all_done}"
        )

        if self._trigger is None or self._scheduled_at is None:
            return

        if all_done:
            # Бонус вже зібрано — переносимо тригер на завтра.
            # Без цього next_fire в минулому → is_due()=True → негайний повтор.
            self._trigger.advance_to_next_day_at(self._scheduled_at)
            get_account_logger(self._account_id).info(
                f"🎁 Бонус вже зібрано — наступний запуск: "
                f"{format_ts(self._trigger.next_fire)}"
            )
        elif self._trigger.next_fire < now_ts():
            # Бонус не зібрано, але час вже пройшов (бот упав до збору).
            self._trigger.reschedule("+0s")
            get_account_logger(self._account_id).info("🎁 Пропущений запуск — запуск негайно")
        # else: час ще не настав — тригер спрацює вчасно

    # ── Triggers ──────────────────────────────────────────────────────────────

    def build_triggers(self, account_id: str) -> list["TriggerProtocol"]:
        scheduled_time     = get_stable_random_time(self.AT_TIME, account_id, max_jitter_minutes=60)
        self._scheduled_at = scheduled_time

        get_account_logger(account_id).info(
            f"Створено розклад щоденного збору: "
            f"{scheduled_time} UTC (базовий: {self.AT_TIME})"
        )

        trigger        = ScheduleDef(
            interval = self.INTERVAL,
            producer = self._make_producer(),
            at       = scheduled_time,
        ).to_trigger(account_id)

        self._trigger = trigger
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
            "daily_done":            inv.last_daily_claimed    == to_day,
            "calendar_done":         inv.last_calendar_claimed == to_day,
            "calendar_day":          inv.day,
            "can_claim_calendar":    inv.can_claim_calendar,
            "stats":                 self._stats.data,
        })

    async def _handle_force_claim(self, ctx: "RequestContext") -> RequestResult:
        """
        Скидає стан бонусів — pipeline запуститься при наступному тіку тригера.

        Зберігаємо явно тут, бо це request-handler (поза worker-loop),
        і автоматичного збереження через finally не відбудеться.
        """
        inv: DailyInventory = ctx.bot.inventory.daily  # type: ignore[attr-defined]
        inv.last_daily_claimed    = None  # type: ignore[assignment]
        inv.last_calendar_claimed = None  # type: ignore[assignment]
        inv.can_claim_calendar    = False
        ctx.bot.repo.inventory.save(ctx.account_id, ctx.bot.inventory)

        if self._trigger is not None:
            self._trigger.reschedule("+0s")

        get_account_logger(ctx.account_id).info(
            "DailyProfession: force_claim → стан скинуто, тригер активовано"
        )
        return RequestResult.approve(data={"status": "reset, will claim on next tick"})

    # ── Event handlers ────────────────────────────────────────────────────────

    async def _on_account_unbanned(self, payload: dict[str, Any]) -> None:
        if payload.get("account_id") != self._account_id:
            return
        get_account_logger(self._account_id).info("DailyProfession: розбан отримано, guard знятий")

    # ── Pipeline ──────────────────────────────────────────────────────────────

    def _make_producer(self) -> Callable[["Account"], Iterable[AnyTask]]:

        def on_cycle_done(bot: "Account") -> None:
            if self._trigger is not None:
                self._trigger.advance(bot)
            else:
                get_account_logger(bot.account_id).warning(
                    "DailyProfession: on_cycle_done — self._trigger is None, тригер не зсунуто!"
                )

        return pipeline(
            name   = "daily",
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
            """
            Виконує збір бонусів згідно плану.

            Зберігати інвентар явно не потрібно: worker._execute зберігає
            його автоматично у finally після завершення кожної задачі.
            """
            inv: DailyInventory = bot.inventory.daily  # type: ignore[attr-defined]
            to_day      = today()
            claimed_any = False

            if plan.get("do_daily"):
                get_account_logger(bot.account_id).info("🎁 Збираємо звичайний бонус…")
                success, result = bot.session.claim_daily()
                self._stats.daily_results = result
                if success:
                    inv.last_daily_claimed = to_day
                    claimed_any = True
                    get_account_logger(bot.account_id).info(f"✅ Звичайний бонус зібрано: {result}")

            if plan.get("do_calendar"):
                day = inv.day
                get_account_logger(bot.account_id).info(f"🎁 Збираємо календарний бонус (день {day})…")
                success, result = bot.session.claim_calendar(day)
                self._stats.calendar_results = result
                if success:
                    inv.last_calendar_claimed = to_day
                    inv.can_claim_calendar    = False
                    claimed_any = True
                    get_account_logger(bot.account_id).info(f"✅ Календарний бонус зібрано: {result}")
                else:
                    get_account_logger(bot.account_id).warning(
                        "⚠️ Помилка збору календаря — спробуємо наступного разу"
                    )

            if self._scheduler is not None and claimed_any:
                both_done = (
                    inv.last_daily_claimed    == to_day
                    and inv.last_calendar_claimed == to_day
                )
                self._scheduler.emit_event(
                    "daily.all_claimed" if both_done else "daily.claimed",
                    {
                        "account_id":    bot.account_id,
                        "daily_done":    inv.last_daily_claimed    == to_day,
                        "calendar_done": inv.last_calendar_claimed == to_day,
                        "calendar_day":  inv.day,
                    },
                    source=bot.account_id,
                )

            on_cycle_done(bot)

        return action