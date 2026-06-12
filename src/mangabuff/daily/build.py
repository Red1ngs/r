"""
daily/build.py — DailyProfession.

Архітектурні зміни:
    - Повністю видалено Pipeline, Step, Priority, triggers та BotWorker.
    - DailyProfession тепер виконує I/O збору бонусів напряму в асинхронному
      обробнику handle_request (інтент "claim") та зберігає стан у БД.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

from src.core.runtime.profession import BaseProfession, RequestResult
from src.mangabuff.daily.inventory import DailyInventory
from src.mangabuff.daily.stats import DailyRewardStats
from src.utils.time import today

if TYPE_CHECKING:
    from src.core.account import Account
    from src.core.runtime.request_router import RequestContext
    from src.core.runtime.scheduler import EventDrivenScheduler

from src.core.logging.loggers import get_account_logger


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
                await self._scheduler.emit_event(
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
                result = await bot.safe_session.fetch_daily_streak()
                day = result.data
                if day is None:
                    log.info("🎁 Календарний бонус зараз недоступний")
                    inv.can_claim_calendar    = False
                    inv.last_calendar_claimed = to_day
                else:
                    log.info(f"🎁 Календар: отримано день {day}")
                    inv.day                = day
                    inv.can_claim_calendar = True
                
                # Проміжний стан буде збережено автоматично через ask (auto-save в router)
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
                result = await bot.safe_session.claim_daily()
                data = result.data or {}
                self._stats.daily_results = data
                if result.ok:
                    inv.last_daily_claimed = to_day
                    claimed_any = True
                    log.info(f"✅ Звичайний бонус зібрано: {data}")
                else:
                    log.warning(f"⚠️ Не вдалося зібрати звичайний бонус: {data}")
            except Exception as e:
                log.error(f"❌ Помилка під час збору звичайного бонусу: {e}", exc_info=True)

        # Збір календарного бонусу
        if needs_calendar and inv.can_claim_calendar:
            day = inv.day
            log.info(f"🎁 Збираємо календарний бонус (день {day})…")
            try:
                result = await bot.safe_session.claim_calendar(day)
                data = result.data or {}
                self._stats.calendar_results = data
                if result.ok:
                    inv.last_calendar_claimed = to_day
                    inv.can_claim_calendar    = False
                    claimed_any = True
                    log.info(f"✅ Календарний бонус зібрано: {data}")
                else:
                    log.warning(f"⚠️ Не вдалося зібрати календарний бонус: {data}")
                    # Сервер відповів, але бонус недоступний (напр. 422).
                    # Вважаємо календар «зробленим» на сьогодні, щоб монітор
                    # не смикав сервер знову і знову до наступного дня.
                    inv.last_calendar_claimed = to_day
                    inv.can_claim_calendar    = False
            except Exception as e:
                log.error(f"❌ Помилка під час збору календарного бонусу: {e}", exc_info=True)

        # Зміни будуть збережені автоматично через auto-save в router після завершення ask
        if inv.last_daily_claimed == to_day:
            both_done = inv.last_calendar_claimed == to_day
            payload: dict[str, Any] = {
                "account_id":    bot.account_id,
                "daily_done":    True,
                "calendar_done": inv.last_calendar_claimed == to_day,
                "calendar_day":  inv.day,
            }

            if self._scheduler is not None:
                await self._scheduler.emit_event("daily.claimed", payload, source=bot.account_id)
                if both_done:
                    await self._scheduler.emit_event("daily.all_claimed", payload, source=bot.account_id)

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
        # Збереження відбудеться автоматично через auto-save в router після ask

        get_account_logger(ctx.account_id).info(
            "DailyProfession: force_claim → стан скинуто. Емітуємо подію для монітора."
        )

        if self._scheduler is not None:
            await self._scheduler.emit_event("daily.force_claim", {"account_id": ctx.account_id}, source=ctx.account_id)

        return RequestResult.approve(data={"status": "reset, monitor notified"})

    # ── Event handlers ────────────────────────────────────────────────────────

    async def _on_account_unbanned(self, payload: dict[str, Any]) -> None:
        if payload.get("account_id") != self._account_id:
            return
        get_account_logger(self._account_id).info("DailyProfession: розбан отримано, сповіщаємо монітор")
        if self._scheduler is not None:
            await self._scheduler.emit_event("daily.force_claim", {"account_id": self._account_id}, source=self._account_id)