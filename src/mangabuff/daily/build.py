"""
Daily bonus — triggered by scheduler.every().

Зберігає дату останнього отримання в daily inventory.
Відпрацьовує звичайний бонус та календар незалежно один від одного.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Callable, Iterable

from src.core.scheduling.profession import Profession
from src.core.scheduling.schedule import ScheduleDef
from src.core.tasks.base import AnyTask, Priority
from src.core.tasks.pipeline import Step, pipeline
from src.mangabuff.daily.stats import DailyRewardStats
from src.utils.time import today_utc

if TYPE_CHECKING:
    from src.core.account import Account

log = logging.getLogger(__name__)

# ── Sentinel ──────────────────────────────────────────────────────────────────
# Унікальний об'єкт для сигналізації action-у, що робити нічого не треба.
_NOTHING_TO_DO: dict[str, Any] = {}


# ═══════════════════════════════════════════════════════════════
# FETCH (Планувальник)
# ═══════════════════════════════════════════════════════════════
def fetch_bonus_status(bot: Account) -> Any:
    """
    Визначає, які бонуси треба зібрати сьогодні.
    - Повертає _NOTHING_TO_DO, якщо все зібрано.
    - Повертає None, якщо треба стягнути календар з API (запустить parse).
    - Повертає dict з планом дій для action, якщо все готово.
    """
    inv = bot.inventory.daily
    today = today_utc()
    
    needs_daily    = (inv.last_daily_claimed != today)
    needs_calendar = (inv.last_calendar_claimed != today)
    
    if not needs_daily and not needs_calendar:
        log.info(f"[{bot.account_id}] 🎁 Всі бонуси на сьогодні вже зібрано, пропускаємо")
        return _NOTHING_TO_DO
    
    if needs_calendar and not inv.can_claim_calendar:
        log.info(f"[{bot.account_id}] 🎁 Потрібно дізнатись день календаря (запускаємо parse)")
        return None
    
    return {
        "do_daily": needs_daily,
        "do_calendar": needs_calendar,
    }


# ═══════════════════════════════════════════════════════════════
# PARSE (Підготовка даних)
# ═══════════════════════════════════════════════════════════════
def parse_calendar_updates(bot: Account) -> None:
    inv = bot.inventory.daily
    day = bot.session.fetch_daily_streak()
    
    if day is None:
        log.info(f"[{bot.account_id}] 🎁 Немає доступного календарного бонусу зараз")
        inv.can_claim_calendar = False
        inv.last_calendar_claimed = today_utc()
        return
    
    log.info(f"[{bot.account_id}] 🎁 Календар оновлено: день {day}")
    inv.day = day
    inv.can_claim_calendar = True


# ═══════════════════════════════════════════════════════════════
# ДОПОМІЖНІ ФУНКЦІЇ ЗБОРУ
# ═══════════════════════════════════════════════════════════════
def claim_daily(bot: Account) -> tuple[bool, dict[str, Any]]:
    log.info(f"[{bot.account_id}] 🎁 Збираємо звичайний бонус…")
    success, claimed = bot.session.claim_daily()
    return success, claimed

def claim_calendar(bot: Account, day: int) -> tuple[bool, dict[str, Any]]:
    log.info(f"[{bot.account_id}] 🎁 Збираємо календарний бонус (день {day})…")
    success, claimed = bot.session.claim_calendar(day)
    return success, claimed


# ═══════════════════════════════════════════════════════════════
# ACTION (Виконання плану)
# ═══════════════════════════════════════════════════════════════
def _make_claim_action(
    on_cycle_done: Callable[[Account], None], 
    stats: DailyRewardStats
) -> Callable[[Any, Account], None]:
    
    def action(plan: dict[str, Any], bot: Account) -> None:
        # Sentinel: роботи немає — просто завершуємо цикл
        if plan is _NOTHING_TO_DO:
            on_cycle_done(bot)
            return

        inv = bot.inventory.daily
        day = inv.day
        today = today_utc()

        # 1. Звичайний бонус
        if plan.get("do_daily"):
            success, result = claim_daily(bot)
            if success:
                inv.last_daily_claimed = today
                log.info(f"[{bot.account_id}] ✅ Звичайний бонус зібрано")
            stats.daily_results = result

        # 2. Календарний бонус
        if plan.get("do_calendar"):
            success, result = claim_calendar(bot, day)
            if success:
                inv.last_calendar_claimed = today
                inv.can_claim_calendar = False
                log.info(f"[{bot.account_id}] ✅ Календарний бонус зібрано")
            else:
                log.warning(f"[{bot.account_id}] 🎁 Помилка збору календаря — спробуємо наступного разу")
            stats.calendar_results = result  # зберігаємо результат для статистики
        on_cycle_done(bot)
    
    return action


# ═══════════════════════════════════════════════════════════════
# PRODUCER ТА ЗБІРКА ПРОФЕСІЇ
# ═══════════════════════════════════════════════════════════════
def make_daily_producer(
    trigger_ref: list[Any],
    stats:       DailyRewardStats,
) -> Callable[[Account], Iterable[AnyTask]]:

    def on_cycle_done(bot: Account) -> None:
        trigger = trigger_ref[0]
        if trigger is not None:
            trigger.advance(bot)

    action_fn = _make_claim_action(on_cycle_done, stats)

    daily_pipeline: Callable[[Account], Iterable[AnyTask]] = pipeline(
        name              = "daily_claimer",
        fetch             = fetch_bonus_status,
        parse             = [
            Step(parse_calendar_updates, priority=Priority.NORMAL, max_retries=1),
        ],
        action            = action_fn,
        max_parse_retries = 2,
    )
    return daily_pipeline

def build_daily_profession(bot: Account) -> tuple["Profession", DailyRewardStats]:
    from src.core.scheduling.conditions import has, not_
    from src.core.scheduling.profession import Profession

    trigger_ref: list[Any] = [None]
    stats = DailyRewardStats()
    account_id = bot.account_id
    producer = make_daily_producer(trigger_ref, stats)

    trigger = ScheduleDef(
        interval   = 86400,
        producer   = producer,
        at         = "04:30",
    ).to_trigger(account_id)
    
    trigger_ref[0] = trigger

    profession = Profession(
        name     = "daily_claimer",
        startup  = [],
        triggers = [trigger],
        guard    = not_(has("is_banned")),
    )
    return profession, stats