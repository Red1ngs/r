"""
Daily bonus — triggered by scheduler.every().

Зберігає дату останнього отримання в personal inventory.
Пропускає якщо бонус вже отримано сьогодні (UTC-дата, не просто 24г).
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from src.utils.time import today_utc
from src.core.tasks.base import Priority, Task

if TYPE_CHECKING:
    from src.core.account import AccountPull

# Ключ у personal.data
_LAST_CLAIMED_KEY = "last_bonus_claimed"   # зберігаємо "YYYY-MM-DD" UTC


def _claim_bonus(bot: "AccountPull") -> None:
    last_claimed = bot.inventory.personal.get(_LAST_CLAIMED_KEY, "")
    today        = today_utc()

    # Порівнюємо UTC-дати — не вразливо до перезапуску бота
    if last_claimed == today:
        logging.info("🎁 Daily bonus already collected today, skipping")
        return

    day = bot.session.fetch_daily_streak()
    if day is None:
        logging.info("🎁 No daily bonus available right now")
        return

    logging.info(f"🎁 Claiming daily bonus (day {day})…")
    claimed = bot.session.claim_daily(day)
    if not claimed:
        logging.warning("🎁 Claim request failed — will retry next run")
        return

    bot.inventory.personal.set(_LAST_CLAIMED_KEY, today)
    logging.info(f"✅ Daily bonus collected (day {day})")


def daily_bonus(bot: "AccountPull"):
    yield Task(
        name="claim_daily_bonus",
        fn=_claim_bonus,
        priority=Priority.LOW,
        max_retries=2,
    )