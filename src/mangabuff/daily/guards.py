# mangabuff/daily/guards.py

from typing import TYPE_CHECKING
from src.core.runtime.scheduler import EventDrivenScheduler
from src.utils.time import today

if TYPE_CHECKING:
    from src.core.account import Account
    
def wait_for_daily(bot: "Account") -> bool:
    """
    True = треба чекати daily.claimed перед початком роботи.
    False = можна працювати (daily немає або вже зібрано).
    """
    scheduler = EventDrivenScheduler.get_instance()
    if not scheduler.has_profession(bot.account_id, "daily"):
        return False

    daily_inv = getattr(bot.inventory, "daily", None)
    return daily_inv is None or daily_inv.last_daily_claimed != today()