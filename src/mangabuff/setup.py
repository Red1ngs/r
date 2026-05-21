"""
mangabuff/setup.py — реєстрація інвентарів та професій.

Викликати ОДИН РАЗ при старті програми.
"""
from __future__ import annotations

from src.core.inventory.factory import inventory_factory
from src.core.runtime.profession import profession_factory
from src.core.tasks.stats import stats_factory

# Інвентарі
from src.mangabuff.daily.inventory import DailyInventory
from src.mangabuff.reader.inventory import ReaderInventory
from src.mangabuff.alliance.inventory import AllianceInventory
from src.mangabuff.personal.inventory import PersonalInventory

# Професії (Білдери)
from src.mangabuff.reader.build import ReaderProfession
from src.mangabuff.daily.build import DailyProfession

# Статистика
from src.mangabuff.daily.stats import DailyRewardStats

def register_inventories() -> None:
    inventory_factory.register("personal", "personal", PersonalInventory)
    inventory_factory.register("alliance", "alliance", AllianceInventory)
    inventory_factory.register("reader",   "reader",   ReaderInventory)
    inventory_factory.register("daily",    "daily",    DailyInventory)

def register_professions() -> None:
    profession_factory.register("reader", ReaderProfession)
    profession_factory.register("daily", DailyProfession)

def register_recorders() -> None:
    stats_factory.register("daily_rewards", "daily_rewards", DailyRewardStats)