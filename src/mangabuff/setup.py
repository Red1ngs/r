"""
mangabuff/setup.py — реєстрація інвентарів, професій та моніторів.

Викликати ОДИН РАЗ при старті програми.
"""
from __future__ import annotations

from src.core.inventory.factory import inventory_factory
from src.core.runtime.profession import profession_factory
from src.core.stats import stats_factory
from src.core.monitoring.monitor import monitor_registry

# Інвентарі
from src.mangabuff.daily.inventory import DailyInventory
from src.mangabuff.quiz.inventory import QuizInventory
from src.mangabuff.manga_load.inventory import CatalogLoaderInventory, LoaderInventory
from src.mangabuff.reader.inventory import ReaderInventory
from src.mangabuff.alliance.inventory import AllianceInventory
from src.mangabuff.personal.inventory import PersonalInventory

# Професії
from src.mangabuff.manga_load.manga_loader import MangaLoaderProfession
from src.mangabuff.manga_load.catalog_loader import CatalogLoaderProfession
from src.mangabuff.reader.reader import ReaderProfession
from src.mangabuff.daily.daily_monitor import DailyMonitor
from src.mangabuff.daily.build import DailyProfession
from src.mangabuff.quiz.build import QuizProfession

# Монітори
from src.mangabuff.reader.reading_monitor import ReadingMonitor
from src.mangabuff.quiz.quiz_monitor import QuizMonitor

# Статистика
from src.mangabuff.daily.stats import DailyRewardStats

def register_inventories() -> None:
    inventory_factory.register("personal", "personal", PersonalInventory)
    inventory_factory.register("alliance", "alliance", AllianceInventory)
    inventory_factory.register("reader",   "reader",   ReaderInventory)
    inventory_factory.register("loader",   "loader",   LoaderInventory)
    inventory_factory.register("daily",    "daily",    DailyInventory)
    inventory_factory.register("quiz",     "quiz",     QuizInventory)
    inventory_factory.register("catalog_loader", "catalog_loader", CatalogLoaderInventory)


def register_professions() -> None:
    profession_factory.register("reader",         ReaderProfession)
    profession_factory.register("manga_loader",   MangaLoaderProfession)
    profession_factory.register("catalog_loader", CatalogLoaderProfession)
    profession_factory.register("daily",          DailyProfession)
    profession_factory.register("quiz",           QuizProfession)


def register_monitors() -> None:
    """
    Реєструє монітори в глобальному monitor_registry.

    Монітори підключаються до конкретних акаунтів пізніше через
    AccountMonitors.attach_all(scheduler, ["reading", ...]).
    """
    monitor_registry.register("reading", ReadingMonitor)
    monitor_registry.register("quiz", QuizMonitor)
    monitor_registry.register("daily", DailyMonitor)


def register_recorders() -> None:
    stats_factory.register("daily_rewards", "daily_rewards", DailyRewardStats)
