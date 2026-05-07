"""
mangabuff/setup.py — реєстрація інвентарів.

Викликати ОДИН РАЗ при старті, до будь-якого store.load() або build().
"""
from __future__ import annotations

from src.core.inventory.factory import inventory_factory
from src.mangabuff.reader.inventory import (
    AllianceInventory,
    PersonalInventory,
    ReaderInventory,
)


def register_inventories() -> None:
    inventory_factory.register("personal", "personal", PersonalInventory)
    inventory_factory.register("alliance", "alliance", AllianceInventory)
    inventory_factory.register("reader",   "reader",   ReaderInventory)