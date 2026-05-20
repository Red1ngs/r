from __future__ import annotations
import json
import logging
import sqlite3
from typing import Any

from src.core.inventory.factory import inventory_factory
from src.core.inventory.model import DynamicInventories

log = logging.getLogger(__name__)

class InventoryRepository: # Перейменували для відповідності стилю
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def load(self, account_id: str) -> DynamicInventories:
        """Завантажує інвентар для конкретного акаунта."""
        inventories = inventory_factory.build()
        registry = inventory_factory.registry

        rows = self._conn.execute(
            "SELECT kind, data FROM inventory WHERE account_id = ?",
            (account_id,),
        ).fetchall()

        for row in rows:
            entry = registry.get(row["kind"])
            if entry:
                attr, _ = entry
                inv = getattr(inventories, attr)
                inv.data = json.loads(row["data"])
        return inventories

    def save(self, account_id: str, inventories: DynamicInventories) -> None:
        """Зберігає весь об'єкт інвентарю в БД."""
        registry = inventory_factory.registry
        for kind, (attr, _) in registry.items():
            inv = getattr(inventories, attr)
            self._upsert_kind(account_id, kind, inv.data)
        self._conn.commit()

    def _upsert_kind(self, account_id: str, kind: str, data: dict[str, Any]) -> None:
        self._conn.execute(
            """
            INSERT INTO inventory (account_id, kind, data) VALUES (?, ?, ?)
            ON CONFLICT(account_id, kind) DO UPDATE SET data = excluded.data
            """,
            (account_id, kind, json.dumps(data, ensure_ascii=False)),
        )