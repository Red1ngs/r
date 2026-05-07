"""
InventoryStore — зберігає і завантажує Inventories з БД.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from typing import Any

from src.core.inventory.factory import inventory_factory
from src.core.inventory.model import BaseInventory, DynamicInventories

log = logging.getLogger(__name__)


class InventoryStore:
    def __init__(self, conn: sqlite3.Connection, account_id: str):
        self._conn       = conn
        self._account_id = account_id

    # ------------------------------------------------------------------
    # Завантаження
    # ------------------------------------------------------------------

    def load(self) -> DynamicInventories:
        inventories = inventory_factory.build()
        registry    = inventory_factory.registry

        # Статистика → PersonalInventory
        personal_entry = registry.get("personal")
        if personal_entry is not None:
            personal_attr, _ = personal_entry
            personal = getattr(inventories, personal_attr, None)
            if personal is not None:
                row = self._conn.execute(
                    "SELECT comments_written, trades_accepted, trades_declined "
                    "FROM accounts WHERE id = ?",
                    (self._account_id,),
                ).fetchone()
                if row:
                    personal.comments_written = row["comments_written"]
                    personal.trades_accepted  = row["trades_accepted"]
                    personal.trades_declined  = row["trades_declined"]

        # JSON-дані для кожного зареєстрованого kind
        rows = self._conn.execute(
            "SELECT kind, data FROM inventory WHERE account_id = ?",
            (self._account_id,),
        ).fetchall()

        for row in rows:
            entry = registry.get(row["kind"])
            if entry is None:
                continue
            attr, _ = entry
            inv: BaseInventory = getattr(inventories, attr)
            inv.data = json.loads(row["data"])

        # Незакриті trade-події → PersonalInventory.pending_trades
        if personal_entry is not None:
            personal_attr, _ = personal_entry
            personal = getattr(inventories, personal_attr, None)
            if personal is not None:
                trade_rows = self._conn.execute(
                    "SELECT payload FROM events "
                    "WHERE account_id = ? AND kind = 'trade' AND status = 'pending' "
                    "ORDER BY created_at",
                    (self._account_id,),
                ).fetchall()
                personal.pending_trades = [
                    json.loads(r["payload"]) for r in trade_rows
                ]
                if personal.pending_trades:
                    log.info(
                        f"[{self._account_id}] Відновлено "
                        f"{len(personal.pending_trades)} незакритих заявок"
                    )

        return inventories

    # ------------------------------------------------------------------
    # Збереження
    # ------------------------------------------------------------------

    def save(self, inventories: DynamicInventories) -> None:
        """Зберігає всі інвентарі. Викликається після кожної задачі."""
        registry = inventory_factory.registry

        personal_entry = registry.get("personal")
        if personal_entry is not None:
            personal_attr, _ = personal_entry
            personal = getattr(inventories, personal_attr, None)
            if personal is not None:
                self._save_stats(personal)

        for kind, (attr, _) in registry.items():
            inv: BaseInventory = getattr(inventories, attr)
            self._upsert_kind(kind, inv.data)

        self._conn.commit()

    def save_kind(self, inventories: DynamicInventories, kind: str) -> None:
        """Зберігає тільки один тип інвентаря."""
        entry = inventory_factory.get(kind)
        if entry is None:
            raise ValueError(f"Невідомий тип інвентаря: {kind!r}")
        attr, _ = entry
        inv: BaseInventory = getattr(inventories, attr)
        if kind == "personal":
            self._save_stats(inv)
        self._upsert_kind(kind, inv.data)
        self._conn.commit()

    def _save_stats(self, personal: Any) -> None:
        self._conn.execute(
            """
            UPDATE accounts
            SET comments_written = ?,
                trades_accepted  = ?,
                trades_declined  = ?
            WHERE id = ?
            """,
            (
                personal.comments_written,
                personal.trades_accepted,
                personal.trades_declined,
                self._account_id,
            ),
        )

    def _upsert_kind(self, kind: str, data: dict[str, Any]) -> None:
        self._conn.execute(
            """
            INSERT INTO inventory (account_id, kind, data) VALUES (?, ?, ?)
            ON CONFLICT(account_id, kind) DO UPDATE SET data = excluded.data
            """,
            (self._account_id, kind, json.dumps(data, ensure_ascii=False)),
        )

    # ------------------------------------------------------------------
    # Події
    # ------------------------------------------------------------------

    def persist_trade(self, trade: dict[str, Any]) -> None:
        self._conn.execute(
            "INSERT INTO events (account_id, kind, payload) VALUES (?, 'trade', ?)",
            (self._account_id, json.dumps(trade, ensure_ascii=False)),
        )
        self._conn.commit()

    def resolve_trade(self, trade_id: str) -> None:
        self._conn.execute(
            """
            UPDATE events SET status = 'done'
            WHERE account_id = ?
              AND kind = 'trade'
              AND json_extract(payload, '$.trade_id') = ?
            """,
            (self._account_id, trade_id),
        )
        self._conn.commit()