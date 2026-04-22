"""
InventoryStore — зберігає і завантажує Inventories з БД.

Кожен тип інвентаря — окремий рядок в таблиці inventory (account_id, kind).
Статистика PersonalInventory — окремі колонки в таблиці accounts.
"""
from __future__ import annotations

import json
import logging
import sqlite3

from src.core.inventory.model import (
    INVENTORY_REGISTRY,
    BaseInventory,
    Inventories,
    PersonalInventory,
)

log = logging.getLogger(__name__)


class InventoryStore:
    def __init__(self, conn: sqlite3.Connection, account_id: str):
        self._conn       = conn
        self._account_id = account_id

    # ------------------------------------------------------------------
    # Завантаження
    # ------------------------------------------------------------------

    def load(self) -> Inventories:
        inventories = Inventories()

        # Статистика з accounts → PersonalInventory
        row = self._conn.execute(
            "SELECT comments_written, trades_accepted, trades_declined "
            "FROM accounts WHERE id = ?",
            (self._account_id,),
        ).fetchone()
        if row:
            inventories.personal.comments_written = row["comments_written"]
            inventories.personal.trades_accepted  = row["trades_accepted"]
            inventories.personal.trades_declined  = row["trades_declined"]

        # JSON-дані для кожного зареєстрованого типу
        rows = self._conn.execute(
            "SELECT kind, data FROM inventory WHERE account_id = ?",
            (self._account_id,),
        ).fetchall()

        for row in rows:
            entry = INVENTORY_REGISTRY.get(row["kind"])
            if entry is None:
                continue
            attr, _ = entry
            inv: BaseInventory = getattr(inventories, attr)
            inv.data = json.loads(row["data"])

        # Незакриті trade-події
        trade_rows = self._conn.execute(
            "SELECT payload FROM events "
            "WHERE account_id = ? AND kind = 'trade' AND status = 'pending' "
            "ORDER BY created_at",
            (self._account_id,),
        ).fetchall()
        inventories.personal.pending_trades = [
            json.loads(r["payload"]) for r in trade_rows
        ]

        if inventories.personal.pending_trades:
            log.info(
                f"[{self._account_id}] Відновлено "
                f"{len(inventories.personal.pending_trades)} незакритих заявок"
            )

        return inventories

    # ------------------------------------------------------------------
    # Збереження
    # ------------------------------------------------------------------

    def save(self, inventories: Inventories) -> None:
        """Зберігає всі інвентарі. Викликається після кожної задачі."""
        self._save_stats(inventories.personal)
        for kind, (attr, _) in INVENTORY_REGISTRY.items():
            inv: BaseInventory = getattr(inventories, attr)
            self._upsert_kind(kind, inv.data)
        self._conn.commit()

    def save_kind(self, inventories: Inventories, kind: str) -> None:
        """Зберігає тільки один тип інвентаря — для часткового оновлення."""
        entry = INVENTORY_REGISTRY.get(kind)
        if entry is None:
            raise ValueError(f"Невідомий тип інвентаря: {kind!r}")
        attr, _ = entry
        inv: BaseInventory = getattr(inventories, attr)
        if kind == "personal":
            self._save_stats(inventories.personal)
        self._upsert_kind(kind, inv.data)
        self._conn.commit()

    def _save_stats(self, personal: PersonalInventory) -> None:
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

    def _upsert_kind(self, kind: str, data: dict) -> None:
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

    def persist_trade(self, trade: dict) -> None:
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