from __future__ import annotations

import json
import sqlite3
import threading
from typing import Any, Optional

from src.core.inventory.model import INVENTORY_REGISTRY
from src.core.database.DTO.account import AccountRow


class AccountRepository:
    """
    CRUD for accounts + inventory tables.

    Thread-safe — all writes are under a lock.
    InventoryStore accesses the connection directly for performance;
    this class is the external-facing API.
    """

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def get(self, account_id: str) -> Optional[AccountRow]:
        row = self._conn.execute(
            """
            SELECT a.*, COALESCE(i.data, '{}') AS inv_data
            FROM accounts a
            LEFT JOIN inventory i ON i.account_id = a.id AND i.kind = 'personal'
            WHERE a.id = ?
            """,
            (account_id,),
        ).fetchone()
        return self._to_model(row) if row else None

    def get_all_active(self) -> list[AccountRow]:
        rows = self._conn.execute(
            """
            SELECT a.*, COALESCE(i.data, '{}') AS inv_data
            FROM accounts a
            LEFT JOIN inventory i ON i.account_id = a.id AND i.kind = 'personal'
            WHERE a.is_active = 1
            ORDER BY a.id
            """
        ).fetchall()
        return [self._to_model(r) for r in rows]

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    def upsert(
        self,
        account_id: str,
        email:      str,
        base_url:   str,
        profession: Optional[str] = None,
    ) -> None:
        """Create or update an account. Seeds default inventory rows."""
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO accounts (id, email, base_url, profession)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    email      = excluded.email,
                    base_url   = excluded.base_url,
                    profession = excluded.profession
                """,
                (account_id, email, base_url, profession),
            )
            # Seed one inventory row per registered kind.
            # The old code did INSERT OR IGNORE with no `kind` column,
            # which violates the NOT NULL constraint on kind.
            for kind in INVENTORY_REGISTRY:
                self._conn.execute(
                    """
                    INSERT OR IGNORE INTO inventory (account_id, kind, data)
                    VALUES (?, ?, '{}')
                    """,
                    (account_id, kind),
                )
            self._conn.commit()

    def set_active(self, account_id: str, active: bool) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE accounts SET is_active = ? WHERE id = ?",
                (int(active), account_id),
            )
            self._conn.commit()

    def set_profession(self, account_id: str, profession: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE accounts SET profession = ? WHERE id = ?",
                (profession, account_id),
            )
            self._conn.commit()

    # ------------------------------------------------------------------
    # Admin / monitoring
    # ------------------------------------------------------------------

    def summary(self) -> list[dict[str, Any]]:
        """Snapshot of all accounts for the monitoring view."""
        rows = self._conn.execute(
            """
            SELECT id, email, profession, is_active,
                   comments_written, trades_accepted, trades_declined,
                   updated_at
            FROM accounts
            ORDER BY id
            """
        ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------

    @staticmethod
    def _to_model(row: sqlite3.Row) -> AccountRow:
        return AccountRow(
            id=row["id"],
            email=row["email"],
            base_url=row["base_url"],
            profession=row["profession"],
            is_active=bool(row["is_active"]),
            comments_written=row["comments_written"],
            trades_accepted=row["trades_accepted"],
            trades_declined=row["trades_declined"],
            inventory=json.loads(row["inv_data"]),
        )