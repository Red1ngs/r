from __future__ import annotations

import sqlite3
import threading
from typing import Any, Optional

from src.database.DTO.account import AccountRow


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
            "SELECT * FROM accounts WHERE id = ?",
            (account_id,),
        ).fetchone()
        return self._to_model(row) if row else None
    
    def get_all_accounts(self) -> dict[str, str]:
        return dict(self._conn.execute("SELECT id, email FROM accounts").fetchall())

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    def upsert(
        self,
        account_id: str,
        email:      str,
        profession: Optional[str] = None,
    ) -> None:
        """Create or update an account."""
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO accounts (id, email, profession)
                VALUES (?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    email      = excluded.email,
                    profession = excluded.profession
                """,
                (account_id, email, profession),
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
            SELECT id, email, profession, updated_at
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
            profession=row["profession"],
            updated_at=row["updated_at"],
        )