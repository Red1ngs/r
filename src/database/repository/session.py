"""
database/repository/session.py — сховище cookie-сесій.

Таблиця `sessions` зберігає серіалізовані cookies кожного акаунта.
Це дозволяє після рестарту бота відновити сесію без повторного логіну
(новий логін — підозріла активність, яку легше детектувати).

Контракт:
  save(account_id, cookies)   — зберегти/оновити cookies після успішного логіну
  load(account_id) → dict     — завантажити cookies (порожній dict якщо немає)
  invalidate(account_id)      — позначити сесію як недійсну (після 401/419)
  is_valid(account_id) → bool — швидка перевірка без завантаження cookies

Логіка «сесія прострочена»:
  Сервер може інвалідувати сесію у будь-який момент.
  BotTransport при 419 викликає login(force=True) → після успішного
  ре-логіну треба знову викликати session_repo.save().
  Якщо login провалився — session_repo.invalidate() щоб не використовувати
  протухлі cookies при наступному старті.

Схема БД (додається міграцією в ddl.py):
  CREATE TABLE sessions (
      account_id  TEXT PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
      cookies     TEXT NOT NULL DEFAULT '{}',   -- JSON dict
      is_valid    INTEGER NOT NULL DEFAULT 1,   -- 0 = протухла
      saved_at    TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
  );
"""
from __future__ import annotations

import json
import sqlite3
import threading
from typing import Any

from src.core.logging.loggers import get_logger

log = get_logger("db.session")


class SessionRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn
        self._lock = threading.Lock()

    # ── Write ─────────────────────────────────────────────────────────────────

    def save(self, account_id: str, cookies: dict[str, str]) -> None:
        """
        Зберігає актуальні cookies після успішної авторизації.
        Позначає сесію як is_valid=1.
        """
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO sessions (account_id, cookies, is_valid, saved_at, updated_at)
                VALUES (?, ?, 1, datetime('now'), datetime('now'))
                ON CONFLICT(account_id) DO UPDATE SET
                    cookies    = excluded.cookies,
                    is_valid   = 1,
                    saved_at   = datetime('now'),
                    updated_at = datetime('now')
                """,
                (account_id, json.dumps(cookies, ensure_ascii=False)),
            )
            self._conn.commit()
        log.debug(f"[{account_id}] session saved ({len(cookies)} cookies)")
        
    def save_browser(self, account_id: str, browser_dict: dict[str, str]) -> None:
        """Зберігає згенерований відбиток браузера у сесію."""
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO sessions (account_id, cookies, browser, is_valid, saved_at, updated_at)
                VALUES (?, '{}', ?, 1, datetime('now'), datetime('now'))
                ON CONFLICT(account_id) DO UPDATE SET
                    browser    = excluded.browser,
                    updated_at = datetime('now')
                """,
                (account_id, json.dumps(browser_dict, ensure_ascii=False)),
            )
            self._conn.commit()
        log.debug(f"[{account_id}] browser fingerprint saved")

    def invalidate(self, account_id: str) -> None:
        """
        Позначає сесію як недійсну (is_valid=0).
        Викликати при провалі логіну або явному розлогіненні.
        Cookies залишаються в БД — зручно для дебагу.
        """
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO sessions (account_id, cookies, is_valid, saved_at, updated_at)
                VALUES (?, '{}', 0, datetime('now'), datetime('now'))
                ON CONFLICT(account_id) DO UPDATE SET
                    is_valid   = 0,
                    updated_at = datetime('now')
                """,
                (account_id,),
            )
            self._conn.commit()
        log.debug(f"[{account_id}] session invalidated")

    # ── Read ──────────────────────────────────────────────────────────────────

    def load(self, account_id: str) -> dict[str, str]:
        """
        Повертає cookies якщо сесія is_valid=1.
        Порожній dict якщо запису немає або сесія інвалідована.
        """
        row = self._conn.execute(
            "SELECT cookies, is_valid FROM sessions WHERE account_id = ?",
            (account_id,),
        ).fetchone()

        if row is None:
            log.debug(f"[{account_id}] no saved session")
            return {}

        if not row["is_valid"]:
            log.debug(f"[{account_id}] saved session is invalidated — skip")
            return {}

        try:
            cookies: dict[str, Any] = json.loads(row["cookies"])
            log.debug(f"[{account_id}] loaded session ({len(cookies)} cookies)")
            return {k: str(v) for k, v in cookies.items()}
        except (json.JSONDecodeError, TypeError) as e:
            log.warning(f"[{account_id}] failed to parse session cookies: {e}")
            return {}
        
    def load_browser(self, account_id: str) -> dict[str, str] | None:
        """Завантажує відбиток браузера з БД (якщо є)."""
        row = self._conn.execute(
            "SELECT browser FROM sessions WHERE account_id = ?",
            (account_id,),
        ).fetchone()

        if row and row["browser"] != '{}':
            try:
                return json.loads(row["browser"])
            except (json.JSONDecodeError, TypeError):
                pass
        return None

    def is_valid(self, account_id: str) -> bool:
        """Швидка перевірка без десеріалізації cookies."""
        row = self._conn.execute(
            "SELECT is_valid FROM sessions WHERE account_id = ?",
            (account_id,),
        ).fetchone()
        return bool(row and row["is_valid"])

    def get_saved_at(self, account_id: str) -> str | None:
        """Повертає час збереження сесії (UTC ISO) або None."""
        row = self._conn.execute(
            "SELECT saved_at FROM sessions WHERE account_id = ?",
            (account_id,),
        ).fetchone()
        return row["saved_at"] if row else None
