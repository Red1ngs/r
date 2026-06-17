from __future__ import annotations

import sqlite3
from pathlib import Path

DDL = """
-- Accounts
CREATE TABLE IF NOT EXISTS accounts (
    id                TEXT PRIMARY KEY,
    email             TEXT NOT NULL UNIQUE,
    -- JSON array: '["reader","daily"]'  (замінює старе TEXT profession)
    professions       TEXT NOT NULL DEFAULT '[]',
    created_at        TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at        TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS inventory (
    account_id  TEXT    NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    kind        TEXT    NOT NULL,
    data        TEXT    NOT NULL DEFAULT '{}',
    updated_at  TEXT    NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (account_id, kind)
);

CREATE TABLE IF NOT EXISTS sessions (
    account_id  TEXT    PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
    cookies     TEXT    NOT NULL DEFAULT '{}',
    browser     TEXT    NOT NULL DEFAULT '{}',
    is_valid    INTEGER NOT NULL DEFAULT 1,
    saved_at    TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at  TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id  TEXT    NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    kind        TEXT    NOT NULL,
    status      TEXT    NOT NULL DEFAULT 'pending',
    payload     TEXT    NOT NULL DEFAULT '{}',
    created_at  TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at  TEXT    NOT NULL DEFAULT (datetime('now'))
);

-- Manga
CREATE TABLE IF NOT EXISTS mangas (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    data_id       INTEGER NOT NULL UNIQUE,
    translit_name TEXT    NOT NULL,
    name          TEXT    NOT NULL,
    rating        TEXT    NOT NULL DEFAULT '',
    info          TEXT    NOT NULL DEFAULT '',
    image         TEXT    NOT NULL DEFAULT '',
    views         INTEGER NOT NULL DEFAULT 0,
    created_at    TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at    TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS chapters (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    data_id      INTEGER NOT NULL UNIQUE,
    manga_id     INTEGER NOT NULL REFERENCES mangas(id) ON DELETE CASCADE,
    chapter_num  REAL    NOT NULL,
    volume       INTEGER NOT NULL,
    date         TEXT,
    created_at   TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at   TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS account_reads (
    account_id  TEXT    NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    chapter_id  INTEGER NOT NULL REFERENCES chapters(id) ON DELETE CASCADE,
    read_at     TEXT    NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (account_id, chapter_id)
);

-- Індекси
CREATE INDEX IF NOT EXISTS idx_events_pending ON events(account_id, kind, status);
CREATE INDEX IF NOT EXISTS idx_chapters_manga_lookup ON chapters(manga_id, chapter_num);
CREATE INDEX IF NOT EXISTS idx_account_reads_lookup ON account_reads(account_id);
CREATE INDEX IF NOT EXISTS idx_mangas_translit_name ON mangas(translit_name);

-- Тригери updated_at
CREATE TRIGGER IF NOT EXISTS trg_accounts_updated AFTER UPDATE ON accounts BEGIN
    UPDATE accounts SET updated_at = datetime('now') WHERE id = NEW.id;
END;
CREATE TRIGGER IF NOT EXISTS trg_inventory_updated AFTER UPDATE ON inventory BEGIN
    UPDATE inventory SET updated_at = datetime('now')
    WHERE account_id = NEW.account_id AND kind = NEW.kind;
END;
CREATE TRIGGER IF NOT EXISTS trg_events_updated AFTER UPDATE ON events BEGIN
    UPDATE events SET updated_at = datetime('now') WHERE id = NEW.id;
END;
CREATE TRIGGER IF NOT EXISTS trg_sessions_updated AFTER UPDATE ON sessions BEGIN
    UPDATE sessions SET updated_at = datetime('now') WHERE account_id = NEW.account_id;
END;

CREATE TRIGGER IF NOT EXISTS trg_mangas_updated AFTER UPDATE ON mangas BEGIN
    UPDATE mangas SET updated_at = datetime('now') WHERE id = NEW.id;
END;
CREATE TRIGGER IF NOT EXISTS trg_chapters_updated AFTER UPDATE ON chapters BEGIN
    UPDATE chapters SET updated_at = datetime('now') WHERE id = NEW.id;
END;
"""

# ─────────────────────────────────────────────────────────────────────────────
# Migration: profession TEXT  →  professions TEXT (JSON array)
# ─────────────────────────────────────────────────────────────────────────────
_MIGRATION_ADD_PROFESSIONS = """
-- Перевіряємо чи є стара колонка 'profession' і переносимо дані.
-- SQLite не підтримує DROP COLUMN до 3.35, тому залишаємо сумісність.
"""

def _apply_migrations(conn: sqlite3.Connection) -> None:
    """
    Ідемпотентні міграції схеми.
    Виконуються після CREATE TABLE IF NOT EXISTS, тому безпечні для нових БД.
    """
    cols = {row[1] for row in conn.execute("PRAGMA table_info(accounts)")}

    # Міграція: додаємо колонку views до mangas якщо відсутня
    manga_cols = {row[1] for row in conn.execute("PRAGMA table_info(mangas)")}
    if "views" not in manga_cols:
        conn.execute("ALTER TABLE mangas ADD COLUMN views INTEGER NOT NULL DEFAULT 0")
        conn.commit()

    # Стара схема мала 'profession TEXT' (одиничну)
    if "profession" in cols and "professions" not in cols:
        conn.execute("ALTER TABLE accounts ADD COLUMN professions TEXT NOT NULL DEFAULT '[]'")
        # Переносимо наявні дані: profession → ["profession"]
        conn.execute("""
            UPDATE accounts
            SET professions = json_array(profession)
            WHERE profession IS NOT NULL AND profession != ''
        """)
        conn.commit()

    # Якщо новій схемі bракує колонки (чиста БД вже має professions через DDL)
    elif "professions" not in cols:
        conn.execute("ALTER TABLE accounts ADD COLUMN professions TEXT NOT NULL DEFAULT '[]'")
        conn.commit()
    
    session_cols = {row[1] for row in conn.execute("PRAGMA table_info(sessions)")}
    if "browser" not in session_cols:
        conn.execute("ALTER TABLE sessions ADD COLUMN browser TEXT NOT NULL DEFAULT '{}'")
        conn.commit()


def get_db(path: str | Path = "bot_state.db") -> sqlite3.Connection:
    conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(DDL)
    conn.commit()
    _apply_migrations(conn)
    return conn
