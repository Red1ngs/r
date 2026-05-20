from __future__ import annotations

import sqlite3
from pathlib import Path

DDL = """
-- Існуючі таблиці (Accounts, Inventory, Events)
CREATE TABLE IF NOT EXISTS accounts (
    id                TEXT PRIMARY KEY,
    email             TEXT NOT NULL UNIQUE,
    profession        TEXT,
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

CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id  TEXT    NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    kind        TEXT    NOT NULL,
    status      TEXT    NOT NULL DEFAULT 'pending',
    payload     TEXT    NOT NULL DEFAULT '{}',
    created_at  TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at  TEXT    NOT NULL DEFAULT (datetime('now'))
);

-- ==========================================
-- НОВІ ТАБЛИЦІ ДЛЯ МАНГИ
-- ==========================================

CREATE TABLE IF NOT EXISTS mangas (
    id            INTEGER PRIMARY KEY AUTOINCREMENT, -- Внутрішній ID манги
    data_id       INTEGER NOT NULL UNIQUE,           -- ID з data-id, зовнішнє
    translit_name TEXT    NOT NULL,
    name          TEXT    NOT NULL,
    rating        TEXT    NOT NULL DEFAULT '',
    info          TEXT    NOT NULL DEFAULT '',
    image         TEXT    NOT NULL DEFAULT '',
    created_at    TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at    TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS chapters (
    id           INTEGER PRIMARY KEY AUTOINCREMENT, -- Внутрішній ID глави
    data_id      INTEGER NOT NULL UNIQUE,           -- Оригінальний ID глави (з сайту)
    manga_id     INTEGER NOT NULL REFERENCES mangas(id) ON DELETE CASCADE, -- Внутрішній ID манги
    chapter_num  REAL    NOT NULL, -- Номер глави (дробовий, напр. 10.5)
    volume       INTEGER NOT NULL, -- Номер тому 
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

-- ==========================================
-- ІНДЕКСИ ДЛЯ ОПТИМІЗАЦІЇ
-- ==========================================

CREATE INDEX IF NOT EXISTS idx_events_pending ON events(account_id, kind, status);

-- Індекс для швидкого пошуку глави за ID манги та сортування за номером
CREATE INDEX IF NOT EXISTS idx_chapters_manga_lookup 
    ON chapters(manga_id, chapter_num);

-- Індекс для швидкого пошуку непрочитаних
CREATE INDEX IF NOT EXISTS idx_account_reads_lookup ON account_reads(account_id);

-- Індекс для пошуку манги за її транслітерацією
CREATE INDEX IF NOT EXISTS idx_mangas_translit_name ON mangas(translit_name);

-- ==========================================
-- ТРИГЕРИ ДЛЯ ОНОВЛЕННЯ ЧАСУ (updated_at)
-- ==========================================

CREATE TRIGGER IF NOT EXISTS trg_accounts_updated AFTER UPDATE ON accounts BEGIN
    UPDATE accounts SET updated_at = datetime('now') WHERE id = NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS trg_inventory_updated AFTER UPDATE ON inventory BEGIN
    UPDATE inventory SET updated_at = datetime('now') WHERE account_id = NEW.account_id AND kind = NEW.kind;
END;

CREATE TRIGGER IF NOT EXISTS trg_events_updated AFTER UPDATE ON events BEGIN
    UPDATE events SET updated_at = datetime('now') WHERE id = NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS trg_mangas_updated AFTER UPDATE ON mangas BEGIN
    UPDATE mangas SET updated_at = datetime('now') WHERE id = NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS trg_chapters_updated AFTER UPDATE ON chapters BEGIN
    UPDATE chapters SET updated_at = datetime('now') WHERE id = NEW.id;
END;
"""

def get_db(path: str | Path = "bot_state.db") -> sqlite3.Connection:
    conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(DDL)
    conn.commit()
    return conn