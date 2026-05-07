from __future__ import annotations

import sqlite3
import threading
from typing import Any, Optional

from src.database.DTO.manga import ChapterRow, MangaRow


class MangaRepository:
    """Керування даними манг у БД."""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn
        self._lock = threading.Lock()

    def get_by_data_id(self, data_id: int) -> Optional[MangaRow]:
        """Отримує мангу за її зовнішнім числовим ID (з сайту)."""
        row = self._conn.execute(
            "SELECT * FROM mangas WHERE data_id = ?", (data_id,)
        ).fetchone()
        return self._to_model(row) if row else None

    def get_by_translit_name(self, translit_name: str) -> Optional[MangaRow]:
        """Отримує мангу за її рядковим ID (з сайту)."""
        row = self._conn.execute(
            "SELECT * FROM mangas WHERE translit_name = ?", (translit_name,)
        ).fetchone()
        return self._to_model(row) if row else None

    def get_stale_mangas(self, days: int = 3, limit: int = 5) -> list[MangaRow]:
        """
        Повертає список манг, які не оновлювалися вказану кількість днів.
        Використовується для перевірки наявності нових глав на сайті.
        """
        rows = self._conn.execute(
            f"""
            SELECT * FROM mangas 
            WHERE datetime(updated_at) <= datetime('now', '-{days} days')
            ORDER BY updated_at ASC
            LIMIT ?
            """,
            (limit,)
        ).fetchall()
        return [self._to_model(r) for r in rows]

    def upsert(
        self,
        data_id: int,
        translit_name: str,
        name: str,
        rating: str = "",
        info: str = "",
        image: str = ""
    ) -> int:
        """Створює або оновлює мангу. Повертає внутрішній ID БД (id)."""
        with self._lock:
            cursor = self._conn.execute(
                """
                INSERT INTO mangas (data_id, translit_name, name, rating, info, image)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(data_id) DO UPDATE SET
                    translit_name   = excluded.translit_name,
                    name            = excluded.name,
                    rating          = excluded.rating,
                    info            = excluded.info,
                    image           = excluded.image
                RETURNING id
                """,
                (data_id, translit_name, name, rating, info, image),
            )
            res = cursor.fetchone()
            self._conn.commit()
            return res["id"]

    @staticmethod
    def _to_model(row: sqlite3.Row) -> MangaRow:
        return MangaRow(**dict(row))


class ChapterRepository:
    """Керування главами та історією їх прочитань."""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn
        self._lock = threading.Lock()

    def get_chapter_sequence(
        self,
        account_id: str,
        limit: int,
        include_tags: Optional[list[str]] = None,
        exclude_tags: Optional[list[str]] = None
    ) -> tuple[list[dict[str, Any]], list[str]]:
        """
        Шукає глави, які конкретний акаунт ЩЕ НЕ ЧИТАВ.
        Повертає зовнішні data_id для манги та глав (для HTTP-запитів до сайту).
        """
        query = """
            SELECT
                m.translit_name,
                m.data_id  AS manga_data_id,
                c.data_id  AS chapter_data_id
            FROM chapters c
            JOIN mangas m ON c.manga_id = m.id          -- manga_id — внутрішній FK
            LEFT JOIN account_reads ar
                ON ar.chapter_id = c.id AND ar.account_id = ?
            WHERE ar.chapter_id IS NULL
        """
        params: list[Any] = [account_id]

        if include_tags:
            for tag in include_tags:
                query += " AND (m.name LIKE ? OR m.info LIKE ?)"
                params.extend([f"%{tag}%", f"%{tag}%"])

        if exclude_tags:
            for tag in exclude_tags:
                query += " AND (m.name NOT LIKE ? AND m.info NOT LIKE ?)"
                params.extend([f"%{tag}%", f"%{tag}%"])

        query += """
            ORDER BY c.manga_id ASC, c.chapter_num ASC, c.id ASC
            LIMIT ?
        """
        params.append(limit)

        rows = self._conn.execute(query, tuple(params)).fetchall()

        sequence: list[dict[str, Any]] = []
        mangas_set: set[str] = set()

        for row in rows:
            sequence.append({
                "manga_id":   row["manga_data_id"],    # зовнішній data_id манги (для сайту)
                "chapter_id": row["chapter_data_id"],  # зовнішній data_id глави (для сайту)
            })
            mangas_set.add(row["translit_name"])

        return sequence, list(mangas_set)

    def mark_chapter_read(self, account_id: str, chapter_data_id: int) -> None:
        """
        Записує главу в історію як прочитану для даного акаунта.
        Приймає зовнішній data_id глави — резолвить у внутрішній id самостійно.
        """
        with self._lock:
            self._conn.execute(
                """
                INSERT OR IGNORE INTO account_reads (account_id, chapter_id)
                SELECT ?, id FROM chapters WHERE data_id = ?
                """,
                (account_id, chapter_data_id)
            )
            self._conn.commit()

    def upsert(
        self,
        data_id: int,
        manga_id: int,       # внутрішній id манги (mangas.id)
        chapter_num: float,
        volume: int,
        date: Optional[str] = None
    ) -> None:
        """Додає або оновлює одну главу."""
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO chapters (data_id, manga_id, chapter_num, volume, date)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(data_id) DO UPDATE SET
                    chapter_num = excluded.chapter_num,
                    volume      = excluded.volume,
                    date        = excluded.date
                """,
                (data_id, manga_id, chapter_num, volume, date),
            )
            self._conn.commit()

    def upsert_many(
        self,
        chapters_data: list[tuple[int, int, float, int, Optional[str]]]
    ) -> None:
        """
        Масове збереження глав. Очікує список кортежів:
        (data_id, manga_id, chapter_num, volume, date)
        де manga_id — внутрішній id манги (mangas.id).
        """
        with self._lock:
            self._conn.executemany(
                """
                INSERT INTO chapters (data_id, manga_id, chapter_num, volume, date)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(data_id) DO UPDATE SET
                    chapter_num = excluded.chapter_num,
                    volume      = excluded.volume,
                    date        = excluded.date
                """,
                chapters_data
            )
            self._conn.commit()

    @staticmethod
    def _to_model(row: sqlite3.Row) -> ChapterRow:
        return ChapterRow(**dict(row))