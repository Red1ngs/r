"""
farmer/manga_loader.py — MangaLoaderProfession.

Архітектура:
    MangaLoaderProfession
        • Отримує батч транслітерацій через handle_request("load_batch").
        • Парсить глави кожної манги, зберігає в БД.
        • Після завершення емітить broadcast «loader.chapters_ready».

force_parse (виклик з бота):
    Отримує translit_name напряму від оператора.
    Взаємодіє виключно з MangaLoaderProfession — каталог не чіпає.
    Якщо манга ще відсутня в БД — отримує data_id зі сторінки манги
    і upsert-ить запис перед збереженням глав.
"""
from __future__ import annotations

from logging import log
from typing import TYPE_CHECKING, Any, Optional

from src.core.runtime.profession import BaseProfession, RequestResult
from src.core.runtime.scheduler import EventDrivenScheduler
from src.mangabuff.manga_load.parsers import parse_chapters, parse_manga_data_id, parse_manga_views

if TYPE_CHECKING:
    from src.core.core_account import Account
    from src.core.runtime.request_router import RequestContext

from src.core.logging.loggers import get_account_logger


async def _load_manga_batch(bot: "Account", translits: list[str]) -> int:
    """
    Парсить глави для кожного translit_name з батчу і зберігає в БД.
    Повертає кількість збережених глав.

    Якщо manga відсутня в БД — пропускаємо (батч-режим передбачає,
    що каталог вже виконав upsert перед dispatch_work).
    """
    total_chapters = 0

    for translit_name in translits:
        manga_row = bot.repo.mangas.get_by_translit_name(translit_name)
        if manga_row is None:
            get_account_logger(bot.account_id).warning(
                f"MangaLoader: "
                f"manga {translit_name!r} не знайдено в БД — пропускаємо"
            )
            continue

        if not bot.is_connected:
            get_account_logger(bot.account_id).warning(f"[{bot.account_id}] manga_loader: акаунт відключено, батч скасовано")
            return 0  # або raise відповідний виняток

        result = await bot.safe_session.fetch_manga_chapters(translit_name, manga_row.data_id)
        html = result.data if result.ok else None
        
        if not html:
            get_account_logger(bot.account_id).warning(
                f"MangaLoader: "
                f"глави недоступні для {translit_name!r}"
            )
            continue

        views = parse_manga_views(html)
        if views > 0:
            bot.repo.mangas.update_views(manga_row.data_id, views)

        chapters = [
            (ch.data_id, manga_row.id, ch.chapter_num, ch.volume, ch.date)
            for ch in parse_chapters(html)
        ]
        if chapters:
            bot.repo.chapters.upsert_many(chapters)
            total_chapters += len(chapters)
            get_account_logger(bot.account_id).debug(
                f"MangaLoader: "
                f"{translit_name!r} → {len(chapters)} глав збережено, views={views}"
            )

    return total_chapters


async def _force_load_manga(bot: "Account", translit_name: str) -> int:
    """
    Парсить та зберігає глави для translit_name без залежності від каталогу.

    На відміну від _load_manga_batch:
      • Якщо манга відсутня в БД — отримує data_id зі сторінки манги
        і робить upsert мінімального запису перед збереженням глав.
      • Ніяк не взаємодіє з каталогом.

    Повертає кількість збережених глав.
    """
    logger = get_account_logger(bot.account_id)
    if not bot.is_connected:
        logger.warning(f"[{bot.account_id}] manga_loader: акаунт відключено, force_parse скасовано")
        return 0
    
    manga_row = bot.repo.mangas.get_by_translit_name(translit_name)

    if manga_row is None:
        # Манга невідома — отримуємо сторінку щоб дізнатися data_id
        result = await bot.safe_session.fetch_manga_page(translit_name) 
        page_html = result.data if result.ok else None
        if not page_html:
            logger.warning(
                f"force_parse: "
                f"сторінка манги {translit_name!r} недоступна"
            )
            return 0

        data_id = parse_manga_data_id(page_html)
        if data_id is None:
            logger.warning(
                f"force_parse: "
                f"не вдалося визначити data_id для {translit_name!r} — "
                f"перевірте _MANGA_PAGE_DATA_ID_SELECTORS у parsers.py"
            )
            return 0

        # Upsert мінімального запису (name = translit_name поки не знаємо справжню назву)
        bot.repo.mangas.upsert(data_id, translit_name, translit_name)
        manga_row = bot.repo.mangas.get_by_translit_name(translit_name)
        if manga_row is None:
            logger.error(f"force_parse: upsert пройшов, але get повернув None")
            return 0

        logger.info(
            f"force_parse: "
            f"нова манга {translit_name!r} зареєстрована в БД (data_id={data_id})"
        )

    result = await bot.safe_session.fetch_manga_chapters(translit_name, manga_row.data_id)
    html = result.data if result.ok else None
    if not html:
        logger.warning(
            f"force_parse: "
            f"глави недоступні для {translit_name!r}"
        )
        return 0

    views = parse_manga_views(html)
    if views > 0:
        bot.repo.mangas.update_views(manga_row.data_id, views)

    chapters = [
        (ch.data_id, manga_row.id, ch.chapter_num, ch.volume, ch.date)
        for ch in parse_chapters(html)
    ]
    if chapters:
        bot.repo.chapters.upsert_many(chapters)
        logger.debug(
            f"force_parse: "
            f"{translit_name!r} → {len(chapters)} глав збережено, views={views}"
        )
    return len(chapters)


class MangaLoaderProfession(BaseProfession):
    """
    Profession «Манга-лоадер».

    Відповідальність:
        • Отримує батч транслітерацій через handle_request("load_batch").
        • Парсить глави кожної манги, зберігає в БД.
        • Емітить broadcast «loader.chapters_ready» — всі читачі прокидаються.
        • Знімає глобальний лок після завершення.
    """

    def __init__(self) -> None:
        self._account_id: str                               = ""
        self._scheduler:  Optional["EventDrivenScheduler"] = None

    @property
    def profession_id(self) -> str:
        return "manga_loader"

    async def setup(self, scheduler: "EventDrivenScheduler", account_id: str) -> None:
        self._account_id = account_id
        self._scheduler  = scheduler

    async def restore_state(self, bot: "Account") -> None:
        get_account_logger(self._account_id).info(f"MangaLoaderProfession відновлено")

    async def teardown(self, scheduler: "EventDrivenScheduler", account_id: str) -> None:
        pass

    def check_guard(self, bot: "Account") -> bool:
        return not bool(bot.inventory.personal.data.get("is_banned"))

    async def handle_request(
        self,
        intent: str,
        data:   dict[str, Any],
        ctx:    "RequestContext",
    ) -> RequestResult:
        if intent == "load_batch":
            return await self._handle_load_batch(data, ctx)
        if intent == "force_parse":
            return await self._handle_force_parse(data, ctx)
        return RequestResult.deny(f"unknown intent: {intent!r}")

    async def _handle_load_batch(
        self,
        data: dict[str, Any],
        ctx:  "RequestContext",
    ) -> RequestResult:
        """Завантажує батч манг від CatalogLoaderProfession."""
        translits: list[str] = data.get("translits", [])
        if not translits:
            return RequestResult.deny("translits не може бути порожнім")

        get_account_logger(ctx.account_id).info(
            f"MangaLoaderProfession: "
            f"отримано батч {len(translits)} манг → {translits}"
        )

        try:
            saved = await _load_manga_batch(ctx.bot, translits)
            get_account_logger(ctx.account_id).info(
                f"MangaLoaderProfession: "
                f"батч завершено — {saved} глав збережено"
            )
            return RequestResult.approve(data={"chapters_saved": saved})
        except Exception as exc:
            get_account_logger(ctx.account_id).exception(f"MangaLoaderProfession: помилка батчу")
            return RequestResult.deny(str(exc))
        finally:
            # Знімаємо лок і будимо всіх читачів після кожного батчу.
            if self._scheduler is not None:
                await self._scheduler.release_loader_lock()
                await self._scheduler.emit_event(
                    "loader.chapters_ready",
                    {},
                    source=self._account_id,
                )

    async def _handle_force_parse(
        self,
        data: dict[str, Any],
        ctx:  "RequestContext",
    ) -> RequestResult:
        """
        Примусово оновлює глави манг за translit_name (без каталогу).

        Якщо манга відсутня в БД — реєструє її самостійно через parse_manga_data_id.
        Ніяк не взаємодіє з каталогом.
        """
        translits: list[str] = data.get("translits", [])
        if not translits:
            return RequestResult.deny("translits (список translit_name) обов'язковий")

        try:
            total_chapters = 0
            saved_mangas   = 0
            for translit_name in translits:
                chapters = await _force_load_manga(ctx.bot, translit_name)
                total_chapters += chapters
                if chapters > 0:
                    saved_mangas += 1

            get_account_logger(ctx.account_id).info(
                f"force_parse завершено: "
                f"{total_chapters} глав збережено для {saved_mangas}/{len(translits)} манг"
            )
            return RequestResult.approve(data={
                "chapters_saved": total_chapters,
                "mangas":         saved_mangas,
            })
        except Exception as exc:
            get_account_logger(ctx.account_id).exception(f"force_parse: помилка")
            return RequestResult.deny(str(exc))