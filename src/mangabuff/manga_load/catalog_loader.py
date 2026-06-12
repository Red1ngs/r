"""
farmer/catalog_loader.py — CatalogLoaderProfession.

Архітектура:
    CatalogLoaderProfession
        • Слухає «reader.chapters_exhausted» — перший хто встиг захоплює лок.
        • Парсить сторінку каталогу (per-account catalog_page).
        • Через scheduler.dispatch_work() рівномірно розподіляє транслітерації
          між усіма активними MangaLoaderProfession в системі.

Універсальний розподіл задач:
    scheduler.dispatch_work(
        profession_id = "manga_loader",
        intent        = "load_batch",
        items         = translits,
        item_key      = "translits",
    )
    — знаходить N акаунтів з manga_loader, ділить items на N частин,
      надсилає кожному через ask().

Баги що виправлені:
    #5  манги не дублюються між ітераціями       — CatalogLoaderProfession дедуплікує по data_id
    #6  catalog_page — per-account через inventory — CatalogLoaderInventory.catalog_page
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Optional

from src.core.runtime.profession import BaseProfession, RequestResult
from src.core.runtime.scheduler import EventDrivenScheduler
from src.mangabuff.manga_load.parsers import parse_catalog, CATALOG_PAGE_SIZE

if TYPE_CHECKING:
    from src.core.account import Account
    from src.core.runtime.request_router import RequestContext

log = logging.getLogger(__name__)


async def _parse_catalog_page(bot: "Account") -> list[str]:
    """
    Парсить поточну сторінку каталогу для цього акаунта.

    Повертає список транслітерацій манг з новими главами.
    Фікс #5: дедуплікація по data_id всередині однієї сторінки.
    Фікс #6: per-account catalog_page через CatalogLoaderInventory.
    """
    inv = bot.inventory.catalog_loader  # type: ignore[attr-defined]
    page = inv.catalog_page

    # 1. Завантажуємо HTML сторінки
    result = await bot.safe_session.fetch_manga_catalog(page=page)
    html = result.data if result.ok else None
    
    if not html:
        log.warning(f"[{bot.account_id}] CatalogLoader: каталог недоступний (сторінка {page})")
        return []

    # 2. Парсимо манги зі сторінки
    mangas = parse_catalog(html)
    if not mangas:
        log.info(f"[{bot.account_id}] CatalogLoader: сторінка {page} порожня → скидаємо на 1")
        inv.catalog_page = 1
        return []

    translits: list[str] = []

    # 3. Зберігаємо результати та дедуплікуємо (через dict ключ data_id)
    existing_ids = bot.repo.mangas.get_existing_data_ids(list(mangas.keys()))
    for data_id, manga in mangas.items():
        # Зберігаємо мангу в БД (якщо вже є — оновиться)
        if data_id in existing_ids:
            continue
        bot.repo.mangas.upsert(
            data_id, 
            manga.translit_name, 
            manga.name,
            manga.rating or "", 
            manga.info or "", 
            manga.image or "",
        )
        translits.append(manga.translit_name)

    # 4. ЛОГІКА ОКРУГЛЕННЯ ВГОРУ ДЛЯ НАСТУПНОЇ СТОРІНКИ
    # Дізнаємося загальну кількість манг у базі після оновлення
    total_mangas = bot.repo.mangas.count()
    
    # Визначаємо, яку сторінку парсити наступного разу:
    # 0-29 манг  -> сторінка 1
    # 30-59 манг -> сторінка 2
    # 60+ манг   -> сторінка 3
    inv.catalog_page = (total_mangas // CATALOG_PAGE_SIZE) + 1

    log.info(
        f"[{bot.account_id}] CatalogLoader: оброблено сторінку {page} → "
        f"отримано {len(translits)} манг. "
        f"В базі всього: {total_mangas}. "
        f"Наступна сторінка для парсингу: {inv.catalog_page}"
    )

    return translits


class CatalogLoaderProfession(BaseProfession):
    """
    Profession «Каталог-лоадер».

    Відповідальність:
        • Слухає «reader.chapters_exhausted».
        • Перший хто встиг захоплює глобальний лок — решта пропускають.
        • Парсить одну сторінку каталогу (per-account catalog_page).
        • Через scheduler.dispatch_work() рівномірно розподіляє транслітерації
          між усіма MangaLoaderProfession в системі.

    Коректно працює як з одним акаунтом, так і з кількома:
        • Один акаунт  → парсить каталог і диспетчить на свій же MangaLoader.
        • Кілька       → перший хто встиг парсить, решта чекають chapters_ready.
    """

    def __init__(self) -> None:
        self._account_id: str                               = ""
        self._scheduler:  Optional["EventDrivenScheduler"] = None

    @property
    def profession_id(self) -> str:
        return "catalog_loader"

    async def setup(self, scheduler: "EventDrivenScheduler", account_id: str) -> None:
        self._account_id = account_id
        self._scheduler  = scheduler
        scheduler.subscribe("reader.chapters_exhausted", self._on_chapters_exhausted)

    async def restore_state(self, bot: "Account") -> None:
        inv = bot.inventory.catalog_loader
        log.info(
            f"[{self._account_id}] CatalogLoaderProfession відновлено: "
            f"catalog_page={inv.catalog_page}"
        )

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
        if intent == "get_state":
            inv = ctx.bot.inventory.catalog_loader  # type: ignore[attr-defined]
            return RequestResult.approve(data={"catalog_page": inv.catalog_page})
        if intent == "reset_catalog_page":
            inv = ctx.bot.inventory.catalog_loader  # type: ignore[attr-defined]
            inv.catalog_page = 1
            log.info(f"[{ctx.account_id}] CatalogLoaderProfession: catalog_page скинуто на 1")
            return RequestResult.approve(data={"catalog_page": 1})
        return RequestResult.deny(f"unknown intent: {intent!r}")

    async def _on_chapters_exhausted(self, payload: dict[str, Any]) -> None:
        if self._scheduler is None:
            return

        # Перший хто встиг — парсить. Решта пропускають і чекають chapters_ready.
        acquired = await self._scheduler.try_acquire_loader_lock()
        if not acquired:
            log.info(
                f"[{self._account_id}] CatalogLoaderProfession: "
                f"інший catalog_loader вже парсить — пропускаємо"
            )
            return

        log.info(f"[{self._account_id}] CatalogLoaderProfession: chapters_exhausted → парсимо каталог")

        bot = self._scheduler.get_bot(self._account_id)
        if bot is None:
            log.warning(f"[{self._account_id}] CatalogLoaderProfession: акаунт не знайдено")
            await self._scheduler.release_loader_lock()
            return

        try:
            translits = await _parse_catalog_page(bot)

            if not translits:
                log.info(f"[{self._account_id}] CatalogLoaderProfession: каталог порожній або недоступний")
                return

            dispatched = await self._scheduler.dispatch_work(
                profession_id = "manga_loader",
                intent        = "load_batch",
                items         = translits,
                item_key      = "translits",
                caller        = self._account_id,
            )
            log.info(
                f"[{self._account_id}] CatalogLoaderProfession: "
                f"{len(translits)} манг розподілено між {dispatched} manga_loader(ів)"
            )
        except Exception:
            log.exception(f"[{self._account_id}] CatalogLoaderProfession: помилка")
            # При помилці — знімаємо лок і будимо читачів щоб не зависли
            await self._scheduler.release_loader_lock()
            await self._scheduler.emit_event("loader.chapters_ready", {"error": True},
                                       source=self._account_id)
        # Лок знімає MangaLoaderProfession після завершення останнього батчу,
        # або тут якщо dispatch_work повернув 0 (немає manga_loader'ів).
        else:
            if dispatched == 0:
                await self._scheduler.release_loader_lock()

