"""
reader/build.py — ReaderProfession.

Відповідальність:
    Читає мангу по слотах (scroll / card / ...) для отримання нагород.
    Інтервал між читаннями визначається за допомогою SlotScheduler.
    Починає роботу ТІЛЬКИ після успішного збору щоденного бонусу (якщо увімкнено разом з Daily).
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Callable, Iterable, Optional

from src.core.runtime.profession import BaseProfession, RequestResult
from src.core.runtime.scheduler import EventDrivenScheduler
from src.core.tasks.base import AnyTask, Priority
from src.core.tasks.pipeline import Step, pipeline
from src.mangabuff.reader.inventory import ReaderInventory
from src.mangabuff.reader.models import Chapter, ItemReceivedEvent, ReaderWork
from src.mangabuff.reader.parsers import parse_catalog, parse_chapters
from src.mangabuff.reader.stats import ReaderRewardStats
from src.mangabuff.reader.trigger import ReaderTrigger

if TYPE_CHECKING:
    from src.core.account import Account
    from src.core.runtime.request_router import RequestContext
    from src.core.runtime.schedule import TriggerProtocol

log = logging.getLogger(__name__)

# ── Sentinel ───────────────────────────────────────────────────────────────────
_SLOT_NOT_READY: dict[str, Any] = {}


# ─────────────────────────────────────────────────────────────────────────────
# Реєстрація inventory
# ─────────────────────────────────────────────────────────────────────────────

def register_inventory() -> None:
    from src.core.inventory.factory import inventory_factory
    inventory_factory.register("reader", "reader", ReaderInventory)


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline functions
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_next_chapter(bot: "Account") -> Any:
    inv  = bot.inventory.reader  # type: ignore[attr-defined]
    slot = inv.slot_scheduler.current()

    if slot is None:
        log.debug(f"[{bot.account_id}] 🎰 Слот не готовий за часом → очікуємо")
        return _SLOT_NOT_READY

    sequence, mangas = bot.repo.chapters.get_chapter_sequence(
        account_id=bot.account_id,
        limit=2,
    )

    if sequence:
        log.info(
            f"[{bot.account_id}] 📖 [{slot.slot_name}] "
            f"Знайдено непрочитані глави ({len(sequence)}): {', '.join(mangas)}"
        )
        return {"sequence": sequence, "mangas": mangas, "slot_name": slot.slot_name}

    log.info(f"[{bot.account_id}] 🎰 [{slot.slot_name}] Непрочитаних глав немає в БД → запуск парсингу")
    return None


def _find_stale_or_new_mangas(bot: "Account") -> None:
    cfg   = bot.app_config.reader
    stale = bot.repo.mangas.get_stale_mangas(days=cfg.update_interval_days, limit=5)

    if stale:
        bot.inventory.reader.work = ReaderWork(mode="stale", targets=stale)  # type: ignore[attr-defined]
        log.info(f"[{bot.account_id}] 🕵️ Стратегія парсингу: stale (актуалізація {len(stale)} манг)")
    else:
        bot.inventory.reader.work = ReaderWork(mode="catalog")  # type: ignore[attr-defined]
        log.info(f"[{bot.account_id}] 🕵️ Стратегія парсингу: catalog (нові надходження)")


def _fetch_manga_updates(bot: "Account") -> None:
    work: Optional[ReaderWork] = bot.inventory.reader.work  # type: ignore[attr-defined]
    if not work:
        log.warning(f"[{bot.account_id}] ⚠️ Спроба оновити глави при порожньому робочому буфері")
        return

    if work.mode == "stale":
        _fetch_stale(bot, work)
    else:
        _fetch_catalog(bot, work)


def _fetch_stale(bot: "Account", work: ReaderWork) -> None:
    for manga_row in (work.targets or []):
        html = bot.session.fetch_manga_chapters(manga_row.translit_name, manga_row.data_id)
        if not html:
            log.warning(f"[{bot.account_id}] Stale: глави недоступні для {manga_row.translit_name}")
            continue
        for ch in parse_chapters(html):
            work.chapters_to_save.append(Chapter(
                data_id     = ch.data_id,
                manga_id    = manga_row.id,
                chapter_num = ch.chapter_num,
                volume      = ch.volume,
                date        = ch.date,
            ))


def _fetch_catalog(bot: "Account", work: ReaderWork) -> None:
    last = work.targets[-1] if work.targets else None
    page = max(1, last.id // 30) if last else 1

    html = bot.session.fetch_manga_catalog(page=page)
    if not html:
        log.warning(f"[{bot.account_id}] ⚠️ Каталог сайту тимчасово недоступний")
        return

    mangas = dict(list(parse_catalog(html).items())[:5])
    log.info(f"[{bot.account_id}] 📖 Отримано {len(mangas)} манг із каталогу")

    for manga in mangas.values():
        work.mangas_to_save.append(manga)
        html2 = bot.session.fetch_manga_chapters(manga.translit_name, manga.data_id)
        if not html2:
            continue
        for ch in parse_chapters(html2):
            work.chapters_to_save.append(Chapter(
                data_id     = ch.data_id,
                manga_id    = manga.data_id,
                chapter_num = ch.chapter_num,
                volume      = ch.volume,
                date        = ch.date,
            ))


def _save_discovered_mangas(bot: "Account") -> None:
    work: Optional[ReaderWork] = bot.inventory.reader.work  # type: ignore[attr-defined]
    if not work or not work.mangas_to_save:
        return

    mapping: dict[int, int] = {}
    for m in work.mangas_to_save:
        db_id = bot.repo.mangas.upsert(
            m.data_id, m.translit_name, m.name,
            m.rating or "", m.info or "", m.image or "",
        )
        mapping[m.data_id] = db_id

    for ch in work.chapters_to_save:
        if ch.manga_id in mapping:
            ch.manga_id = mapping[ch.manga_id]

    log.info(f"[{bot.account_id}] 💾 Успішно збережено {len(work.mangas_to_save)} нових манг у базу даних")


def _save_discovered_chapters(bot: "Account") -> None:
    work: Optional[ReaderWork] = bot.inventory.reader.work  # type: ignore[attr-defined]
    if not work:
        return
    try:
        if work.chapters_to_save:
            bot.repo.chapters.upsert_many([
                (ch.data_id, ch.manga_id, ch.chapter_num, ch.volume, ch.date)
                for ch in work.chapters_to_save
                if ch.manga_id is not None
            ])
        log.info(f"[{bot.account_id}] 💾 Успішно збережено {len(work.chapters_to_save)} нових глав у базу даних")
    finally:
        bot.inventory.reader.clear_work()  # type: ignore[attr-defined]


# ─────────────────────────────────────────────────────────────────────────────
# ReaderProfession
# ─────────────────────────────────────────────────────────────────────────────

class ReaderProfession(BaseProfession):
    """
    Profession «Читач манги».
    """

    def __init__(self) -> None:
        self._account_id: str                               = ""
        self._stats:      ReaderRewardStats                 = ReaderRewardStats()
        self._trigger:    Optional[ReaderTrigger]           = None
        self._scheduler:  Optional["EventDrivenScheduler"] = None

    @property
    def profession_id(self) -> str:
        return "reader"

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def setup(self, scheduler: "EventDrivenScheduler", account_id: str) -> None:
        self._account_id = account_id
        self._scheduler  = scheduler
        
        scheduler.subscribe("daily.claimed", self._on_daily_claimed)

    async def restore_state(self, bot: "Account") -> None:
        """Відновлення стану (синхронізація конфігу з інвентарем БД)."""
        inv: ReaderInventory = bot.inventory.reader  # type: ignore[attr-defined]
        inv.init_slots(bot.app_config.reader.reward_slots)
        inv.slot_scheduler.initialize()

        log.info(
            f"[{self._account_id}] ReaderProfession відновлено: "
            f"slots={[s.slot_name for s in inv.all_slots()]} "
            f"pending={len(inv.pending_slots())} "
            f"goal_reached={inv.goal_reached()}"
        )

    async def teardown(self, scheduler: "EventDrivenScheduler", account_id: str) -> None:
        self._stats.dump()

    # ── Triggers ──────────────────────────────────────────────────────────────

    def build_triggers(self, account_id: str) -> list["TriggerProtocol"]:
        """Автоматично створює та реєструє тригер у планувальнику."""
        trigger = ReaderTrigger(
            name       = "reader_slot",
            account_id = account_id,
            _producer  = self._make_producer(),
        )
        self._trigger = trigger
        return [trigger]

    def check_guard(self, bot: "Account") -> bool:
        return not bool(bot.inventory.personal.data.get("is_banned"))

    # ── handle_request ────────────────────────────────────────────────────────

    async def handle_request(
        self,
        intent: str,
        data:   dict[str, Any],
        ctx:    "RequestContext",
    ) -> RequestResult:
        if intent == "get_stats":
            return RequestResult.approve(data={"summary": self._stats.summary()})
        if intent == "get_state":
            return await self._handle_get_state(ctx)
        if intent == "reset_slots":
            return await self._handle_reset_slots(ctx)
        if intent == "set_targets":
            return await self._handle_set_targets(data, ctx)
        if intent == "force_parse":
            return await self._handle_force_parse(data, ctx)
        if intent == "mark_read":
            return await self._handle_mark_read(data, ctx)
        return RequestResult.deny(f"unknown intent: {intent!r}")

    async def _handle_get_state(self, ctx: "RequestContext") -> RequestResult:
        inv: ReaderInventory = ctx.bot.inventory.reader  # type: ignore[attr-defined]
        return RequestResult.approve(data={
            "slots": [
                {
                    "name":        s.slot_name,
                    "collected":   s.collected,
                    "daily_limit": s.daily_limit,
                    "complete":    s.is_complete(),
                    "remaining":   s.remaining(),
                }
                for s in inv.all_slots()
            ],
            "pending":      len(inv.pending_slots()),
            "goal_reached": inv.goal_reached(),
            "delay_next":   inv.slot_scheduler.delay_until_next(),
            "stats":        self._stats.summary(),
        })

    async def _handle_reset_slots(self, ctx: "RequestContext") -> RequestResult:
        inv: ReaderInventory = ctx.bot.inventory.reader  # type: ignore[attr-defined]
        inv.slot_scheduler.reset()
        log.info(f"[{ctx.account_id}] ReaderProfession: примусове скидання розкладу слотів")
        return RequestResult.approve(data={"status": "slots reset"})

    async def _handle_set_targets(
        self,
        data: dict[str, Any],
        ctx:  "RequestContext",
    ) -> RequestResult:
        targets = data.get("targets", [])
        if not isinstance(targets, list):
            return RequestResult.deny("targets must be a list of slot names")
        inv: ReaderInventory = ctx.bot.inventory.reader  # type: ignore[attr-defined]
        inv.target_slots = targets
        log.info(f"[{ctx.account_id}] ReaderProfession: змінено список цільових слотів → {targets}")
        return RequestResult.approve(data={"targets": targets})
    
    async def _handle_force_parse(
        self,
        data: dict[str, Any],
        ctx:  "RequestContext",
    ) -> RequestResult:
        """
        Примусово парсить вказану кількість манг із каталогу або за translit_name.

        data:
          limit   — кількість манг із каталогу (default=5, якщо targets не передано)
          targets — список translit_name для точкового парсингу (stale-режим)
        """
        bot     = ctx.bot
        limit   = int(data.get("limit", 5))
        targets: list[str] = data.get("targets", [])

        try:
            if targets:
                mangas_to_parse: list[Any] = []
                for translit_name in targets:
                    row = bot.repo.mangas.get_by_translit_name(translit_name)
                    if row is None:
                        log.warning(
                            f"[{ctx.account_id}] force_parse: "
                            f"manga {translit_name!r} не знайдено в БД — пропускаємо"
                        )
                        continue
                    mangas_to_parse.append(row)

                work = ReaderWork(mode="stale", targets=mangas_to_parse)
                bot.inventory.reader.work = work  # type: ignore[attr-defined]
                _fetch_stale(bot, work)

            else:
                work = ReaderWork(mode="catalog")
                bot.inventory.reader.work = work  # type: ignore[attr-defined]

                html = bot.session.fetch_manga_catalog(page=1)
                if not html:
                    return RequestResult.deny("Каталог тимчасово недоступний")

                mangas = dict(list(parse_catalog(html).items())[:limit])
                log.info(
                    f"[{ctx.account_id}] force_parse: отримано {len(mangas)} манг із каталогу"
                )

                for manga in mangas.values():
                    work.mangas_to_save.append(manga)
                    html2 = bot.session.fetch_manga_chapters(manga.translit_name, manga.data_id)
                    if not html2:
                        continue
                    for ch in parse_chapters(html2):
                        work.chapters_to_save.append(Chapter(
                            data_id     = ch.data_id,
                            manga_id    = manga.data_id,
                            chapter_num = ch.chapter_num,
                            volume      = ch.volume,
                            date        = ch.date,
                        ))

            _save_discovered_mangas(bot)
            _save_discovered_chapters(bot)

            saved_mangas   = len(work.mangas_to_save)
            saved_chapters = len(work.chapters_to_save)
            log.info(
                f"[{ctx.account_id}] force_parse завершено: "
                f"{saved_mangas} манг, {saved_chapters} глав збережено"
            )
            return RequestResult.approve(data={
                "mangas":   saved_mangas,
                "chapters": saved_chapters,
            })

        except Exception as exc:
            log.exception(f"[{ctx.account_id}] force_parse: помилка")
            return RequestResult.deny(str(exc))

    async def _handle_mark_read(
        self,
        data: dict[str, Any],
        ctx:  "RequestContext",
    ) -> RequestResult:
        """
        Позначає всі збережені глави зазначених манг як прочитані для цього акаунта.

        data:
          targets — список translit_name манг (обов'язково)
        """
        bot     = ctx.bot
        targets: list[str] = data.get("targets", [])

        if not targets:
            return RequestResult.deny("targets (список translit_name) обов'язковий")

        total = 0
        try:
            for translit_name in targets:
                row = bot.repo.mangas.get_by_translit_name(translit_name)
                if row is None:
                    log.warning(
                        f"[{ctx.account_id}] mark_read: "
                        f"manga {translit_name!r} не знайдено в БД — пропускаємо"
                    )
                    continue

                sequence, _ = bot.repo.chapters.get_chapter_sequence(
                    account_id=ctx.account_id,
                    limit=10_000,
                )
                target_chapters = [
                    ch for ch in sequence
                    if ch["manga_id"] == row.data_id
                ]
                for ch in target_chapters:
                    bot.repo.chapters.mark_chapter_read(ctx.account_id, int(ch["chapter_id"]))
                    total += 1

                log.info(
                    f"[{ctx.account_id}] mark_read: "
                    f"{translit_name!r} — позначено {len(target_chapters)} глав як прочитані"
                )

            return RequestResult.approve(data={"marked": total, "mangas": targets})

        except Exception as exc:
            log.exception(f"[{ctx.account_id}] mark_read: помилка")
            return RequestResult.deny(str(exc))

    # ── Event handlers ────────────────────────────────────────────────────────

    # СТАЛО (просто і чисто):
    async def _on_daily_claimed(self, payload: dict[str, Any]) -> None:
        if payload.get("account_id") != self._account_id:
            return
        log.info(f"[{self._account_id}] ReaderProfession: отримано сигнал daily.claimed, запускаємо читання.")
        if self._trigger is not None:
            self._trigger.reschedule("+0s")
        if self._scheduler is not None:
            self._scheduler.wakeup()

    # ── Pipeline ──────────────────────────────────────────────────────────────

    def _make_producer(self) -> Callable[["Account"], Iterable[AnyTask]]:

        def on_cycle_done(bot: "Account") -> None:
            if self._trigger is not None:
                self._trigger.advance(bot)

        return pipeline(
            name   = "manga_reader",
            fetch  = _fetch_next_chapter,
            parse  = [
                Step(_find_stale_or_new_mangas,  priority=Priority.NORMAL, max_retries=1),
                Step(_fetch_manga_updates,        priority=Priority.NORMAL, max_retries=2),
                Step(_save_discovered_mangas,     priority=Priority.NORMAL, max_retries=2),
                Step(_save_discovered_chapters,   priority=Priority.NORMAL, max_retries=1),
            ],
            action            = self._make_read_action(on_cycle_done),
            max_parse_retries = 3,
        )

    def _make_read_action(
        self,
        on_cycle_done: Callable[["Account"], None],
    ) -> Callable[[Any, "Account"], None]:

        def read_chapter(data: Any, bot: "Account") -> None:
            if data is _SLOT_NOT_READY:
                on_cycle_done(bot)
                return

            inv: ReaderInventory = bot.inventory.reader  # type: ignore[attr-defined]
            sequence:  list[dict[str, Any]] = data.get("sequence", [])
            slot_name: str  = data.get("slot_name", "")

            if not sequence:
                on_cycle_done(bot)
                return

            log.info(f"[{bot.account_id}] 🎰 [{slot_name}] Запуск читання {len(sequence)} глав")

            # Надсилаємо історію
            reward = bot.session.submit_add_history([
                {"manga_id": ch["manga_id"], "chapter_id": ch["chapter_id"]}
                for ch in sequence
            ])

            for ch in sequence:
                bot.repo.chapters.mark_chapter_read(bot.account_id, int(ch["chapter_id"]))

            if slot_name:
                inv.slot_scheduler.mark_done(slot_name)

            self._stats.record(slot_name=slot_name, reward=reward if reward else None)

            if not reward:
                log.info(f"[{bot.account_id}] 🎰 [{slot_name}] Глави успішно прочитані (нагорода відсутня)")
                on_cycle_done(bot)
                return

            reward_keys: frozenset[str] = frozenset(reward.keys())
            closed = inv.record_reward(reward_keys)

            if closed:
                slot_info   = next((s for s in inv.all_slots() if s.slot_name == closed), None)
                collected   = slot_info.collected   if slot_info else "?"
                daily_limit = slot_info.daily_limit if slot_info else "?"
                log.info(f"[{bot.account_id}] 🎉 Слот {closed!r} повністю закрито на сьогодні ({collected}/{daily_limit})")

                bot.inventory.personal.push_item_received(ItemReceivedEvent(
                    account_id = bot.account_id,
                    slot_name  = closed,
                    reward     = reward,
                ))

                if self._scheduler is not None:
                    self._scheduler.emit_event(
                        "reader.slot_closed",
                        {"account_id": bot.account_id, "slot_name": closed, "reward": reward},
                        source=bot.account_id,
                    )

            log.info(
                f"[{bot.account_id}] 🎰 [{slot_name}] Нагорода отримана: {reward} | "
                f"активних слотів залишилось: {len(inv.pending_slots())}"
            )

            if inv.goal_reached():
                log.info(f"[{bot.account_id}] 🎯 Усі цільові слоти на сьогодні повністю закриті")
                if self._scheduler is not None:
                    self._scheduler.emit_event(
                        "reader.goal_reached",
                        {"account_id": bot.account_id},
                        source=bot.account_id,
                    )

            on_cycle_done(bot)

        return read_chapter