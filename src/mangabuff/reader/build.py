"""
reader/build.py — Професія «Читач манги».

Ключова логіка fetch:
  _SLOT_NOT_READY  — слот ще не готовий за часом.
                     Pipeline отримує "дані" (не None) → НЕ запускає parse.
                     Action бачить sentinel → викликає on_cycle_done і виходить.
  None             — слот готовий, але глав у БД немає → Pipeline запускає parse.
  dict             — все є, читаємо.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Iterable, Optional

from src.core.account import Account
from src.core.scheduling.schedule import BaseTrigger
from src.core.tasks.base import AnyTask, Priority
from src.core.tasks.pipeline import Step, pipeline

from src.mangabuff.reader.parsers import parse_catalog, parse_chapters
from src.mangabuff.reader.models import Chapter, ItemReceivedEvent, ReaderWork
from src.mangabuff.reader.stats import ReaderRewardStats

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from src.core.scheduling.profession import Profession


# ── Sentinel ──────────────────────────────────────────────────────────────────
# Унікальний об'єкт — identity check через `is`, не `==`.
# fetch повертає його коли слот ще не готовий.
# action його бачить → одразу викликає on_cycle_done без читання і без parse.
_SLOT_NOT_READY: dict[str, Any] = {}


# ═══════════════════════════════════════════════════════════════
# ReaderTrigger
# ═══════════════════════════════════════════════════════════════

@dataclass
class ReaderTrigger(BaseTrigger):
    """
    Тригер читача. Наслідує BaseTrigger — не потребує interval.
    Інтервал визначається SlotScheduler.delay_until_next().
    """
    _producer: Callable[[Account], Iterable[AnyTask]]

    def next_delay(self, bot: Account) -> float:
        return bot.inventory.reader.slot_scheduler.delay_until_next()

    def producer(self, bot: Account) -> Iterable[AnyTask]:
        return self._producer(bot)


# ═══════════════════════════════════════════════════════════════
# Ініціалізація
# ═══════════════════════════════════════════════════════════════

def init_reader(bot: Account) -> list[AnyTask]:
    inv = bot.inventory.reader
    inv.init_slots(bot.app_config.reader.reward_slots)
    inv.slot_scheduler.initialize()
    log.info(
        f"[{bot.account_id}] Reader ініціалізовано: "
        f"slots={[s.slot_name for s in inv.all_slots()]} "
        f"total_reads={inv.total_reads_for_goals()}"
    )
    return []


# ═══════════════════════════════════════════════════════════════
# FETCH
# ═══════════════════════════════════════════════════════════════

def fetch_next_chapter(bot: Account) -> Optional[dict[str, Any]]:
    """
    Три можливих результати:

    _SLOT_NOT_READY  — слот не готовий за часом.
                       Pipeline отримує ненульове значення → action,
                       action бачить sentinel → on_cycle_done → кінець.
                       Parse НЕ запускається.

    None             — слот готовий, але глав у БД немає.
                       Pipeline → parse steps → нові глави → fetch знову.

    dict             — є слот і глави → action читає.
    """
    inv  = bot.inventory.reader
    slot = inv.slot_scheduler.current()

    if slot is None:
        log.debug(f"[{bot.account_id}] Слот не готовий → чекаємо")
        return _SLOT_NOT_READY

    sequence, mangas = bot.repo.chapters.get_chapter_sequence(
        account_id=bot.account_id,
        limit=2,
    )

    if sequence:
        log.info(
            f"[{bot.account_id}] [{slot.slot_name}] "
            f"Знайдено {len(sequence)} глав: manga={', '.join(mangas)}"
        )
        return {"sequence": sequence, "mangas": mangas, "slot_name": slot.slot_name}

    log.info(f"[{bot.account_id}] [{slot.slot_name}] Непрочитаних глав у БД немає → парсинг")
    return None


# ═══════════════════════════════════════════════════════════════
# PARSE: Steps
# ═══════════════════════════════════════════════════════════════

def find_stale_or_new_mangas(bot: Account) -> None:
    cfg   = bot.app_config.reader
    stale = bot.repo.mangas.get_stale_mangas(
        days=cfg.update_interval_days, limit=5
    )
    if stale:
        bot.inventory.reader.work = ReaderWork(mode="stale", targets=stale)
        log.info(f"[{bot.account_id}] Стратегія: stale ({len(stale)} манг)")
    else:
        bot.inventory.reader.work = ReaderWork(mode="catalog")
        log.info(f"[{bot.account_id}] Стратегія: catalog")


def fetch_manga_updates(bot: Account) -> None:
    work = bot.inventory.reader.work
    if not work:
        log.warning(f"[{bot.account_id}] fetch_manga_updates: work порожній")
        return
    if work.mode == "stale":
        _fetch_stale(bot, work)
    else:
        _fetch_catalog(bot, work)


def _fetch_stale(bot: Account, work: ReaderWork) -> None:
    if not work.targets:
        return
    for manga_row in work.targets:
        html = bot.session.fetch_manga_chapters(
            manga_row.translit_name, manga_row.data_id
        )
        if not html:
            log.warning(
                f"[{bot.account_id}] Stale: глави недоступні для "
                f"{manga_row.translit_name!r}"
            )
            continue
        for ch in parse_chapters(html):
            work.chapters_to_save.append(Chapter(
                data_id=ch.data_id, manga_id=manga_row.id,
                chapter_num=ch.chapter_num, volume=ch.volume, date=ch.date,
            ))


def _fetch_catalog(bot: Account, work: ReaderWork) -> None:
    last = work.targets[-1] if work.targets else None
    page = max(1, last.id // 30) if last else 1
    html = bot.session.fetch_manga_catalog(page=page)
    if not html:
        log.warning(f"[{bot.account_id}] Каталог недоступний")
        return
    mangas = dict(list(parse_catalog(html).items())[:5])
    log.info(f"[{bot.account_id}] Каталог: {len(mangas)} манг")
    for manga in mangas.values():
        work.mangas_to_save.append(manga)
        html2 = bot.session.fetch_manga_chapters(manga.translit_name, manga.data_id)
        if not html2:
            continue
        for ch in parse_chapters(html2):
            work.chapters_to_save.append(Chapter(
                data_id=ch.data_id, manga_id=manga.data_id,
                chapter_num=ch.chapter_num, volume=ch.volume, date=ch.date,
            ))


def save_discovered_mangas(bot: Account) -> None:
    work = bot.inventory.reader.work
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
    log.info(f"[{bot.account_id}] Збережено {len(work.mangas_to_save)} манг")


def save_discovered_chapters(bot: Account) -> None:
    work = bot.inventory.reader.work
    if not work:
        return
    try:
        if work.chapters_to_save:
            bot.repo.chapters.upsert_many([
                (ch.data_id, ch.manga_id, ch.chapter_num, ch.volume, ch.date)
                for ch in work.chapters_to_save
                if ch.manga_id is not None
            ])
        log.info(f"[{bot.account_id}] Збережено {len(work.chapters_to_save)} глав")
    finally:
        bot.inventory.reader.clear_work()


# ═══════════════════════════════════════════════════════════════
# ACTION
# ═══════════════════════════════════════════════════════════════

def _make_read_chapter(
    on_cycle_done: Callable[[Account], None],
    stats:         ReaderRewardStats,
) -> Callable[[dict[str, Any], Account], None]:

    def read_chapter(data: dict[str, Any], bot: Account) -> None:
        # Sentinel: слот не був готовий — просто завершуємо цикл
        if data is _SLOT_NOT_READY:
            on_cycle_done(bot)
            return

        inv       = bot.inventory.reader
        sequence  = data.get("sequence", [])
        slot_name = data.get("slot_name", "")

        if not sequence:
            on_cycle_done(bot)
            return

        log.info(f"[{bot.account_id}] [{slot_name}] Читаємо {len(sequence)} глав")

        reward = bot.session.submit_add_history([
            {"manga_id": ch["manga_id"], "chapter_id": ch["chapter_id"]}
            for ch in sequence
        ])

        for ch in sequence:
            bot.repo.chapters.mark_chapter_read(
                bot.account_id, int(ch["chapter_id"])
            )

        if slot_name:
            inv.slot_scheduler.mark_done(slot_name)

        # Записуємо в статистику (і reward=None і reward=dict)
        stats.record(slot_name=slot_name, reward=reward if reward else None)

        if not reward:
            log.info(f"[{bot.account_id}] [{slot_name}] Прочитано (без нагороди)")
            on_cycle_done(bot)
            return

        reward_keys: frozenset[str] = frozenset(reward.keys())
        if reward_keys:
            closed = inv.record_reward(reward_keys)
            if closed:
                slot_info   = next((s for s in inv.all_slots() if s.slot_name == closed), None)
                collected   = slot_info.collected   if slot_info else "?"
                daily_limit = slot_info.daily_limit if slot_info else "?"
                log.info(
                    f"[{bot.account_id}] Слот {closed!r} закрито "
                    f"({collected}/{daily_limit})"
                )
                bot.inventory.personal.push_item_received(ItemReceivedEvent(
                    account_id=bot.account_id,
                    slot_name=closed,
                    reward=reward,
                ))
            log.info(
                f"[{bot.account_id}] [{slot_name}] Нагорода: {reward} | "
                f"pending={len(inv.pending_slots())} слотів"
            )
            if inv.goal_reached():
                log.info(f"[{bot.account_id}] 🎯 Всі слоти закриті на сьогодні")

        on_cycle_done(bot)

    return read_chapter


# ═══════════════════════════════════════════════════════════════
# Producer
# ═══════════════════════════════════════════════════════════════

def make_reader_producer(
    trigger_ref: list[Any],
    stats:       ReaderRewardStats,
) -> Callable[[Account], Iterable[AnyTask]]:

    def on_cycle_done(bot: Account) -> None:
        trigger = trigger_ref[0]
        if trigger is not None:
            trigger.advance(bot)

    read_chapter = _make_read_chapter(on_cycle_done, stats)

    reader_pipeline: Callable[[Account], Iterable[AnyTask]] = pipeline(
        name   = "manga_reader",
        fetch  = fetch_next_chapter,
        parse  = [
            Step(find_stale_or_new_mangas, priority=Priority.NORMAL, max_retries=1),
            Step(fetch_manga_updates,      priority=Priority.NORMAL, max_retries=2),
            Step(save_discovered_mangas,   priority=Priority.NORMAL, max_retries=2),
            Step(save_discovered_chapters, priority=Priority.NORMAL, max_retries=1),
        ],
        action            = read_chapter,
        max_parse_retries = 3,
    )
    return reader_pipeline


# ═══════════════════════════════════════════════════════════════
# Збірка Profession
# ═══════════════════════════════════════════════════════════════

def build_reader_profession(bot: Account) -> "tuple[Profession, ReaderRewardStats]":
    from src.core.scheduling.conditions import has, not_
    from src.core.scheduling.profession import Profession

    trigger_ref: list[Any] = [None]
    stats   = ReaderRewardStats()
    producer = make_reader_producer(trigger_ref, stats)

    trigger = ReaderTrigger(
        name       = "reader_slot",
        account_id = "",
        _producer  = producer,
    )
    trigger_ref[0] = trigger

    profession = Profession(
        name     = "reader",
        startup  = [init_reader],
        triggers = [trigger],
        guard    = not_(has("is_banned")),
    )
    return profession, stats