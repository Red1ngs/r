"""
build.py — Професія «Читач манги».

Архітектура після рефакторингу
──────────────────────────────
Pipeline відповідає за ЩО: fetch → parse → action (один цикл).
Trigger відповідає за КОЛИ: dynamic_next = delay_until_next() зі SlotScheduler.

Схема одного циклу:
    [FETCH] fetch_next_chapter
        │
        ├── глава є ──► [ACTION] read_chapter
        │                   scheduler.mark_done(slot)   ← завжди
        │                   record_reward(keys)          ← лише при reward
        │                   push_item_received()         ← лише при closed slot
        │                   trigger.advance(bot)         ← знімає in-flight,
        │                                                   рахує наступний fire
        │
        └── глав немає → PARSE-ланцюг:
               Step 1: find_stale_or_new_mangas
               Step 2: fetch_manga_updates
               Step 3a: save_discovered_mangas
               Step 3b: save_discovered_chapters
            → знову [FETCH]
            (якщо parse_retries вичерпано → Scheduler сам викличе advance)

Гарантія "один цикл за раз":
    trigger.dispatch() блокує is_due() на час виконання.
    trigger.advance()  знімає блок після завершення action.
    Це унеможливлює накопичення дублікатів у черзі воркера.
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Optional

from src.core.account_pull import AccountPull
from src.core.inventory.model import Chapter, ItemReceivedEvent, ReaderWork
from src.core.pipeline import Step, pipeline
from src.core.task import AnyTask, Priority
from src.mangabuff.parsers.reader import parse_catalog, parse_chapters

log = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# Ініціалізація (startup — один раз при старті воркера)
# ═══════════════════════════════════════════════════════════════

def init_reader(bot: AccountPull) -> list[AnyTask]:
    """
    Ініціалізує слоти і SlotScheduler.
    Використовується як Profession.startup функція.
    Повертає порожній список — задач при ініціалізації немає.
    """
    inv = bot.inventory.reader
    inv.init_slots(bot.app_config.reader.reward_slots)
    inv.scheduler.initialize()
    log.info(
        f"[{bot.account_id}] Reader ініціалізовано: "
        f"slots={[s.slot_name for s in inv.all_slots()]} "
        f"total_reads={inv.total_reads_for_goals()}"
    )
    return []


# ═══════════════════════════════════════════════════════════════
# FETCH — наступна непрочитана глава з БД
# ═══════════════════════════════════════════════════════════════

def fetch_next_chapter(bot: AccountPull) -> Optional[dict[str, Any]]:
    """
    Повертає {\"sequence\": [...], \"slot_name\": \"...\"} або None.

    None → немає готового слота або немає глав у БД (→ parse-chain).
    """
    inv  = bot.inventory.reader
    slot = inv.scheduler.current()

    if slot is None:
        log.info(f"[{bot.account_id}] Немає готового слота")
        return None

    sequence, mangas = bot.app_config.chapter_repo.get_chapter_sequence(
        account_id=bot.account_id,
        limit=2,
    )

    if sequence:
        log.info(
            f"[{bot.account_id}] [{slot.slot_name}] "
            f"Знайдено {len(sequence)} глав: manga={', '.join(mangas)}"
        )
        return {"sequence": sequence, "mangas": mangas, "slot_name": slot.slot_name}

    log.info(f"[{bot.account_id}] Непрочитаних глав у БД немає → парсинг")
    return None


# ═══════════════════════════════════════════════════════════════
# PARSE: Steps
# ═══════════════════════════════════════════════════════════════

def find_stale_or_new_mangas(bot: AccountPull) -> None:
    """Step 1: вибір стратегії. Без HTTP."""
    cfg   = bot.app_config.reader
    stale = bot.app_config.manga_repo.get_stale_mangas(
        days=cfg.update_interval_days, limit=5
    )
    if stale:
        bot.inventory.reader.work = ReaderWork(mode="stale", targets=stale)
        log.info(f"[{bot.account_id}] Стратегія: stale ({len(stale)} манг)")
    else:
        bot.inventory.reader.work = ReaderWork(mode="catalog")
        log.info(f"[{bot.account_id}] Стратегія: catalog")


def fetch_manga_updates(bot: AccountPull) -> None:
    """Step 2: HTTP — завантаження даних."""
    work = bot.inventory.reader.work
    if not work:
        log.warning(f"[{bot.account_id}] fetch_manga_updates: work порожній")
        return
    if work.mode == "stale":
        _fetch_stale(bot, work)
    else:
        _fetch_catalog(bot, work)


def _fetch_stale(bot: AccountPull, work: ReaderWork) -> None:
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


def _fetch_catalog(bot: AccountPull, work: ReaderWork) -> None:
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


def save_discovered_mangas(bot: AccountPull) -> None:
    """Step 3a: зберегти манги, резолвити ext_id → db_id."""
    work = bot.inventory.reader.work
    if not work or not work.mangas_to_save:
        return
    mapping: dict[int, int] = {}
    for m in work.mangas_to_save:
        db_id = bot.app_config.manga_repo.upsert(
            m.data_id, m.translit_name, m.name,
            m.rating or "", m.info or "", m.image or "",
        )
        mapping[m.data_id] = db_id
    for ch in work.chapters_to_save:
        if ch.manga_id in mapping:
            ch.manga_id = mapping[ch.manga_id]
    log.info(f"[{bot.account_id}] Збережено {len(work.mangas_to_save)} манг")


def save_discovered_chapters(bot: AccountPull) -> None:
    """Step 3b: зберегти глави, очистити work."""
    work = bot.inventory.reader.work
    if not work:
        return
    try:
        if work.chapters_to_save:
            bot.app_config.chapter_repo.upsert_many([
                (ch.data_id, ch.manga_id, ch.chapter_num, ch.volume, ch.date)
                for ch in work.chapters_to_save
                if ch.manga_id is not None
            ])
        log.info(
            f"[{bot.account_id}] Збережено {len(work.chapters_to_save)} глав"
        )
    finally:
        bot.inventory.reader.clear_work()


# ═══════════════════════════════════════════════════════════════
# ACTION — прочитати главу і записати нагороду
# ═══════════════════════════════════════════════════════════════

def _make_read_chapter(
    on_cycle_done: Callable[[AccountPull], None],
) -> Callable[[dict[str, Any], AccountPull], None]:
    """
    Фабрика action-функції.

    on_cycle_done — callback що викликається в кінці action.
    Використовується для trigger.advance(bot) — знімає in-flight блок
    і рахує наступний _next_fire вже після mark_done() і record_reward(),
    тому dynamic_next(bot) бачить актуальний стан SlotScheduler.
    """
    def read_chapter(data: dict[str, Any], bot: AccountPull) -> None:
        """
        Надсилає /addHistory, записує нагороду.

        scheduler.mark_done() — ЗАВЖДИ (час витрачено).
        record_reward()       — тільки якщо reward непустий.
        push_item_received()  — тільки якщо слот закрився.
        on_cycle_done()       — завжди в кінці (trigger.advance).
        """
        inv       = bot.inventory.reader
        sequence  = data.get("sequence", [])
        slot_name = data.get("slot_name")

        if not sequence:
            on_cycle_done(bot)
            return

        log.info(f"[{bot.account_id}] [{slot_name}] Читаємо {len(sequence)} глав")

        reward = bot.session.submit_add_history([
            {"manga_id": ch["manga_id"], "chapter_id": ch["chapter_id"]}
            for ch in sequence
        ])

        for ch in sequence:
            bot.app_config.chapter_repo.mark_chapter_read(
                bot.account_id, int(ch["chapter_id"])
            )

        # Завжди — час читання витрачено
        if slot_name:
            inv.scheduler.mark_done(slot_name)

        if not reward:
            log.info(f"[{bot.account_id}] [{slot_name}] Прочитано (без нагороди)")
            on_cycle_done(bot)   # ← advance ПІСЛЯ mark_done
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

        on_cycle_done(bot)   # ← advance ПІСЛЯ mark_done + record_reward

    return read_chapter


# ═══════════════════════════════════════════════════════════════
# Producer для Trigger
# ═══════════════════════════════════════════════════════════════

def make_reader_producer(
    bot: AccountPull,
    trigger_ref: "list",   # list з одним елементом — Trigger (заповнюється після)
) -> "Callable[[AccountPull], list[AnyTask]]":
    """
    Будує pipeline-producer прив'язаний до конкретного бота.

    trigger_ref — mutable контейнер [Trigger | None].
    Заповнюється після створення Trigger-а (нижче в build_reader_profession).
    on_cycle_done викликає trigger.advance(bot) — знімає in-flight після action.

    Кожен виклик producer(bot) → один цикл читання:
        fetch → (parse якщо треба) → action → on_cycle_done → завершено.
    """
    def on_cycle_done(b: AccountPull) -> None:
        trigger = trigger_ref[0]
        if trigger is not None:
            trigger.advance(b)

    read_chapter = _make_read_chapter(on_cycle_done)

    reader_pipeline = pipeline(
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

def build_reader_profession(bot: AccountPull) -> "Profession":  # type: ignore[name-defined]
    """
    Будує повну Profession для читача.

    Використання в main.py:
        from src.mangabuff.professions.reader.build import build_reader_profession
        reader = build_reader_profession(bot_01)
        scheduler = Scheduler(workers={"acc_01": AccountEntry(worker, [reader])})

    Profession містить:
        startup  = [init_reader]          — ініціалізація слотів одноразово
        triggers = [SlotTrigger]          — Scheduler викликає producer за розкладом
        guard    = not_(has("is_banned"))
    """
    from src.core.conditions import has, not_
    from src.core.profession import Profession
    from src.core.schedule import Trigger

    # Mutable контейнер для посилання на тригер.
    # Заповнюється після створення Trigger — producer замикається на нього.
    trigger_ref: list = [None]

    producer = make_reader_producer(bot, trigger_ref)

    trigger = Trigger(
        name         = "reader_slot",
        account_id   = "",   # заповнює Scheduler при реєстрації
        interval     = 0,
        producer     = producer,
        # dynamic_next викликається в trigger.advance() — після mark_done().
        # Тому delay_until_next() повертає коректне значення.
        dynamic_next = lambda b: b.inventory.reader.scheduler.delay_until_next(),
    )
    trigger_ref[0] = trigger   # замикаємо посилання

    return Profession(
        name     = "reader",
        startup  = [init_reader],
        triggers = [trigger],
        guard    = not_(has("is_banned")),
    )