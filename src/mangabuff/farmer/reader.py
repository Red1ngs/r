"""
farmer/reader.py — ReaderProfession.

Архітектура:
    ReaderProfession
        • Читає мангу по слотах (scroll / card / ...) для нагород.
        • Коли в БД немає непрочитаних глав → емітить «reader.chapters_exhausted».
        • Слухає «loader.chapters_ready» (broadcast) → скидає тригер у +0s.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Iterable, Optional

from src.core.runtime.profession import BaseProfession, RequestResult
from src.core.runtime.scheduler import EventDrivenScheduler
from src.core.tasks.base import AnyTask
from src.core.tasks.pipeline import pipeline, Ready, Skip
from src.mangabuff.farmer.inventory import ReaderInventory
from src.mangabuff.farmer.models import ItemReceivedEvent
from src.mangabuff.farmer.stats import ReaderRewardStats
from src.mangabuff.farmer.trigger import ReaderTrigger

if TYPE_CHECKING:
    from src.core.account import Account
    from src.core.runtime.request_router import RequestContext
    from src.core.runtime.schedule import TriggerProtocol

from src.core.logging.loggers import get_account_logger


def _fetch_next_chapter(bot: "Account") -> Ready[dict[str, Any]] | Skip:
    """
    Fetch-крок pipeline читача.

    Повертає:
        Skip                        — слот не готовий за часом; pipeline
                                      завершується без action і без parse.
        Ready({"sequence": [], …})  — глав немає; action побачить порожній
                                      sequence і викличе on_exhausted.
        Ready({"sequence": […], …}) — є непрочитані глави; action читає.

    None і виняток з цієї функції не використовуються для сигналізації стану.
    """
    inv  = bot.inventory.reader  # type: ignore[attr-defined]
    slot = inv.slot_scheduler.current()

    if slot is None:
        get_account_logger(bot.account_id).debug("🎰 Слот не готовий за часом → очікуємо")
        return Skip(reason="slot not ready")

    sequence, mangas = bot.repo.chapters.get_chapter_sequence(
        account_id=bot.account_id,
        limit=2,
    )

    if sequence:
        get_account_logger(bot.account_id).info(
            f"📖 [{slot.slot_name}] "
            f"Знайдено непрочитані глави ({len(sequence)}): {', '.join(mangas)}"
        )
        return Ready({"sequence": sequence, "mangas": mangas, "slot_name": slot.slot_name})

    get_account_logger(bot.account_id).info(f"🎰 [{slot.slot_name}] Непрочитаних глав немає → сигнал завантажувачу")
    return Ready({"sequence": [], "mangas": [], "slot_name": slot.slot_name})


class ReaderProfession(BaseProfession):
    """
    Profession «Читач манги».

    Відповідальність:
        • Читає глави зі слотів для отримання нагород.
        • Якщо в БД немає непрочитаних глав — емітить «reader.chapters_exhausted»
          і чекає. Самостійно нічого не завантажує.
        • При отриманні «loader.chapters_ready» → скидає тригер у +0s.
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

        scheduler.subscribe("daily.claimed",         self._on_daily_claimed)
        scheduler.subscribe("loader.chapters_ready", self._on_chapters_ready)

    async def restore_state(self, bot: "Account") -> None:
        inv: ReaderInventory = bot.inventory.reader  # type: ignore[attr-defined]
        inv.init_slots(bot.app_config.reader.reward_slots)
        inv.slot_scheduler.initialize()

        get_account_logger(self._account_id).info(
            f"ReaderProfession відновлено: "
            f"slots={[s.slot_name for s in inv.all_slots()]} "
            f"pending={len(inv.pending_slots())} "
            f"goal_reached={inv.goal_reached()}"
        )

    async def teardown(self, scheduler: "EventDrivenScheduler", account_id: str) -> None:
        self._stats.dump()

    # ── Triggers ──────────────────────────────────────────────────────────────

    def build_triggers(self, account_id: str) -> list["TriggerProtocol"]:
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
        get_account_logger(ctx.account_id).info("ReaderProfession: примусове скидання розкладу слотів")
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
        get_account_logger(ctx.account_id).info(f"ReaderProfession: змінено список цільових слотів → {targets}")
        return RequestResult.approve(data={"targets": targets})

    async def _handle_mark_read(
        self,
        data: dict[str, Any],
        ctx:  "RequestContext",
    ) -> RequestResult:
        bot     = ctx.bot
        targets: list[str] = data.get("targets", [])

        if not targets:
            return RequestResult.deny("targets (список translit_name) обов'язковий")

        total = 0
        try:
            for translit_name in targets:
                row = bot.repo.mangas.get_by_translit_name(translit_name)
                if row is None:
                    get_account_logger(ctx.account_id).warning(
                        f"mark_read: manga {translit_name!r} не знайдено в БД — пропускаємо"
                    )
                    continue

                sequence, _ = bot.repo.chapters.get_chapter_sequence(
                    account_id=ctx.account_id,
                    limit=2,
                )
                target_chapters = [
                    ch for ch in sequence
                    if ch["manga_id"] == row.data_id
                ]
                for ch in target_chapters:
                    bot.repo.chapters.mark_chapter_read(ctx.account_id, int(ch["chapter_id"]))
                    total += 1

                get_account_logger(ctx.account_id).info(
                    f"mark_read: {translit_name!r} — позначено {len(target_chapters)} глав як прочитані"
                )

            return RequestResult.approve(data={"marked": total, "mangas": targets})

        except Exception as exc:
            get_account_logger(ctx.account_id).exception("mark_read: помилка")
            return RequestResult.deny(str(exc))

    # ── Event handlers ────────────────────────────────────────────────────────

    async def _on_daily_claimed(self, payload: dict[str, Any]) -> None:
        if payload.get("account_id") != self._account_id:
            return
        get_account_logger(self._account_id).info("ReaderProfession: daily.claimed → запускаємо читання")
        if self._trigger is not None:
            self._trigger.reschedule("+0s")
        if self._scheduler is not None:
            self._scheduler.wakeup()

    async def _on_chapters_ready(self, payload: dict[str, Any]) -> None:
        """
        Broadcast від MangaLoader — глави є, прокидаємось.

        reschedule("+0s") → _next_fire = now → is_ready() = True одразу.
        wakeup() виводить scheduler з sleep щоб не чекати наступного тіку.
        """
        get_account_logger(self._account_id).info("ReaderProfession: loader.chapters_ready → прокидаємось")
        if self._trigger is not None:
            self._trigger.reschedule("+0s")
        if self._scheduler is not None:
            self._scheduler.wakeup()

    # ── Pipeline ──────────────────────────────────────────────────────────────

    def _make_producer(self) -> Callable[["Account"], Iterable[AnyTask]]:

        def on_cycle_done(bot: "Account") -> None:
            if self._trigger is not None:
                self._trigger.advance(bot)

        def on_exhausted(bot: "Account") -> None:
            """
            Глав немає — емітуємо подію і явно переводимо тригер у сплячку.

            reschedule(inf) миттєво звільняє слот пулу задач:
                _in_flight = False, _next_fire = inf → is_ready() = False.
            Тригер оживає тільки через _on_chapters_ready → reschedule("+0s").
            """
            if self._scheduler is not None:
                self._scheduler.emit_event(
                    "reader.chapters_exhausted",
                    {"account_id": bot.account_id},
                    source=bot.account_id,
                )
            if self._trigger is not None:
                self._trigger.reschedule(float("inf"))  # сплячка — пул вільний

        return pipeline(
            name   = "manga_reader",
            fetch  = _fetch_next_chapter,
            parse  = [],   # Читач не парсить — пустий parse-chain
            action = self._make_read_action(on_cycle_done, on_exhausted),
        )

    def _make_read_action(
        self,
        on_cycle_done: Callable[["Account"], None],
        on_exhausted:  Callable[["Account"], None],
    ) -> Callable[[Any, "Account"], None]:

        def read_chapter(data: Any, bot: "Account") -> None:
            inv: ReaderInventory = bot.inventory.reader  # type: ignore[attr-defined]
            sequence:  list[dict[str, Any]] = data.get("sequence", [])
            slot_name: str                  = data.get("slot_name", "")

            if not sequence:
                # fetch повернув Ready з порожнім sequence — глав немає
                on_exhausted(bot)
                return

            get_account_logger(bot.account_id).info(f"🎰 [{slot_name}] Запуск читання {len(sequence)} глав")

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
                get_account_logger(bot.account_id).info(f"🎰 [{slot_name}] Прочитано (нагорода відсутня)")
                on_cycle_done(bot)
                return

            reward_keys: frozenset[str] = frozenset(reward.keys())
            closed = inv.record_reward(reward_keys)

            if closed:
                slot_info   = next((s for s in inv.all_slots() if s.slot_name == closed), None)
                collected   = slot_info.collected   if slot_info else "?"
                daily_limit = slot_info.daily_limit if slot_info else "?"
                get_account_logger(bot.account_id).info(f"🎉 Слот {closed!r} закрито ({collected}/{daily_limit})")

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

            get_account_logger(bot.account_id).info(
                f"🎰 [{slot_name}] Нагорода: {reward} | "
                f"активних слотів: {len(inv.pending_slots())}"
            )

            if inv.goal_reached():
                get_account_logger(bot.account_id).info("🎯 Усі цільові слоти закриті")
                if self._scheduler is not None:
                    self._scheduler.emit_event(
                        "reader.goal_reached",
                        {"account_id": bot.account_id},
                        source=bot.account_id,
                    )

            on_cycle_done(bot)

        return read_chapter