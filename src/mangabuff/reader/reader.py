"""
farmer/reader.py — ReaderProfession.

Відповідальність:
    ТІЛЬКИ виконання: отримати ask, взяти глави з БД, відправити на сайт,
    записати прочитані, емітити події.

    КОЛИ читати — вирішує ReadingMonitor (не Reader).
    СТАТИСТИКА    — не зберігається тут; логується через події.
    СЛОТИ         — видалено; Reader не знає про нагородні слоти.

Зовнішні виклики:
    scheduler.ask(account_id, "reader", "do_read", {
        "limit":        int,
        "include_tags": list[str] | None,
        "exclude_tags": list[str] | None,
    })

    scheduler.ask(account_id, "reader", "mark_read",  {"targets": [translit_name, ...]})
    scheduler.ask(account_id, "reader", "get_state",  {})
    scheduler.ask(account_id, "reader", "set_reading_params", {
        "limit":        int,
        "include_tags": list[str] | None,
        "exclude_tags": list[str] | None,
    })

Події що емітуються:
    reader.chapters_exhausted  — {account_id}
    reader.chapters_read       — {account_id, count, mangas}
    reader.reward_received     — {account_id, reward}
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

from src.core.runtime.profession import BaseProfession, RequestResult
from src.core.runtime.scheduler import EventDrivenScheduler
from src.mangabuff.reader.inventory import ReaderInventory

if TYPE_CHECKING:
    from src.core.account import Account
    from src.core.runtime.request_router import RequestContext

from src.core.logging.loggers import get_account_logger


# ─────────────────────────────────────────────────────────────────────────────
# ReaderProfession
# ─────────────────────────────────────────────────────────────────────────────

class ReaderProfession(BaseProfession):
    """
    Profession «Читач манги».

    Чистий виконавець: отримує ask → читає → емітить події.
    Не веде статистику, не знає про слоти, не планує наступний запуск.
    Таймінг цілком делегований ReadingMonitor.
    """

    def __init__(self) -> None:
        self._account_id: str                               = ""
        self._scheduler:  Optional["EventDrivenScheduler"] = None

    @property
    def profession_id(self) -> str:
        return "reader"

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def setup(self, scheduler: "EventDrivenScheduler", account_id: str) -> None:
        self._account_id = account_id
        self._scheduler  = scheduler

    async def restore_state(self, bot: "Account") -> None:
        get_account_logger(self._account_id).info("ReaderProfession відновлено")

    async def teardown(self, scheduler: "EventDrivenScheduler", account_id: str) -> None:
        self._scheduler = None

    def check_guard(self, bot: "Account") -> bool:
        return not bool(bot.inventory.personal.data.get("is_banned"))

    # ── handle_request ────────────────────────────────────────────────────────

    async def handle_request(
        self,
        intent: str,
        data:   dict[str, Any],
        ctx:    "RequestContext",
    ) -> RequestResult:
        if intent == "do_read":
            return await self._handle_do_read(data, ctx)
        if intent == "claim_candy":
            return await self._handle_claim_candy(data, ctx)
        if intent == "get_state":
            return await self._handle_get_state(ctx)
        if intent == "set_reading_params":
            return await self._handle_set_reading_params(data, ctx)
        if intent == "mark_read":
            return await self._handle_mark_read(data, ctx)
        return RequestResult.deny(f"unknown intent: {intent!r}")

    # ── do_read ───────────────────────────────────────────────────────────────

    async def _handle_do_read(
        self,
        data: dict[str, Any],
        ctx:  "RequestContext",
    ) -> RequestResult:
        """
        Запускає один цикл читання з параметрами від ReadingMonitor.

        Параметри:
            limit        : int           — кількість глав за раз
            include_tags : list[str]     — фільтр за тегами (включити)
            exclude_tags : list[str]     — фільтр за тегами (виключити)
        """
        limit:        int                 = int(data.get("limit", 2))
        include_tags: Optional[list[str]] = data.get("include_tags") or None
        exclude_tags: Optional[list[str]] = data.get("exclude_tags") or None

        bot = ctx.bot
        log = get_account_logger(bot.account_id)

        sequence, mangas = bot.repo.chapters.get_chapter_sequence(
            account_id   = bot.account_id,
            limit        = limit,
            include_tags = include_tags,
            exclude_tags = exclude_tags,
        )

        if not sequence:
            log.info("📖 Непрочитаних глав немає → chapters_exhausted")
            if self._scheduler is not None:
                await self._scheduler.emit_event(
                    "reader.chapters_exhausted",
                    {"account_id": bot.account_id},
                    source=bot.account_id,
                )
            return RequestResult.approve(data={"read": 0, "mangas": []})

        log.info(f"📖 Знайдено непрочитані глави ({len(sequence)}): {', '.join(mangas)}")

        reward = await bot.safe_session.submit_add_history([
            {"manga_id": ch["manga_id"], "chapter_id": ch["chapter_id"]}
            for ch in sequence
        ])

        if not reward.ok:
            log.warning("📖 submit_add_history провалився — глави не позначено як прочитані")
            return RequestResult.deny("submit_add_history failed")

        data = reward.data or {}
        reward_data = data  # alias for clarity below
        
        for ch in sequence:
            bot.repo.chapters.mark_chapter_read(bot.account_id, int(ch["chapter_id"]))

        reward_str = f" | нагорода: {reward_data}" if reward_data else ""
        log.info(f"📖 Прочитано {len(sequence)} глав: {', '.join(mangas)}{reward_str}")

        if self._scheduler is not None:
            await self._scheduler.emit_event(
                "reader.chapters_read",
                {
                    "account_id": bot.account_id,
                    "count":      len(sequence),
                    "mangas":     mangas,
                },
                source=bot.account_id,
            )
            if reward_data:
                await self._scheduler.emit_event(
                    "reader.reward_received",
                    {"account_id": bot.account_id, "reward": reward_data},
                    source=bot.account_id,
                )

        return RequestResult.approve(data={
            "read":   len(sequence),
            "mangas": mangas,
            "reward": reward_data,
        })

    async def _handle_claim_candy(
        self,
        data: dict[str, Any],
        ctx:  "RequestContext",
    ) -> RequestResult:
        get_account_logger(ctx.account_id).info(f"Claiming candy with data: {data}")
        token: str = data.get("token", "")
        if not token:
            return RequestResult.deny("token обов'язковий")

        bot = ctx.bot
        reward = await bot.safe_session.claim_candy(token)
        if not reward.ok:
            return RequestResult.deny("Не вдалося отримати цукерку")

        return RequestResult.approve(data={"reward": reward.data})
    # ── get_state ─────────────────────────────────────────────────────────────

    async def _handle_get_state(self, ctx: "RequestContext") -> RequestResult:
        inv: ReaderInventory = ctx.bot.inventory.reader  # type: ignore[attr-defined]
        params_raw = inv.data.get("reading_params", {})
        return RequestResult.approve(data={"reading_params": params_raw})

    # ── set_reading_params ────────────────────────────────────────────────────

    async def _handle_set_reading_params(
        self,
        data: dict[str, Any],
        ctx:  "RequestContext",
    ) -> RequestResult:
        """
        Оновлює ReadingParams що ReadingMonitor передаватиме при наступних ask.
        """
        from src.mangabuff.reader.reading_monitor import ReadingParams

        params = ReadingParams(
            limit        = int(data.get("limit", 2)),
            include_tags = data.get("include_tags") or None,
            exclude_tags = data.get("exclude_tags") or None,
        )
        inv: ReaderInventory = ctx.bot.inventory.reader  # type: ignore[attr-defined]
        inv.data["reading_params"] = params.to_dict()

        get_account_logger(ctx.account_id).info(
            f"ReaderProfession: reading_params оновлено → {params}"
        )
        return RequestResult.approve(data={"reading_params": params.to_dict()})

    # ── mark_read ─────────────────────────────────────────────────────────────

    async def _handle_mark_read(
        self,
        data: dict[str, Any],
        ctx:  "RequestContext",
    ) -> RequestResult:
        bot = ctx.bot
        targets: list[str] = data.get("targets", [])

        if not targets:
            return RequestResult.deny("targets (список translit_name) обов'язковий")

        valid: list[str] = []
        log = get_account_logger(ctx.account_id)

        try:
            for name in targets:
                if bot.repo.mangas.get_by_translit_name(name) is None:
                    log.warning(f"mark_read: manga {name!r} не знайдено в БД — пропускаємо")
                else:
                    valid.append(name)

            if not valid:
                return RequestResult.approve(data={"marked": 0, "mangas": []})

            total = bot.repo.chapters.mark_mangas_read(
                account_id=ctx.account_id,
                translit_names=valid,
            )
            log.info(f"mark_read: {total} глав позначено для {valid}")
            return RequestResult.approve(data={"marked": total, "mangas": valid})

        except Exception as exc:
            log.exception("mark_read: помилка")
            return RequestResult.deny(str(exc))