"""
mangabuff/reader/reader.py — ReaderProfession.

Відповідальність:
    ТІЛЬКИ виконання: отримати ask, взяти глави з БД, відправити на сайт,
    записати прочитані, оновити inventory, емітити події.

    КОЛИ читати     — вирішує ReadingMonitor.
    ВИБІР СЛОТА     — вирішує ReadingMonitor; передає ім'я через ask-data.
    ЗБЕРЕЖЕННЯ INV  — ТІЛЬКИ через auto-save у RequestRouter (після approve).

Зовнішні виклики (через scheduler.ask):
    do_read         — {limit, include_tags, exclude_tags, active_slot}
    account_reward  — {reward, chapters_read, active_slot}  ← після reward_received
    claim_candy     — {token, ...}
    get_state       — {}
    set_reading_params — {limit, include_tags, exclude_tags}
    mark_read       — {targets: [translit_name, ...]}

Події що емітуються:
    reader.chapters_exhausted  — {account_id}
    reader.chapters_read       — {account_id, count, mangas}
    reader.reward_received     — {account_id, reward, chapters_read, active_slot}
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

from src.core.runtime.profession import BaseProfession, RequestResult
from src.core.runtime.scheduler import EventDrivenScheduler
from src.mangabuff.reader.inventory import ReaderInventory

if TYPE_CHECKING:
    from src.core.core_account import Account
    from src.core.runtime.request_router import RequestContext

from src.core.logging.loggers import get_account_logger


class ReaderProfession(BaseProfession):
    """
    Profession «Читач манги».

    Чистий виконавець: ask → дія → оновлення inventory → approve.
    RequestRouter гарантує auto-save після кожного approve.

    Жодних прямих викликів repo.inventory.save() тут і в моніторах.
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
        return not bool(bot.inventory.personal.is_banned)

    # ── handle_request ────────────────────────────────────────────────────────

    async def handle_request(
        self,
        intent: str,
        data:   dict[str, Any],
        ctx:    "RequestContext",
    ) -> RequestResult:
        if intent == "do_read":
            return await self._handle_do_read(data, ctx)
        if intent == "account_reward":
            return await self._handle_account_reward(data, ctx)
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
        Один цикл читання.

        Якщо сервер повернув reward — НЕ списуємо глави тут.
        Монітор отримає reward_received, зробить ask("account_reward"),
        і списання відбудеться там — теж всередині RequestRouter з auto-save.
        Якщо reward немає — списуємо одразу і повертаємо нове значення у result.data.
        """
        limit:        int                 = int(data.get("limit", 2))
        include_tags: Optional[list[str]] = data.get("include_tags") or None
        exclude_tags: Optional[list[str]] = data.get("exclude_tags") or None
        active_slot:  Optional[str]       = data.get("active_slot") or None

        log = get_account_logger(self._account_id)
        try:
            # 1. Валідація системних об'єктів
            if self._scheduler is None:
                raise ValueError("Scheduler не доступний")
            
            bot = self._scheduler.get_bot(ctx.account_id)
            if bot is None:
                raise ValueError(f"Бот для акаунта {ctx.account_id} не знайдений")

            sequence, mangas = bot.repo.chapters.get_chapter_sequence(
                account_id   = bot.account_id,
                limit        = limit,
                include_tags = include_tags,
                exclude_tags = exclude_tags,
            )

            if not sequence:
                log.info("📖 Непрочитаних глав немає → chapters_exhausted")
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
                log.warning("📖 submit_add_history провалився")
                return RequestResult.deny("submit_add_history failed")

            reward_data = reward.data or {}

            for ch in sequence:
                bot.repo.chapters.mark_chapter_read(bot.account_id, int(ch["chapter_id"]))

            reward_str = f" | нагорода: {reward_data}" if reward_data else ""
            log.info(f"📖 Прочитано {len(sequence)} глав: {', '.join(mangas)}{reward_str}")

            # Без reward — списуємо глави прямо тут (auto-save захопить при approve)
            chapters_spent_new: Optional[int] = None
            if active_slot and not reward_data:
                inv: ReaderInventory = bot.inventory.reader
                chapters_spent_new = inv.add_slot_chapters_spent(active_slot, len(sequence))

            await self._scheduler.emit_event(
                "reader.chapters_read",
                {"account_id": bot.account_id, "count": len(sequence), "mangas": mangas},
                source=bot.account_id,
            )
            if reward_data:
                # Монітор підхопить і зробить ask("account_reward") →
                # списання глав відбудеться там, теж під auto-save
                await self._scheduler.emit_event(
                    "reader.reward_received",
                    {
                        "account_id":    bot.account_id,
                        "reward":        reward_data,
                        "chapters_read": len(sequence),
                        "active_slot":   active_slot,
                    },
                    source=bot.account_id,
                )

            return RequestResult.approve(data={
                "read":                len(sequence),
                "mangas":              mangas,
                "reward":              reward_data,
                "active_slot":         active_slot,
                "slot_chapters_spent": chapters_spent_new,  # None якщо reward
            })
        except Exception as exc:
            log.exception("do_read: критична помилка")
            return RequestResult.deny(f"Помилка при читанні: {exc}")

    # ── account_reward ────────────────────────────────────────────────────────

    async def _handle_account_reward(
        self,
        data: dict[str, Any],
        ctx:  "RequestContext",
    ) -> RequestResult:
        """
        Обробляє нагороду що прийшла від сайту.

        Викликається з ReadingMonitor._on_reward_received через scheduler.ask —
        тому виконується всередині RequestRouter і auto-save гарантований.

        Дії:
          1. Знаходимо слот за reward_data.
          2. increment_slot_count  — лічильник нагород слота.
          3. add_slot_chapters_spent — списання глав.
          4. Якщо у reward є token — робимо claim_candy через сесію.

        Повертає у result.data: {slot, new_count, new_spent, cap}
        щоб монітор вирішив чи потрібен emit slot_limit_reached.
        """
        reward_data:   dict[str, Any]  = data.get("reward", {})
        chapters_read: int             = int(data.get("chapters_read", 0))

        log = get_account_logger(self._account_id)
        try:
            # 1. Валідація системних об'єктів
            if self._scheduler is None:
                raise ValueError("Scheduler не доступний")
            
            bot = self._scheduler.get_bot(ctx.account_id)
            if bot is None:
                raise ValueError(f"Бот для акаунта {ctx.account_id} не знайдений")
            cfg = bot.app_config.reader

            slot = cfg.find_slot(reward_data)
            if slot is None:
                log.debug(f"[Reader] account_reward: слот не знайдено для {reward_data}")
                return RequestResult.approve(data={"slot": None})

            inv: ReaderInventory = bot.inventory.reader

            new_count = inv.increment_slot_count(slot.name)
            log.info(f"[Reader] slot={slot.name!r} count={new_count}/{slot.daily_limit}")

            new_spent: int = 0
            if chapters_read > 0:
                new_spent = inv.add_slot_chapters_spent(slot.name, chapters_read)
                cap = slot.max_chapters_per_slot
                cap_str = f"/{cap}" if cap > 0 else ""
                log.info(f"[Reader] slot={slot.name!r} chapters_spent={new_spent}{cap_str}")
            else:
                new_spent = inv.slot_chapters_spent.get(slot.name, 0)

            # Claim candy якщо є токен (через сесію, не через окремий ask)
            if reward_data.get("token"):
                candy = await bot.safe_session.claim_candy(reward_data["token"])
                if not candy.ok:
                    log.warning("[Reader] claim_candy провалився")

            return RequestResult.approve(data={
                "slot":      slot.name,
                "new_count": new_count,
                "new_spent": new_spent,
                "cap":       slot.max_chapters_per_slot,
                "daily_limit": slot.daily_limit,
            })
            
        except Exception as exc:
            log.exception("handle_reward: критична помилка")
            return RequestResult.deny(f"Помилка при зборі нагороди: {exc}")

    # ── claim_candy ───────────────────────────────────────────────────────────

    async def _handle_claim_candy(
        self,
        data: dict[str, Any],
        ctx:  "RequestContext",
    ) -> RequestResult:
        token: str = data.get("token", "")
        log = get_account_logger(self._account_id)
        try:
            # 1. Валідація системних об'єктів
            if self._scheduler is None:
                raise ValueError("Scheduler не доступний")
            
            bot = self._scheduler.get_bot(ctx.account_id)
            
            if bot is None:
                raise ValueError(f"Бот для акаунта {ctx.account_id} не знайдений")
            
            if not token:
                raise ValueError("token обов'язковий")
            
            reward = await bot.safe_session.claim_candy(token)
            if not reward.ok:
                return RequestResult.deny("claim_candy провалився")
            return RequestResult.approve(data={"reward": reward.data})
        except Exception as exc:
            log.exception("claim_candy: критична помилка")
            return RequestResult.deny(f"Помилка при зборі цукерки: {exc}")

    # ── get_state ─────────────────────────────────────────────────────────────

    async def _handle_get_state(self, ctx: "RequestContext") -> RequestResult:
        log = get_account_logger(self._account_id)
        try:
            # 1. Валідація системних об'єктів
            if self._scheduler is None:
                raise ValueError("Scheduler не доступний")
            
            bot = self._scheduler.get_bot(ctx.account_id)
            
            if bot is None:
                raise ValueError("bot не доступний")
            
            inv = bot.inventory.reader
            return RequestResult.approve(data={"reading_params": inv.reading_params})
        except Exception as exc:
            log.exception("claim_candy: критична помилка")
            return RequestResult.deny(f"Помилка при зборі цукерки: {exc}")

    # ── set_reading_params ────────────────────────────────────────────────────

    async def _handle_set_reading_params(
        self,
        data: dict[str, Any],
        ctx:  "RequestContext",
    ) -> RequestResult:
        from src.mangabuff.reader.reading_monitor import ReadingParams

        params = ReadingParams(
            limit        = int(data.get("limit", 2)),
            include_tags = data.get("include_tags") or None,
            exclude_tags = data.get("exclude_tags") or None,
        )
        
        log = get_account_logger(self._account_id)
        try:
            # 1. Валідація системних об'єктів
            if self._scheduler is None:
                raise ValueError("Scheduler не доступний")
            
            bot = self._scheduler.get_bot(ctx.account_id)
            
            if bot is None:
                raise ValueError("bot не доступний")
            
            inv: ReaderInventory = bot.inventory.reader
            inv.data["reading_params"] = params.to_dict()

            get_account_logger(ctx.account_id).info(
                f"[Reader] reading_params оновлено → {params}"
            )
            return RequestResult.approve(data={"reading_params": params.to_dict()})
        except Exception as exc:
            log.exception("set_reading_params: критична помилка")
            return RequestResult.deny(f"Помилка при установці параметрів читання: {exc}")

    # ── mark_read ─────────────────────────────────────────────────────────────

    async def _handle_mark_read(
        self,
        data: dict[str, Any],
        ctx:  "RequestContext",
    ) -> RequestResult:
        log = get_account_logger(self._account_id)
        try:
            # 1. Валідація системних об'єктів
            if self._scheduler is None:
                raise ValueError("Scheduler не доступний")
            
            bot = self._scheduler.get_bot(ctx.account_id)
            
            if bot is None:
                raise ValueError("bot не доступний")
            
            targets: list[str] = data.get("targets", [])
            log = get_account_logger(ctx.account_id)

            if not targets:
                return RequestResult.deny("targets (список translit_name) обов'язковий")

            valid: list[str] = []
            for name in targets:
                if bot.repo.mangas.get_by_translit_name(name) is None:
                    log.warning(f"mark_read: manga {name!r} не знайдено — пропускаємо")
                else:
                    valid.append(name)

            if not valid:
                return RequestResult.approve(data={"marked": 0, "mangas": []})

            total = bot.repo.chapters.mark_mangas_read(
                account_id     = ctx.account_id,
                translit_names = valid,
            )
            log.info(f"mark_read: {total} глав позначено для {valid}")
            return RequestResult.approve(data={"marked": total, "mangas": valid})

        except Exception as exc:
            log.exception("mark_read: помилка")
            return RequestResult.deny(str(exc))