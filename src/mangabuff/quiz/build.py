"""
quiz/build.py — QuizProfession.

Відповідальність:
    ТІЛЬКИ виконання: отримати ask, зробити HTTP, оновити inventory, емітити події.

    КОЛИ запускати      — вирішує QuizMonitor (не Profession).
    СКИДАННЯ ЛІЧИЛЬНИКА — QuizMonitor в _on_daily_claimed (не Profession).
    ПЛАНУВАННЯ ЗАТРИМОК — QuizMonitor через asyncio.sleep(answer_delay).

Два режими (mode зберігається в inventory):
    daily — N правильних відповідей на день.
    fixed — N правильних відповідей один раз назавжди.

Intents:
    do_open     → відкрити quiz-сесію, зберегти питання в inventory
    do_answer   → відповісти на поточне питання
    get_status  → поточний стан
    set_config  → змінити mode / answer_limit / answer_delay
    force_restart → скинути стан (тільки inventory, QuizMonitor сам стартує)

Events що емітуються:
    quiz.session_opened   — {account_id, question_id}
    quiz.answered_correct — {account_id, question_id, correct_count, counter, limit}
    quiz.milestone        — {account_id, milestone, message, correct_count}
    quiz.limit_reached    — {account_id, counter, correct_count, mode}
    quiz.session_ended    — {account_id, reason, correct_count, mode}
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

from src.core.runtime.profession import BaseProfession, RequestResult
from src.mangabuff.quiz.inventory import QuizInventory
from src.utils.time import today

if TYPE_CHECKING:
    from src.core.core_account import Account
    from src.core.runtime.request_router import RequestContext
    from src.core.runtime.scheduler import EventDrivenScheduler

from src.core.logging.loggers import get_account_logger


class QuizProfession(BaseProfession):
    """
    Profession «Квіз».

    Чистий виконавець: отримує ask → HTTP → inventory → emit.
    Не знає про таймінги, не планує наступний запуск, не скидає лічильник.
    """

    def __init__(self) -> None:
        self._account_id: str                              = ""
        self._scheduler:  Optional["EventDrivenScheduler"] = None

    @property
    def profession_id(self) -> str:
        return "quiz"

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def setup(self, scheduler: "EventDrivenScheduler", account_id: str) -> None:
        self._account_id = account_id
        self._scheduler  = scheduler

    async def restore_state(self, bot: "Account") -> None:
        inv: QuizInventory = bot.inventory.quiz  # type: ignore[attr-defined]
        inv.init_from_config(bot.app_config.quiz)

        to_day = today()
        if inv.mode == "daily" and inv.answers_reset_date != to_day:
            inv.reset_daily_counter(to_day)
            inv.session_active   = False
            inv.current_question = None
            get_account_logger(self._account_id).info(
                "Quiz(daily): новий день → лічильник скинуто"
            )

        get_account_logger(self._account_id).info(
            f"QuizProfession відновлено: "
            f"mode={inv.mode!r} "
            f"counter={inv.current_counter()}/{inv.answer_limit} "
            f"delay={inv.answer_delay}s "
            f"session_active={inv.session_active} "
            f"fixed_done={inv.fixed_done}"
        )

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
        if intent == "do_open":
            return await self._handle_do_open(ctx)
        if intent == "do_answer":
            return await self._handle_do_answer(ctx)
        if intent == "get_status":
            return await self._handle_get_status(ctx)
        if intent == "set_config":
            return await self._handle_set_config(data, ctx)
        if intent == "force_restart":
            return await self._handle_force_restart(ctx)
        if intent == "reset_daily":
            return await self._handle_reset_daily(ctx)
        return RequestResult.deny(f"unknown intent: {intent!r}")

    # ── do_open ───────────────────────────────────────────────────────────────

    async def _handle_do_open(self, ctx: "RequestContext") -> RequestResult:
        """Відкриває нову quiz-сесію. Зберігає перше питання в inventory."""
        bot = ctx.bot
        inv: QuizInventory = bot.inventory.quiz  # type: ignore[attr-defined]
        log = get_account_logger(ctx.account_id)

        if inv.session_active:
            log.debug("do_open: сесія вже активна, пропускаємо")
            return RequestResult.approve(data={"status": "already_open"})

        log.info("📝 Quiz: відкриваємо сесію…")
        question = await bot.safe_session.quiz_start()

        data = question.data
        if data is None:
            log.warning("⚠️ /quiz/start не відповів")
            return RequestResult.deny("quiz_start returned None")

        inv.open_session(data)

        log.info(
            f"✅ Сесія відкрита | "
            f"q_id={data.get('id')} | "
            f"{data.get('question', '')[:60]}"
        )

        if self._scheduler is not None:
            await self._scheduler.emit_event(
                "quiz.session_opened",
                {"account_id": ctx.account_id, "question_id": question.data.get("id")},
                source=ctx.account_id,
            )

        return RequestResult.approve(data={
            "question_id": data.get("id"),
            "question":    data.get("question", ""),
        })

    # ── do_answer ─────────────────────────────────────────────────────────────

    async def _handle_do_answer(self, ctx: "RequestContext") -> RequestResult:
        """
        Відповідає на поточне питання в inventory.

        Повертає data з полем status:
            "correct"   — правильно, сесія продовжується
            "milestone" — правильно + досягнення
            "limit"     — ліміт досягнуто, сесія закрита
            "restart"   — невірна відповідь, сесія закрита
            "no_next"   — сервер вичерпав питання, сесія закрита
        """
        bot = ctx.bot
        inv: QuizInventory = bot.inventory.quiz  # type: ignore[attr-defined]
        log = get_account_logger(ctx.account_id)

        question = inv.current_question
        if question is None:
            log.warning("⚠️ do_answer: питання в inventory відсутнє")
            return RequestResult.deny("no current question")

        answer_text: str = question.get("correct_text", question["answers"][0])

        log.info(
            f"❓ [Q#{question.get('id')}] "
            f"{question.get('question', '')[:60]} → «{answer_text}»"
        )

        result = await bot.safe_session.quiz_answer(answer_text)

        if result.data is None:
            log.warning("⚠️ /quiz/answer не відповів")
            return RequestResult.deny("quiz_answer returned None")

        status = result.data.get("status")

        # ── Правильна відповідь ────────────────────────────────────────────────
        if status in ("success", "milestone"):
            inv.correct_count = result.data.get("correct_count", inv.correct_count + 1)
            counter = inv.increment_counter()

            log.info(
                f"✅ Вірно! "
                f"correct={inv.correct_count} | "
                f"counter={counter}/{inv.answer_limit} "
                f"(mode={inv.mode})"
            )

            if self._scheduler is not None:
                await self._scheduler.emit_event(
                    "quiz.answered_correct",
                    {
                        "account_id":    ctx.account_id,
                        "question_id":   question.get("id"),
                        "correct_count": inv.correct_count,
                        "counter":       counter,
                        "limit":         inv.answer_limit,
                    },
                    source=ctx.account_id,
                )

            if status == "milestone":
                log.info(
                    f"🏅 Milestone! "
                    f"milestone={result.data.get('milestone')} "
                    f"message={result.data.get('message', '')!r}"
                )
                if self._scheduler is not None:
                    await self._scheduler.emit_event(
                        "quiz.milestone",
                        {
                            "account_id":    ctx.account_id,
                            "milestone":     result.data.get("milestone"),
                            "message":       result.data.get("message"),
                            "correct_count": inv.correct_count,
                        },
                        source=ctx.account_id,
                    )

            # ── Ліміт досягнуто ───────────────────────────────────────────────
            if counter >= inv.answer_limit:
                log.info(
                    f"🎯 Ліміт досягнуто "
                    f"({counter}/{inv.answer_limit}, mode={inv.mode}) → закриваємо"
                )
                inv.close_session(today())

                if self._scheduler is not None:
                    await self._scheduler.emit_event(
                        "quiz.limit_reached",
                        {
                            "account_id":    ctx.account_id,
                            "counter":       counter,
                            "correct_count": inv.correct_count,
                            "mode":          inv.mode,
                        },
                        source=ctx.account_id,
                    )

                return RequestResult.approve(data={"status": "limit"})

            # ── Наступне питання ──────────────────────────────────────────────
            next_question = result.data.get("question")
            if next_question:
                inv.current_question = next_question
                log.info(f"➡️  Наступне питання id={next_question.get('id')}")
                return RequestResult.approve(data={
                    "status": "milestone" if status == "milestone" else "correct",
                })

            # Сервер вичерпав питання
            log.info("🏆 Сервер вичерпав питання")
            inv.close_session(today())
            return RequestResult.approve(data={"status": "no_next"})

        # ── Невірна відповідь / restart ───────────────────────────────────────
        if status == "restart":
            log.info(
                f"❌ Невірна відповідь. "
                f"Результат: {inv.correct_count}. "
                f"mode={inv.mode}. {result.data.get('message', '')}"
            )
            inv.close_session(today())

            if self._scheduler is not None:
                await self._scheduler.emit_event(
                    "quiz.session_ended",
                    {
                        "account_id":    ctx.account_id,
                        "reason":        "restart",
                        "correct_count": inv.correct_count,
                        "mode":          inv.mode,
                    },
                    source=ctx.account_id,
                )

            return RequestResult.approve(data={"status": "restart"})

        log.warning(f"⚠️ Невідомий status: {status!r}")
        return RequestResult.deny(f"unknown quiz status: {status!r}")

    # ── get_status ────────────────────────────────────────────────────────────

    async def _handle_get_status(self, ctx: "RequestContext") -> RequestResult:
        inv: QuizInventory = ctx.bot.inventory.quiz  # type: ignore[attr-defined]
        q = inv.current_question
        return RequestResult.approve(data={
            "mode":             inv.mode,
            "answer_limit":     inv.answer_limit,
            "answer_delay":     inv.answer_delay,
            "session_active":   inv.session_active,
            "counter":          inv.current_counter(),
            "correct_count":    inv.correct_count,
            "last_quiz_date":   inv.last_quiz_date,
            "fixed_done":       inv.fixed_done,
            "current_question": q.get("question") if q else None,
        })

    # ── set_config ────────────────────────────────────────────────────────────

    async def _handle_set_config(
        self, data: dict[str, Any], ctx: "RequestContext"
    ) -> RequestResult:
        inv: QuizInventory = ctx.bot.inventory.quiz  # type: ignore[attr-defined]
        changed: dict[str, Any] = {}

        if "mode" in data:
            mode = data["mode"]
            if mode not in ("daily", "fixed"):
                return RequestResult.deny("mode має бути 'daily' або 'fixed'")
            inv.mode = mode
            changed["mode"] = mode

        if "answer_limit" in data:
            limit = data["answer_limit"]
            if not isinstance(limit, int) or limit < 1:
                return RequestResult.deny("answer_limit має бути цілим числом >= 1")
            inv.answer_limit = limit
            changed["answer_limit"] = limit

        if "answer_delay" in data:
            delay = data["answer_delay"]
            if not isinstance(delay, (int, float)) or delay < 0:
                return RequestResult.deny("answer_delay має бути числом >= 0")
            inv.answer_delay = float(delay)
            changed["answer_delay"] = float(delay)

        if not changed:
            return RequestResult.deny(
                "нічого не змінено — передайте mode, answer_limit або answer_delay"
            )

        get_account_logger(ctx.account_id).info(f"Quiz: конфіг оновлено → {changed}")
        return RequestResult.approve(data={"changed": changed})

    # ── reset_daily ───────────────────────────────────────────────────────────

    async def _handle_reset_daily(self, ctx: "RequestContext") -> RequestResult:
        """
        Скидає щоденний лічильник квіза після daily.claimed.

        Викликається QuizMonitor через ask() — монітор НЕ мутує inventory напряму.
        Збереження відбувається автоматично воркером після завершення задачі.
        """
        inv: QuizInventory = ctx.bot.inventory.quiz  # type: ignore[attr-defined]

        if inv.mode != "daily":
            return RequestResult.deny(f"reset_daily не застосовний для mode={inv.mode!r}")

        to_day = today()
        if inv.last_quiz_date == to_day and inv.current_counter() >= inv.answer_limit:
            get_account_logger(ctx.account_id).info(
                "QuizProfession: reset_daily → ліміт вже вичерпано сьогодні, пропускаємо"
            )
            return RequestResult.deny("daily limit already reached today")

        inv.reset_daily_counter(to_day)
        inv.session_active   = False
        inv.current_question = None
        inv.last_quiz_date   = None  # знімаємо блок _should_run у QuizMonitor

        get_account_logger(ctx.account_id).info(
            "QuizProfession: reset_daily → лічильник скинуто"
        )
        return RequestResult.approve(data={"status": "reset"})

    # ── force_restart ─────────────────────────────────────────────────────────

    async def _handle_force_restart(self, ctx: "RequestContext") -> RequestResult:
        """
        Скидає стан inventory. QuizMonitor сам підхопить через _should_run()
        і запустить новий цикл при наступній перевірці або на daily.claimed.
        """
        inv: QuizInventory = ctx.bot.inventory.quiz  # type: ignore[attr-defined]
        inv.session_active   = False
        inv.current_question = None

        if inv.mode == "daily":
            inv.reset_daily_counter(today())
            inv.last_quiz_date = None
        else:
            inv.answers_done = 0
            inv.fixed_done   = False

        get_account_logger(ctx.account_id).info("QuizProfession: force_restart")

        # Повідомляємо монітор щоб стартував негайно
        if self._scheduler is not None:
            await self._scheduler.emit_event(
                "quiz.force_restarted",
                {"account_id": ctx.account_id},
                source=ctx.account_id,
            )

        return RequestResult.approve(data={"status": "restarting"})