"""
quiz/build.py — QuizProfession.

Два режими (зберігаються в inv.mode, дефолт з yaml):

┌─────────┬──────────────────────────────────────────────────────────────────┐
│  daily  │ N правильних відповідей на день.                                 │
│         │ Старт: після daily.claimed (як Reader).                          │
│         │ Зупинка: counter >= inv.answer_limit АБО restart.               │
│         │ Скидання: daily.claimed → answers_today=0, last_quiz_date=None.  │
├─────────┼──────────────────────────────────────────────────────────────────┤
│  fixed  │ N правильних відповідей один раз, без прив'язки до часу.        │
│         │ Старт: одразу при запуску бота.                                  │
│         │ Зупинка: counter >= inv.answer_limit АБО restart.               │
│         │ Після зупинки: fixed_done=True, тригер помирає назавжди.         │
└─────────┴──────────────────────────────────────────────────────────────────┘

Всі параметри (mode, answer_limit, answer_delay) живуть в inventory.data.
Config (yaml) заповнює тільки відсутні поля через inv.init_from_config(cfg).
Після першого запуску yaml більше не читається — тільки inventory.

Статуси /quiz/answer:
    success   — правильна відповідь, наступне питання в тілі
    milestone — те саме що success + дані про досягнення (обробляється однаково)
    restart   — невірна відповідь, сесія скидається сервером

Events:
    "quiz.session_opened"   — сесія відкрита
    "quiz.answered_correct" — правильна відповідь (включає milestone)
    "quiz.milestone"        — досягнення (додатково до answered_correct)
    "quiz.limit_reached"    — ліміт досягнуто → сесія закрита
    "quiz.session_ended"    — сесія закрита через restart

handle_request intents:
    "get_status"    → поточний стан
    "set_config"    → змінити mode/answer_limit/answer_delay в inventory
                      data: {"mode"?, "answer_limit"?, "answer_delay"?}
    "force_restart" → скинути стан і запустити негайно
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Iterable, Optional

from src.core.runtime.profession import BaseProfession, RequestResult
from src.core.runtime.schedule import BaseTrigger
from src.core.tasks.base import AnyTask, Priority, Task
from src.mangabuff.quiz.inventory import QuizInventory
from src.utils.time import today

if TYPE_CHECKING:
    from src.core.account import Account
    from src.core.runtime.request_router import RequestContext
    from src.core.runtime.schedule import TriggerProtocol
    from src.core.runtime.scheduler import EventDrivenScheduler

from src.core.logging.loggers import get_account_logger


# ─────────────────────────────────────────────────────────────────────────────
# Реєстрація inventory
# ─────────────────────────────────────────────────────────────────────────────

def register_inventory() -> None:
    from src.core.inventory.factory import inventory_factory
    inventory_factory.register("quiz", "quiz", QuizInventory)


# ─────────────────────────────────────────────────────────────────────────────
# QuizTrigger
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class QuizTrigger(BaseTrigger):
    """
    Тригер що чергує open і answer задачі.

    Всі параметри читаються з inventory при кожному виклику —
    без hardcode, без полів у самому тригері.

    is_expired:
        daily → session_active=False AND last_quiz_date=today
        fixed → fixed_done=True

    next_delay:
        читається з inv.answer_delay
    """
    _open_producer:   Callable[["Account"], Iterable[AnyTask]]
    _answer_producer: Callable[["Account"], Iterable[AnyTask]]

    def next_delay(self, bot: "Account") -> float:
        inv: QuizInventory = bot.inventory.quiz  # type: ignore[attr-defined]
        return inv.answer_delay

    def is_expired(self, inv: Any) -> bool:
        quiz_inv = getattr(inv, "quiz", None)
        if quiz_inv is None:
            return False
        if quiz_inv.mode == "daily":
            return (
                not quiz_inv.session_active
                and quiz_inv.last_quiz_date == today()
            )
        # fixed
        return quiz_inv.fixed_done

    def producer(self, bot: "Account") -> Iterable[AnyTask]:
        inv: QuizInventory = bot.inventory.quiz  # type: ignore[attr-defined]
        if not inv.session_active:
            get_account_logger(bot.account_id).info(f"📝 Quiz: сесія не активна → відкриваємо")
            return self._open_producer(bot)
        return self._answer_producer(bot)


# ─────────────────────────────────────────────────────────────────────────────
# QuizProfession
# ─────────────────────────────────────────────────────────────────────────────

class QuizProfession(BaseProfession):

    def __init__(self) -> None:
        self._account_id: str                              = ""
        self._trigger:    Optional[QuizTrigger]            = None
        self._scheduler:  Optional["EventDrivenScheduler"] = None

    @property
    def profession_id(self) -> str:
        return "quiz"

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def setup(self, scheduler: "EventDrivenScheduler", account_id: str) -> None:
        self._account_id = account_id
        self._scheduler  = scheduler
        scheduler.subscribe("daily.claimed", self._on_daily_claimed)

    async def restore_state(self, bot: "Account") -> None:
        inv: QuizInventory = bot.inventory.quiz  # type: ignore[attr-defined]

        # Заповнюємо відсутні поля з yaml — один раз при першому запуску
        inv.init_from_config(bot.app_config.quiz)

        to_day = today()

        if inv.mode == "daily" and inv.answers_reset_date != to_day:
            # Бот перезапустився на новий день — скидаємо лічильник.
            # daily.claimed ще не прийшов, тому просто чекаємо події.
            inv.reset_daily_counter(to_day)
            inv.session_active   = False
            inv.current_question = None
            get_account_logger(self._account_id).info(f"Quiz(daily): новий день → лічильник скинуто")

        get_account_logger(self._account_id).info(
            f"QuizProfession відновлено: "
            f"mode={inv.mode!r} "
            f"counter={inv.current_counter()}/{inv.answer_limit} "
            f"delay={inv.answer_delay}s "
            f"session_active={inv.session_active} "
            f"fixed_done={inv.fixed_done}"
        )

    # ── Triggers ──────────────────────────────────────────────────────────────

    def build_triggers(self, account_id: str) -> list["TriggerProtocol"]:
        trigger = QuizTrigger(
            name             = "quiz_cycle",
            account_id       = account_id,
            _open_producer   = self._make_open_producer(),
            _answer_producer = self._make_answer_producer(),
        )
        self._trigger = trigger
        return [trigger]

    # ── Events ────────────────────────────────────────────────────────────────

    async def _on_daily_claimed(self, payload: dict[str, Any]) -> None:
        if payload.get("account_id") != self._account_id:
            return
        if self._scheduler is None:
            return

        bot = self._scheduler.get_bot(self._account_id)
        if bot is None:
            return

        inv: QuizInventory = bot.inventory.quiz  # type: ignore[attr-defined]

        if inv.mode != "daily":
            return

        if inv.last_quiz_date == today():
            get_account_logger(self._account_id).info(f"Quiz(daily): ліміт вже вичерпано сьогодні → пропускаємо")
            return

        get_account_logger(self._account_id).info(f"Quiz(daily): daily.claimed → скидаємо лічильник, стартуємо")

        inv.reset_daily_counter(today())
        inv.session_active   = False
        inv.current_question = None
        inv.last_quiz_date   = None  # знімаємо блок is_expired

        if self._trigger is not None:
            self._trigger.reschedule("+0s")
        self._scheduler.wakeup()

    # ── handle_request ────────────────────────────────────────────────────────

    async def handle_request(
        self,
        intent: str,
        data:   dict[str, Any],
        ctx:    "RequestContext",
    ) -> RequestResult:
        if intent == "get_status":
            return await self._handle_get_status(ctx)
        if intent == "set_config":
            return await self._handle_set_config(data, ctx)
        if intent == "force_restart":
            return await self._handle_force_restart(ctx)
        return RequestResult.deny(f"unknown intent: {intent!r}")

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

    async def _handle_set_config(
        self, data: dict[str, Any], ctx: "RequestContext"
    ) -> RequestResult:
        """
        Змінює конфігураційні поля безпосередньо в inventory.
        Приймає будь-яку підмножину: mode, answer_limit, answer_delay.
        """
        inv: QuizInventory = ctx.bot.inventory.quiz  # type: ignore[attr-defined]
        changed = {}

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
            return RequestResult.deny("нічого не змінено — передайте mode, answer_limit або answer_delay")

        get_account_logger(ctx.account_id).info(f"Quiz: конфіг оновлено → {changed}")
        return RequestResult.approve(data={"changed": changed})

    async def _handle_force_restart(self, ctx: "RequestContext") -> RequestResult:
        inv: QuizInventory = ctx.bot.inventory.quiz  # type: ignore[attr-defined]
        inv.session_active   = False
        inv.current_question = None
        if inv.mode == "daily":
            inv.reset_daily_counter(today())
            inv.last_quiz_date = None
        else:
            inv.answers_done = 0
            inv.fixed_done   = False
        if self._trigger is not None:
            self._trigger.reschedule("+0s")
        if self._scheduler is not None:
            self._scheduler.wakeup()
        get_account_logger(ctx.account_id).info(f"QuizProfession: force_restart")
        return RequestResult.approve(data={"status": "restarting"})

    # ── Producers ─────────────────────────────────────────────────────────────

    def _make_open_producer(self) -> Callable[["Account"], Iterable[AnyTask]]:

        def on_done(bot: "Account") -> None:
            if self._trigger is not None:
                self._trigger.advance(bot)
            if self._scheduler is not None:
                self._scheduler.wakeup()

        def open_quiz(bot: "Account") -> None:
            get_account_logger(bot.account_id).info(f"📝 Відкриваємо quiz-сесію…")
            question = bot.session.quiz_start()

            if question is None:
                get_account_logger(bot.account_id).warning(f"⚠️ /quiz/start не відповів")
                on_done(bot)
                return

            inv: QuizInventory = bot.inventory.quiz  # type: ignore[attr-defined]
            inv.open_session(question)

            get_account_logger(bot.account_id).info(
                f"✅ Сесія відкрита | "
                f"q_id={question.get('id')} | "
                f"{question.get('question', '')[:60]}"
            )

            if self._scheduler is not None:
                self._scheduler.emit_event(
                    "quiz.session_opened",
                    {"account_id": bot.account_id, "question_id": question.get("id")},
                    source=bot.account_id,
                )
            on_done(bot)

        return lambda bot: [Task(
            name        = "quiz:open",
            fn          = open_quiz,
            priority    = Priority.NORMAL,
            max_retries = 2,
        )]

    def _make_answer_producer(self) -> Callable[["Account"], Iterable[AnyTask]]:

        def on_done(bot: "Account") -> None:
            if self._trigger is not None:
                self._trigger.advance(bot)

        def answer_quiz(bot: "Account") -> None:
            inv: QuizInventory = bot.inventory.quiz  # type: ignore[attr-defined]

            question = inv.current_question
            if question is None:
                get_account_logger(bot.account_id).warning(f"⚠️ answer_quiz: питання в inventory відсутнє")
                on_done(bot)
                return

            answer_text: str = question.get("correct_text", question["answers"][0])

            get_account_logger(bot.account_id).info(
                f"❓ [Q#{question.get('id')}] "
                f"{question.get('question', '')[:60]} → «{answer_text}»"
            )

            result = bot.session.quiz_answer(answer_text)

            if result is None:
                get_account_logger(bot.account_id).warning(f"⚠️ /quiz/answer не відповів")
                on_done(bot)
                return

            status = result.get("status")

            # ── Правильна відповідь (або milestone — той самий success + досягнення) ──
            if status in ("success", "milestone"):
                if status == "milestone":
                    get_account_logger(bot.account_id).info(
                        f"🏅 Milestone! "
                        f"milestone={result.get('milestone')} "
                        f"message={result.get('message', '')!r}"
                    )
                inv.correct_count = result.get("correct_count", inv.correct_count + 1)
                counter = inv.increment_counter()

                get_account_logger(bot.account_id).info(
                    f"✅ Вірно! "
                    f"correct={inv.correct_count} | "
                    f"counter={counter}/{inv.answer_limit} "
                    f"(mode={inv.mode})"
                )

                if self._scheduler is not None:
                    self._scheduler.emit_event(
                        "quiz.answered_correct",
                        {
                            "account_id":    bot.account_id,
                            "question_id":   question.get("id"),
                            "correct_count": inv.correct_count,
                            "counter":       counter,
                            "limit":         inv.answer_limit,
                        },
                        source=bot.account_id,
                    )
                    if status == "milestone":
                        self._scheduler.emit_event(
                            "quiz.milestone",
                            {
                                "account_id":    bot.account_id,
                                "milestone":     result.get("milestone"),
                                "message":       result.get("message"),
                                "correct_count": inv.correct_count,
                            },
                            source=bot.account_id,
                        )

                # ── Ліміт досягнуто ───────────────────────────────────────────
                if counter >= inv.answer_limit:
                    get_account_logger(bot.account_id).info(
                        f"🎯 Ліміт досягнуто "
                        f"({counter}/{inv.answer_limit}, mode={inv.mode}) → закриваємо"
                    )
                    inv.close_session(today())

                    if self._scheduler is not None:
                        self._scheduler.emit_event(
                            "quiz.limit_reached",
                            {
                                "account_id":    bot.account_id,
                                "counter":       counter,
                                "correct_count": inv.correct_count,
                                "mode":          inv.mode,
                            },
                            source=bot.account_id,
                        )
                    on_done(bot)
                    return

                # ── Продовжуємо ───────────────────────────────────────────────
                next_question = result.get("question")
                if next_question:
                    inv.current_question = next_question
                    get_account_logger(bot.account_id).info(f"➡️  Наступне питання id={next_question.get('id')}")
                else:
                    get_account_logger(bot.account_id).info(f"🏆 Сервер вичерпав питання")
                    inv.close_session(today())

                on_done(bot)
                return

            # ── Невірна відповідь / restart ───────────────────────────────────
            if status == "restart":
                get_account_logger(bot.account_id).info(
                    f"❌ Невірна відповідь. "
                    f"Результат: {inv.correct_count}. "
                    f"mode={inv.mode}. {result.get('message', '')}"
                )
                inv.close_session(today())

                if self._scheduler is not None:
                    self._scheduler.emit_event(
                        "quiz.session_ended",
                        {
                            "account_id":    bot.account_id,
                            "reason":        "restart",
                            "correct_count": inv.correct_count,
                            "mode":          inv.mode,
                        },
                        source=bot.account_id,
                    )
                on_done(bot)
                return

            get_account_logger(bot.account_id).warning(f"⚠️ Невідомий status: {status!r}")
            on_done(bot)

        return lambda bot: [Task(
            name        = "quiz:answer",
            fn          = answer_quiz,
            priority    = Priority.NORMAL,
            max_retries = 1,
        )]