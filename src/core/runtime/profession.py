"""
src/core/runtime/profession.py — BaseProfession + RequestResult.

Архітектурна роль:
    BaseProfession — абстрактний stateful domain agent.
    Підкласи реалізують конкретну бізнес-логіку (Reader, Daily, Trader...).

    Profession знає лише про:
      - свій inventory namespace (bot.inventory.{attr})
      - scheduler як спосіб комунікації (emit_event / ask)

    Profession НЕ знає про:
      - BotWorker
      - інші Profession
      - scheduling loop

Де зберігається стан?
    bot.inventory.{namespace} (Inventories = BlackBoard).
    Persistується автоматично через BotWorker після кожного task.
    Для критичних checkpoint-ів — явно через bot.repo.inventory.save().

Lifecycle:
    setup(scheduler, account_id)  — підписки + реєстрація triggers
    restore_state(bot)            — відновлення in-memory state з inventory
    handle_request(intent, data)  — відповідь на scheduler.ask()
    on_event(event_name, payload) — реакція на EventBus події
    teardown(scheduler, account_id) — звільнення ресурсів
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, Optional, List, Type

if TYPE_CHECKING:
    from src.core.account import Account
    from src.core.runtime.request_router import RequestContext
    from src.core.runtime.scheduler import EventDrivenScheduler
    from src.core.runtime.schedule import TriggerProtocol
    from src.core.tasks.base import AnyTask

from src.core.logging.loggers import get_logger
log = get_logger("runtime.profession")


class BaseProfession(ABC):
    """
    Stateful behavioral component — autonomous domain agent.

    Підклас реалізує:
        profession_id    — унікальний str ідентифікатор
        setup()          — реєструє triggers та event-підписки
        handle_request() — відповідає на scheduler.ask()

    Підклас може перевизначити:
        on_event()       — реакція на EventBus події (за замовч. нічого)
        restore_state()  — відновлення in-memory cache з bot.inventory
        teardown()       — звільнення ресурсів при видаленні акаунта
        build_triggers() — повертає список triggers для реєстрації
        startup_tasks()  — tasks що виконуються одразу при старті
        check_guard()    — чи може profession бути активною

    Підклас НЕ повинен:
        - зберігати посилання на BotWorker
        - викликати інші Profession напряму
        - містити scheduling loop або sleep()
        - зберігати критичний стан лише in-memory
    """

    # ── Abstract ──────────────────────────────────────────────────────────────

    @property
    @abstractmethod
    def profession_id(self) -> str:
        """Унікальний рядковий ідентифікатор. Наприклад: "trader", "reader"."""
        ...

    @abstractmethod
    async def setup(self, scheduler: "EventDrivenScheduler", account_id: str) -> None:
        """
        Викликається один раз при додаванні акаунта в scheduler.

        Тут profession:
          - підписується на події: scheduler.subscribe("event", self._on_event)
          - може зберегти посилання: self._scheduler = scheduler

        НЕ робить IO тут — тільки реєстрація.
        IO — в startup_tasks() або restore_state().
        """
        ...

    @abstractmethod
    async def handle_request(
        self,
        intent: str,
        data: dict[str, Any],
        ctx: "RequestContext",
    ) -> "RequestResult":
        """
        Обробляє запит від scheduler.ask().

        intent — що хоче caller ("initiate_trade", "get_stats", ...)
        data   — параметри запиту
        ctx    — контекст: account_id, caller, bot

        Повертає RequestResult.approve() або RequestResult.deny().
        """
        ...

    # ── Optional overrides ────────────────────────────────────────────────────

    async def restore_state(self, bot: "Account") -> None:
        """
        Відновлює in-memory state після restart з bot.inventory.

        Викликається після setup(). bot.inventory вже завантажений з БД.

        Приклад:
            inv = bot.inventory.trader  # BaseInventory
            self._pending = inv.get("pending", [])
        """
        pass

    async def teardown(self, scheduler: "EventDrivenScheduler", account_id: str) -> None:
        """
        Викликається при видаленні акаунта.
        Override щоб скинути resources.
        """
        pass

    async def on_event(self, event_name: str, payload: dict[str, Any]) -> None:
        """
        Реагує на подію з EventBus.
        За замовч. нічого не робить. Override для конкретної реакції.
        НЕ викликай інші Profession напряму — тільки через scheduler.
        """
        pass

    def build_triggers(self, account_id: str) -> list["TriggerProtocol"]:
        """
        Повертає triggers для реєстрації в Scheduler.
        Override якщо profession має time-based triggers.
        За замовч. — порожній список.
        """
        return []

    def startup_tasks(self, bot: "Account") -> list["AnyTask"]:
        """
        Tasks що виконуються одразу при старті акаунта.
        Override якщо треба init IO (завантажити дані, синхронізувати).

        Базова реалізація автоматично тегує всі задачі через tag_profession.
        Підкласам НЕ потрібно викликати tag_profession вручну.
        """
        tasks = self._startup_tasks(bot)
        if tasks:
            from src.core.tasks.base import tag_profession
            tag_profession(tasks, self.profession_id)
        return tasks

    def _startup_tasks(self, bot: "Account") -> list["AnyTask"]:
        """
        Hook для підкласів. Повертає стартові задачі БЕЗ тегування.
        Замінює пряме override startup_tasks() у підкласах.
        """
        return []

    def check_guard(self, bot: "Account") -> bool:
        """
        Перевіряє чи може Profession бути активною.
        True = все добре. False = profession suspended.
        Override для domain-specific умов (бан, ліміти тощо).
        """
        return True

    # ── Repr ──────────────────────────────────────────────────────────────────

    def __repr__(self) -> str:
        return f"<{type(self).__name__} id={self.profession_id!r}>"


# ─────────────────────────────────────────────────────────────────────────────
# RequestResult
# ─────────────────────────────────────────────────────────────────────────────

class RequestResult:
    """
    Результат обробки запиту Profession-ою.

    Не є Task — це відповідь на питання "що робити?".
    Scheduler (або caller) сам вирішує як виконувати tasks.

    Використання:
        return RequestResult.approve(tasks=[some_task], data={"id": 123})
        return RequestResult.deny("insufficient balance", data={"have": 5})
    """

    def __init__(
        self,
        *,
        approved: bool,
        tasks:    Optional[list["AnyTask"]] = None,
        reason:   str = "",
        data:     Optional[dict[str, Any]] = None,
    ) -> None:
        self.approved = approved
        self.tasks    = tasks or []
        self.reason   = reason
        self.data     = data or {}

    @classmethod
    def approve(
        cls,
        tasks: Optional[list["AnyTask"]] = None,
        data:  Optional[dict[str, Any]] = None,
    ) -> "RequestResult":
        """Запит схвалено. Опційно — список tasks для виконання."""
        return cls(approved=True, tasks=tasks or [], data=data or {})

    @classmethod
    def deny(
        cls,
        reason: str,
        data:   Optional[dict[str, Any]] = None,
    ) -> "RequestResult":
        """Запит відхилено з поясненням."""
        return cls(approved=False, reason=reason, data=data or {})

    def __repr__(self) -> str:
        status = "APPROVED" if self.approved else f"DENIED({self.reason!r})"
        return f"<RequestResult {status} tasks={len(self.tasks)}>"
    
    
class ProfessionFactory:
    def __init__(self) -> None:
        # Реєструємо самі класи (Type[BaseProfession]), а не лямбди
        self._registry: Dict[str, Type[BaseProfession]] = {}

    def register(self, name: str, profession_cls: Type[BaseProfession]) -> None:
        """Реєстрація класу професії у системі."""
        self._registry[name] = profession_cls

    def names(self) -> List[str]:
        """Повертає список назв зареєстрованих професій."""
        return list(self._registry.keys())

    def build(self, name: str) -> BaseProfession:
        """Створює чистий екземпляр професії без зайвих параметрів."""
        profession_cls = self._registry.get(name)
        if not profession_cls:
            raise ValueError(f"Професію {name!r} не знайдено в реєстрі фабрики")
        return profession_cls()  # Виклик без аргументів


# Створюємо глобальний синглтон реєстру
profession_factory = ProfessionFactory()