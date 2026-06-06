"""
scheduler.py — EventDrivenScheduler. Єдиний Scheduler в системі.

Зміни:
  - Повністю видалено Tasks, BotWorker, ProfessionPriority та чергу задач.
  - Повністю прибрано тригери.
  - Управління AccountMonitors інтегровано у життєвий цикл ядра.
  - Призупинення акаунта (pause_account) відключає сесію акаунта та монітори,
    а відновлення (resume_account) — поновлює підключення та запускає монітори.
"""
from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, TYPE_CHECKING

from src.core.monitoring.monitor import BaseMonitor
from src.core.monitoring.account_monitors import AccountMonitors

if TYPE_CHECKING:
    from src.core.runtime.profession import BaseProfession

from src.core.account import Account
from src.core.inventory.model import DynamicInventories as Inventories
from src.core.logging.loggers import get_scheduler_logger
from src.core.runtime.event_bus import EventBus, EventCallback
from src.core.runtime.request_router import RequestContext, RequestRouter
from src.core.runtime.conditions import Condition
from src.core.runtime.profession import BaseProfession, RequestResult
from src.core.status import AccountStatus

log = get_scheduler_logger()

# Маппінг: яку profession активувати → які монітори підключити.
_PROFESSION_MONITORS: dict[str, list[str]] = {
    "reader":  ["reading"],
    "quiz":    ["quiz"],
}

def _split_evenly(items: list[Any], n: int) -> list[list[Any]]:
    if n <= 0:
        return []
    size = max(1, -(-len(items) // n))
    return [items[i: i + size] for i in range(0, len(items), size)]


@dataclass
class AccountContainer:
    """
    Контейнер стану одного акаунта в Scheduler.
    """
    bot: Account
    guard: Optional[Condition] = None

    professions: dict[str, BaseProfession] = field(
        default_factory=dict
    )

    monitors: dict[str, BaseMonitor] = field(
        default_factory=dict
    )

    def check_account_guard(self, inv: Inventories) -> bool:
        return self.guard is None or self.guard(inv)

    def add_profession(self, profession: "BaseProfession") -> None:
        self.professions[profession.profession_id] = profession

    def remove_profession(self, profession: "BaseProfession") -> None:
        self.professions.pop(profession.profession_id, None)

    def remove_profession_by_id(self, profession_id: str) -> None:
        self.professions.pop(profession_id, None)

    def get_profession(self, profession_id: str) -> Optional["BaseProfession"]:
        return self.professions.get(profession_id)

    def has_profession(self, profession_id: str) -> bool:
        return profession_id in self.professions

    def profession_list(self) -> list["BaseProfession"]:
        return list(self.professions.values())


# ─────────────────────────────────────────────────────────────────────────────
# EventDrivenScheduler
# ─────────────────────────────────────────────────────────────────────────────

class EventDrivenScheduler:
    """Singleton runtime kernel."""

    _instance:  Optional["EventDrivenScheduler"] = None
    _init_lock: threading.Lock = threading.Lock()

    # ── Singleton ─────────────────────────────────────────────────────────────

    @classmethod
    def initialize(
        cls,
        on_dead: Optional[Callable[[Account], None]] = None,
    ) -> "EventDrivenScheduler":
        with cls._init_lock:
            if cls._instance is not None:
                raise RuntimeError("EventDrivenScheduler вже ініціалізований.")
            inst = cls.__new__(cls)
            inst._init(on_dead)
            cls._instance = inst
            return inst

    @classmethod
    def get_instance(cls) -> "EventDrivenScheduler":
        if cls._instance is None:
            raise RuntimeError("EventDrivenScheduler не ініціалізований.")
        return cls._instance

    @classmethod
    def _reset_for_tests(cls) -> None:
        with cls._init_lock:
            if cls._instance is not None:
                try:
                    cls._instance.stop()
                except Exception:
                    pass
            cls._instance = None

    # ── Init ──────────────────────────────────────────────────────────────────

    def _init(self, on_dead: Optional[Callable[[Account], None]]) -> None:
        self._on_dead = on_dead

        self._containers:  dict[str, AccountContainer] = {}
        self._lock        = threading.Lock()
        self._monitors:    dict[str, AccountMonitors] = {}

        self._event_bus    = EventBus()
        self._router       = RequestRouter()
        self._async_loop:  Optional[asyncio.AbstractEventLoop] = None
        self._loop_thread: Optional[threading.Thread] = None
        self._loading_lock: Optional[asyncio.Lock] = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        self._async_loop = asyncio.new_event_loop()
        self._loop_thread = threading.Thread(
            target=self._run_async_loop,
            daemon=True,
            name="scheduler-async",
        )
        self._loop_thread.start()
        log.info("EventDrivenScheduler started")

    def stop(self) -> None:
        if self._async_loop and self._async_loop.is_running():
            self._async_loop.call_soon_threadsafe(self._async_loop.stop)
        if self._loop_thread and self._loop_thread.is_alive():
            self._loop_thread.join(timeout=10)

        with self._lock:
            entries = dict(self._containers)
        for entry in entries.values():
            entry.bot.disconnect()

        log.info("EventDrivenScheduler stopped")

    def _run_async_loop(self) -> None:
        asyncio.set_event_loop(self._async_loop)
        self._loading_lock = asyncio.Lock()
        self._async_loop.run_forever()

    # ── Monitors management (internal) ────────────────────────────────────────

    def _get_or_create_monitors(self, account_id: str) -> AccountMonitors:
        if account_id not in self._monitors:
            self._monitors[account_id] = AccountMonitors(account_id)
        return self._monitors[account_id]

    def _attach_monitors_for(self, account_id: str, profession_name: str) -> None:
        """Підключає монітори, що відповідають вказаній profession."""
        monitor_ids = _PROFESSION_MONITORS.get(profession_name, [])
        if not monitor_ids:
            return

        monitors = self._get_or_create_monitors(account_id)
        loop = self._async_loop
        if loop is None or not loop.is_running():
            return
        import asyncio
        asyncio.run_coroutine_threadsafe(
            monitors.attach_all(self, monitor_ids), loop
        )

    def _detach_monitor_if_unused(self, account_id: str, monitor_id: str) -> None:
        """Відключає монітор, якщо жодна з активних професій його більше не потребує."""
        owners = [
            prof for prof, mids in _PROFESSION_MONITORS.items()
            if monitor_id in mids
        ]
        if any(self.has_profession(account_id, p) for p in owners):
            return
        monitors = self._monitors.get(account_id)
        if monitors is None:
            return
        loop = self._async_loop
        if loop is None or not loop.is_running():
            return
        import asyncio
        asyncio.run_coroutine_threadsafe(
            monitors.detach(self, monitor_id), loop
        )

    def _detach_all_monitors(self, account_id: str) -> None:
        """Відключає всі монітори акаунта (при видаленні або паузі)."""
        monitors = self._monitors.pop(account_id, None)
        if monitors is None:
            return
        loop = self._async_loop
        if loop is None or not loop.is_running():
            return
        import asyncio
        asyncio.run_coroutine_threadsafe(
            monitors.detach_all(self), loop
        )

    # ── Account management ────────────────────────────────────────────────────

    def add_account(
        self,
        account_id:  str,
        bot:         Account,
        professions: list["BaseProfession"],
        guard:       Optional[Condition] = None,
    ) -> None:
        container = AccountContainer(bot=bot, guard=guard)

        with self._lock:
            if account_id in self._containers:
                raise ValueError(f"Акаунт {account_id!r} вже існує")
            self._containers[account_id] = container

        if bot.status == AccountStatus.DEAD:
            with self._lock:
                self._containers.pop(account_id, None)
            if self._on_dead:
                self._on_dead(bot)
            return

        self._run_async(self._setup_professions(account_id, bot, professions))
        log.info(f"[{account_id}] додано ({len(professions)} professions)")

    def connect_account(self, account_id: str) -> bool:
        """
        Встановлює сесію і підключає монітори.

        Пара до add_account(): той реєструє акаунт і піднімає профессії,
        цей — викликає bot.connect() і лише після успіху чіпляє монітори.

        Повертає True при успіху. При невдачі акаунт залишається
        зареєстрованим, монітори не підключаються.
        """
        with self._lock:
            container = self._containers.get(account_id)
        if container is None:
            log.warning(f"[{account_id}] connect_account: акаунт не знайдено")
            return False

        if container.bot.is_connected:
            log.debug(f"[{account_id}] connect_account: вже підключено")
            self._attach_all_monitors(account_id, container)
            return True

        ok = container.bot.connect()
        if not ok:
            log.error(f"[{account_id}] connect_account: connect() провалився — {container.bot.error}")
            return False

        self._attach_all_monitors(account_id, container)
        log.info(f"[{account_id}] connect_account: підключено, монітори активні")
        return True

    def _attach_all_monitors(self, account_id: str, container: "AccountContainer") -> None:
        """Чіпляє монітори для всіх активних profession контейнера."""
        for profession in container.profession_list():
            self._attach_monitors_for(account_id, profession.profession_id)

    def remove_account(self, account_id: str) -> bool:
        with self._lock:
            entry = self._containers.pop(account_id, None)
        if entry is None:
            return False

        professions = entry.profession_list()
        self._run_async(self._teardown_professions(account_id, professions))
        self._router.unregister_account(account_id)
        self._detach_all_monitors(account_id)
        log.info(f"[{account_id}] видалено")
        return True

    def pause_account(self, account_id: str) -> bool:
        with self._lock:
            container = self._containers.get(account_id)
        if container is None or container.bot.status == AccountStatus.SUSPENDED:
            return False
        
        # Відключаємо монітори на час паузи
        self._detach_all_monitors(account_id)

        container.bot.status = AccountStatus.SUSPENDED
        container.bot.disconnect()  # Закриваємо активне мережеве з'єднання
        log.info(f"[{account_id}] призупинено")
        return True

    def resume_account(self, account_id: str) -> bool:
        with self._lock:
            container = self._containers.get(account_id)
        if container is None or container.bot.status != AccountStatus.SUSPENDED:
            return False

        container.bot.status = AccountStatus.IDLE
        
        if not container.bot.is_connected:
            container.bot.connect()

        if container.bot.status == AccountStatus.DEAD:
            log.error(f"[{account_id}] resume: connect() провалився")
            return False

        # Повноцінно відновлюємо професії та запускаємо монітори заново
        professions = container.profession_list()
        self._run_async(
            self._restore_professions(
                account_id, container.bot, professions, container
            )
        )

        log.info(f"[{account_id}] відновлено")
        return True

    # ── Dynamic Profession Management ─────────────────────────────────────────

    def add_profession_to_account(
        self,
        account_id: str,
        profession: "BaseProfession",
    ) -> None:
        with self._lock:
            container = self._containers.get(account_id)
            if container is None:
                raise ValueError(f"Акаунт {account_id!r} не знайдено")
            if container.has_profession(profession.profession_id):
                log.warning(
                    f"[{account_id}] profession {profession.profession_id!r} вже зареєстрована"
                )
                return
            container.add_profession(profession)

        self._run_async(
            self._setup_single_profession(
                account_id, container.bot, profession, container
            )
        )

    async def _setup_single_profession(
        self,
        account_id: str,
        bot:        Account,
        profession: "BaseProfession",
        container:  AccountContainer,
    ) -> None:
        try:
            await profession.setup(self, account_id)
            self._router.register(account_id, profession)

            await profession.restore_state(bot)

            self._attach_monitors_for(account_id, profession.profession_id)
            log.info(f"[{account_id}] dynamic setup {profession.profession_id!r} complete")
        except Exception as e:
            with self._lock:
                container.professions.pop(profession.profession_id, None)
            log.error(
                f"[{account_id}] dynamic setup {profession.profession_id!r} failed: {e}",
                exc_info=True,
            )

    def remove_profession_from_account(self, account_id: str, profession_id: str) -> None:
        profession_obj: Optional["BaseProfession"] = None
        with self._lock:
            container = self._containers.get(account_id)
            if container is not None:
                profession_obj = container.get_profession(profession_id)
                container.remove_profession_by_id(profession_id)

        if profession_obj is not None:
            self._run_async(self._teardown_professions(account_id, [profession_obj]))

        # Відключаємо монітори цієї profession, якщо вони більше не використовуються
        for monitor_id in _PROFESSION_MONITORS.get(profession_id, []):
            self._detach_monitor_if_unused(account_id, monitor_id)

        self._router.unregister(account_id, profession_id)
        log.info(f"[{account_id}] profession {profession_id!r} dynamically removed")

    # ── Public API ────────────────────────────────────────────────────────────

    def has_account(self, account_id: str) -> bool:
        with self._lock:
            return account_id in self._containers

    def has_profession(self, account_id: str, profession_id: str) -> bool:
        with self._lock:
            container = self._containers.get(account_id)
        return container.has_profession(profession_id) if container else False

    def get_bot(self, account_id: str) -> Optional[Account]:
        with self._lock:
            container = self._containers.get(account_id)
        return container.bot if container else None

    def get_entry(self, account_id: str) -> Optional[AccountContainer]:
        with self._lock:
            return self._containers.get(account_id)

    def account_ids(self) -> list[str]:
        with self._lock:
            return list(self._containers.keys())

    def get_accounts_with_profession(self, profession_id: str) -> list[str]:
        with self._lock:
            return [
                account_id
                for account_id, container in self._containers.items()
                if container.has_profession(profession_id)
            ]

    async def dispatch_work(
        self,
        profession_id: str,
        intent:        str,
        items:         list[Any],
        item_key:      str,
        *,
        caller:        str   = "system",
        timeout:       float = 30.0,
    ) -> int:
        workers = self.get_accounts_with_profession(profession_id)
        if not workers:
            log.warning(
                f"dispatch_work: немає акаунтів з професією {profession_id!r} "
                f"— {len(items)} завдань не розподілено"
            )
            return 0

        chunks = _split_evenly(items, len(workers))

        dispatched = 0
        for account_id, chunk in zip(workers, chunks):
            if not chunk:
                continue
            await self.ask(
                account_id    = account_id,
                profession_id = profession_id,
                intent        = intent,
                data          = {item_key: chunk},
                caller        = caller,
                timeout       = timeout,
            )
            dispatched += 1

        return dispatched

    def status(self, account_id: str) -> Optional[AccountStatus]:
        with self._lock:
            entry = self._containers.get(account_id)
        return entry.bot.status if entry else None

    def all_statuses(self) -> dict[str, AccountStatus]:
        with self._lock:
            entries = dict(self._containers)
        return {aid: e.bot.status for aid, e in entries.items()}

    def profession_names(self, account_id: str) -> list[str]:
        with self._lock:
            entry = self._containers.get(account_id)
        if entry is None:
            return []
        return [p.profession_id for p in entry.profession_list()]

    # ── EventBus API ──────────────────────────────────────────────────────────

    def subscribe(self, event_name: str, callback: EventCallback) -> None:
        self._event_bus.subscribe(event_name, callback)

    async def try_acquire_loader_lock(self) -> bool:
        if self._loading_lock is None:
            return True
        if self._loading_lock.locked():
            return False
        await self._loading_lock.acquire()
        return True

    async def release_loader_lock(self) -> None:
        if self._loading_lock is not None and self._loading_lock.locked():
            self._loading_lock.release()

    def emit_event(
        self,
        event_name: str,
        payload:    dict[str, Any],
        source:     str = "system",
    ) -> None:
        self._run_async(self._event_bus.emit(event_name, payload, source=source))

    async def emit_event_async(
        self,
        event_name: str,
        payload:    dict[str, Any],
        source:     str = "system",
    ) -> int:
        return await self._event_bus.emit(event_name, payload, source=source)

    # ── RequestRouter API ─────────────────────────────────────────────────────
    
    async def ask(
        self,
        account_id: str,
        profession_id: str,
        intent: str,
        data: dict[str, Any] | None = None,
        *,
        caller: str = "admin",
        timeout: float = 60.0,
    ) -> "RequestResult":
        from src.core.runtime.profession import RequestResult

        bot = self.get_bot(account_id)
        if bot is None:
            return RequestResult.deny(f"account {account_id!r} not found")

        ctx = RequestContext(
            account_id    = account_id,
            profession_id = profession_id,
            intent        = intent,
            caller        = caller,
            bot           = bot,
            timeout       = timeout,
        )
        return await self._router.route(ctx, data or {})

    # ── Profession lifecycle (async) ──────────────────────────────────────────

    async def _setup_professions(
        self,
        account_id:  str,
        bot:         Account,
        professions: list["BaseProfession"],
    ) -> None:
        with self._lock:
            container = self._containers.get(account_id)
        if container is None:
            return

        for profession in professions:
            try:
                await profession.setup(self, account_id)
                self._router.register(account_id, profession)

                await profession.restore_state(bot)

                container.add_profession(profession)

                log.info(f"[{account_id}] {profession.profession_id!r} ready")
            except Exception as e:
                log.error(
                    f"[{account_id}] setup {profession.profession_id!r} failed: {e}",
                    exc_info=True,
                )

    async def _restore_professions(
        self,
        account_id:  str,
        bot:         Account,
        professions: list["BaseProfession"],
        container:   AccountContainer,
    ) -> None:
        for profession in professions:
            try:
                self._router.register(account_id, profession)

                await profession.restore_state(bot)

                container.add_profession(profession)

                # Відновлюємо монітори
                self._attach_monitors_for(account_id, profession.profession_id)

                log.info(f"[{account_id}] {profession.profession_id!r} resumed")
            except Exception as e:
                log.error(
                    f"[{account_id}] resume {profession.profession_id!r}: {e}",
                    exc_info=True,
                )

    async def _teardown_professions(
        self,
        account_id:  str,
        professions: list["BaseProfession"],
    ) -> None:
        for profession in professions:
            try:
                self._event_bus.unsubscribe_owner(profession)
                await profession.teardown(self, account_id)
            except Exception as e:
                log.error(f"[{account_id}] teardown {profession.profession_id!r}: {e}")

    # ── Guard loops ───────────────────────────────────────────────────────────

    def _check_guards(self) -> None:
        with self._lock:
            containers = dict(self._containers)

        for account_id, entry in containers.items():
            bot = entry.bot
            inv = bot.inventory

            if not entry.check_account_guard(inv):
                log.warning(f"[{account_id}] account guard failed → kill")
                self._kill_account(account_id, entry)
                continue

            for profession in entry.profession_list():
                if not profession.check_guard(bot):
                    entry.remove_profession(profession)
                    log.info(
                        f"[{account_id}] {profession.profession_id!r} guard failed "
                        f"→ profession removed"
                    )

    def _reap_dead_workers(self) -> None:
        with self._lock:
            containers = dict(self._containers)

        for account_id, entry in containers.items():
            if entry.bot.status != AccountStatus.DEAD:
                continue
            log.warning(f"[{account_id}] dead → cleanup")
            entry.bot.disconnect()
            with self._lock:
                self._containers.pop(account_id, None)
            self._router.unregister_account(account_id)
            self._detach_all_monitors(account_id)
            if self._on_dead:
                self._on_dead(entry.bot)

    def _kill_account(self, account_id: str, container: AccountContainer) -> None:
        container.bot.mark_dead("account guard failed")
        container.bot.repo.inventory.save(account_id, container.bot.inventory)
        container.bot.disconnect()
        with self._lock:
            self._containers.pop(account_id, None)
        self._router.unregister_account(account_id)
        self._detach_all_monitors(account_id)
        if self._on_dead:
            self._on_dead(container.bot)

    # ── Async bridge ──────────────────────────────────────────────────────────

    def _run_async(self, coro: Any) -> None:
        if self._async_loop is None or not self._async_loop.is_running():
            log.warning("[Scheduler] async loop not running, skipping coroutine")
            return
        future = asyncio.run_coroutine_threadsafe(coro, self._async_loop)
        future.add_done_callback(self._log_async_error)

    @staticmethod
    def _log_async_error(future: asyncio.Future) -> None:
        try:
            future.result()
        except Exception as e:
            log.error(f"[Scheduler] async error: {e}", exc_info=True)