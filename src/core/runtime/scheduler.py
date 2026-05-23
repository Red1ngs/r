"""
scheduler.py — EventDrivenScheduler. Єдиний Scheduler в системі.

Старий Scheduler.py видалено повністю.
Старий клас Profession (dataclass) — більше не підтримується.

Що є:
  - AccountContainer             — тільки worker + triggers.
  - EventDrivenScheduler         — singleton, runtime kernel.
      monitor loop (sync thread) — dispatch triggers, check guards, reap dead
      async loop (daemon thread) — EventBus, RequestRouter
      BaseProfession registry    — setup/restore/teardown lifecycle

Lifecycle акаунта:
  add_account(id, bot, professions)
    → worker.start()                        (sync thread)
    → async: _setup_professions()
        → profession.setup()                (підписки на events)
        → profession.restore_state(bot)     (відновлення з Inventories)
        → triggers з profession реєструються в container
        → startup tasks → worker.assign()
    → _wakeup.set()                         (monitor прокидається)

  remove_account(id)
    → async: profession.teardown()
    → worker.stop()                         (зберігає inventory)
    → router.unregister_account()

Guard check (кожен tick):
  Для кожного BaseProfession → profession.check_guard(bot)
  False → знімаємо triggers цієї profession, worker.clear()

Recovery після restart:
  Inventories завантажені в Account.__init__ з БД.
  profession.restore_state(bot) читає звідти — без нової БД.
"""
from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from src.core.runtime.profession import BaseProfession

from src.core.account import Account
from src.core.inventory.model import Inventories
from src.core.logging.loggers import get_scheduler_logger
from src.core.runtime.event_bus import EventBus, EventCallback
from src.core.runtime.request_router import RequestContext, RequestRouter
from src.core.runtime.conditions import Condition
from src.core.runtime.profession import BaseProfession
from src.core.runtime.schedule import RunAt, TriggerProtocol
from src.core.status import AccountStatus
from src.core.tasks.base import AnyTask
from src.core.worker import BotWorker

log = get_scheduler_logger()

_MAX_SLEEP = 30.0
_MIN_SLEEP = 0.5


@dataclass
class AccountContainer:
    """
    Контейнер стану одного акаунта в Scheduler.

    worker      — виконує tasks
    guard       — account-level умова; False → kill account
    professions — profession → її власні triggers (ownership закодований у ключі)
    """
    worker:     BotWorker
    guard:      Optional[Condition] = None
    professions: dict["BaseProfession", list[TriggerProtocol]] = field(
        default_factory=lambda: {}, init=False, repr=False
    )

    def check_account_guard(self, inv: Inventories) -> bool:
        return self.guard is None or self.guard(inv)

    # ── Triggers ──────────────────────────────────────────────────────────────

    @property
    def triggers(self) -> list[TriggerProtocol]:
        """Всі тригери акаунта (з усіх profession)."""
        return [t for ts in self.professions.values() for t in ts]

    def add_profession(
        self,
        profession: "BaseProfession",
        triggers: list[TriggerProtocol],
    ) -> None:
        self.professions[profession] = list(triggers)

    def remove_profession(self, profession: "BaseProfession") -> int:
        removed = self.professions.pop(profession, [])
        return len(removed)

    def remove_profession_by_id(self, profession_id: str) -> int:
        target = next(
            (p for p in self.professions if p.profession_id == profession_id), None
        )
        if target is None:
            return 0
        return self.remove_profession(target)

    def remove_trigger(self, trigger: TriggerProtocol) -> None:
        for ts in self.professions.values():
            try:
                ts.remove(trigger)
                return
            except ValueError:
                continue

    def remove_all_triggers(self) -> None:
        for ts in self.professions.values():
            ts.clear()

    def get_profession(self, profession_id: str) -> Optional["BaseProfession"]:
        return next(
            (p for p in self.professions if p.profession_id == profession_id), None
        )

    def has_profession(self, profession_id: str) -> bool:
        return any(p.profession_id == profession_id for p in self.professions)

    def profession_list(self) -> list["BaseProfession"]:
        return list(self.professions.keys())

    # ── Timing ────────────────────────────────────────────────────────────────

    def trigger_names(self) -> list[str]:
        return [t.name for t in self.triggers]

    def next_trigger_in(self) -> float:
        finite = [
            s for t in self.triggers
            if (s := t.seconds_until()) != float("inf")
        ]
        return min(finite) if finite else _MAX_SLEEP


# ─────────────────────────────────────────────────────────────────────────────
# EventDrivenScheduler
# ─────────────────────────────────────────────────────────────────────────────

class EventDrivenScheduler:
    """
    Singleton runtime kernel.
    """

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

        self._containers:    dict[str, AccountContainer] = {}
        self._lock        = threading.Lock()
        self._stop        = threading.Event()
        self._wakeup      = threading.Event()
        self._monitor     = threading.Thread(
            target=self._monitor_loop,
            daemon=True,
            name="scheduler-monitor",
        )

        self._event_bus   = EventBus()
        self._router      = RequestRouter()
        self._async_loop: Optional[asyncio.AbstractEventLoop] = None
        self._loop_thread: Optional[threading.Thread] = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        self._async_loop = asyncio.new_event_loop()
        self._loop_thread = threading.Thread(
            target=self._run_async_loop,
            daemon=True,
            name="scheduler-async",
        )
        self._loop_thread.start()
        self._monitor.start()
        log.info("EventDrivenScheduler started")

    def stop(self) -> None:
        self._stop.set()
        self._wakeup.set()

        if self._monitor.is_alive():
            self._monitor.join(timeout=10)

        if self._async_loop and self._async_loop.is_running():
            self._async_loop.call_soon_threadsafe(self._async_loop.stop)
        if self._loop_thread and self._loop_thread.is_alive():
            self._loop_thread.join(timeout=10)

        with self._lock:
            entries = dict(self._containers)
        for entry in entries.values():
            entry.worker.stop()

        log.info("EventDrivenScheduler stopped")

    def _run_async_loop(self) -> None:
        asyncio.set_event_loop(self._async_loop)
        self._async_loop.run_forever()

    # ── Account management ────────────────────────────────────────────────────

    def add_account(
        self,
        account_id:  str,
        bot:         Account,
        professions: list[BaseProfession],
        guard:       Optional[Condition] = None,
    ) -> None:
        worker = BotWorker(bot, on_error=lambda b: self._wakeup.set())
        container  = AccountContainer(worker=worker, guard=guard)

        with self._lock:
            if account_id in self._containers:
                raise ValueError(f"Акаунт {account_id!r} вже існує")
            self._containers[account_id] = container

        worker.start()

        if bot.status == AccountStatus.DEAD:
            with self._lock:
                self._containers.pop(account_id, None)
            if self._on_dead:
                self._on_dead(bot)
            return

        self._run_async(self._setup_professions(account_id, bot, professions))
        self._wakeup.set()
        log.info(f"[{account_id}] додано ({len(professions)} professions)")

    def remove_account(self, account_id: str) -> bool:
        with self._lock:
            entry = self._containers.pop(account_id, None)
        if entry is None:
            return False

        professions = entry.profession_list()
        self._run_async(self._teardown_professions(account_id, professions))
        self._router.unregister_account(account_id)
        entry.remove_all_triggers()
        entry.worker.stop()
        log.info(f"[{account_id}] видалено")
        return True

    def pause_account(self, account_id: str) -> bool:
        with self._lock:
            entry = self._containers.get(account_id)
        if entry is None or entry.worker.bot.status == AccountStatus.SUSPENDED:
            return False
        entry.worker.clear()
        entry.remove_all_triggers()
        entry.worker.bot.status = AccountStatus.SUSPENDED
        entry.worker.stop()
        log.info(f"[{account_id}] призупинено")
        return True

    def resume_account(self, account_id: str) -> bool:
        with self._lock:
            container = self._containers.get(account_id)
        if container is None or container.worker.bot.status != AccountStatus.SUSPENDED:
            return False

        professions = container.profession_list()

        container.worker.bot.status = AccountStatus.IDLE
        container.worker.start()

        if container.worker.bot.status == AccountStatus.DEAD:
            log.error(f"[{account_id}] resume: connect() провалився")
            return False

        self._run_async(
            self._restore_professions(account_id, container.worker.bot, professions, container)
        )
        self._wakeup.set()
        log.info(f"[{account_id}] відновлено")
        return True

    # ── Dynamic Profession Management ─────────────────────────────────────────

    def add_profession_to_account(self, account_id: str, profession: BaseProfession) -> None:
        with self._lock:
            container = self._containers.get(account_id)
            if container is None:
                raise ValueError(f"Акаунт {account_id!r} не знайдено")
            if container.has_profession(profession.profession_id):
                log.warning(f"[{account_id}] profession {profession.profession_id!r} already registered")
                return

        self._run_async(self._setup_single_profession(account_id, container.worker.bot, profession, container))

    async def _setup_single_profession(
        self,
        account_id: str,
        bot:        Account,
        profession: BaseProfession,
        container:      AccountContainer,
    ) -> None:
        try:
            await profession.setup(self, account_id)
            self._router.register(account_id, profession)

            triggers = profession.build_triggers(account_id)
            await profession.restore_state(bot)

            container.add_profession(profession, triggers)

            tasks = profession.startup_tasks(bot)
            if tasks:
                container.worker.assign(*tasks)

            self._wakeup.set()
            log.info(f"[{account_id}] dynamic setup of {profession.profession_id!r} complete")
        except Exception as e:
            log.error(
                f"[{account_id}] dynamic setup of {profession.profession_id!r} failed: {e}",
                exc_info=True,
            )

    def remove_profession_from_account(self, account_id: str, profession_id: str) -> None:
        profession_obj = None
        with self._lock:
            container = self._containers.get(account_id)
            if container is not None:
                profession_obj = container.get_profession(profession_id)
                container.remove_profession_by_id(profession_id)
                container.worker.clear()

        if profession_obj is not None:
            self._run_async(self._teardown_professions(account_id, [profession_obj]))

        self._router.unregister(account_id, profession_id)
        log.info(f"[{account_id}] profession {profession_id!r} dynamically removed")
        
    def wakeup(self) -> None:
        """Публічний метод для примусового пробудження monitor loop.
        Використовується profession-ами замість прямого доступу до _wakeup.
        """
        self._wakeup.set()

    def reschedule_trigger(self, account_id: str, trigger_name: str, run_at: RunAt) -> bool:
        """
        Знаходить тригер акаунта за назвою та змінює його запланований час.
        Після цього миттєво сигналізує монітору планувальника перерахувати час сну.
        """
        with self._lock:
            container = self._containers.get(account_id)
        if not container:
            return False
            
        for trigger in list(container.triggers):
            if trigger.name == trigger_name:
                trigger.reschedule(run_at)
                log.info(f"[{account_id}] Тригер {trigger_name!r} успішно перенесено на {run_at}")
                
                # Важливо: прокидаємо монітор планувальника, щоб він не спав зайвий час
                self._wakeup.set()  
                return True
                
        log.warning(f"[{account_id}] Не вдалося знайти тригер {trigger_name!r} для перенесення")
        return False

    # ── Public API ────────────────────────────────────────────────────────────

    def push_task(self, account_id: str, task: AnyTask) -> bool:
        with self._lock:
            entry = self._containers.get(account_id)
        if entry is None:
            return False
        entry.worker.assign(task)
        return True

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
        return container.worker.bot if container else None

    def get_entry(self, account_id: str) -> Optional[AccountContainer]:
        with self._lock:
            return self._containers.get(account_id)

    def account_ids(self) -> list[str]:
        with self._lock:
            return list(self._containers.keys())

    def status(self, account_id: str) -> Optional[AccountStatus]:
        with self._lock:
            entry = self._containers.get(account_id)
        return entry.worker.bot.status if entry else None

    def all_statuses(self) -> dict[str, AccountStatus]:
        with self._lock:
            entries = dict(self._containers)
        return {aid: e.worker.bot.status for aid, e in entries.items()}

    def queue_size(self, account_id: str) -> Optional[int]:
        with self._lock:
            entry = self._containers.get(account_id)
        return entry.worker.queue_size if entry else None

    def trigger_names(self, account_id: str) -> list[str]:
        with self._lock:
            entry = self._containers.get(account_id)
        return entry.trigger_names() if entry else []

    def seconds_until_next(self, account_id: str) -> Optional[float]:
        with self._lock:
            entry = self._containers.get(account_id)
        return entry.next_trigger_in() if entry else None

    # ── EventBus API ──────────────────────────────────────────────────────────

    def subscribe(self, event_name: str, callback: EventCallback) -> None:
        self._event_bus.subscribe(event_name, callback)

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
        account_id:    str,
        profession_id: str,
        intent:        str,
        data:          dict[str, Any],
        *,
        caller:  str   = "system",
        timeout: float = 30.0,
    ) -> Any:
        bot = self.get_bot(account_id)
        if bot is None:
            from src.core.runtime.profession import RequestResult
            return RequestResult.deny(f"account {account_id!r} not found")
        ctx = RequestContext(
            account_id=account_id,
            profession_id=profession_id,
            intent=intent,
            caller=caller,
            bot=bot,
            timeout=timeout,
        )
        return await self._router.route(ctx, data)

    def ask_sync(
        self,
        account_id:    str,
        profession_id: str,
        intent:        str,
        data:          dict[str, Any],
        *,
        caller:  str   = "system",
        timeout: float = 30.0,
    ) -> Any:
        future = asyncio.run_coroutine_threadsafe(
            self.ask(account_id, profession_id, intent, data,
                     caller=caller, timeout=timeout),
            self._async_loop,
        )
        return future.result(timeout=timeout + 1)

    # ── Profession lifecycle (async) ──────────────────────────────────────────

    async def _setup_professions(
        self,
        account_id:  str,
        bot:         Account,
        professions: list[BaseProfession],
    ) -> None:
        with self._lock:
            container = self._containers.get(account_id)
        if container is None:
            return

        for profession in professions:
            try:
                await profession.setup(self, account_id)
                self._router.register(account_id, profession)

                triggers = profession.build_triggers(account_id)
                await profession.restore_state(bot)

                container.add_profession(profession, triggers)

                tasks = profession.startup_tasks(bot)
                if tasks:
                    container.worker.assign(*tasks)

                log.info(f"[{account_id}] {profession.profession_id!r} ready")
            except Exception as e:
                log.error(
                    f"[{account_id}] setup {profession.profession_id!r} failed: {e}",
                    exc_info=True,
                )

        self._wakeup.set()

    async def _restore_professions(
        self,
        account_id:  str,
        bot:         Account,
        professions: list[BaseProfession],
        container:       AccountContainer,
    ) -> None:
        for profession in professions:
            try:
                self._router.register(account_id, profession)

                triggers = profession.build_triggers(account_id)
                await profession.restore_state(bot)

                container.add_profession(profession, triggers)

                tasks = profession.startup_tasks(bot)
                if tasks:
                    container.worker.assign(*tasks)

                log.info(f"[{account_id}] {profession.profession_id!r} resumed")
            except Exception as e:
                log.error(
                    f"[{account_id}] resume {profession.profession_id!r}: {e}",
                    exc_info=True,
                )

    async def _teardown_professions(
        self,
        account_id:  str,
        professions: list[BaseProfession],
    ) -> None:
        for profession in professions:
            try:
                await profession.teardown(self, account_id)
            except Exception as e:
                log.error(f"[{account_id}] teardown {profession.profession_id!r}: {e}")

    # ── Monitor loop (sync) ───────────────────────────────────────────────────

    def _monitor_loop(self) -> None:
        while not self._stop.is_set():
            self._wakeup.clear()
            sleep = self._tick()
            self._wakeup.wait(timeout=max(_MIN_SLEEP, min(sleep, _MAX_SLEEP)))

    def _tick(self) -> float:
        self._check_guards()
        next_in = self._dispatch_triggers()
        self._reap_dead_workers()
        return next_in

    def _check_guards(self) -> None:
        with self._lock:
            containers = dict(self._containers)

        for account_id, entry in containers.items():
            bot = entry.worker.bot
            inv = bot.inventory

            if not entry.check_account_guard(inv):
                log.warning(f"[{account_id}] account guard failed → kill")
                self._kill_account(account_id, entry)
                continue

            for profession in entry.profession_list():
                if not profession.check_guard(bot):
                    removed = entry.remove_profession(profession)
                    entry.worker.clear()
                    log.info(
                        f"[{account_id}] {profession.profession_id!r} guard failed "
                        f"→ {removed} triggers removed"
                    )

    def _dispatch_triggers(self) -> float:
        with self._lock:
            entries = dict(self._containers)
        next_wakeup = _MAX_SLEEP

        for account_id, entry in entries.items():
            bot = entry.worker.bot
            if bot.status in (AccountStatus.DEAD, AccountStatus.SUSPENDED):
                continue

            to_remove: list[TriggerProtocol] = []

            for trigger in list(entry.triggers):
                if trigger.is_expired(bot.inventory):
                    to_remove.append(trigger)
                    continue

                if not trigger.is_due():
                    secs = trigger.seconds_until()
                    if secs != float("inf"):
                        next_wakeup = min(next_wakeup, secs)
                    continue

                trigger.dispatch()
                try:
                    tasks = list(trigger.producer(bot))
                except Exception as e:
                    log.error(f"[{account_id}] trigger {trigger.name!r}: {e}", exc_info=True)
                    tasks = []

                if tasks:
                    entry.worker.assign(*tasks)
                else:
                    trigger.advance(bot)
                    if self._is_one_shot(trigger):
                        to_remove.append(trigger)
                        continue
                    secs = trigger.seconds_until()
                    if secs != float("inf"):
                        next_wakeup = min(next_wakeup, secs)

            if to_remove:
                with self._lock:
                    for t in to_remove:
                        entry.remove_trigger(t)

        return next_wakeup

    def _reap_dead_workers(self) -> None:
        with self._lock:
            containers = dict(self._containers)

        for account_id, entry in containers.items():
            if entry.worker.bot.status != AccountStatus.DEAD:
                continue
            log.warning(f"[{account_id}] dead → cleanup")
            entry.remove_all_triggers()
            entry.worker.stop()
            with self._lock:
                self._containers.pop(account_id, None)
            self._router.unregister_account(account_id)
            if self._on_dead:
                self._on_dead(entry.worker.bot)

    def _kill_account(self, account_id: str, container: AccountContainer) -> None:
        container.worker.clear()
        container.remove_all_triggers()
        container.worker.bot.mark_dead("account guard failed")
        container.worker.bot.repo.inventory.save(account_id, container.worker.bot.inventory)
        container.worker.stop()
        with self._lock:
            self._containers.pop(account_id, None)
        self._router.unregister_account(account_id)
        if self._on_dead:
            self._on_dead(container.worker.bot)

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

    @staticmethod
    def _is_one_shot(trigger: TriggerProtocol) -> bool:
        val = getattr(trigger, "is_one_shot", None)
        if callable(val):
            return bool(val())
        return bool(val) if val is not None else False