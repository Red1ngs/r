"""
scheduler.py — EventDrivenScheduler. Єдиний Scheduler в системі.

Зміни відносно попередньої версії:
  1. add_profession_to_account():
       — резервує слот під lock (container.add_profession з [] triggers)
         щоб усунути race condition між двома паралельними викликами.
       — передає profession_priority в _setup_single_profession.
  2. remove_profession_from_account():
       — замість container.worker.clear() викликає worker.clear_profession()
         щоб не знищувати задачі інших profession.
  3. _setup_professions / _setup_single_profession:
       — обчислює ProfessionPriority за індексом profession у списку.
       — передає його в trigger.producer і tag_profession для задач.
  4. FLIGHT_TIMEOUT знижено до 300 s (з 3600).
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
from src.core.tasks.base import AnyTask, ProfessionPriority, tag_profession
from src.core.worker import BotWorker

log = get_scheduler_logger()

_MAX_SLEEP = 30.0
_MIN_SLEEP = 0.5


@dataclass
class AccountContainer:
    """
    Контейнер стану одного акаунта в Scheduler.

    professions: dict[BaseProfession, list[TriggerProtocol]]
        Ключ — profession об'єкт, значення — її тригери.
        Порядок dict (insertion order) = пріоритет:
          перший вставлений → PRIMARY, другий → SECONDARY, решта → BACKGROUND.
    """
    worker:      BotWorker
    guard:       Optional[Condition] = None
    professions: dict["BaseProfession", list[TriggerProtocol]] = field(
        default_factory=dict, init=False, repr=False
    )
    # Trigger-об'єкти збережені при pause(); None = акаунт не паузований.
    _suspended_triggers: Optional[dict["BaseProfession", list[TriggerProtocol]]] = field(
        default=None, init=False, repr=False
    )

    def check_account_guard(self, inv: Inventories) -> bool:
        return self.guard is None or self.guard(inv)

    # ── Triggers ──────────────────────────────────────────────────────────────

    @property
    def triggers(self) -> list[TriggerProtocol]:
        return [t for ts in self.professions.values() for t in ts]

    def add_profession(
        self,
        profession: "BaseProfession",
        triggers:   list[TriggerProtocol],
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
            
    def suspend_triggers(self) -> "dict[BaseProfession, list[TriggerProtocol]]":
        """
        Зберігає trigger-об'єкти і очищає списки (для pause).
        trigger._next_fire залишається в об'єкті — resume відновить таймер точно.
        """
        snapshot: dict["BaseProfession", list[TriggerProtocol]] = {}
        for profession, triggers in self.professions.items():
            snapshot[profession] = list(triggers)
            triggers.clear()
        return snapshot

    def restore_triggers(
        self,
        snapshot: "dict[BaseProfession, list[TriggerProtocol]]",
    ) -> None:
        """
        Відновлює trigger-об'єкти збережені suspend_triggers().
        Працює лише для profession що ще присутні в dict (за identity).
        """
        for profession, triggers in snapshot.items():
            if profession in self.professions:
                self.professions[profession] = list(triggers)

    def get_profession(self, profession_id: str) -> Optional["BaseProfession"]:
        return next(
            (p for p in self.professions if p.profession_id == profession_id), None
        )

    def has_profession(self, profession_id: str) -> bool:
        return any(p.profession_id == profession_id for p in self.professions)

    def profession_list(self) -> list["BaseProfession"]:
        return list(self.professions.keys())

    def profession_priority(self, profession: "BaseProfession") -> ProfessionPriority:
        """Повертає ProfessionPriority за позицією у dict (insertion order)."""
        for idx, p in enumerate(self.professions):
            if p is profession:
                return ProfessionPriority.from_index(idx)
        return ProfessionPriority.BACKGROUND

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
        professions: list["BaseProfession"],
        guard:       Optional[Condition] = None,
    ) -> None:
        worker    = BotWorker(bot, on_error=lambda b: self._wakeup.set())
        container = AccountContainer(worker=worker, guard=guard)

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
            container = self._containers.get(account_id)
        if container is None or container.worker.bot.status == AccountStatus.SUSPENDED:
            return False
        container.worker.clear()
        # Зберігаємо trigger-об'єкти з їх _next_fire — щоб resume міг відновити
        # таймери точно без скидання (не створювати нові об'єкти).
        # EventBus підписки НЕ знімаємо — вони живуть весь час pause.
        container._suspended_triggers = container.suspend_triggers()
        container.worker.bot.status = AccountStatus.SUSPENDED
        container.worker.stop()
        log.info(f"[{account_id}] призупинено")
        return True

    def resume_account(self, account_id: str) -> bool:
        with self._lock:
            container = self._containers.get(account_id)
        if container is None or container.worker.bot.status != AccountStatus.SUSPENDED:
            return False

        container.worker.bot.status = AccountStatus.IDLE
        container.worker.start()

        if container.worker.bot.status == AccountStatus.DEAD:
            log.error(f"[{account_id}] resume: connect() провалився")
            return False

        # Відновлюємо trigger-об'єкти що були збережені при pause.
        # Ті самі об'єкти → _next_fire зберігається → таймери не скидаються.
        # EventBus підписки не чіпаємо — вони живі весь час.
        suspended = getattr(container, "_suspended_triggers", None)
        if suspended is not None:
            container.restore_triggers(suspended)
            container._suspended_triggers = None
            self._run_async(
                self._resume_restore_state(account_id, container.worker.bot, container)
            )
        else:
            # Холодний resume (немає snapshot — наприклад після рестарту процесу):
            # повна реініціалізація через setup() + build_triggers().
            professions = container.profession_list()
            self._run_async(
                self._restore_professions(
                    account_id, container.worker.bot, professions, container
                )
            )

        self._wakeup.set()
        log.info(f"[{account_id}] відновлено")
        return True

    # ── Dynamic Profession Management ─────────────────────────────────────────

    def add_profession_to_account(
        self,
        account_id: str,
        profession: "BaseProfession",
    ) -> None:
        """
        Динамічно додає profession до активного акаунта.

        Race condition protection:
          Резервуємо слот під lock (add_profession з порожнім списком triggers)
          перед тим як запустити async setup. Повторний виклик з тим самим
          profession_id буде відхилено has_profession() ще під lock.
        """
        with self._lock:
            container = self._containers.get(account_id)
            if container is None:
                raise ValueError(f"Акаунт {account_id!r} не знайдено")
            if container.has_profession(profession.profession_id):
                log.warning(
                    f"[{account_id}] profession {profession.profession_id!r} вже зареєстрована"
                )
                return
            # Резервуємо слот — async setup заповнить triggers пізніше
            container.add_profession(profession, [])

        # Визначаємо пріоритет на основі поточної позиції у dict
        prof_priority = container.profession_priority(profession)

        self._run_async(
            self._setup_single_profession(
                account_id, container.worker.bot, profession, container, prof_priority
            )
        )

    async def _setup_single_profession(
        self,
        account_id:    str,
        bot:           Account,
        profession:    "BaseProfession",
        container:     AccountContainer,
        prof_priority: ProfessionPriority,
    ) -> None:
        try:
            await profession.setup(self, account_id)
            self._router.register(account_id, profession)

            raw_triggers = profession.build_triggers(account_id)
            await profession.restore_state(bot)

            # Оновлюємо triggers у вже зарезервованому слоті
            container.professions[profession] = list(raw_triggers)

            tasks = profession.startup_tasks(bot)
            if tasks:
                # Теґуємо задачі profession і коригуємо priority
                _apply_profession_priority(tasks, profession.profession_id, prof_priority)
                container.worker.assign(*tasks)

            self._wakeup.set()
            log.info(
                f"[{account_id}] dynamic setup {profession.profession_id!r} complete"
                f" (priority={prof_priority.name})"
            )
        except Exception as e:
            # Відкочуємо зарезервований слот при помилці setup
            with self._lock:
                container.professions.pop(profession, None)
            log.error(
                f"[{account_id}] dynamic setup {profession.profession_id!r} failed: {e}",
                exc_info=True,
            )

    def remove_profession_from_account(self, account_id: str, profession_id: str) -> None:
        """
        Видаляє profession з акаунта.
        Видаляє ТІЛЬКИ задачі цієї profession з черги (не торкається інших).
        """
        profession_obj: Optional["BaseProfession"] = None
        with self._lock:
            container = self._containers.get(account_id)
            if container is not None:
                profession_obj = container.get_profession(profession_id)
                container.remove_profession_by_id(profession_id)

        if profession_obj is not None:
            # Вибіркове очищення черги — тільки задачі цієї profession
            if container is not None:
                container.worker.clear_profession(profession_id)
            self._run_async(self._teardown_professions(account_id, [profession_obj]))

        self._router.unregister(account_id, profession_id)
        log.info(f"[{account_id}] profession {profession_id!r} dynamically removed")

    # ── Public API ────────────────────────────────────────────────────────────

    def wakeup(self) -> None:
        self._wakeup.set()

    def reschedule_trigger(self, account_id: str, trigger_name: str, run_at: RunAt) -> bool:
        with self._lock:
            container = self._containers.get(account_id)
        if not container:
            return False
        for trigger in list(container.triggers):
            if trigger.name == trigger_name:
                trigger.reschedule(run_at)
                log.info(f"[{account_id}] Тригер {trigger_name!r} перенесено на {run_at}")
                self._wakeup.set()
                return True
        log.warning(f"[{account_id}] Тригер {trigger_name!r} не знайдено")
        return False

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

    def profession_names(self, account_id: str) -> list[str]:
        """Список profession акаунта у порядку пріоритету."""
        with self._lock:
            entry = self._containers.get(account_id)
        if entry is None:
            return []
        return [p.profession_id for p in entry.profession_list()]

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
        professions: list["BaseProfession"],
    ) -> None:
        with self._lock:
            container = self._containers.get(account_id)
        if container is None:
            return

        for idx, profession in enumerate(professions):
            prof_priority = ProfessionPriority.from_index(idx)
            try:
                await profession.setup(self, account_id)
                self._router.register(account_id, profession)

                triggers = profession.build_triggers(account_id)
                await profession.restore_state(bot)

                container.add_profession(profession, triggers)

                tasks = profession.startup_tasks(bot)
                if tasks:
                    _apply_profession_priority(tasks, profession.profession_id, prof_priority)
                    container.worker.assign(*tasks)

                log.info(
                    f"[{account_id}] {profession.profession_id!r} ready"
                    f" (priority={prof_priority.name})"
                )
            except Exception as e:
                log.error(
                    f"[{account_id}] setup {profession.profession_id!r} failed: {e}",
                    exc_info=True,
                )

        self._wakeup.set()
        
    async def _resume_restore_state(
        self,
        account_id: str,
        bot:        Account,
        container:  AccountContainer,
    ) -> None:
        """
        Гарячий resume після pause: відновлює лише in-memory state та стартові задачі.
        НЕ викликає setup() (підписки EventBus живі) і НЕ будує нові triggers
        (використовуються збережені об'єкти з коректним _next_fire).
        """
        for idx, profession in enumerate(container.profession_list()):
            prof_priority = ProfessionPriority.from_index(idx)
            try:
                await profession.restore_state(bot)
                tasks = profession.startup_tasks(bot)
                if tasks:
                    _apply_profession_priority(tasks, profession.profession_id, prof_priority)
                    container.worker.assign(*tasks)
                log.info(f"[{account_id}] {profession.profession_id!r} hot-resumed")
            except Exception as e:
                log.error(
                    f"[{account_id}] hot-resume {profession.profession_id!r}: {e}",
                    exc_info=True,
                )

    async def _restore_professions(
        self,
        account_id:  str,
        bot:         Account,
        professions: list["BaseProfession"],
        container:   AccountContainer,
    ) -> None:
        for idx, profession in enumerate(professions):
            prof_priority = ProfessionPriority.from_index(idx)
            try:
                self._router.register(account_id, profession)

                triggers = profession.build_triggers(account_id)
                await profession.restore_state(bot)

                container.add_profession(profession, triggers)

                tasks = profession.startup_tasks(bot)
                if tasks:
                    _apply_profession_priority(tasks, profession.profession_id, prof_priority)
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
        professions: list["BaseProfession"],
    ) -> None:
        for profession in professions:
            try:
                self._event_bus.unsubscribe_owner(profession)
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
                    # Тільки задачі цієї profession
                    entry.worker.clear_profession(profession.profession_id)
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

                # Визначаємо profession і її пріоритет для цього тригера
                owning_profession = self._find_trigger_owner(entry, trigger)
                prof_priority = (
                    entry.profession_priority(owning_profession)
                    if owning_profession else ProfessionPriority.PRIMARY
                )
                prof_id = owning_profession.profession_id if owning_profession else None

                try:
                    tasks = list(trigger.producer(bot))
                except Exception as e:
                    log.error(f"[{account_id}] trigger {trigger.name!r}: {e}", exc_info=True)
                    tasks = []

                if tasks:
                    if prof_id:
                        _apply_profession_priority(tasks, prof_id, prof_priority)
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

    @staticmethod
    def _find_trigger_owner(
        container: AccountContainer,
        trigger:   TriggerProtocol,
    ) -> Optional["BaseProfession"]:
        """Знаходить profession-власника тригера."""
        for profession, triggers in container.professions.items():
            if trigger in triggers:
                return profession
        return None

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


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _apply_profession_priority(
    tasks:         list[AnyTask],
    profession_id: str,
    prof_priority: ProfessionPriority,
) -> None:
    """
    In-place:
      1. Проставляє source_profession якщо ще не встановлено.
      2. Коригує priority задачі відповідно до ProfessionPriority profession.
    """
    tag_profession(tasks, profession_id)
    if prof_priority == ProfessionPriority.PRIMARY:
        return  # Не зміщуємо — задачі Primary profession ідуть без штрафу
    for task in tasks:
        task.priority = prof_priority.adjust(task.priority)  # type: ignore[attr-defined]
