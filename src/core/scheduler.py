"""
scheduler.py — центральний планувальник. SINGLETON.

Єдина точка доступу до всіх акаунтів і воркерів.
Ніхто не може обійти Scheduler для доступу до BotWorker.

Динамічне керування акаунтами (з адмін-панелі):
    scheduler.add_account(account_id, entry)
    scheduler.remove_account(account_id)
    scheduler.pause_account(account_id)
    scheduler.resume_account(account_id)
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Callable, Optional

from src.core.account import Account
from src.core.inventory.model import Inventories
from src.core.logging.loggers import get_scheduler_logger
from src.core.scheduling.conditions import Condition
from src.core.scheduling.profession import Profession
from src.core.scheduling.schedule import TriggerProtocol
from src.core.status import AccountStatus
from src.core.tasks.base import AnyTask
from src.core.worker import BotWorker

log = get_scheduler_logger()
_MAX_SLEEP = 30.0
_MIN_SLEEP = 0.5


@dataclass
class AccountEntry:
    worker:      BotWorker
    professions: list[Profession]    = field(default_factory=lambda: [])
    guard:       Optional[Condition] = None

    triggers:       list[TriggerProtocol] = field(default_factory=lambda: [], init=False, repr=False)
    trigger_owner:  dict[int, str]        = field(default_factory=lambda: {}, init=False, repr=False)

    def check_account_guard(self, inv: Inventories) -> bool:
        return self.guard is None or self.guard(inv)

    def add_triggers(self, new_triggers: list[TriggerProtocol], profession_name: str) -> None:
        for t in new_triggers:
            self.triggers.append(t)
            self.trigger_owner[id(t)] = profession_name

    def remove_profession_triggers(self, profession_name: str) -> int:
        to_remove = [t for t in self.triggers if self.trigger_owner.get(id(t)) == profession_name]
        for t in to_remove:
            self.triggers.remove(t)
            self.trigger_owner.pop(id(t), None)
        return len(to_remove)

    def remove_trigger(self, trigger: TriggerProtocol) -> None:
        """Видаляє один конкретний тригер (викликається з Scheduler._dispatch_triggers)."""
        try:
            self.triggers.remove(trigger)
        except ValueError:
            pass
        self.trigger_owner.pop(id(trigger), None)

    def remove_all_triggers(self) -> None:
        self.triggers.clear()
        self.trigger_owner.clear()

    def trigger_names(self) -> list[str]:
        return [t.name for t in self.triggers]

    def next_trigger_in(self) -> float:
        finite = [s for t in self.triggers if (s := t.seconds_until()) != float("inf")]
        return min(finite) if finite else _MAX_SLEEP


class Scheduler:
    """
    Singleton-планувальник.

    main.py:
        scheduler = Scheduler.initialize(on_dead=on_dead)
        scheduler.start()

    Будь-де:
        scheduler = Scheduler.get_instance()
        scheduler.add_account(...)
    """

    _instance:  Optional["Scheduler"] = None
    _init_lock: threading.Lock = threading.Lock()

    @classmethod
    def initialize(
        cls,
        on_dead: Optional[Callable[[Account], None]] = None,
    ) -> "Scheduler":
        with cls._init_lock:
            if cls._instance is not None:
                raise RuntimeError("Scheduler вже ініціалізований. Використовуй get_instance().")
            inst = cls.__new__(cls)
            inst._setup(on_dead)
            cls._instance = inst
            return inst

    @classmethod
    def get_instance(cls) -> "Scheduler":
        if cls._instance is None:
            raise RuntimeError("Scheduler не ініціалізований. Спочатку виклич initialize().")
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

    def _setup(self, on_dead: Optional[Callable[[Account], None]]) -> None:
        self._on_dead  = on_dead
        self._entries: dict[str, AccountEntry]     = {}
        self._active:  dict[str, list[Profession]] = {}
        self._lock     = threading.Lock()
        self._stop     = threading.Event()
        self._wakeup   = threading.Event()
        self._monitor  = threading.Thread(
            target=self._monitor_loop,
            daemon=True,
            name="scheduler-monitor",
        )

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        self._monitor.start()
        log.info("Scheduler started (empty, accounts added dynamically)")

    def stop(self) -> None:
        self._stop.set()
        self._wakeup.set()
        if self._monitor.is_alive():
            self._monitor.join(timeout=10)
        with self._lock:
            entries = dict(self._entries)
        for entry in entries.values():
            entry.worker.stop()
        log.info("Scheduler stopped")

    # ── Динамічне керування акаунтами ─────────────────────────────────────────

    def add_account(self, account_id: str, entry: AccountEntry) -> None:
        """Додає новий акаунт і запускає його воркер. Thread-safe."""
        with self._lock:
            if account_id in self._entries:
                raise ValueError(f"Акаунт {account_id!r} вже існує в Scheduler")
            self._entries[account_id] = entry
            self._active[account_id]  = list(entry.professions)

        self._init_entry(account_id, entry)
        entry.worker.start()
        self._wakeup.set()
        log.info(f"[{account_id}] додано до Scheduler")

    def remove_account(self, account_id: str) -> bool:
        """Зупиняє воркер і повністю видаляє акаунт. Зберігає інвентар."""
        with self._lock:
            entry = self._entries.get(account_id)
        if entry is None:
            return False
        entry.worker.clear()
        entry.remove_all_triggers()
        entry.worker.stop()
        with self._lock:
            self._entries.pop(account_id, None)
            self._active.pop(account_id, None)
        log.info(f"[{account_id}] видалено зі Scheduler")
        return True

    def has_account(self, account_id: str) -> bool:
        with self._lock:
            return account_id in self._entries

    # ── Публічний API ─────────────────────────────────────────────────────────

    def push_task(self, account_id: str, task: AnyTask) -> bool:
        with self._lock:
            entry = self._entries.get(account_id)
        if entry is None:
            return False
        entry.worker.assign(task)
        return True

    def notify_error(self, account_id: str) -> None:
        self._wakeup.set()

    def account_ids(self) -> list[str]:
        with self._lock:
            return list(self._entries.keys())

    def status(self, account_id: str) -> Optional[AccountStatus]:
        with self._lock:
            entry = self._entries.get(account_id)
        return entry.worker.bot.status if entry else None

    def all_statuses(self) -> dict[str, AccountStatus]:
        with self._lock:
            entries = dict(self._entries)
        return {aid: e.worker.bot.status for aid, e in entries.items()}

    def queue_size(self, account_id: str) -> Optional[int]:
        with self._lock:
            entry = self._entries.get(account_id)
        return entry.worker.queue_size if entry else None

    def get_bot(self, account_id: str) -> Optional[Account]:
        with self._lock:
            entry = self._entries.get(account_id)
        return entry.worker.bot if entry else None

    def get_entry(self, account_id: str) -> Optional[AccountEntry]:
        with self._lock:
            return self._entries.get(account_id)

    def pause_account(self, account_id: str) -> bool:
        """
        Призупиняє акаунт:
          - очищує чергу задач
          - видаляє всі тригери (Scheduler більше не буде їх диспетчеризувати)
          - зупиняє воркер-потік (зберігає інвентар)
          - статус → SUSPENDED

        Воркер-потік зупиняється чисто: поточна задача доробляється,
        після чого потік виходить.
        """
        with self._lock:
            entry = self._entries.get(account_id)
        if entry is None:
            return False
        if entry.worker.bot.status == AccountStatus.SUSPENDED:
            return False  # вже призупинено

        entry.worker.clear()           # очищуємо чергу
        entry.remove_all_triggers()    # прибираємо тригери
        entry.worker.bot.status = AccountStatus.SUSPENDED
        entry.worker.stop()            # зупиняємо потік (зберігає інвентар)
        log.info(f"[{account_id}] призупинено (SUSPENDED)")
        return True

    def resume_account(self, account_id: str) -> bool:
        """
        Відновлює призупинений акаунт:
          - перезапускає воркер-потік (connect → новий threading.Thread)
          - виконує startup_tasks всіх profession (init_reader тощо)
          - відновлює тригери

        Якщо connect() провалюється — статус → ERROR, повертає False.
        """
        with self._lock:
            entry = self._entries.get(account_id)
        if entry is None:
            return False
        if entry.worker.bot.status != AccountStatus.SUSPENDED:
            return False

        # Перезапускаємо воркер (новий потік + нова сесія)
        entry.worker.bot.status = AccountStatus.IDLE
        entry.worker.start()   # всередині: connect() → якщо провалюється → DEAD

        if entry.worker.bot.status == AccountStatus.DEAD: # pyright: ignore[reportUnnecessaryComparison]
            log.error(f"[{account_id}] resume: connect() провалився → DEAD")
            return False

        # Startup tasks (init_reader ініціалізує SlotScheduler)
        for profession in entry.professions:
            tasks = profession.startup_tasks(entry.worker.bot)
            if tasks:
                entry.worker.assign(*tasks)

        # Відновлюємо тригери
        for profession in entry.professions:
            triggers = profession.build_triggers(account_id)
            entry.add_triggers(triggers, profession.name)

        self._wakeup.set()
        log.info(f"[{account_id}] відновлено (IDLE)")
        return True

    def trigger_names(self, account_id: str) -> list[str]:
        with self._lock:
            entry = self._entries.get(account_id)
        if entry is None:
            return []
        return entry.trigger_names()

    def seconds_until_next(self, account_id: str) -> Optional[float]:
        """Скільки секунд до наступного тригера (для дисплею в боті)."""
        with self._lock:
            entry = self._entries.get(account_id)
        if entry is None:
            return None
        return entry.next_trigger_in()

    # ── Monitor loop ──────────────────────────────────────────────────────────

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

    def _init_entry(self, account_id: str, entry: AccountEntry) -> None:
        def _on_error(bot: Account) -> None:
            self.notify_error(bot.account_id)

        entry.worker.set_error_callback(_on_error)
        for profession in entry.professions:
            tasks = profession.startup_tasks(entry.worker.bot)
            if tasks:
                entry.worker.assign(*tasks)
            triggers = profession.build_triggers(account_id)
            entry.add_triggers(triggers, profession.name)
            log.debug(f"[{account_id}] profession={profession.name!r} startup={len(tasks)} triggers={len(triggers)}")

    def _dispatch_triggers(self) -> float:
        with self._lock:
            entries = dict(self._entries)
        next_wakeup = _MAX_SLEEP

        for account_id, entry in entries.items():
            if entry.worker.bot.status in (AccountStatus.DEAD, AccountStatus.SUSPENDED):
                continue
            inv       = entry.worker.bot.inventory
            to_remove: list[TriggerProtocol] = []

            for trigger in list(entry.triggers):
                if trigger.is_expired(inv):
                    to_remove.append(trigger)
                    continue
                if not trigger.is_due():
                    secs = trigger.seconds_until()
                    if secs != float("inf"):
                        next_wakeup = min(next_wakeup, secs)
                    continue

                trigger.dispatch()
                try:
                    tasks = list(trigger.producer(entry.worker.bot))
                except Exception as e:
                    log.error(f"[{account_id}] trigger {trigger.name!r}: {e}", exc_info=True)
                    tasks = []

                if tasks:
                    entry.worker.assign(*tasks)
                else:
                    trigger.advance(entry.worker.bot)
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

    def _check_guards(self) -> None:
        with self._lock:
            entries = dict(self._entries)
            active  = {k: list(v) for k, v in self._active.items()}

        for account_id, entry in entries.items():
            inv = entry.worker.bot.inventory
            if not entry.check_account_guard(inv):
                log.warning(f"[{account_id}] account guard failed → kill")
                self._kill_account(account_id, entry, inv, "account guard failed")
                continue

            professions   = active.get(account_id, [])
            still_active: list[Profession] = []
            removed_any   = False
            for profession in professions:
                if profession.check_guard(inv):
                    still_active.append(profession)
                    continue
                self._suspend_profession(account_id, entry, profession)
                removed_any = True

            if removed_any:
                with self._lock:
                    self._active[account_id] = still_active
                if not still_active:
                    entry.worker.bot.status = AccountStatus.SUSPENDED

    def _kill_account(self, account_id: str, entry: AccountEntry, inv: Inventories, reason: str) -> None:
        worker = entry.worker
        worker.clear()
        entry.remove_all_triggers()
        worker.bot.mark_dead(reason)
        worker.bot.repo.inventory.save(account_id, inv)
        worker.stop()
        with self._lock:
            self._entries.pop(account_id, None)
            self._active.pop(account_id, None)
        if self._on_dead:
            self._on_dead(entry.worker.bot)

    def _suspend_profession(self, account_id: str, entry: AccountEntry, profession: Profession) -> None:
        worker = entry.worker
        removed = entry.remove_profession_triggers(profession.name)
        worker.clear()
        log.info(f"[{account_id}] profession {profession.name!r} suspended: {removed} triggers")

    def _reap_dead_workers(self) -> None:
        with self._lock:
            entries = dict(self._entries)
        for account_id, entry in entries.items():
            if entry.worker.bot.status != AccountStatus.DEAD:
                continue
            log.warning(f"[{account_id}] dead → cleanup")
            entry.remove_all_triggers()
            worker = entry.worker
            worker.stop()
            with self._lock:
                self._entries.pop(account_id, None)
                self._active.pop(account_id, None)
            if self._on_dead:
                self._on_dead(worker.bot)

    @staticmethod
    def _is_one_shot(trigger: TriggerProtocol) -> bool:
        val = getattr(trigger, "is_one_shot", None)
        if callable(val):
            return bool(val())
        return bool(val) if val is not None else False