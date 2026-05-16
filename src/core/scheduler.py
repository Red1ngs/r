"""
scheduler.py — моніторинг тригерів і диспетчеризація задач.

Guard-механіка
──────────────
Два рівні guard:

1. AccountEntry.guard (рівень акаунта):
   False → негайно:
     - worker.clear()           ← скидаємо всі задачі в черзі
     - remove_all_triggers()    ← жодних нових задач
     - bot.mark_dead(reason)    ← статус DEAD
     - worker.stop()            ← зупиняємо воркер
     - on_dead callback         ← сповіщення зовні

2. Profession.guard (рівень profession):
   False → негайно:
     - видаляємо тригери тільки цієї profession
     - worker.clear()           ← скидаємо поточну чергу (задачі могли бути від цієї profession)
     - bot.mark_suspended()     ← статус SUSPENDED (акаунт живий)

Scheduler перевіряє guards при кожному _tick() — тобто з інтервалом
_MIN_SLEEP..MAX_SLEEP. Для миттєвої реакції на помилку сесії —
BotWorker викликає on_error callback після кожного FAIL, що будить monitor.

Lifecycle тригера
─────────────────
    is_expired(inv) → True           → видаляємо
    is_due()        → True           → dispatch() → producer(bot)
    producer → задачі                → assign до воркера
    producer → []                    → advance(bot) одразу; one_shot → видаляємо
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Callable, Optional

from src.core.account import AccountPull
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


# ─────────────────────────────────────────────────────────────────────────────
# AccountEntry
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class AccountEntry:
    """
    Запис про один акаунт у Scheduler-і.

    worker      : BotWorker що виконує задачі
    professions : список профілів (startup + тригери)
    guard       : умова «акаунт живий» — False → негайна зупинка
    """
    worker:      BotWorker
    professions: list[Profession]      = field(default_factory=lambda: [])
    guard:       Optional[Condition]   = None

    # (trigger → profession_name) — для точного видалення при guard fail
    _triggers:         list[TriggerProtocol]        = field(default_factory=lambda: [], init=False, repr=False)
    _trigger_owner:    dict[int, str]               = field(default_factory=lambda: {}, init=False, repr=False)

    def check_account_guard(self, inv: Inventories) -> bool:
        return self.guard is None or self.guard(inv)

    def add_triggers(self, triggers: list[TriggerProtocol], profession_name: str) -> None:
        for t in triggers:
            self._triggers.append(t)
            self._trigger_owner[id(t)] = profession_name

    def remove_profession_triggers(self, profession_name: str) -> int:
        """Видаляє всі тригери що належать profession. Повертає кількість."""
        to_remove = [t for t in self._triggers if self._trigger_owner.get(id(t)) == profession_name]
        for t in to_remove:
            self._triggers.remove(t)
            self._trigger_owner.pop(id(t), None)
        return len(to_remove)

    def remove_all_triggers(self) -> None:
        self._triggers.clear()
        self._trigger_owner.clear()

    def next_trigger_in(self) -> float:
        finite = [s for t in self._triggers if (s := t.seconds_until()) != float("inf")]
        return min(finite) if finite else _MAX_SLEEP


# ─────────────────────────────────────────────────────────────────────────────
# Scheduler
# ─────────────────────────────────────────────────────────────────────────────

class Scheduler:
    """
    Центральний планувальник.

    Не знає про деталі жодного тригера — тільки TriggerProtocol.
    Реагує на guard-умови негайно (не чекає наступної ітерації monitor).
    """

    def __init__(
        self,
        workers: dict[str, AccountEntry],
        on_dead: Optional[Callable[[AccountPull], None]] = None,
    ):
        self._on_dead  = on_dead
        self._entries: dict[str, AccountEntry]     = dict(workers)
        self._active:  dict[str, list[Profession]] = {}
        self._lock     = threading.Lock()
        self._stop     = threading.Event()
        self._wakeup   = threading.Event()   # будить monitor достроково
        self._monitor  = threading.Thread(
            target=self._monitor_loop,
            daemon=True,
            name="scheduler-monitor",
        )

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        with self._lock:
            entries = dict(self._entries)
        for account_id, entry in entries.items():
            self._init_entry(account_id, entry)
            entry.worker.start()
        self._monitor.start()
        log.info(f"Scheduler started ({len(entries)} accounts)")

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

    # ── Публічний API ─────────────────────────────────────────────────────────

    def push_task(self, account_id: str, task: AnyTask) -> bool:
        with self._lock:
            entry = self._entries.get(account_id)
        if entry is None:
            return False
        entry.worker.assign(task)
        return True

    def notify_error(self, account_id: str) -> None:
        """
        Викликається BotWorker після кожного FAIL.
        Будить monitor достроково — guard перевіряється негайно.
        """
        log.debug(f"[{account_id}] error notified → wakeup monitor")
        self._wakeup.set()

    def get_worker(self, account_id: str) -> Optional[BotWorker]:
        with self._lock:
            entry = self._entries.get(account_id)
        return entry.worker if entry else None

    def get_bot(self, account_id: str) -> Optional[AccountPull]:
        worker = self.get_worker(account_id)
        return worker.bot if worker else None

    # ── Monitor loop ──────────────────────────────────────────────────────────

    def _monitor_loop(self) -> None:
        while not self._stop.is_set():
            self._wakeup.clear()
            sleep = self._tick()
            # Чекаємо або поки не прийде wakeup (помилка), або таймер
            self._wakeup.wait(timeout=max(_MIN_SLEEP, min(sleep, _MAX_SLEEP)))

    def _tick(self) -> float:
        self._check_guards()
        next_in = self._dispatch_triggers()
        self._reap_dead_workers()
        return next_in

    # ── Ініціалізація акаунта ─────────────────────────────────────────────────

    def _init_entry(self, account_id: str, entry: AccountEntry) -> None:
        with self._lock:
            self._active[account_id] = list(entry.professions)

        # Підключаємо on_error воркера → scheduler прокидається негайно
        entry.worker._on_error = lambda bot: self.notify_error(bot.account_id)

        for profession in entry.professions:
            tasks = profession.startup_tasks(entry.worker.bot)
            if tasks:
                entry.worker.assign(*tasks)
            triggers = profession.build_triggers(account_id)
            entry.add_triggers(triggers, profession.name)
            log.debug(
                f"[{account_id}] profession={profession.name!r} "
                f"startup={len(tasks)} triggers={len(triggers)}"
            )

    # ── Диспетчеризація тригерів ──────────────────────────────────────────────

    def _dispatch_triggers(self) -> float:
        with self._lock:
            entries = dict(self._entries)

        next_wakeup = _MAX_SLEEP

        for account_id, entry in entries.items():
            if entry.worker.bot.status in (AccountStatus.DEAD, AccountStatus.SUSPENDED):
                continue

            inv       = entry.worker.bot.inventory
            to_remove: list[TriggerProtocol] = []

            for trigger in list(entry._triggers):

                if trigger.is_expired(inv):
                    to_remove.append(trigger)
                    log.debug(f"[{account_id}] trigger {trigger.name!r} expired")
                    continue

                if not trigger.is_due():
                    secs = trigger.seconds_until()
                    if secs != float("inf"):
                        next_wakeup = min(next_wakeup, secs)
                    continue

                trigger.dispatch()
                log.debug(f"[{account_id}] trigger {trigger.name!r} fired")

                try:
                    tasks = list(trigger.producer(entry.worker.bot))
                except Exception as e:
                    log.error(
                        f"[{account_id}] trigger {trigger.name!r} producer error: {e}",
                        exc_info=True,
                    )
                    tasks = []

                if tasks:
                    entry.worker.assign(*tasks)
                else:
                    trigger.advance(entry.worker.bot)
                    log.debug(f"[{account_id}] trigger {trigger.name!r} empty → advance")
                    if self._is_one_shot(trigger):
                        to_remove.append(trigger)
                        continue
                    secs = trigger.seconds_until()
                    if secs != float("inf"):
                        next_wakeup = min(next_wakeup, secs)

            if to_remove:
                with self._lock:
                    for t in to_remove:
                        try:
                            entry._triggers.remove(t)
                            entry._trigger_owner.pop(id(t), None)
                        except ValueError:
                            pass

        return next_wakeup

    # ── Guards ────────────────────────────────────────────────────────────────

    def _check_guards(self) -> None:
        with self._lock:
            entries = dict(self._entries)
            active  = {k: list(v) for k, v in self._active.items()}

        for account_id, entry in entries.items():
            inv = entry.worker.bot.inventory

            # ── Guard акаунта — зупиняємо все негайно ────────────────────────
            if not entry.check_account_guard(inv):
                log.warning(f"[{account_id}] account guard failed → kill")
                self._kill_account(account_id, entry, inv, "account guard failed")
                continue

            # ── Guard кожної profession ───────────────────────────────────────
            professions = active.get(account_id, [])
            still_active: list[Profession] = []
            removed_any = False

            for profession in professions:
                if profession.check_guard(inv):
                    still_active.append(profession)
                    continue

                log.warning(
                    f"[{account_id}] profession {profession.name!r} "
                    f"guard failed → suspend"
                )
                self._suspend_profession(account_id, entry, profession)
                removed_any = True

            if removed_any:
                with self._lock:
                    self._active[account_id] = still_active

                # Якщо всі profession зупинені → переводимо акаунт в SUSPENDED
                if not still_active:
                    log.warning(f"[{account_id}] всі profession зупинені → SUSPENDED")
                    entry.worker.bot.status = AccountStatus.SUSPENDED

    def _kill_account(
        self,
        account_id: str,
        entry:      AccountEntry,
        inv:        Inventories,
        reason:     str,
    ) -> None:
        """
        Негайна зупинка акаунта.
        Очищує чергу, видаляє тригери, зупиняє воркер.
        """
        entry.worker.clear()
        entry.remove_all_triggers()
        entry.worker.bot.mark_dead(reason)
        entry.worker.bot.store.save(inv)
        entry.worker.stop()

        with self._lock:
            self._entries.pop(account_id, None)
            self._active.pop(account_id, None)

        if self._on_dead:
            self._on_dead(entry.worker.bot)

    def _suspend_profession(
        self,
        account_id: str,
        entry:      AccountEntry,
        profession: Profession,
    ) -> None:
        """
        Негайна зупинка profession.
        Видаляє тільки тригери цієї profession і скидає чергу воркера.
        """
        removed = entry.remove_profession_triggers(profession.name)
        # Скидаємо чергу — задачі від цієї profession вже в черзі
        entry.worker.clear()
        log.info(
            f"[{account_id}] profession {profession.name!r} suspended: "
            f"removed {removed} triggers, queue cleared"
        )

    # ── Reap dead workers ─────────────────────────────────────────────────────

    def _reap_dead_workers(self) -> None:
        """
        Підчищає воркери що стали DEAD поза _kill_account
        (наприклад через BotWorker._try_recover).
        """
        with self._lock:
            entries = dict(self._entries)

        for account_id, entry in entries.items():
            if entry.worker.bot.status != AccountStatus.DEAD:
                continue
            log.warning(f"[{account_id}] worker dead (external) → cleanup")
            entry.remove_all_triggers()
            entry.worker.stop()
            with self._lock:
                self._entries.pop(account_id, None)
                self._active.pop(account_id, None)
            if self._on_dead:
                self._on_dead(entry.worker.bot)

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _is_one_shot(trigger: TriggerProtocol) -> bool:
        is_one_shot = getattr(trigger, "is_one_shot", None)
        if callable(is_one_shot):
            return bool(is_one_shot())
        if isinstance(is_one_shot, bool):
            return is_one_shot
        return False