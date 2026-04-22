"""
scheduler.py — Scheduler, фасад над колекцією BotWorker-ів.

Ключові зміни відносно попередньої версії
──────────────────────────────────────────
1. TriggerTable замість _schedule (list[ScheduledEntry]).
   Кожен AccountEntry зберігає свій список Trigger-ів.

2. Dynamic sleep у _monitor_loop.
   Монітор спить рівно стільки, скільки треба до наступного тригера
   (але не більше MAX_SLEEP). Усуває зайве CPU-навантаження.

3. Гарантія "один цикл за раз" через dispatch()/advance().
   Scheduler викликає trigger.dispatch() перед передачею задач воркеру —
   це встановлює _in_flight=True і блокує is_due().
   advance() викликається після завершення action (через on_cycle_done
   callback у pipeline) — знімає блок і рахує наступний _next_fire.
   Якщо producer повернув порожній список — advance() викликається одразу
   в Scheduler-і (нічого не запустили, чекаємо наступного тіку).

4. Повна зворотна сумісність:
   - push_task / get_worker / get_bot / report — без змін
   - add_worker / remove_worker — без змін
   - set_professions / add_profession / remove_profession — без змін
   - scheduler.every() — тепер створює Trigger напряму

Два рівні guard збережено:
    AccountEntry.guard   → False → бот DEAD, всі тригери знімаються
    Profession.guard     → False → тільки ця Profession знімається
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Iterable, Optional

from src.core.account_pull import AccountPull
from src.core.conditions import Condition
from src.core.inventory.model import Inventories
from src.core.logging.loggers import get_scheduler_logger
from src.core.profession import Profession
from src.core.schedule import Trigger
from src.core.status import AccountStatus
from src.core.task import AnyTask
from src.core.worker import BotWorker

log = get_scheduler_logger()

# Максимальний час сну монітора між тіками (секунди).
_MAX_SLEEP = 30.0
# Мінімальний sleep щоб не спінити CPU при _next_fire=0
_MIN_SLEEP = 0.5


# ─────────────────────────────────────────────────────────────────────────────
# AccountEntry
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class AccountEntry:
    """
    Один акаунт у Scheduler.

    worker      : готовий BotWorker (Scheduler його не створює)
    professions : ролі бота з власними guard і triggers
    guard       : умова акаунта загалом; False → бот DEAD повністю
    _triggers   : активні тригери акаунта (заповнюються при старті)
    """
    worker:      BotWorker
    professions: list[Profession]    = field(default_factory=list)
    guard:       Optional[Condition] = None
    _triggers:   list[Trigger]       = field(default_factory=list, init=False, repr=False)

    def check_account_guard(self, inv: Inventories) -> bool:
        return self.guard is None or self.guard(inv)

    # ── Управління тригерами ──────────────────────────────────────────────────

    def add_triggers(self, triggers: list[Trigger]) -> None:
        self._triggers.extend(triggers)

    def remove_triggers_for_profession(self, profession: Profession) -> None:
        prof_names = {
            t.name for t in profession.build_triggers(self.worker.bot.account_id)
        }
        self._triggers = [
            t for t in self._triggers
            if t.name not in prof_names
        ]

    def remove_all_triggers(self) -> None:
        self._triggers.clear()

    def next_trigger_in(self) -> float:
        """Секунд до найближчого тригера. _MAX_SLEEP якщо тригерів немає."""
        if not self._triggers:
            return _MAX_SLEEP
        secs = [t.seconds_until() for t in self._triggers]
        finite = [s for s in secs if s != float("inf")]
        return min(finite) if finite else _MAX_SLEEP


# ─────────────────────────────────────────────────────────────────────────────
# Scheduler
# ─────────────────────────────────────────────────────────────────────────────

class Scheduler:
    """
    Фасад над колекцією BotWorker-ів.

    _monitor_loop виконує _tick() кожні N секунд, де N розраховується
    динамічно — рівно до найближчого тригера (але не більше _MAX_SLEEP).
    """

    def __init__(
        self,
        workers: dict[str, AccountEntry],
        on_dead: Optional[Callable[[AccountPull], None]] = None,
    ):
        self._on_dead = on_dead
        self._entries:            dict[str, AccountEntry]     = dict(workers)
        self._active_professions: dict[str, list[Profession]] = {}
        self._lock       = threading.Lock()
        self._stop_event = threading.Event()

        self._monitor = threading.Thread(
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
            log.info(f"Started worker '{account_id}'")

        self._monitor.start()
        log.info(f"Monitor started ({len(entries)} accounts)")

    def stop(self) -> None:
        log.info("Stopping…")
        self._stop_event.set()
        if self._monitor.is_alive():
            self._monitor.join(timeout=10)
        with self._lock:
            entries = dict(self._entries)
        for entry in entries.values():
            entry.worker.stop()

    # ── Monitor loop ──────────────────────────────────────────────────────────

    def _monitor_loop(self) -> None:
        while not self._stop_event.is_set():
            sleep = self._tick()
            self._stop_event.wait(max(_MIN_SLEEP, min(sleep, _MAX_SLEEP)))

    def _tick(self) -> float:
        """
        Один тік: guard-перевірка, dispatch тригерів, прибирання мертвих.
        Повертає секунд до наступного тіку.
        """
        self._check_guards()
        next_in = self._dispatch_triggers()
        self._reap_dead_workers()
        return next_in

    # ── Управління акаунтами ──────────────────────────────────────────────────

    def add_worker(self, account_id: str, entry: AccountEntry) -> None:
        with self._lock:
            if account_id in self._entries:
                log.warning(f"add_worker: '{account_id}' вже існує, ігноруємо")
                return
            self._entries[account_id] = entry

        self._init_entry(account_id, entry)
        entry.worker.start()
        log.info(f"Added worker '{account_id}'")

    def remove_worker(self, account_id: str) -> None:
        with self._lock:
            entry = self._entries.pop(account_id, None)
            self._active_professions.pop(account_id, None)

        if entry:
            entry.remove_all_triggers()
            entry.worker.stop()
            log.info(f"Removed worker '{account_id}'")

    # ── Управління профессіями ─────────────────────────────────────────────────

    def set_professions(self, account_id: str, professions: list[Profession]) -> None:
        with self._lock:
            entry = self._entries.get(account_id)
        if entry is None:
            log.warning(f"set_professions: '{account_id}' не знайдено")
            return

        with self._lock:
            old = self._active_professions.get(account_id, [])
        for p in old:
            entry.remove_triggers_for_profession(p)

        with self._lock:
            self._active_professions[account_id] = list(professions)

        for p in professions:
            tasks = p.startup_tasks(entry.worker.bot)
            if tasks:
                entry.worker.assign(*tasks)
            entry.add_triggers(p.build_triggers(account_id))

        log.info(f"'{account_id}' professions → {[p.name for p in professions]}")

    def add_profession(self, account_id: str, profession: Profession) -> None:
        with self._lock:
            entry = self._entries.get(account_id)
            if entry is None:
                log.warning(f"add_profession: '{account_id}' не знайдено")
                return
            active = self._active_professions.setdefault(account_id, [])
            if any(p.name == profession.name for p in active):
                log.warning(f"'{account_id}' вже має profession={profession.name!r}")
                return
            active.append(profession)

        tasks = profession.startup_tasks(entry.worker.bot)
        if tasks:
            entry.worker.assign(*tasks)
        entry.add_triggers(profession.build_triggers(account_id))
        log.info(f"'{account_id}' + profession={profession.name!r}")

    def remove_profession(self, account_id: str, profession_name: str) -> None:
        with self._lock:
            active = self._active_professions.get(account_id, [])
            target = next((p for p in active if p.name == profession_name), None)
            if target is None:
                log.warning(f"'{account_id}' profession={profession_name!r} не знайдено")
                return
            self._active_professions[account_id] = [
                p for p in active if p.name != profession_name
            ]
            entry = self._entries.get(account_id)

        if entry:
            entry.remove_triggers_for_profession(target)
        log.info(f"'{account_id}' - profession={profession_name!r}")

    # ── Задачі і тригери ──────────────────────────────────────────────────────

    def push_task(self, account_id: str, task: AnyTask) -> bool:
        with self._lock:
            entry = self._entries.get(account_id)
        if entry is None:
            log.warning(f"push_task: '{account_id}' не знайдено")
            return False
        entry.worker.assign(task)
        return True

    def every(
        self,
        account_id: str,
        interval:   float,
        producer:   Callable[[AccountPull], Iterable[AnyTask]],
        until:      Optional[Callable[[Inventories], bool]] = None,
        name:       str = "",
    ) -> None:
        trigger = Trigger(
            name       = name or getattr(producer, "__name__", "dynamic"),
            account_id = account_id,
            interval   = interval,
            producer   = producer,
            until      = until,
        )
        with self._lock:
            entry = self._entries.get(account_id)
        if entry:
            entry.add_triggers([trigger])
            log.info(f"'{account_id}' every {interval:.0f}s → '{trigger.name}'")

    # ── Стан / звіт ───────────────────────────────────────────────────────────

    def get_worker(self, account_id: str) -> Optional[BotWorker]:
        with self._lock:
            entry = self._entries.get(account_id)
        return entry.worker if entry else None

    def get_bot(self, account_id: str) -> Optional[AccountPull]:
        worker = self.get_worker(account_id)
        return worker.bot if worker else None

    def report(self) -> list[dict[str, object]]:
        with self._lock:
            entries  = dict(self._entries)
            active_p = {k: list(v) for k, v in self._active_professions.items()}
        rows: list[dict[str, object]] = []
        for account_id, entry in entries.items():
            rows.append({
                "id":              account_id,
                "status":          entry.worker.bot.status.name,
                "queue_size":      entry.worker.queue_size,
                "professions":     [p.name for p in active_p.get(account_id, [])],
                "triggers":        len(entry._triggers),
                "next_trigger_in": f"{entry.next_trigger_in():.0f}s",
            })
        return rows

    # ── Внутрішня логіка ──────────────────────────────────────────────────────

    def _init_entry(self, account_id: str, entry: AccountEntry) -> None:
        with self._lock:
            self._active_professions[account_id] = list(entry.professions)

        for profession in entry.professions:
            tasks = profession.startup_tasks(entry.worker.bot)
            if tasks:
                entry.worker.assign(*tasks)
            entry.add_triggers(profession.build_triggers(account_id))

    def _dispatch_triggers(self) -> float:
        """
        Перебирає всі активні тригери.

        Для кожного due тригера:
          1. Викликає trigger.dispatch()  — блокує повторний dispatch
          2. Запитує задачі у producer
          3a. Якщо є задачі → передає воркеру (advance() викличе pipeline)
          3b. Якщо задач немає → одразу advance() (нічого не запускали)

        Повертає секунд до найближчого наступного тригера.
        """
        with self._lock:
            entries = dict(self._entries)

        next_wakeup = _MAX_SLEEP

        for account_id, entry in entries.items():
            if entry.worker.bot.status == AccountStatus.DEAD:
                continue

            inv       = entry.worker.bot.inventory
            to_remove: list[Trigger] = []

            for trigger in list(entry._triggers):
                if trigger.is_expired(inv):
                    to_remove.append(trigger)
                    log.info(
                        f"'{account_id}' trigger '{trigger.name}' expired (until fired)"
                    )
                    continue

                if not trigger.is_due():
                    secs = trigger.seconds_until()
                    if secs != float("inf"):
                        next_wakeup = min(next_wakeup, secs)
                    continue

                # ── Час настав, цикл вільний — dispatch ───────────────────────
                trigger.dispatch()   # _in_flight=True, блокує повторний dispatch

                try:
                    tasks = list(trigger.producer(entry.worker.bot))
                except Exception as e:
                    log.error(
                        f"'{account_id}' trigger '{trigger.name}' producer error: {e}"
                    )
                    tasks = []

                if tasks:
                    entry.worker.assign(*tasks)
                    log.info(
                        f"'{account_id}' trigger '{trigger.name}' → "
                        f"{len(tasks)} tasks"
                    )
                    # advance() буде викликано pipeline після завершення action.
                    # next_wakeup не рахуємо — тригер in-flight, seconds_until()=inf.
                else:
                    # Нічого не запустили — знімаємо блок одразу.
                    # One-shot (interval=0, dynamic_next=None) — видаляємо.
                    if trigger.interval <= 0 and trigger.dynamic_next is None:
                        trigger._in_flight = False   # знімаємо перед видаленням
                        to_remove.append(trigger)
                    else:
                        trigger.advance(entry.worker.bot)
                        secs = trigger.seconds_until()
                        if secs != float("inf"):
                            next_wakeup = min(next_wakeup, secs)
                        log.debug(
                            f"'{account_id}' trigger '{trigger.name}' "
                            f"no tasks → next in {secs:.1f}s"
                        )

            if to_remove:
                with self._lock:
                    for t in to_remove:
                        try:
                            entry._triggers.remove(t)
                        except ValueError:
                            pass

        return next_wakeup

    def _check_guards(self) -> None:
        with self._lock:
            entries  = dict(self._entries)
            active_p = {k: list(v) for k, v in self._active_professions.items()}

        for account_id, entry in entries.items():
            inv = entry.worker.bot.inventory

            if not entry.check_account_guard(inv):
                log.critical(f"'{account_id}' account guard=False → DEAD")
                entry.worker.bot.mark_dead("account guard failed")
                entry.worker.bot.store.save(inv)
                continue

            professions   = active_p.get(account_id, [])
            still_active: list[Profession] = []
            removed_any   = False

            for profession in professions:
                if not profession.check_guard(inv):
                    log.info(
                        f"'{account_id}' profession={profession.name!r} guard=False → знімаємо"
                    )
                    entry.remove_triggers_for_profession(profession)
                    removed_any = True
                else:
                    still_active.append(profession)

            if removed_any:
                with self._lock:
                    self._active_professions[account_id] = still_active
                if not still_active:
                    log.info(f"'{account_id}' всі профессії знялись → DEAD")
                    entry.worker.bot.mark_dead("all professions exhausted")
                    entry.worker.bot.store.save(inv)

    def _reap_dead_workers(self) -> None:
        with self._lock:
            entries = dict(self._entries)

        for account_id, entry in entries.items():
            if entry.worker.bot.status != AccountStatus.DEAD:
                continue
            entry.worker.stop()
            with self._lock:
                self._entries.pop(account_id, None)
                self._active_professions.pop(account_id, None)
            if self._on_dead:
                self._on_dead(entry.worker.bot)
            log.info(f"Removed dead worker '{account_id}'")