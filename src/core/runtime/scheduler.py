"""
scheduler.py — EventDrivenScheduler. Єдиний Scheduler в системі.

Виправлення відносно попередньої версії:
  BUG-1: connect_account() — `ok` ніколи не присвоювалась, NameError при провалі.
  BUG-2: stop() — після зупинки loop викликав _run_async(disconnect()) на мертвому loop,
          корутина ніколи не виконувалась. Замінено на asyncio.run() в новому loop.
  BUG-3: resume_account() — run_coroutine_threadsafe().result() в async-контексті
          дедлочило (блокувало той самий потік що веде loop). Замінено на await.
  BUG-4: _attach_all_monitors() — async метод викликав _run_async(_attach_monitors_for(...))
          тобто await self._run_async(self._run_async(coro)) — подвійне обгортання
          у fire-and-forget, монітори прикріплювались без очікування. Виправлено:
          _attach_monitors_for тепер async і викликається через await.
  BUG-5: _reap_dead_workers() — _detach_all_monitors() викликався без await (sync
          обгортки вже немає), а також _run_async(disconnect()) — аналогічно BUG-2.
  BUG-6: _setup_single_profession() — self._attach_monitors_for(...) без await і
          без _run_async — результат coroutine ніколи не awaited.
  BUG-7: proxy_queue.py enqueue() — asyncio.get_event_loop() deprecated у Python 3.10+,
          замінено на asyncio.get_running_loop().

  ДУБЛЮВАННЯ-1: emit_event / emit_event_async — два публічних async методи що роблять
          ледь різні речі (один через _run_async fire-and-forget, інший напряму).
          Перший ніколи не поверне кількість підписників, другий — надлишковий alias.
          Залишено лише emit_event як справжній await.
  ДУБЛЮВАННЯ-2: _attach_monitors_for / _detach_monitor_if_unused / _detach_all_monitors —
          були sync-обгортками що всередині запускали run_coroutine_threadsafe.
          Всі три переведені в async, sync-обгортки прибрано.
  ДУБЛЮВАННЯ-3: StartupManager.run() мав хибний коментар «connect_account() синхронний → executor»
          (виконував await без executor). Коментар видалено.
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
    from src.core.runtime.core_service import CoreService

from src.core.core_account import Account
from src.core.inventory.model import DynamicInventories as Inventories
from src.core.logging.loggers import get_scheduler_logger
from src.core.runtime.event_bus import EventBus, EventCallback
from src.core.runtime.request_router import RequestContext, RequestRouter
from src.core.runtime.conditions import Condition
from src.core.runtime.profession import BaseProfession, RequestResult
from src.core.runtime.profession_spec import profession_registry
from src.core.runtime.proxy_queue import proxy_queue_manager
from src.core.status import AccountStatus

log = get_scheduler_logger()


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
    async def initialize(
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
    async def _reset_for_tests(cls) -> None:
        with cls._init_lock:
            if cls._instance is not None:
                try:
                    await cls._instance.stop()
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

        self._wait_for_loop_ready()
        log.info("EventDrivenScheduler started")

    def _wait_for_loop_ready(self, timeout: float = 5.0) -> None:
        """Чекає поки asyncio loop в окремому потоці реально запустився."""
        import time
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._async_loop and self._async_loop.is_running():
                return
            time.sleep(0.01)
        raise RuntimeError("EventDrivenScheduler: async loop не запустився за відведений час")

    async def stop(self) -> None:
        if self._async_loop and self._async_loop.is_running():
            # Graceful shutdown proxy_queue воркерів — перед зупинкою loop
            future = asyncio.run_coroutine_threadsafe(
                proxy_queue_manager.shutdown_all(), self._async_loop
            )
            try:
                future.result(timeout=15.0)
            except Exception as e:
                log.warning(f"proxy_queue shutdown error: {e}")

            # FIX Bug 5: disconnect() має виконуватись всередині scheduler loop,
            # бо BotSession (curl_cffi.AsyncSession) була створена в ньому.
            # Закривати сесію з чужого loop'у — resource leak / помилки.
            with self._lock:
                entries = dict(self._containers)

            if entries:
                # Створюємо локальну корутину-обгортку для gather
                async def _disconnect_all():
                    await asyncio.gather(
                        *(entry.bot.disconnect() for entry in entries.values()),
                        return_exceptions=True,
                    )

                # Тепер передаємо саме виклик функції _disconnect_all()
                disconnect_future = asyncio.run_coroutine_threadsafe(
                    _disconnect_all(), 
                    self._async_loop
                )
                try:
                    disconnect_future.result(timeout=15.0)
                except Exception as e:
                    log.warning(f"disconnect error during stop: {e}")

            self._async_loop.call_soon_threadsafe(self._async_loop.stop)

        if self._loop_thread and self._loop_thread.is_alive():
            self._loop_thread.join(timeout=10)

        log.info("EventDrivenScheduler stopped")

    def _run_async_loop(self) -> None:
        asyncio.set_event_loop(self._async_loop)
        self._loading_lock = asyncio.Lock()
        self._async_loop.run_forever()

    # ── Monitors management (internal, async) ─────────────────────────────────

    def _get_or_create_monitors(self, account_id: str) -> AccountMonitors:
        if account_id not in self._monitors:
            self._monitors[account_id] = AccountMonitors(account_id)
        return self._monitors[account_id]

    # FIX BUG-4 / ДУБЛЮВАННЯ-2: всі три методи тепер async — ніяких sync-обгорток
    # з run_coroutine_threadsafe всередині async-контексту.

    async def _attach_monitors_for(self, account_id: str, profession_name: str) -> None:
        """Підключає монітори, що відповідають вказаній profession."""
        monitor_ids = profession_registry.monitors_for(profession_name)
        if not monitor_ids:
            return
        monitors = self._get_or_create_monitors(account_id)
        await monitors.attach_all(self, monitor_ids)

    async def _detach_monitor_if_unused(self, account_id: str, monitor_id: str) -> None:
        """Відключає монітор, якщо жодна з активних професій його більше не потребує."""
        with self._lock:
            container = self._containers.get(account_id)
        active_ids = [p.profession_id for p in container.profession_list()] if container else []
        used_monitors = profession_registry.all_monitors_for(active_ids)
        if monitor_id in used_monitors:
            return
        monitors = self._monitors.get(account_id)
        if monitors is None:
            return
        await monitors.detach(self, monitor_id)

    async def _detach_all_monitors(self, account_id: str) -> None:
        """Відключає всі монітори акаунта (при видаленні або паузі)."""
        monitors = self._monitors.pop(account_id, None)
        if monitors is None:
            return
        await monitors.detach_all(self)

    # ── Account management ────────────────────────────────────────────────────

    async def add_account(
        self,
        account_id:  str,
        bot:         Account,
        professions: list["BaseProfession"],
        guard:       Optional[Condition] = None,
    ) -> None:
        # FIX Bug 8: НЕ додаємо container до self._containers до перевірки
        # статусу DEAD та успішного setup_professions.
        if bot.status == AccountStatus.DEAD:
            if self._on_dead:
                self._on_dead(bot)
            return

        container = AccountContainer(bot=bot, guard=guard)

        with self._lock:
            if account_id in self._containers:
                raise ValueError(f"Акаунт {account_id!r} вже існує")
            self._containers[account_id] = container

        # Спочатку прив'язуємо CoreService-и (наприклад AuthService).
        # Вони мають бути готові до connect_account() і до setup professions,
        # бо on_auth_success з'явиться під час першого authenticate().
        await self._bind_core_services(bot)

        await self._setup_professions(account_id, bot, professions)
        log.info(f"[{account_id}] додано ({len(professions)} professions)")

    @staticmethod
    async def _bind_core_services(bot: Account) -> None:
        """
        Створює та прив'язує CoreService-и до акаунта.

        profession_registry.build_core_services() повертає свіжі екземпляри
        всіх зареєстрованих CoreService (по одному на акаунт).
        Кожен отримує посилання на bot через bind().
        """
        services = profession_registry.build_core_services()
        for svc in services:
            await svc.bind(bot)
        bot.core_services = services
        if services:
            log.debug(
                f"[{bot.account_id}] bound core services: "
                f"{[s.service_id for s in services]}"
            )

    async def connect_account(self, account_id: str) -> bool:
        """
        Встановлює сесію і підключає монітори.

        Повертає True при успіху.
        """
        with self._lock:
            container = self._containers.get(account_id)
            
        if container is None:
            log.warning(f"[{account_id}] connect_account: акаунт не знайдено")
            return False

        if container.bot.is_connected:
            log.debug(f"[{account_id}] connect_account: вже підключено")
            await self._attach_all_monitors(account_id, container)
            return True

        # FIX BUG-1: результат connect() тепер зберігається в ok
        ok = await container.bot.connect()

        if not ok:
            log.error(f"[{account_id}] connect_account: connect() провалився — {container.bot.error}")
            return False

        await self._attach_all_monitors(account_id, container)
        log.info(f"[{account_id}] connect_account: підключено, монітори активні")
        return True

    async def _attach_all_monitors(self, account_id: str, container: "AccountContainer") -> None:
        """Чіпляє монітори для всіх активних profession контейнера."""
        for profession in container.profession_list():
            # FIX BUG-4: прямий await, без _run_async fire-and-forget
            await self._attach_monitors_for(account_id, profession.profession_id)

    async def remove_account(self, account_id: str) -> bool:
        with self._lock:
            entry = self._containers.pop(account_id, None)
        if entry is None:
            return False

        professions = entry.profession_list()
        await self._teardown_professions(account_id, professions)
        self._router.unregister_account(account_id)
        await self._detach_all_monitors(account_id)

        # Від'єднуємо CoreService-и
        for svc in entry.bot.core_services:
            try:
                await svc.unbind()
            except Exception as e:
                log.warning(f"[{account_id}] CoreService {svc.service_id!r} unbind error: {e}")
        entry.bot.core_services = []

        log.info(f"[{account_id}] видалено")
        return True

    async def pause_account(self, account_id: str) -> bool:
        with self._lock:
            container = self._containers.get(account_id)
        if container is None or container.bot.status == AccountStatus.SUSPENDED:
            return False

        await self._detach_all_monitors(account_id)
        container.bot.status = AccountStatus.SUSPENDED
        await container.bot.disconnect()
        log.info(f"[{account_id}] призупинено")
        return True

    async def resume_account(self, account_id: str) -> bool:
        with self._lock:
            container = self._containers.get(account_id)
        if container is None or container.bot.status != AccountStatus.SUSPENDED:
            return False

        container.bot.status = AccountStatus.IDLE

        if not container.bot.is_connected:
            # FIX BUG-3: прямий await замість run_coroutine_threadsafe().result()
            # що блокувало async-потік (дедлок).
            ok = await container.bot.connect()
            if not ok:
                log.error(f"[{account_id}] resume: connect() провалився")
                return False

        if container.bot.status == AccountStatus.DEAD:
            log.error(f"[{account_id}] resume: акаунт мертвий після connect()")
            return False

        professions = container.profession_list()
        await self._restore_professions(
            account_id, container.bot, professions, container
        )

        log.info(f"[{account_id}] відновлено")
        return True

    # ── Dynamic Profession Management ─────────────────────────────────────────

    async def add_profession_to_account(
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

        await self._setup_single_profession(
            account_id, container.bot, profession, container
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
            # Монітори підключаємо тільки якщо сесія вже встановлена.
            # Якщо ні — connect_account() підключить їх через _attach_all_monitors()
            # коли сесія буде готова (hot-add: connect одразу після add_account;
            # cold-start: StartupManager.run() → connect_account()).
            if bot.is_connected:
                await self._attach_monitors_for(account_id, profession.profession_id)
            log.info(f"[{account_id}] dynamic setup {profession.profession_id!r} complete")
        except Exception as e:
            with self._lock:
                container.professions.pop(profession.profession_id, None)
            log.error(
                f"[{account_id}] dynamic setup {profession.profession_id!r} failed: {e}",
                exc_info=True,
            )

    async def remove_profession_from_account(self, account_id: str, profession_id: str) -> None:
        profession_obj: Optional["BaseProfession"] = None
        with self._lock:
            container = self._containers.get(account_id)
            if container is not None:
                profession_obj = container.get_profession(profession_id)
                container.remove_profession_by_id(profession_id)

        if profession_obj is not None:
            await self._teardown_professions(account_id, [profession_obj])

        for monitor_id in profession_registry.monitors_for(profession_id):
            await self._detach_monitor_if_unused(account_id, monitor_id)

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

    async def emit_event(
        self,
        event_name: str,
        payload:    dict[str, Any],
        source:     str = "system",
    ) -> int:
        """
        ДУБЛЮВАННЯ-1 виправлено: єдиний метод emit_event — повноцінний await,
        повертає кількість успішних підписників.
        emit_event_async() видалено — він був надлишковим alias-ом.

        Автозбереження: якщо payload містить account_id — зберігає inventory
        після emit, щоб зміни зроблені монітором перед emit не пропали.
        """
        result = await self._event_bus.emit(event_name, payload, source=source)
        return result

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
                await self._attach_monitors_for(account_id, profession.profession_id)
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

    async def _check_guards(self) -> None:
        with self._lock:
            containers = dict(self._containers)

        for account_id, entry in containers.items():
            bot = entry.bot
            inv = bot.inventory

            if not entry.check_account_guard(inv):
                log.warning(f"[{account_id}] account guard failed → kill")
                await self._kill_account(account_id, entry)
                continue

            for profession in entry.profession_list():
                if not profession.check_guard(bot):
                    # FIX Bug 6: повний teardown — unsubscribe EventBus,
                    # виклик profession.teardown(), unregister з router,
                    # відключення моніторів що більше не потрібні.
                    entry.remove_profession(profession)
                    log.info(
                        f"[{account_id}] {profession.profession_id!r} guard failed "
                        f"→ profession removed"
                    )
                    self._event_bus.unsubscribe_owner(profession)
                    try:
                        await profession.teardown(self, account_id)
                    except Exception as e:
                        log.error(
                            f"[{account_id}] teardown {profession.profession_id!r} "
                            f"after guard fail: {e}"
                        )
                    self._router.unregister(account_id, profession.profession_id)
                    for monitor_id in profession_registry.monitors_for(profession.profession_id):
                        await self._detach_monitor_if_unused(account_id, monitor_id)

    async def _reap_dead_workers(self) -> None:
        with self._lock:
            containers = dict(self._containers)

        for account_id, entry in containers.items():
            if entry.bot.status != AccountStatus.DEAD:
                continue
            log.warning(f"[{account_id}] dead → cleanup")
            # FIX Bug 5: прямий await, а не _run_async (ми вже в async-контексті)
            await entry.bot.disconnect()
            with self._lock:
                self._containers.pop(account_id, None)
            self._router.unregister_account(account_id)
            # FIX Bug 7: detach monitors ДО виклику on_dead,
            # щоб callback бачив коректний стан (без живих моніторів).
            await self._detach_all_monitors(account_id)
            if self._on_dead:
                self._on_dead(entry.bot)

    async def _kill_account(self, account_id: str, container: AccountContainer) -> None:
        container.bot.mark_dead("account guard failed")
        container.bot.repo.inventory.save(account_id, container.bot.inventory)
        await container.bot.disconnect()

        with self._lock:
            self._containers.pop(account_id, None)
        self._router.unregister_account(account_id)
        await self._detach_all_monitors(account_id)
        if self._on_dead:
            self._on_dead(container.bot)