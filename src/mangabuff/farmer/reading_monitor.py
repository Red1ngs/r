"""
mangabuff/farmer/reading_monitor.py — ReadingMonitor.

Відповідальність:
    КОЛИ читати і З ЯКИМИ параметрами — це тут.
    ЩО САМЕ робити — в ReaderProfession через scheduler.ask().

Логіка:
    1. При attach — планує перший ask через _schedule_next().
    2. Слухає «loader.chapters_ready»       → прокидається достроково.
    3. Слухає «daily.claimed»               → прокидається достроково.
    4. Слухає «reader.chapters_exhausted»   → засинає (chapters_ready розбудить).

    Якщо daily-profession присутня і ще не зібрана сьогодні — ask не надсилається
    (бот чекає daily.claimed). Це запобігає читанню до отримання daily-бонусу.

ReadingParams зберігається в inventory акаунта і може змінюватись через
handle_request("set_reading_params") прямо під час роботи.

Монітор НЕ знає про слоти, нагороди, статистику.
Reader НЕ знає про інтервали і таймінги.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional

from src.core.monitoring.monitor import BaseMonitor
from src.core.logging.loggers import get_account_logger
from src.utils.time import today

if TYPE_CHECKING:
    from src.core.runtime.scheduler import EventDrivenScheduler


# ─────────────────────────────────────────────────────────────────────────────
# ReadingParams — що передаємо в reader при кожному ask
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ReadingParams:
    """
    Параметри одного циклу читання.

    Зберігаються в ReaderInventory.data["reading_params"] і можуть
    змінюватися через handle_request("set_reading_params") у ReaderProfession.
    """
    limit:        int                    = 2
    include_tags: Optional[list[str]]   = None
    exclude_tags: Optional[list[str]]   = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "limit":        self.limit,
            "include_tags": self.include_tags,
            "exclude_tags": self.exclude_tags,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ReadingParams":
        return cls(
            limit        = int(d.get("limit", 2)),
            include_tags = d.get("include_tags") or None,
            exclude_tags = d.get("exclude_tags") or None,
        )


# ─────────────────────────────────────────────────────────────────────────────
# ReadingMonitor
# ─────────────────────────────────────────────────────────────────────────────

class ReadingMonitor(BaseMonitor):
    """
    Монітор що вирішує КОЛИ надсилати ask("reader", "do_read", {...}).

    Інтервал між читаннями визначається активним reading_mode акаунта:
        • active_mode береться з ReaderInventory.active_mode (per-account).
        • Якщо порожній — використовується ReaderAppCfg.default_mode.
        • ReaderAppCfg.interval_for_mode(mode_name) знаходить перший
          reward_slot зі списку mode.slots і повертає його interval_seconds.
          Якщо жоден слот не знайдено — повертає mode.fallback_interval_s.

    Параметри читання (limit / include_tags / exclude_tags) беруться з
    ReaderInventory.reading_params і можуть мінятись під час роботи.

    Стан _sleeping:
        True  — глав немає, монітор чекає «loader.chapters_ready»
                щоб прокинутись. Планові ask не надсилаються.
        False — нормальна робота, ask надсилається за розкладом.

    Очікування daily:
        Якщо акаунт має profession "daily" і бонус ще не зібрано сьогодні —
        ask не надсилається. Монітор прокинеться на «daily.claimed».
    """

    @property
    def monitor_id(self) -> str:
        return "reading"

    def __init__(self) -> None:
        self._account_id: str                               = ""
        self._scheduler:  Optional["EventDrivenScheduler"] = None
        self._wakeup_task: Optional[asyncio.Task[None]]    = None
        self._sleeping:           bool                     = False
        self._slot_limit_reached: bool                     = False

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def attach(
        self,
        scheduler:  "EventDrivenScheduler",
        account_id: str,
    ) -> None:
        self._account_id = account_id
        self._scheduler  = scheduler

        scheduler.subscribe("loader.chapters_ready",      self._on_chapters_ready)
        scheduler.subscribe("daily.claimed",              self._on_daily_claimed)
        scheduler.subscribe("reader.chapters_exhausted",  self._on_chapters_exhausted)
        scheduler.subscribe("reader.reward_received", self._on_reward_received)
        scheduler.subscribe("reader.slot_limit_reached",  self._on_slot_limit_reached)  

        await self._schedule_next(delay=0.0)

    async def detach(
        self,
        scheduler:  "EventDrivenScheduler",
        account_id: str,
    ) -> None:
        self._cancel_wakeup()
        self._scheduler = None

    # ── Scheduling ────────────────────────────────────────────────────────────

    async def _schedule_next(self, delay: Optional[float] = None) -> None:
        self._cancel_wakeup()
        if self._scheduler is None:
            return
        if delay is None:
            delay = self._interval()
            if delay < 0:
                return
        loop = self._scheduler._async_loop
        if loop is None or not loop.is_running():
            return

        async def _fire() -> None:
            try:
                await asyncio.sleep(delay)
                await self._send_ask()
            except asyncio.CancelledError:
                # Нормальне скасування таски через _cancel_wakeup()
                pass
            except Exception as exc:
                get_account_logger(self._account_id).error(
                    f"[ReadingMonitor] Помилка у фоновому циклі: {exc}",
                    exc_info=True
                )

        self._wakeup_task = loop.create_task(_fire())

    def _cancel_wakeup(self) -> None:
        if self._wakeup_task and not self._wakeup_task.done():
            self._wakeup_task.cancel()
        self._wakeup_task = None

    def _interval(self) -> float:
        """
        Повертає інтервал між читаннями.
        Повертає -1.0 якщо всі слоти вичерпані (сигнал «не планувати»).
        """
        scheduler = self._scheduler
        if scheduler is None:
            return 5400.0

        bot = scheduler.get_bot(self._account_id)
        if bot is None:
            return 5400.0

        cfg = bot.app_config.reader
        inv = getattr(bot.inventory, "reader", None)
        mode_name = (inv.active_mode if inv is not None else "") or cfg.default_mode
        mode = cfg.get_mode(mode_name)
        log = get_account_logger(self._account_id)

        # mode без слотів → fallback без лімітів
        if not mode.slots:
            return mode.fallback_interval_s

        slot_counts = inv.slot_counts if inv is not None else {}
        slot = cfg.next_available_slot_for_mode(mode_name, slot_counts)

        if slot is not None:
            log.debug(
                f"[ReadingMonitor] slot={slot.name!r} "
                f"used={slot_counts.get(slot.name, 0)}/{slot.daily_limit} "
                f"interval={slot.interval_seconds}s"
            )
            return slot.interval_seconds

        # Всі слоти вичерпані
        log.info(
            f"[ReadingMonitor] всі слоти mode={mode_name!r} вичерпано → "
            f"emit reader.slot_limit_reached"
        )
        scheduler.emit_event(
            "reader.slot_limit_reached",
            {"account_id": self._account_id, "mode": mode_name, "counts": slot_counts},
            source=self._account_id,
        )
        return -1.0

    # ── Daily guard ───────────────────────────────────────────────────────────

    def _waiting_for_daily(self) -> bool:
        """
        True = треба чекати daily.claimed перед початком читання.
        False = можна читати (daily profession відсутня або бонус вже зібрано).

        Логіка:
          - Якщо profession "daily" не зареєстрована на цьому акаунті → False.
          - Якщо daily зібрано сьогодні (last_daily_claimed == today()) → False.
          - Інакше → True (чекаємо сигналу daily.claimed).
        """
        scheduler = self._scheduler
        if scheduler is None:
            return False

        if not scheduler.has_profession(self._account_id, "daily"):
            return False

        bot = scheduler.get_bot(self._account_id)
        if bot is None:
            return False

        daily_inv = getattr(bot.inventory, "daily", None)
        if daily_inv is None:
            return False

        return daily_inv.last_daily_claimed != today()

    # ── Ask ───────────────────────────────────────────────────────────────────

    async def _send_ask(self) -> None:
        """
        Надсилає ask до ReaderProfession з поточними ReadingParams.
        Після відповіді планує наступний цикл.
        """
        scheduler = self._scheduler
        if scheduler is None:
            return

        log = get_account_logger(self._account_id)

        if self._sleeping:
            log.debug("[ReadingMonitor] sleeping — пропускаємо ask")
            return
        
        if self._slot_limit_reached:
            log.info(
                "[ReadingMonitor] slot limit reached — "
                "чекаємо daily.claimed"
            )
            return

        if self._waiting_for_daily():
            log.info(
                "[ReadingMonitor] daily ще не зібрано — "
                "чекаємо daily.claimed (не плануємо наступний ask)"
            )
            # Не плануємо наступний цикл — прокинемось на daily.claimed
            return

        params = self._get_params()
        mode_name = self._active_mode_name()
        log.info(
            f"[ReadingMonitor] → ask reader do_read "
            f"mode={mode_name!r} "
            f"limit={params.limit} "
            f"include={params.include_tags} "
            f"exclude={params.exclude_tags}"
        )

        result = await scheduler.ask(
            account_id    = self._account_id,
            profession_id = "reader",
            intent        = "do_read",
            data          = params.to_dict(),
            caller        = "reading_monitor",
        )

        if not result.approved:
            log.warning(f"[ReadingMonitor] ask відхилено: {result.reason}")

        # Плануємо наступний цикл тільки якщо не переведено у sleeping
        if not self._sleeping and not self._slot_limit_reached:
            await self._schedule_next()

    def _get_params(self) -> ReadingParams:
        """Бере ReadingParams з inventory акаунта."""
        scheduler = self._scheduler
        if scheduler is None:
            return ReadingParams()
        bot = scheduler.get_bot(self._account_id)
        if bot is None:
            return ReadingParams()
        inv = getattr(bot.inventory, "reader", None)
        if inv is None:
            return ReadingParams()
        raw = inv.data.get("reading_params")
        if not raw:
            return ReadingParams()
        return ReadingParams.from_dict(raw)

    def _active_mode_name(self) -> str:
        """Повертає ім'я активного режиму для логування."""
        scheduler = self._scheduler
        if scheduler is None:
            return "unknown"
        bot = scheduler.get_bot(self._account_id)
        if bot is None:
            return "unknown"
        cfg = bot.app_config.reader
        inv = getattr(bot.inventory, "reader", None)
        return (inv.active_mode if inv is not None else "") or cfg.default_mode

    # ── Event handlers ────────────────────────────────────────────────────────

    async def _on_chapters_ready(self, payload: dict[str, Any]) -> None:
        """Broadcast від Loader — глави є, прокидаємось негайно."""
        if payload.get("account_id") != self._account_id:
            return
        
        if self._sleeping:
            get_account_logger(self._account_id).info(
                "[ReadingMonitor] loader.chapters_ready → виходимо зі sleeping"
            )
            self._sleeping = False
        else:
            get_account_logger(self._account_id).info(
                "[ReadingMonitor] loader.chapters_ready → достроковий ask"
            )
        await self._schedule_next(delay=0.0)

    async def _on_daily_claimed(self, payload: dict[str, Any]) -> None:
        if payload.get("account_id") != self._account_id:
            return
        log = get_account_logger(self._account_id)

        scheduler = self._scheduler
        if scheduler is not None:
            bot = scheduler.get_bot(self._account_id)
            if bot is not None:
                inv = getattr(bot.inventory, "reader", None)
                if inv is not None:
                    inv.reset_slot_counts()
                    log.info("[ReadingMonitor] daily.claimed → slot_counts скинуто")

        log.info("[ReadingMonitor] daily.claimed → достроковий ask")
        self._sleeping = False
        self._slot_limit_reached = False
        await self._schedule_next(delay=0.0)

    async def _on_chapters_exhausted(self, payload: dict[str, Any]) -> None:
        """Reader повідомив що глав немає — переходимо у sleeping."""
        if payload.get("account_id") != self._account_id:
            return
        get_account_logger(self._account_id).info(
            "[ReadingMonitor] reader.chapters_exhausted → sleeping "
            "(прокинемось на loader.chapters_ready)"
        )
        self._sleeping = True
        self._cancel_wakeup()
        
    async def _on_slot_limit_reached(self, payload: dict[str, Any]) -> None:
        """Всі слоти вичерпано — стоп до daily.claimed."""
        if payload.get("account_id") != self._account_id:
            return
        get_account_logger(self._account_id).info(
            f"[ReadingMonitor] slot_limit_reached mode={payload.get('mode')!r} → "
            f"зупиняємо читання до daily.claimed"
        )
        self._slot_limit_reached = True
        self._cancel_wakeup()
        
    async def _on_reward_received(self, payload: dict[str, Any]) -> None:
        """
        Reader отримав нагороду — оновлюємо лічильник слота.
        Якщо досягнуто daily_limit → emit reader.slot_limit_reached.
        """
        if payload.get("account_id") != self._account_id:
            return

        scheduler = self._scheduler
        if scheduler is None:
            return

        bot = scheduler.get_bot(self._account_id)
        if bot is None:
            return

        reward = payload.get("reward", {})
        cfg = bot.app_config.reader
        log = get_account_logger(self._account_id)

        slot = cfg.find_slot(reward)
        if slot is None:
            log.debug(f"[ReadingMonitor] reward={reward} — слот не знайдено")
            return

        inv = getattr(bot.inventory, "reader", None)
        if inv is None:
            return

        new_count = inv.increment_slot_count(slot.name)
        log.info(
            f"[ReadingMonitor] slot={slot.name!r} "
            f"count={new_count}/{slot.daily_limit}"
        )

        if new_count >= slot.daily_limit:
            log.info(
                f"[ReadingMonitor] slot={slot.name!r} досяг ліміту "
                f"{slot.daily_limit} → emit reader.slot_limit_reached"
            )
            await scheduler.emit_event_async(
                "reader.slot_limit_reached",
                {
                    "account_id":  self._account_id,
                    "slot":        slot.name,
                    "count":       new_count,
                    "daily_limit": slot.daily_limit,
                },
                source=self._account_id,
            )
            
        if reward.get("type") == "candy":
            result = await scheduler.ask(
                account_id    = self._account_id,
                profession_id = "reader",
                intent        = "claim_candy",
                data          =  reward,
                caller        = "reading_monitor",
            )

            if not result.approved:
                log.warning(f"[ReadingMonitor] ask відхилено: {result.reason}")