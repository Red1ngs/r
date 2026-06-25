"""
mangabuff/reader/reading_monitor.py — ReadingMonitor.

Відповідальність:
    КОЛИ читати і З ЯКИМИ параметрами.
    ЩО САМЕ робити — в ReaderProfession через scheduler.ask().

Правила монітора:
    • НЕ мутує inventory напряму.
    • НЕ викликає repo.inventory.save().
    • Усі зміни стану — через scheduler.ask() → RequestRouter → auto-save.

Потік при reward:
    do_read → emit reader.reward_received
           → _on_reward_received → ask("reader", "account_reward")
           → ReaderProfession._handle_account_reward оновлює inventory
           → RequestRouter auto-save
           → result.data → монітор вирішує emit slot_limit_reached

Логіка читання:
    1. attach  → _schedule_next(delay=0).
    2. loader.chapters_ready   → достроковий ask.
    3. daily.claimed           → reset флагів + новий ask.
    4. reader.chapters_exhausted → sleeping до chapters_ready.
    5. reader.slot_limit_reached → наступний слот або зупинка до daily.claimed.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional

from src.core.monitoring.monitor import BaseMonitor
from src.core.logging.loggers import get_account_logger

if TYPE_CHECKING:
    from src.core.runtime.scheduler import EventDrivenScheduler
    from src.core.config.app import RewardSlotCfg


# ─────────────────────────────────────────────────────────────────────────────
# ReadingParams
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ReadingParams:
    limit:        int                  = 2
    include_tags: Optional[list[str]] = None
    exclude_tags: Optional[list[str]] = None

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

    @property
    def monitor_id(self) -> str:
        return "reading"

    def __init__(self) -> None:
        self._account_id:         str                           = ""
        self._scheduler:          Optional["EventDrivenScheduler"] = None
        self._wakeup_task:        Optional[asyncio.Task[None]]  = None
        self._sleeping:           bool                          = False
        self._slot_limit_reached: bool                          = False

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def attach(self, scheduler: "EventDrivenScheduler", account_id: str) -> None:
        self._account_id = account_id
        self._scheduler  = scheduler

        scheduler.subscribe("loader.chapters_ready",     self._on_chapters_ready)
        scheduler.subscribe("daily.claimed",             self._on_daily_claimed)
        scheduler.subscribe("reader.chapters_exhausted", self._on_chapters_exhausted)
        scheduler.subscribe("reader.reward_received",    self._on_reward_received)
        scheduler.subscribe("reader.slot_limit_reached", self._on_slot_limit_reached)

        await self._schedule_next(delay=0.0)

    async def detach(self, scheduler: "EventDrivenScheduler", account_id: str) -> None:
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

        async def _fire() -> None:
            try:
                await asyncio.sleep(delay)
                await self._send_ask()
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                get_account_logger(self._account_id).error(
                    f"[ReadingMonitor] помилка у фоновому циклі: {exc}", exc_info=True
                )

        self._wakeup_task = asyncio.ensure_future(_fire())

    def _cancel_wakeup(self) -> None:
        if self._wakeup_task and not self._wakeup_task.done():
            self._wakeup_task.cancel()
        self._wakeup_task = None

    def _interval(self) -> float:
        """Інтервал для поточного слота. -1.0 = всі слоти вичерпані."""
        bot = self._bot()
        if bot is None:
            return 5400.0
        cfg  = bot.app_config.reader
        inv  = bot.inventory.reader
        
        mode_name = inv.active_mode or cfg.default_mode
        mode = cfg.get_mode(mode_name)

        if not mode.slots:
            return mode.fallback_interval_s

        slot = cfg.next_available_slot_for_mode(
            mode_name, inv.slot_counts, inv.slot_chapters_spent
        )
        if slot is not None:
            return slot.interval_seconds

        # Всі слоти вичерпані — emit і сигналізуємо «не планувати»
        get_account_logger(self._account_id).info(
            f"[ReadingMonitor] всі слоти mode={mode_name!r} вичерпано "
            f"→ emit reader.slot_limit_reached"
        )
        asyncio.ensure_future(self._scheduler.emit_event(
            "reader.slot_limit_reached",
            {"account_id": self._account_id, "mode": mode_name, "slot": None},
            source=self._account_id,
        ))
        return -1.0

    def _active_slot(self) -> Optional["RewardSlotCfg"]:
        bot = self._bot()
        if bot is None:
            return None
        cfg = bot.app_config.reader
        inv = bot.inventory.reader
        mode_name = inv.active_mode or cfg.default_mode
        return cfg.next_available_slot_for_mode(
            mode_name, inv.slot_counts, inv.slot_chapters_spent
        )

    def _bot(self):
        if self._scheduler is None:
            return None
        return self._scheduler.get_bot(self._account_id)

    # ── Daily guard ───────────────────────────────────────────────────────────

    def _waiting_for_daily(self) -> bool:
        scheduler = self._scheduler
        if scheduler is None:
            return False
        if not scheduler.has_profession(self._account_id, "daily"):
            return False
        bot = self._bot()
        if bot is None:
            return False
        daily_inv = bot.inventory.daily
        personal = bot.inventory.personal

        return daily_inv.last_daily_claimed != personal.to_day

    # ── Ask ───────────────────────────────────────────────────────────────────

    async def _send_ask(self) -> None:
        """
        Надсилає ask("reader", "do_read").
        Після відповіді — якщо не було reward — перевіряє ліміт по главах
        з result.data (inventory вже збережено RequestRouter).
        """
        scheduler = self._scheduler
        if scheduler is None:
            return

        log = get_account_logger(self._account_id)

        if self._sleeping:
            log.debug("[ReadingMonitor] sleeping — пропускаємо ask")
            return
        if self._slot_limit_reached:
            log.info("[ReadingMonitor] slot limit — чекаємо daily.claimed")
            return
        if self._waiting_for_daily():
            log.info("[ReadingMonitor] daily ще не зібрано → чекаємо daily.claimed")
            return

        params      = self._reading_params()
        active_slot = self._active_slot()

        log.info(
            f"[ReadingMonitor] → ask reader do_read "
            f"mode={self._active_mode_name()!r} "
            f"limit={params.limit} "
            f"include={params.include_tags} exclude={params.exclude_tags}"
        )

        ask_data = params.to_dict()
        ask_data["active_slot"] = active_slot.name if active_slot else None

        result = await scheduler.ask(
            account_id    = self._account_id,
            profession_id = "reader",
            intent        = "do_read",
            data          = ask_data,
            caller        = "reading_monitor",
        )

        if not result.approved:
            log.warning(f"[ReadingMonitor] do_read відхилено: {result.reason}")
            if not self._sleeping and not self._slot_limit_reached:
                await self._schedule_next()
            return

        data = result.data or {}

        # Якщо reward — ліміт перевірить _on_reward_received після ask("account_reward")
        if data.get("reward") or active_slot is None:
            if not self._sleeping and not self._slot_limit_reached:
                await self._schedule_next()
            return

        # Без reward — перевіряємо ліміт по главах з result.data
        spent_new = data.get("slot_chapters_spent")
        if spent_new is not None:
            cap = active_slot.max_chapters_per_slot
            log.info(
                f"[ReadingMonitor] slot={active_slot.name!r} "
                f"chapters_spent={spent_new}"
                + (f"/{cap}" if cap > 0 else "")
            )
            if cap > 0 and spent_new >= cap:
                log.info(
                    f"[ReadingMonitor] slot={active_slot.name!r} вичерпано по главах "
                    f"({spent_new}/{cap}) → emit reader.slot_limit_reached"
                )
                await scheduler.emit_event(
                    "reader.slot_limit_reached",
                    {
                        "account_id":  self._account_id,
                        "slot":        active_slot.name,
                        "daily_limit": active_slot.daily_limit,
                    },
                    source=self._account_id,
                )
                return  # _on_slot_limit_reached вирішить що далі

        if not self._sleeping and not self._slot_limit_reached:
            await self._schedule_next()

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _reading_params(self) -> ReadingParams:
        bot = self._bot()
        if bot is None:
            return ReadingParams()
        inv = getattr(bot.inventory, "reader", None)
        if inv is None:
            return ReadingParams()
        raw = inv.data.get("reading_params")
        return ReadingParams.from_dict(raw) if raw else ReadingParams()

    def _active_mode_name(self) -> str:
        bot = self._bot()
        if bot is None:
            return "unknown"
        cfg = bot.app_config.reader
        inv = getattr(bot.inventory, "reader", None)
        return (inv.active_mode if inv is not None else "") or cfg.default_mode

    # ── Event handlers ────────────────────────────────────────────────────────

    async def _on_chapters_ready(self, payload: dict[str, Any]) -> None:
        if payload.get("account_id") != self._account_id:
            return
        log = get_account_logger(self._account_id)
        if self._sleeping:
            log.info("[ReadingMonitor] loader.chapters_ready → виходимо зі sleeping")
            self._sleeping = False
        else:
            log.info("[ReadingMonitor] loader.chapters_ready → достроковий ask")
        await self._schedule_next(delay=0.0)

    async def _on_daily_claimed(self, payload: dict[str, Any]) -> None:
        if payload.get("account_id") != self._account_id:
            return
        log = get_account_logger(self._account_id)
        self._sleeping           = False
        self._slot_limit_reached = False
        delay = max(self._interval(), 0.0)
        log.info(f"[ReadingMonitor] daily.claimed → наступний ask через {delay:.0f}s")
        await self._schedule_next(delay=delay)

    async def _on_chapters_exhausted(self, payload: dict[str, Any]) -> None:
        if payload.get("account_id") != self._account_id:
            return
        get_account_logger(self._account_id).info(
            "[ReadingMonitor] reader.chapters_exhausted → sleeping"
        )
        self._sleeping = True
        self._cancel_wakeup()

    async def _on_slot_limit_reached(self, payload: dict[str, Any]) -> None:
        if payload.get("account_id") != self._account_id:
            return
        log  = get_account_logger(self._account_id)
        next_slot = self._active_slot()
        if next_slot is not None:
            log.info(
                f"[ReadingMonitor] slot={payload.get('slot')!r} вичерпано "
                f"→ переходимо на slot={next_slot.name!r}"
            )
            await self._schedule_next()
        else:
            log.info(
                f"[ReadingMonitor] slot={payload.get('slot')!r} вичерпано, "
                f"всі слоти закриті → зупиняємо до daily.claimed"
            )
            self._slot_limit_reached = True
            self._cancel_wakeup()

    async def _on_reward_received(self, payload: dict[str, Any]) -> None:
        """
        Монітор НЕ мутує inventory. Делегує в ReaderProfession через ask("account_reward").
        RequestRouter зробить auto-save після approve.
        Після отримання result.data — вирішує чи потрібен emit slot_limit_reached.
        """
        if payload.get("account_id") != self._account_id:
            return

        scheduler = self._scheduler
        if scheduler is None:
            return

        log = get_account_logger(self._account_id)

        result = await scheduler.ask(
            account_id    = self._account_id,
            profession_id = "reader",
            intent        = "account_reward",
            data          = {
                "reward":        payload.get("reward", {}),
                "chapters_read": payload.get("chapters_read", 0),
                "active_slot":   payload.get("active_slot"),
            },
            caller = "reading_monitor",
        )
        
        if result.data.get("token", False):
            await scheduler.ask(
                account_id    = self._account_id,
                profession_id = "reader",
                intent        = "claim_candy",
                data          = {
                    "token":    result.data["token"],
                },
                caller = "reading_monitor",
            )

        if not result.approved:
            log.warning(f"[ReadingMonitor] account_reward відхилено: {result.reason}")
            if not self._sleeping and not self._slot_limit_reached:
                await self._schedule_next()
            return

        data       = result.data or {}
        slot_name  = data.get("slot")
        new_count  = data.get("new_count", 0)
        new_spent  = data.get("new_spent", 0)
        cap        = data.get("cap", 0)
        daily_lim  = data.get("daily_limit", 0)

        if slot_name is None:
            if not self._sleeping and not self._slot_limit_reached:
                await self._schedule_next()
            return

        # Перевіряємо ліміт по главах
        if cap > 0 and new_spent >= cap:
            log.info(
                f"[ReadingMonitor] slot={slot_name!r} вичерпано по главах "
                f"({new_spent}/{cap}) → emit reader.slot_limit_reached"
            )
            await scheduler.emit_event(
                "reader.slot_limit_reached",
                {"account_id": self._account_id, "slot": slot_name,
                 "count": new_count, "daily_limit": daily_lim},
                source=self._account_id,
            )
            return

        # Перевіряємо ліміт по нагородах
        if new_count >= daily_lim:
            log.info(
                f"[ReadingMonitor] slot={slot_name!r} досяг ліміту "
                f"({new_count}/{daily_lim}) → emit reader.slot_limit_reached"
            )
            await scheduler.emit_event(
                "reader.slot_limit_reached",
                {"account_id": self._account_id, "slot": slot_name,
                 "count": new_count, "daily_limit": daily_lim},
                source=self._account_id,
            )
            return

        if not self._sleeping and not self._slot_limit_reached:
            await self._schedule_next()