from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional, cast

from src.core.monitoring.monitor import BaseMonitor
from src.core.logging.loggers import get_account_logger

if TYPE_CHECKING:
    from src.core.runtime.scheduler import EventDrivenScheduler
    from src.core.core_account import Account
    
    
# ─────────────────────────────────────────────────────────────────────────────
# MiningParams & PurchasePlan
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class MiningParams:
    exchange: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "exchange": self.exchange,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "MiningParams":
        return cls(
            exchange = bool(d.get("exchange", True))
        )


@dataclass
class PurchasePlan:
    """Описує заплановану покупку або обмін."""
    intent: str          # "upgrade_pickaxe", "buy_power" або "exchange_diamonds"
    name: str            # Зрозуміла назва для логування
    cost: int            # Загальна вартість у руді
    quantity: int = 1    # Кількість для купівлі
    has_enough: bool = False  # Чи вистачає руди на цю покупку


class MiningMonitor(BaseMonitor):
    """
    Монітор Шахти. Веде повний цикл mine_hit для одного акаунта.

    Не містить IO — тільки планування ask().
    Стан (hits_count, )
    читається з MiningInventory при кожному циклі.
    """

    @property
    def monitor_id(self) -> str:
        return "mining"

    def __init__(self) -> None:
        self._account_id:         str                              = ""
        self._scheduler:          Optional["EventDrivenScheduler"] = None
        self._bot:                Optional[Account]                = None
        self._wakeup_task:        Optional[asyncio.Task[None]]     = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def attach(self, scheduler: "EventDrivenScheduler", account_id: str) -> None:
        self._account_id = account_id
        self._scheduler  = scheduler
        self._bot        = scheduler.get_bot(account_id)
        
        scheduler.subscribe("daily.claimed", self._on_daily_claimed)
        scheduler.subscribe("mining.mining_complete", self._on_mining_complete)

        await self._start_mining()

    async def detach(self, scheduler: "EventDrivenScheduler", account_id: str) -> None:
        self._cancel_wakeup()
        self._scheduler = None

    # ── Scheduling ────────────────────────────────────────────────────────────

    async def _schedule_next(self, delay: Optional[float] = None) -> None:
        self._cancel_wakeup()
        if self._scheduler is None:
            return
        
        if delay is None:
            delay = await self._interval()
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
                    f"[MiningMonitor] помилка у фоновому циклі: {exc}", exc_info=True
                )

        self._wakeup_task = asyncio.ensure_future(_fire())

    def _cancel_wakeup(self) -> None:
        if self._wakeup_task and not self._wakeup_task.done():
            self._wakeup_task.cancel()
        self._wakeup_task = None

    async def _interval(self) -> float:
        bot = self._bot
        
        if bot is None:
            raise ValueError("Account не доступний")
        
        cfg  = bot.app_config.mining
        
        if not await self._mining_complete():
            return cfg.delay

        return -1.0

    # ── Daily guard ───────────────────────────────────────────────────────────

    def _waiting_for_daily(self) -> bool:
        bot = self._bot

        if self._scheduler is None:
            raise ValueError("Scheduler не доступний")
        
        if bot is None:
            raise ValueError("Account не доступний")
        
        if not self._scheduler.has_profession(self._account_id, "daily"):
            return False
        
        daily_inv = bot.inventory.daily
        personal = bot.inventory.personal

        return daily_inv.last_daily_claimed != personal.to_day

    # ── Ask ───────────────────────────────────────────────────────────────────
    
    async def _start_mining(self) -> None:
        bot = self._bot
        log = get_account_logger(self._account_id)
        
        scheduler = self._scheduler
        if scheduler is None:
            return
        
        if bot is None:
            raise ValueError("Account не доступний")
        
        result = await scheduler.ask(
            account_id    = self._account_id,
            profession_id = "mining",
            intent        = "start_mining",
            caller        = "mining_monitor",
        )
        
        if not result.approved:
            log.warning(f"[MiningMonitor] mining_hit відхилено: {result.reason}")
            return
        
        result_data = result.data
        self._update_inventory(result_data)
        
        await self._schedule_next()

    def _get_purchase_plan(self) -> Optional[PurchasePlan]:
        """
        Аналізує інвентар та визначає першочергову потребу в покупці/обміні.
        Повертає PurchasePlan або None, якщо купувати нічого не потрібно.
        """
        bot = self._bot
        if bot is None:
            return None

        inv = bot.inventory.mining
        ore = int(inv.ore)

        # 1. Покращення кирки
        if not inv.upgrade_max and inv.upgrade_cost is not None:
            return PurchasePlan(
                intent="upgrade_pickaxe",
                name="покращення кирки",
                cost=inv.upgrade_cost,
                quantity=1,
                has_enough=(ore >= inv.upgrade_cost)
            )

        # 2. Сильний удар
        if not inv.power_bought and inv.power_cost is not None:
            return PurchasePlan(
                intent="buy_power",
                name="сильний удар",
                cost=inv.power_cost,
                quantity=1,
                has_enough=(ore >= inv.power_cost)
            )

        # 3. Обмін на алмази
        if inv.exchange_diamonds_get and inv.exchange_diamonds_get > 0 and inv.exchange_ore_cost:
            cost_of_one = inv.exchange_ore_cost // inv.exchange_diamonds_get
            to_buy = ore // cost_of_one
            to_buy = min(to_buy, inv.exchange_diamonds_get)

            if to_buy > 0:
                return PurchasePlan(
                    intent="exchange_diamonds",
                    name="обмін на алмази",
                    cost=to_buy * cost_of_one,
                    quantity=to_buy,
                    has_enough=True
                )
            else:
                # Навіть на 1 алмаз не вистачає руди
                return PurchasePlan(
                    intent="exchange_diamonds",
                    name="обмін на алмази",
                    cost=cost_of_one,
                    quantity=1,
                    has_enough=False
                )

        return None
        
    async def _sale_ore(self) -> bool:
        bot = self._bot

        if self._scheduler is None:
            raise ValueError("Scheduler не доступний")
        if bot is None:
            raise ValueError("Account не доступний")
        
        log = get_account_logger(self._account_id)
        inv = bot.inventory.mining

        # Крок 1. Перевіряємо умови: чи треба щось купувати, скільки та яка вартість
        plan = self._get_purchase_plan()

        # Крок 2. Виконуємо дію на основі сформованого плану
        if plan is not None:
            if not plan.has_enough:
                log.info(
                    f"[MiningMonitor] Не вистачає руди на {plan.name}. "
                    f"Потрібно мінімум: {plan.cost}, є: {inv.ore}"
                )
            else:
                await self._execute_purchase(plan)
        else:
            log.info("[MiningMonitor] Всі завдання виконано, вільних алмазів для обміну немає.")

        return getattr(inv, "needs_upgrade", False)

    async def _execute_purchase(self, plan: PurchasePlan) -> None:
        """Викликає відповідний метод для здійснення покупки відповідно до плану."""
        if plan.intent == "upgrade_pickaxe":
            await self._upgrade_pickaxe()
        elif plan.intent == "buy_power":
            await self._buy_power()
        elif plan.intent == "exchange_diamonds":
            await self._exchange_diamonds(plan.quantity)
        
    async def _upgrade_pickaxe(self) -> None:
        scheduler = self._scheduler
        if scheduler is None:
            return
        
        log = get_account_logger(self._account_id)
        log.info("[MiningMonitor] → ask upgrade_pickaxe")
        
        result = await scheduler.ask(
            account_id    = self._account_id,
            profession_id = "mining",
            intent        = "upgrade_pickaxe",
            caller        = "mining_monitor",
        )
        
        if result.approved:
            log.info("[MiningMonitor] Кирку успішно покращено.")
            self._update_inventory(result.data)
        else:
            log.warning(f"[MiningMonitor] Відхилено покращення кирки: {result.reason}")
        
    async def _buy_power(self) -> None:
        scheduler = self._scheduler
        if scheduler is None:
            return
        
        log = get_account_logger(self._account_id)
        log.info("[MiningMonitor] → ask buy_power")
        
        result = await scheduler.ask(
            account_id    = self._account_id,
            profession_id = "mining",
            intent        = "buy_power",
            caller        = "mining_monitor",
        )
        
        if result.approved:
            log.info("[MiningMonitor] Сильний удар успішно куплено.")
            self._update_inventory(result.data)
        else:
            log.warning(f"[MiningMonitor] Відхилено купівлю сильного удару: {result.reason}")

    async def _exchange_diamonds(self, to_buy: int) -> None:
        scheduler = self._scheduler
        bot = self._bot
        if scheduler is None or bot is None:
            return
        
        log = get_account_logger(self._account_id)
        inv = bot.inventory.mining
        
        ore = int(inv.ore)
        exchange_ore_cost = inv.exchange_ore_cost
        exchange_diamonds_get = inv.exchange_diamonds_get
        
        if not (exchange_diamonds_get and exchange_diamonds_get > 0 and exchange_ore_cost):
            return

        cost_of_one = exchange_ore_cost // exchange_diamonds_get

        log.info(
            f"[MiningMonitor] → ask exchange_diamonds: купуємо {to_buy} 💎 "
            f"за {to_buy * cost_of_one} руди (залишок руди: {ore})"
        )
        
        result = await scheduler.ask(
            account_id    = self._account_id,
            profession_id = "mining",
            intent        = "exchange_diamonds",
            caller        = "mining_monitor",
            data          = {"diamonds": to_buy}, 
        )
        
        if result.approved:
            log.info(f"[MiningMonitor] Успішно обміняно руду на {to_buy} 💎.")
            self._update_inventory(result.data)
        else:
            log.warning(f"[MiningMonitor] Відхилено обмін руди: {result.reason}")

    async def _send_ask(self) -> None:
        """
        Надсилає ask("mining", "mining_hit") для здійснення чергового удару в шахті.
        
        Після отримання відповіді:
          - Якщо запит схвалено, перевіряє залишок ударів. Якщо ліміт не вичерпано,
            планує наступний удар через _schedule_next().
          - Якщо шахту повністю розроблено або виникла помилка, зупиняє цикл 
            до настання події daily.claimed.
        """
        scheduler = self._scheduler
        if scheduler is None:
            return

        log = get_account_logger(self._account_id)

        await self._sale_ore()
        
        if await self._mining_complete():
            log.info("[MiningMonitor] Шахта видобута — чекаємо daily.claimed")
            return
        
        if self._waiting_for_daily():
            log.info("[MiningMonitor] daily ще не зібрано → чекаємо daily.claimed")
            return

        params = self._mining_params()

        log.info(
            f"[MiningMonitor] → ask mining_hit "
            f"exchange={params.exchange!r}"
        )
        
        result = await scheduler.ask(
            account_id    = self._account_id,
            profession_id = "mining",
            intent        = "mining_hit",
            caller        = "mining_monitor",
        )
        
        result_data = result.data
        self._update_inventory(result_data)

        if not result.approved:
            log.warning(f"[MiningMonitor] mining_hit відхилено: {result.reason}")
            if not await self._mining_complete():
                await self._schedule_next()
            return

        if not await self._mining_complete():
            await self._schedule_next()

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _mining_params(self) -> MiningParams:
        bot = self._bot

        if bot is None:
            return MiningParams()
        
        inv = bot.inventory.mining
        raw = inv.mining_params
        return MiningParams.from_dict(raw) if raw else MiningParams()

    async def _mining_complete(self) -> bool:
        bot = self._bot

        if self._scheduler is None:
            raise ValueError("Scheduler не доступний")
        
        if bot is None:
            raise ValueError("Account не доступний")
        
        inv = bot.inventory.mining
        
        hits_left = inv.hits_left
        # Якщо hits_left ще не завантажено (None), орієнтуємося на збережений стан
        if hits_left is None:
            return inv.mining_complete
        
        hits_left = int(hits_left)
        
        if hits_left > 0:
            inv.mining_complete = False
            return False
        elif hits_left == 0:
            if not inv.mining_complete:
                inv.mining_complete = True
                await self._scheduler.emit_event(
                    "mining.mining_complete",
                    {"account_id": self._account_id},
                    source=self._account_id,
                )
            return True
        else:
            raise ValueError(f"hits_left не може бути {hits_left}")
    
    def _update_inventory(self, data: dict[str, Any]) -> None:
        """Оновлює локальний інвентар шахти на основі отриманих від ask() даних."""
        bot = self._bot
        if bot is None or not data:
            return
        
        inv = bot.inventory.mining
        
        # Оновлюємо лише ті параметри, які повернув планувальник
        if "hits_left" in data:
            inv.hits_left = cast(int, data["hits_left"])
        if "max_hits" in data:
            inv.max_hits = cast(int, data["max_hits"])
        if "ore" in data:
            inv.ore = cast(int, data["ore"])
        if "upgrade_cost" in data:
            inv.upgrade_cost = cast(Optional[int], data["upgrade_cost"])
        if "upgrade_max" in data:
            inv.upgrade_max = cast(bool, data["upgrade_max"])
        if "power_cost" in data:
            inv.power_cost = cast(Optional[int], data["power_cost"])
        if "power_bought" in data:
            inv.power_bought = cast(bool, data["power_bought"])
        if "exchange_ore_cost" in data:
            inv.exchange_ore_cost = cast(Optional[int], data["exchange_ore_cost"])
        if "exchange_diamonds_get" in data:
            inv.exchange_diamonds_get = cast(Optional[int], data["exchange_diamonds_get"])
        
        # Також оновлюємо прапорець завершення
        if inv.hits_left is not None:
            inv.mining_complete = (inv.hits_left == 0)
    
    
    # ── Event handlers ────────────────────────────────────────────────────────

    async def _on_daily_claimed(self, payload: dict[str, Any]) -> None:
        log = get_account_logger(self._account_id)
        if payload.get("account_id") != self._account_id:
            return    
        
        delay = max(await self._interval(), 0.0)
        log.info(f"[MiningMonitor] daily.claimed → наступний ask через {delay:.0f}s")
        await self._schedule_next(delay=delay)
        
    async def _on_mining_complete(self, payload: dict[str, Any]) -> None:
        log = get_account_logger(self._account_id)
        if payload.get("account_id") != self._account_id:
            return
        
        bot = self._bot
        
        scheduler = self._scheduler
        if scheduler is None:
            return
        
        if bot is None:
            raise ValueError("Account не доступний")
        
        inv  = bot.inventory.mining
        
        # Шахта видобута
        log.info(
            f"[MiningMonitor] було успішно видобуто руди: {inv.max_hits}"
            f"→ emit mining.mining_complete"
        )