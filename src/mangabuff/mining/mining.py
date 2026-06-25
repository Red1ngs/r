


from typing import Any, Optional

from src.core.core_account import Account
from src.core.logging.loggers import get_account_logger
from src.core.runtime.profession import BaseProfession, RequestResult
from src.core.runtime.request_router import RequestContext
from src.core.runtime.scheduler import EventDrivenScheduler
from src.mangabuff.mining.inventory import MiningInventory


class MiningProfession(BaseProfession):
    """
    Profession «Шахта».

    Чистий виконавець: отримує ask → HTTP → inventory → emit.
    Не знає про таймінги, не планує наступний запуск, не скидає лічильник.
    """

    def __init__(self) -> None:
        self._account_id: str                              = ""
        self._inv:        Optional[MiningInventory]        = None
        self._scheduler:  Optional["EventDrivenScheduler"] = None

    @property
    def profession_id(self) -> str:
        return "mining"

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def setup(self, scheduler: "EventDrivenScheduler", account_id: str) -> None:
        self._account_id = account_id
        self._scheduler  = scheduler

    async def restore_state(self, bot: "Account") -> None:
        inv: MiningInventory = bot.inventory.mining
        self._inv = inv

        get_account_logger(self._account_id).info(
            f"MiningProfession відновлено: "
            f"mining_complete={inv.mining_complete!r} "
        )

    async def teardown(self, scheduler: "EventDrivenScheduler", account_id: str) -> None:
        self._scheduler = None

    def check_guard(self, bot: "Account") -> bool:
        return not bool(bot.inventory.personal.is_banned)

    # ── handle_request ────────────────────────────────────────────────────────

    async def handle_request(
        self,
        intent: str,
        data:   dict[str, Any],
        ctx:    "RequestContext",
    ) -> RequestResult:
        if intent == "start_mining":
            return await self._handle_start_mining(ctx)
        if intent == "mining_hit":
            return await self._handle_mining_hit(ctx)
        return RequestResult.deny(f"unknown intent: {intent!r}")

    async def _handle_start_mining(self, ctx: "RequestContext") -> RequestResult:
        log = get_account_logger(self._account_id)
        try:
            # 1. Валідація системних об'єктів
            if self._scheduler is None:
                raise ValueError("Scheduler не доступний")
            
            bot = self._scheduler.get_bot(ctx.account_id)
            if bot is None:
                raise ValueError(f"Бот для акаунта {ctx.account_id} не знайдений")
            
            cfg = bot.app_config.mining
            
            # 2. Виконання запиту
            log.info("📝 Mining: надсилаємо запит на початок копання…")
            result = await bot.safe_session.mine(self._account_id,cfg)

            if not result.ok:
                log.warning(f"⚠️ /mine провалився: {result.reason}")
                return RequestResult.deny(str(result.reason))

            data = result.data
            if data is None:
                raise ValueError("API повернуло порожні дані (data is None)")

            # 3. Перевірка обов'язкових полів
            # Використовуємо set для перевірки наявності ключів
            required_keys = {"hits_left", "ore", "max_hits"}
            if not required_keys.issubset(data.keys()) or any(data[k] is None for k in required_keys):
                missing = [k for k in required_keys if data.get(k) is None]
                raise ValueError(f"API не повернуло обов'язкові поля: {', '.join(missing)}")
            
            log.info(f"⛏️ Майнінг статус: Ударів: {data["hits_left"]}, Руди: {data["ore"]}")
                
            return RequestResult.approve(data=data)
            
        except Exception as exc:
            log.exception("start_mining: критична помилка")
            return RequestResult.deny(f"Помилка ініціалізації майнінгу: {exc}")
        
    async def _handle_mining_hit(self, ctx: "RequestContext") -> RequestResult:
        log = get_account_logger(self._account_id)
        try:
            # 1. Перевірка внутрішнього стану
            if self._scheduler is None:
                raise ValueError("Scheduler не ініціалізовано")
            
            bot = self._scheduler.get_bot(ctx.account_id)
            if bot is None:
                raise ValueError(f"Бот для акаунта {ctx.account_id} не знайдений")
            
            # 2. Виконання запиту
            cfg = bot.app_config.mining
            result = await bot.safe_session.mine_hit(cfg)

            if not result.ok:
                log.warning(f"⚠️ /mining провалився: {result.reason}")
                return RequestResult.deny(str(result.reason))

            data = result.data
            if data is None:
                raise ValueError("Отримано порожні дані від сервера (data is None)")

            # 3. Валідація отриманих даних
            # Складаємо список ключів, які ми очікуємо отримати
            required_fields = ["hits_left", "hits_used", "ore", "added"]
            missing_fields = [field for field in required_fields if data.get(field) is None]
            
            if missing_fields:
                raise ValueError(f"Відсутні обов'язкові поля в API: {', '.join(missing_fields)}")

            # 4. Оновлення інвентарю
            inv: MiningInventory = bot.inventory.mining
            inv.hits_left = data["hits_left"]
            inv.hits_used = data["hits_used"]
            inv.ore       = data["ore"]
            inv.added     = data["added"]
                
            return RequestResult.approve(data=data)
            
        except Exception as exc:
            log.exception("mining_hit: критична помилка")
            return RequestResult.deny(f"Помилка обробки майнінгу: {exc}")