"""
accounts/slots.py

Редагування target_slots для reader profession.
Пункт меню реєструється через ProfessionMenuRegistry.
"""
from __future__ import annotations

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message

from src.bot.admin.services.scheduler_service import SchedulerService
from src.core.runtime.scheduler import EventDrivenScheduler
from ._common import cancel_add_kb
from .profession_menu import profession_menu_registry

router = Router(name="accounts:slots")


# ── Реєстрація пункту меню ────────────────────────────────────────────────────

profession_menu_registry.register(
    profession_id="reader",
    label="🎰 Слоти читача",
    callback_template="acc:slots:{acc_id}",
)


# ── FSM ───────────────────────────────────────────────────────────────────────

class EditSlotsFSM(StatesGroup):
    wait_slots = State()


@router.callback_query(F.data.startswith("acc:slots:"))
async def cb_slots(call: CallbackQuery, state: FSMContext, svc: SchedulerService) -> None:
    acc_id = call.data.split(":", 2)[2]
    info   = svc.account_info(acc_id)
    if info is None:
        await call.answer("❌ Акаунт не знайдено", show_alert=True)
        return

    if "reader" not in info.professions:
        await call.answer("❌ Слоти доступні тільки для акаунтів з reader", show_alert=True)
        return

    res = EventDrivenScheduler.get_instance().ask_sync(
        account_id=acc_id,
        profession_id="reader",
        intent="get_state",
        data={},
    )

    if not res.approved:
        await call.answer(f"❌ Не вдалося отримати стан: {res.reason}", show_alert=True)
        return

    slots_data = res.data.get("slots", [])
    current = [s["name"] for s in slots_data]

    await state.set_state(EditSlotsFSM.wait_slots)
    await state.update_data(acc_id=acc_id)

    await call.message.answer(
        f"🎰 <b>Слоти читача для {acc_id}</b>\n\n"
        f"Поточні: <code>{', '.join(current) or 'немає'}</code>\n\n"
        "Введи нові слоти через кому (наприклад: <code>card, scroll</code>):",
        reply_markup=cancel_add_kb(),
    )
    await call.answer()


@router.message(EditSlotsFSM.wait_slots)
async def fsm_slots_input(message: Message, state: FSMContext, svc: SchedulerService) -> None:
    slots = [s.strip() for s in (message.text or "").split(",") if s.strip()]
    if not slots:
        await message.answer("❌ Введи хоча б один слот. Спробуй ще раз:", reply_markup=cancel_add_kb())
        return

    data   = await state.get_data()
    acc_id = data.get("acc_id", "")
    await state.clear()

    if svc.update_reader_slots(acc_id, slots):
        await message.answer(
            f"✅ Слоти оновлено: <code>{', '.join(slots)}</code>\n"
            "Зміни вступлять в силу після наступного циклу читання."
        )
    else:
        await message.answer("❌ Не вдалося оновити слоти — акаунт або reader inventory не знайдено")