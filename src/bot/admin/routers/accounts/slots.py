"""
accounts/slots.py
"""
from __future__ import annotations

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from src.bot.admin.services.scheduler_service import SchedulerService
from ._common import cancel_add_kb
from .profession_menu import profession_menu_registry

router = Router(name="accounts:slots")

profession_menu_registry.register(
    profession_id      = "reader",
    label              = "⚙️ Параметри читання",
    callback_template  = "acc:slots:{acc_id}",
)


# ── FSM ───────────────────────────────────────────────────────────────────────

class EditSlotsFSM(StatesGroup):
    wait_limit        = State()
    wait_include_tags = State()
    wait_exclude_tags = State()


# ── Entry point ───────────────────────────────────────────────────────────────

@router.callback_query(F.data.startswith("acc:slots:"))
async def cb_slots(call: CallbackQuery, state: FSMContext, svc: SchedulerService) -> None:
    acc_id = call.data.split(":", 2)[2]
    info   = svc.account_info(acc_id)
    if info is None:
        await call.answer("❌ Акаунт не знайдено", show_alert=True)
        return

    ok, reader_data = await svc.get_reader_state(acc_id)
    params = reader_data.get("reading_params", {})

    await state.set_state(EditSlotsFSM.wait_limit)
    await state.update_data(acc_id=acc_id)

    await call.message.answer(  # type: ignore[union-attr]
        f"⚙️ <b>Параметри читання</b> для <code>{acc_id}</code>\n\n"
        f"Поточні налаштування:\n"
        f"  limit        = <b>{params.get('limit', 2)}</b>\n"
        f"  include_tags = <b>{params.get('include_tags') or '—'}</b>\n"
        f"  exclude_tags = <b>{params.get('exclude_tags') or '—'}</b>\n\n"
        "Введіть нове <b>limit</b> (кількість глав за раз, ціле число):",
        reply_markup=cancel_add_kb(),
    )
    await call.answer()


@router.message(EditSlotsFSM.wait_limit)
async def fsm_limit_input(message: Message, state: FSMContext, svc: SchedulerService) -> None:
    text = (message.text or "").strip()
    try:
        limit = int(text)
        if limit < 1:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введіть ціле число ≥ 1. Спробуйте ще раз:")
        return

    await state.update_data(limit=limit)
    await state.set_state(EditSlotsFSM.wait_include_tags)
    await message.answer(
        "Введіть <b>include_tags</b> через кому (або <code>-</code> щоб пропустити):\n\n"
        "<i>Приклад: <code>shounen, fantasy</code></i>",
        reply_markup=cancel_add_kb(),
    )


@router.message(EditSlotsFSM.wait_include_tags)
async def fsm_include_tags_input(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    include_tags = None if text == "-" else [t.strip() for t in text.split(",") if t.strip()]
    await state.update_data(include_tags=include_tags)
    await state.set_state(EditSlotsFSM.wait_exclude_tags)
    await message.answer(
        "Введіть <b>exclude_tags</b> через кому (або <code>-</code> щоб пропустити):\n\n"
        "<i>Приклад: <code>ecchi, harem</code></i>",
        reply_markup=cancel_add_kb(),
    )


@router.message(EditSlotsFSM.wait_exclude_tags)
async def fsm_exclude_tags_input(message: Message, state: FSMContext, svc: SchedulerService) -> None:
    text = (message.text or "").strip()
    exclude_tags = None if text == "-" else [t.strip() for t in text.split(",") if t.strip()]

    data   = await state.get_data()
    acc_id: str                  = data["acc_id"]
    limit:  int                  = data["limit"]
    include = data.get("include_tags")
    await state.clear()

    ok = await svc.update_reading_params(
        acc_id,
        limit        = limit,
        include_tags = include,
        exclude_tags = exclude_tags,
    )

    if ok:
        await message.answer(
            f"✅ <b>Параметри читання оновлено</b>\n"
            f"  limit        = <b>{limit}</b>\n"
            f"  include_tags = <b>{include or '—'}</b>\n"
            f"  exclude_tags = <b>{exclude_tags or '—'}</b>"
        )
    else:
        await message.answer("❌ Не вдалося оновити параметри. Перевірте лог.")