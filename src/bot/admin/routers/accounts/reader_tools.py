"""
accounts/reader_tools.py

Інструменти Reader-професії:
  • Примусовий парсинг манг (force_parse)
  • Позначення манг як прочитаних (mark_read)

Доступно тільки для акаунтів із profession «reader».
"""
from __future__ import annotations

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message

from src.bot.admin.services.scheduler_service import SchedulerService
from ._common import cancel_add_kb

router = Router(name="accounts:reader_tools")


# ── FSM ───────────────────────────────────────────────────────────────────────

class ForceParseFSM(StatesGroup):
    wait_input = State()


class MarkReadFSM(StatesGroup):
    wait_input = State()


# ── Force parse ───────────────────────────────────────────────────────────────

@router.callback_query(F.data.startswith("acc:force_parse:"))
async def cb_force_parse_start(
    call: CallbackQuery,
    state: FSMContext,
    svc: SchedulerService,
) -> None:
    acc_id = call.data.split(":", 2)[2]
    info   = svc.account_info(acc_id)
    if info is None:
        await call.answer("❌ Акаунт не знайдено", show_alert=True)
        return

    if "reader" not in info.professions:
        await call.answer("❌ Доступно тільки для reader", show_alert=True)
        return

    await state.set_state(ForceParseFSM.wait_input)
    await state.update_data(acc_id=acc_id)

    await call.message.answer(  # type: ignore[union-attr]
        f"🔍 <b>Примусовий парсинг манг</b> для <code>{acc_id}</code>\n\n"
        "Введіть <b>число</b> (кількість манг із каталогу)\n"
        "або <b>список translit_name через кому</b> для точкового парсингу:\n\n"
        "<i>Приклади:</i>\n"
        "• <code>10</code>\n"
        "• <code>vsevedushchii-chitatel, naruto, one-piece</code>",
        reply_markup=cancel_add_kb(),
    )
    await call.answer()


@router.message(ForceParseFSM.wait_input)
async def fsm_force_parse_input(
    message: Message,
    state: FSMContext,
    svc: SchedulerService,
) -> None:
    text = (message.text or "").strip()
    data = await state.get_data()
    acc_id: str = data.get("acc_id", "")
    await state.clear()

    if text.isdigit():
        limit   = int(text)
        targets = None
        mode_label = f"каталог (перші {limit} манг)"
    else:
        targets = [t.strip() for t in text.split(",") if t.strip()]
        limit   = 5
        mode_label = f"цільові манги: {', '.join(targets)}"

    await message.answer(f"⏳ Запускаю парсинг: {mode_label}…")

    ok, reason, result = svc.force_parse_mangas(acc_id, limit=limit, targets=targets)

    if ok:
        await message.answer(
            f"✅ <b>Парсинг завершено</b>\n"
            f"Збережено: <b>{result.get('mangas', 0)}</b> манг, "
            f"<b>{result.get('chapters', 0)}</b> глав."
        )
    else:
        await message.answer(f"❌ Помилка парсингу:\n<code>{reason}</code>")


# ── Mark read ─────────────────────────────────────────────────────────────────

@router.callback_query(F.data.startswith("acc:mark_read:"))
async def cb_mark_read_start(
    call: CallbackQuery,
    state: FSMContext,
    svc: SchedulerService,
) -> None:
    acc_id = call.data.split(":", 2)[2]
    info   = svc.account_info(acc_id)
    if info is None:
        await call.answer("❌ Акаунт не знайдено", show_alert=True)
        return

    if "reader" not in info.professions:
        await call.answer("❌ Доступно тільки для reader", show_alert=True)
        return

    await state.set_state(MarkReadFSM.wait_input)
    await state.update_data(acc_id=acc_id)

    await call.message.answer(  # type: ignore[union-attr]
        f"✅ <b>Позначити манги як прочитані</b> для <code>{acc_id}</code>\n\n"
        "Введіть <b>список translit_name через кому</b>:\n\n"
        "<i>Приклад:</i>\n"
        "• <code>vsevedushchii-chitatel, naruto</code>",
        reply_markup=cancel_add_kb(),
    )
    await call.answer()


@router.message(MarkReadFSM.wait_input)
async def fsm_mark_read_input(
    message: Message,
    state: FSMContext,
    svc: SchedulerService,
) -> None:
    text = (message.text or "").strip()
    data = await state.get_data()
    acc_id: str = data.get("acc_id", "")
    await state.clear()

    targets = [t.strip() for t in text.split(",") if t.strip()]
    if not targets:
        await message.answer("❌ Введіть хоча б одну мангу. Операцію скасовано.")
        return

    await message.answer(f"⏳ Позначаю прочитаними: {', '.join(targets)}…")

    ok, reason, result = svc.mark_mangas_read(acc_id, targets)

    if ok:
        await message.answer(
            f"✅ <b>Готово!</b>\n"
            f"Позначено <b>{result.get('marked', 0)}</b> глав як прочитані\n"
            f"Манги: <code>{', '.join(targets)}</code>"
        )
    else:
        await message.answer(f"❌ Помилка:\n<code>{reason}</code>")