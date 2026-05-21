"""
accounts/profession.py

Призначення та зміна profession для акаунта.

acc:profession:{acc_id}         — показує список доступних profession
acc:set_profession:{acc_id}:{name} — призначає обрану profession
"""
from __future__ import annotations

from aiogram import F, Router
from aiogram.types import CallbackQuery

from src.bot.admin.services.scheduler_service import SchedulerService
from src.core.runtime.profession import profession_factory
from ._common import account_text, account_menu_kb, profession_pick_kb

router = Router(name="accounts:profession")


@router.callback_query(F.data.startswith("acc:profession:"))
async def cb_profession_pick(call: CallbackQuery) -> None:
    acc_id = call.data.split(":", 2)[2]
    if not profession_factory.names():
        await call.answer("❌ Жодної профессії не зареєстровано", show_alert=True)
        return
    await call.message.edit_text(
        f"🎓 <b>Оберіть профессію для {acc_id}</b>\n\n"
        "Профессія визначає тригери і автоматичні дії акаунта.",
        reply_markup=profession_pick_kb(acc_id),
    )
    await call.answer()


@router.callback_query(F.data.startswith("acc:set_profession:"))
async def cb_set_profession(call: CallbackQuery, svc: SchedulerService) -> None:
    parts = call.data.split(":", 3)
    if len(parts) < 4:
        await call.answer("❌ Помилка даних", show_alert=True)
        return

    acc_id, prof_name = parts[2], parts[3]
    
    # Використовуємо очищення старої професії перед встановленням нової
    ok, err = svc.change_profession(acc_id, prof_name)
    if not ok:
        await call.answer(f"❌ {err}", show_alert=True)
        return

    info = svc.account_info(acc_id)
    if info:
        await call.message.edit_text(
            f"✅ Профессію <b>{prof_name}</b> призначено.\n\n" + account_text(info),
            reply_markup=account_menu_kb(acc_id, info.status, bool(info.profession)),
        )
    await call.answer()