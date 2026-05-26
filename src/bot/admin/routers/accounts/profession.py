"""
accounts/profession.py

Управління списком profession акаунта (додавання, видалення).

Callbacks:
  acc:professions:{acc_id}          — меню управління
  acc:prof_add:{acc_id}:{name}      — додати profession
  acc:prof_remove:{acc_id}:{name}   — видалити profession
"""
from __future__ import annotations

from aiogram import F, Router
from aiogram.types import CallbackQuery

from src.bot.admin.services.scheduler_service import SchedulerService
from ._common import account_text, account_menu_kb, professions_manage_kb

router = Router(name="accounts:profession")


def _professions_text(acc_id: str, professions: list[str]) -> str:
    if not professions:
        return f"🎓 <b>Акаунт {acc_id}</b>\n\nПрофесій ще не призначено."
    lines = "\n".join(
        f"  {i+1}. <b>{name}</b>" for i, name in enumerate(professions)
    )
    return (
        f"🎓 <b>Акаунт {acc_id}</b>\n\n"
        f"Активні професії (за пріоритетом):\n{lines}\n\n"
        f"Перша в списку має найвищий пріоритет — її задачі виконуються першими."
    )


# ── Меню управління ───────────────────────────────────────────────────────────

@router.callback_query(F.data.startswith("acc:professions:"))
async def cb_professions_menu(call: CallbackQuery, svc: SchedulerService) -> None:
    acc_id = call.data.split(":", 2)[2]
    info   = svc.account_info(acc_id)
    if info is None:
        await call.answer("❌ Акаунт не знайдено", show_alert=True)
        return
    await call.message.edit_text(
        _professions_text(acc_id, info.professions),
        reply_markup=professions_manage_kb(acc_id, info.professions),
    )
    await call.answer()


# ── Додавання ─────────────────────────────────────────────────────────────────

@router.callback_query(F.data.startswith("acc:prof_add:"))
async def cb_prof_add(call: CallbackQuery, svc: SchedulerService) -> None:
    parts = call.data.split(":", 3)
    if len(parts) < 4:
        await call.answer("❌ Помилка даних", show_alert=True)
        return

    acc_id, prof_name = parts[2], parts[3]
    ok, err = svc.add_profession(acc_id, prof_name)
    if not ok:
        await call.answer(f"❌ {err}", show_alert=True)
        return

    # Перемальовуємо меню з оновленим списком
    info = svc.account_info(acc_id)
    if info:
        await call.message.edit_text(
            _professions_text(acc_id, info.professions),
            reply_markup=professions_manage_kb(acc_id, info.professions),
        )
    await call.answer(f"✅ Професію {prof_name!r} додано")


# ── Видалення ─────────────────────────────────────────────────────────────────

@router.callback_query(F.data.startswith("acc:prof_remove:"))
async def cb_prof_remove(call: CallbackQuery, svc: SchedulerService) -> None:
    parts = call.data.split(":", 3)
    if len(parts) < 4:
        await call.answer("❌ Помилка даних", show_alert=True)
        return

    acc_id, prof_name = parts[2], parts[3]
    ok, err = svc.remove_profession(acc_id, prof_name)
    if not ok:
        await call.answer(f"❌ {err}", show_alert=True)
        return

    info = svc.account_info(acc_id)
    if info:
        await call.message.edit_text(
            _professions_text(acc_id, info.professions),
            reply_markup=professions_manage_kb(acc_id, info.professions),
        )
    await call.answer(f"✅ Професію {prof_name!r} видалено")


# ── noop (кнопки-роздільники) ─────────────────────────────────────────────────

@router.callback_query(F.data == "noop")
async def cb_noop(call: CallbackQuery) -> None:
    await call.answer()
