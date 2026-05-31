"""
accounts/profession_menu.py

Реєстр підменю для profession-специфічних операцій.

Концепція:
  Кожна profession може зареєструвати власні пункти меню через
  ProfessionMenuRegistry.register(). При відображенні меню акаунта
  система автоматично генерує кнопки для всіх активних profession
  цього акаунта, що мають зареєстровані пункти.

Реєстрація пункту (в модулі відповідної profession, наприклад reader_tools.py):

    from src.bot.admin.routers.accounts.profession_menu import profession_menu_registry

    profession_menu_registry.register(
        profession_id="reader",
        label="🔍 Парсинг манг",
        callback_template="acc:force_parse:{acc_id}",
    )

Callback-шаблони використовують {acc_id} як плейсхолдер для account_id.

Клавіатура автоматично додає ці кнопки в account_menu_kb при наявності
відповідної profession у списку акаунта.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ProfessionMenuItem:
    """Один пункт меню для певної profession."""
    profession_id:     str
    label:             str
    callback_template: str   # шаблон: "acc:force_parse:{acc_id}"

    def build_callback(self, acc_id: str) -> str:
        return self.callback_template.format(acc_id=acc_id)


class ProfessionMenuRegistry:
    """
    Глобальний реєстр пунктів меню profession.

    Використання:
        # Реєстрація (в ініціалізації profession-модуля)
        profession_menu_registry.register(
            "reader",
            "🔍 Парсинг манг",
            "acc:force_parse:{acc_id}",
        )

        # Отримання пунктів для набору активних profession
        items = profession_menu_registry.items_for(["reader", "daily"])
    """

    def __init__(self) -> None:
        self._items: list[ProfessionMenuItem] = []

    def register(
        self,
        profession_id:     str,
        label:             str,
        callback_template: str,
    ) -> None:
        """Реєструє пункт меню для profession_id."""
        self._items.append(ProfessionMenuItem(
            profession_id=profession_id,
            label=label,
            callback_template=callback_template,
        ))

    def items_for(self, active_professions: list[str]) -> list[ProfessionMenuItem]:
        """
        Повертає всі зареєстровані пункти для вказаних profession,
        зберігаючи порядок реєстрації.
        """
        active_set = set(active_professions)
        return [item for item in self._items if item.profession_id in active_set]


# Глобальний синглтон
profession_menu_registry = ProfessionMenuRegistry()