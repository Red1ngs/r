import re
import logging
from bs4 import BeautifulSoup, Tag
from typing import Any, Optional


def parse_int(value: Any) -> Optional[int]:
    """Безпечно перетворює числовий рядок на int, прибираючи пробіли і роздільники."""
    if value is None:
        return None
    if isinstance(value, int):
        return value

    text = str(value).strip()
    if not text:
        return None

    # Прибираємо пробіли та неразривні пробіли як тисячні роздільники.
    normalized = text.replace('\u00A0', '').replace(' ', '')

    # Дозволяємо тільки цифри та негативний знак.
    normalized = re.sub(r'[^0-9\-]', '', normalized)
    if not normalized:
        return None

    try:
        return int(normalized)
    except ValueError:
        return None


def parse_csrf_token(html: str) -> Optional[str]:
    """Витягує CSRF-токен зі сторінки HTML."""
    soup = BeautifulSoup(html, 'html.parser')
    meta_tag = soup.select_one('meta[name="csrf-token"]')
    if isinstance(meta_tag, Tag):
        content = meta_tag.get('content')
        if isinstance(content, str):
            return content
    logging.warning("⚠️ Мета-тег 'csrf-token' не знайдено або має некоректний формат.")
    return None

def parse_user_name(soup: BeautifulSoup) -> str:
    """Витягує ім'я користувача зі сторінки HTML."""
    user_div = soup.select_one(".menu__name")
    if isinstance(user_div, Tag):
        return user_div.get_text(strip=True)
    return ""

def parse_user_id(soup: BeautifulSoup) -> str:
    """Витягує user_id зі сторінки HTML."""
    bookmark_link = soup.select_one('a.header-bookmark')
    if isinstance(bookmark_link, Tag):
        href = bookmark_link.get('href')
        if isinstance(href, str):
            match = re.search(r'/users/(\d+)', href)
            if match:
                return match.group(1)
    logging.warning("⚠️ ID користувача не знайдено.")
    return ""

def parse_is_banned(soup: BeautifulSoup) -> bool:
    """Визначає, чи користувач забанений, за наявністю відповідного банера."""
    is_banned = bool(soup.find('img', src='/img/frames/x150/216.png'))
    return is_banned

def parse_hits_count(soup: BeautifulSoup) -> Optional[int]:
    element = soup.find('span', class_='main-mine__game-hits-left')

    if not element:
        return None
    
    return parse_int(element.text)

def parse_mine_count(soup: BeautifulSoup) -> Optional[int]:
    element = soup.find('div', class_='main-mine__game-panel')

    if not element:
        return None
    
    # Використовуємо .get(), щоб не отримати KeyError, якщо атрибута немає
    # Перетворюємо на str, щоб заспокоїти Pylance (тип _AttributeValue -> str)
    number_raw = element.get('data-max-hit')

    if number_raw is None:
        return None

    try:
        # number_raw може бути списком у теорії BS4, тому явно робимо рядок
        return parse_int(number_raw)
    except (ValueError, TypeError):
        # Якщо в атрибуті не число (наприклад, порожньо або текст)
        return None
    
def parse_ore(soup: BeautifulSoup) -> Optional[int]:
    element = soup.find('span', class_='mine-shop__ore-count')
    
    if not element:
        return None
    
    return parse_int(element.text)

def parse_mining_page(html: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, 'html.parser')
    
    mine_count = parse_hits_count(soup)
    
    ore = parse_ore(soup)
    
    max_hits = parse_mine_count(soup)
    
    data: dict[str, Any] = {
        "hits_left": mine_count,
        "ore": ore,
        "max_hits": max_hits
    }
    
    return data

def parse_main_page(html: str, only_token: bool = False) -> dict[str, Any]:
    """
    Витягує CSRF-токен, ім'я користувача та user_id зі сторінки HTML.
    """
    soup = BeautifulSoup(html, 'html.parser')

    # 1. CSRF Token
    csrf_token = parse_csrf_token(html)
    
    if only_token:
        return {"csrf_token": csrf_token}
    
    # 2. User Name
    user_name = parse_user_name(soup)
        
    # 3. User ID
    user_id = parse_user_id(soup)
    
    # 4. Is Banned
    is_banned = parse_is_banned(soup)

    if not user_id:
        logging.warning("⚠️ ID користувача не знайдено.")
        
    data: dict[str, Any] = {
        "csrf_token": csrf_token,
        "user_name": user_name,
        "user_id": user_id,
        "is_banned": is_banned
    }

    return data