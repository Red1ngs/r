import logging
from typing import Optional, Tuple

from bs4 import BeautifulSoup

def get_csrf_from_html(html: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Витягує CSRF-токен та ім'я користувача зі сторінки HTML.
    Завжди повертає кортеж (token, user_name). Якщо щось не знайдено, значення буде None.
    """
    soup = BeautifulSoup(html, 'html.parser')
    
    token: Optional[str] = None
    user_name: Optional[str] = None
    
    # Шукаємо токен
    meta_tag = soup.find('meta', attrs={'name': 'csrf-token'})
    if meta_tag:
        content = meta_tag.get('content')
        if content:
            token = str(content)
    else:
        logging.warning("⚠️ Мета-тег 'csrf-token' не знайдено на сторінці.")
    
    # Шукаємо ім'я
    user_div = soup.find("div", class_="menu__name")
    if user_div:
        user_name = user_div.get_text(strip=True)
    
    return token, user_name