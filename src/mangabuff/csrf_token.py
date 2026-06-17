import re
import logging
from bs4 import BeautifulSoup, Tag
from typing import Tuple, Optional

def parse_main_page(html: str) -> Tuple[Optional[str], str, str]:
    """
    Витягує CSRF-токен, ім'я користувача та user_id зі сторінки HTML.
    """
    soup = BeautifulSoup(html, 'html.parser')
    
    token:      Optional[str] = None
    user_name:  str = ""
    user_id:    str = ""
    
    # 1. CSRF Token
    meta_tag = soup.select_one('meta[name="csrf-token"]')
    if isinstance(meta_tag, Tag):
        content = meta_tag.get('content')
        # Перевіряємо, що content це рядок, а не список чи None
        if isinstance(content, str):
            token = content
    else:
        logging.warning("⚠️ Мета-тег 'csrf-token' не знайдено.")
    
    # 2. User Name
    user_div = soup.select_one(".menu__name")
    if isinstance(user_div, Tag):
        user_name = user_div.get_text(strip=True)
        
    # 3. User ID
    bookmark_link = soup.select_one('a.header-bookmark')
    if isinstance(bookmark_link, Tag):
        href = bookmark_link.get('href')
        # Переконуємося, що href - це рядок, перед тим як пхати його в regex
        if isinstance(href, str):
            match = re.search(r'/users/(\d+)', href)
            if match:
                user_id = match.group(1)
    
    if not user_id:
        logging.warning("⚠️ ID користувача не знайдено.")

    return token, user_name, user_id