from __future__ import annotations

from typing import Optional
from urllib.parse import urlparse

from bs4 import BeautifulSoup, Tag

from src.mangabuff.farmer.models import Chapter, Manga

from src.core.logging.loggers import get_logger
log = get_logger("farmer.parsers")

# Кількість манг на одній сторінці каталогу (для розрахунку номера сторінки)
CATALOG_PAGE_SIZE: int = 30


def _extract_image_url(img_tag: Tag) -> str:
    # BeautifulSoup.get може повернути list[str], тому перетворюємо на str
    style = str(img_tag.get("style", ""))
    if "url(" not in style:
        return ""
    try:
        # Додаємо явне перетворення на str для Pylance
        return style.split("url(")[1].split(")")[0].strip("'\"")
    except IndexError:
        return ""

def _parse_vol_chap_from_url(url: str) -> tuple[Optional[int], Optional[float]]:
    try:
        parts = urlparse(url).path.strip("/").split("/")
        if len(parts) < 2:
            return None, None
        return int(parts[-2]), float(parts[-1])
    except (ValueError, IndexError):
        log.debug("Не вдалося визначити том/главу з URL: %s", url)
        return None, None


def _parse_manga_item(item: Tag) -> Optional[Manga]:
    # Перетворюємо атрибути на рядки, щоб уникнути проблем з типами
    data_id_raw = item.get("data-id")
    url_raw = item.get("href")

    if not data_id_raw or not url_raw:
        return None

    name_tag = item.select_one(".cards__name")
    if not name_tag:
        log.warning("Відсутній .cards__name (data-id=%s)", data_id_raw)
        return None

    img_tag = item.select_one(".cards__img")

    rating_tag = item.select_one(".cards__rating")

    info_tag = item.select_one(".cards__info")

    # translit_name береться з href: "/manga/vsevedushchii-chitatel" → "vsevedushchii-chitatel"
    translit_name = str(url_raw).rstrip("/").split("/")[-1]
    if not translit_name:
        raise ValueError(f"Порожній translit_name (data-id={data_id_raw}, href={url_raw})")

    return Manga(
        data_id=int(str(data_id_raw)),
        translit_name=translit_name,
        name=name_tag.get_text(strip=True),
        rating=rating_tag.get_text(strip=True) if rating_tag else "",
        info=info_tag.get_text(strip=True) if info_tag else "",
        image=_extract_image_url(img_tag) if img_tag else "",
    )

def _parse_chapter_item(item: Tag) -> Optional[Chapter]:
    href = item.get("href")
    if not href or isinstance(href, list):
        return None
    
    log.debug(f"chapter href: {href}")

    like_btn = item.select_one("button.favourite-send-btn[data-id]")
    if not like_btn:
        return None
    
    chapter_data_id = like_btn.get("data-id")
    if not chapter_data_id:
        return None

    date_tag = item.select_one(".chapters__add-date")
    volume, chapter_num = _parse_vol_chap_from_url(str(href))
    
    if chapter_num is None or volume is None:
        log.warning(f"Не вдалося розпарсити том/главу з URL: {href}")
        return None  # пропускаємо невалідну главу

    # Отримуємо дату з атрибута або тексту
    date_attr = item.get("data-chapter-date")
    date_val = str(date_attr) if date_attr else (date_tag.get_text(strip=True) if date_tag else None)

    return Chapter(
        data_id=int(str(chapter_data_id)),
        volume=volume,
        chapter_num=chapter_num, # Виправлено назву аргумента (було chapter)
        date=date_val,
    )

def parse_catalog(html: str) -> dict[int, Manga]:
    """Повертає {data_id: Manga} з HTML каталогу."""
    soup = BeautifulSoup(html, "html.parser")
    result: dict[int, Manga] = {}
    
    for item in soup.select("a.cards__item"):
        manga = _parse_manga_item(item)
        if manga:
            result[manga.data_id] = manga
    return result

def parse_chapters(html: str) -> list[Chapter]:
    """Повертає список об'єктів Chapter з HTML сторінки манги."""
    soup = BeautifulSoup(html, "html.parser")
    chapters: list[Chapter] = []
    
    for item in soup.select("a.chapters__item"):
        ch = _parse_chapter_item(item)
        if ch:
            chapters.append(ch)
    return chapters

def parse_manga_data_id(html: str) -> Optional[int]:
    """Парсить data_id манги зі сторінки манги (не з каталогу)."""
    soup = BeautifulSoup(html, "html.parser")
    manga_element = soup.find('div', class_='manga')
    if manga_element:
        data_id = manga_element.get('data-id')
        if data_id:
            return int(str(data_id))
    log.warning("Не вдалося знайти data_id на сторінці манги")
    return None

def parse_manga_views(html: str) -> int:
    """Парсить кількість переглядів манги з <div class="manga__views">."""
    soup = BeautifulSoup(html, "html.parser")
    views_tag = soup.find("div", class_="manga__views")
    if not views_tag:
        log.debug("manga__views не знайдено на сторінці манги")
        return 0
    try:
        # "40 108 015" → 40108015  (пробіли/nbsp як роздільник тисяч)
        raw = views_tag.get_text(strip=True).replace("\xa0", "").replace(" ", "")
        return int(raw)
    except (ValueError, AttributeError):
        log.debug("Не вдалося розпарсити manga__views: %s", views_tag.get_text(strip=True))
        return 0