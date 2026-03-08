from dataclasses import asdict, dataclass
from typing import Optional
from urllib.parse import urlparse


@dataclass
class BaseHeaders:
    user_agent: str
    sec_ch_ua: str
    sec_ch_ua_platform: str = '"Windows"'
    sec_ch_ua_mobile: str = "?0"
    accept_language: str = "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7"
    accept_encoding: str = "gzip, deflate, br, zstd"
    dnt: str = "1"

    def to_dict(self) -> dict[str, str]:
        """Конвертує атрибути класу в HTTP-заголовки з явним вказанням типів"""
        result: dict[str, str] = {}
        
        # Ми явно кажемо Pylance, що k - це рядок, а v - це рядок (або Any)
        for k, v in asdict(self).items():
            # Форматуємо ключ: snake_case -> Kebab-Case
            key = k.replace("_", "-").title().replace("Sec-Ch-Ua", "sec-ch-ua")
            result[key] = str(v)
            
        return result


@dataclass
class AuthConfig:
    email: str
    password: str
    

@dataclass
class ClientConfig:
    base_url: str
    auth: Optional[AuthConfig] = None
    cookies: Optional[dict[str, str]] = None
    
    def __post_init__(self):
        """Валідація даних після ініціалізації об'єкта"""
        if self.cookies is None and self.auth is None:
            raise ValueError("Помилка: Потрібно передати 'cookies' або 'auth' (або обидва).")

    @property
    def host(self) -> str:
        return urlparse(self.base_url).netloc


@dataclass
class NetworkConfig:
    proxy: Optional[str] = None
    timeout: int = 15


@dataclass
class Config:
    client: ClientConfig
    browser: BaseHeaders
    network: NetworkConfig