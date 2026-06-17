from pathlib import Path
from src.database.repository.factory import repository_factory, Repositories
from src.database.repository.account import AccountRepository
from src.database.repository.inventory import InventoryRepository
from src.database.repository.manga import MangaRepository, ChapterRepository
from src.database.repository.session import SessionRepository


def register_repositories():
    repository_factory.register("accounts",  "accounts",  AccountRepository)
    repository_factory.register("mangas",    "mangas",    MangaRepository)
    repository_factory.register("chapters",  "chapters",  ChapterRepository)
    repository_factory.register("inventory", "inventory", InventoryRepository)
    repository_factory.register("sessions",  "sessions",  SessionRepository)


def init_database(path: str | Path = "bot_state.db") -> Repositories:
    """
    Повний цикл: реєстрація репозиторіїв -> підключення до БД -> створення контейнера.
    """
    register_repositories()
    
    from src.database.ddl import get_db 
    conn = get_db(path)
    
    return repository_factory.build(conn)