from src.core.config import Config
from src.mangabuff.session import BotSession

class AccountPull:
    def __init__(self, config: Config):
        self.config = config
        self.session = BotSession(config)
    
    def start(self):
        pass

def create_account_pull(config: Config) -> AccountPull:
    return AccountPull(config)
