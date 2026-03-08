import logging
import time

from src.mangabuff.session import BotSession
from src.core.config import Config, BaseHeaders, AuthConfig, NetworkConfig, ClientConfig

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def main():
    config = Config(
        client=ClientConfig(
            base_url="https://mangabuff.ru",
            auth=AuthConfig(
                email="",
                password=""
            )
        ),
        browser=BaseHeaders(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            sec_ch_ua='"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
            sec_ch_ua_platform='"Windows"',
            sec_ch_ua_mobile="?0",
            accept_language="uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7",
            accept_encoding="gzip, deflate, br, zstd",
            dnt="1"
        ),
        network=NetworkConfig(
            timeout=15
        )
    )
    
    session = BotSession(config)
    session.authenticate() 
    session.client.get("/")
    time.sleep(5)  
    session.authenticate() 
    session.client.get("/")
    
if __name__ == "__main__":
    main()
    
    