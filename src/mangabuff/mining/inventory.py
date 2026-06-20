from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from src.core.inventory.model import BaseInventory


@dataclass
class MiningInventory(BaseInventory):
    
    @property
    def ore(self) -> str:
        return str(self.data.get("ore"))
    
    @ore.setter
    def ore(self, value: str | int) -> None:
        self.data["ore"] = str(value)
        
    @property
    def hits_left(self) -> Optional[int]:
        value = self.data.get("hits_left")
        return int(value) if value is not None else None
    
    @hits_left.setter
    def hits_left(self, value: int) -> None:
        self.data["hits_left"] = int(value)
        
    @property
    def hits_used(self) -> str:
        return str(self.data.get("hits_used"))
    
    @hits_used.setter
    def hits_used(self, value: str | int) -> None:
        self.data["hits_used"] = str(value)
        
    @property
    def max_hits(self) -> str:
        return str(self.data.get("max_hits"))
    
    @max_hits.setter
    def max_hits(self, value: str | int) -> None:
        self.data["max_hits"] = str(value)
        
    @property
    def added(self) -> str:
        return str(self.data.get("added"))
    
    @added.setter
    def added(self, value: str | int) -> None:
        self.data["added"] = str(value)
        
    @property
    def last_daily_complete(self) -> str:
        return str(self.data.get("last_daily_complete"))
    
    @last_daily_complete.setter
    def last_daily_complete(self, value: str | int) -> None:
        self.data["last_daily_complete"] = str(value)
        
    @property
    def mining_complete(self) -> bool:
        return bool(self.data.get("mining_complete"))
    
    @mining_complete.setter
    def mining_complete(self, value: bool) -> None:
        self.data["mining_complete"] = value
        
    