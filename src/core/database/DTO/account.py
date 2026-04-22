from dataclasses import dataclass, field
from typing import Any, Optional

@dataclass
class AccountRow:
    """Snapshot of an account row from the DB."""
    id:               str
    email:            str
    base_url:         str
    profession:       Optional[str]
    is_active:        bool
    comments_written: int
    trades_accepted:  int
    trades_declined:  int
    inventory:        dict[str, Any] = field(default_factory=dict)
    
    