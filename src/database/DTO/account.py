from dataclasses import dataclass
from typing import Optional

@dataclass
class AccountRow:
    """Snapshot of an account row from the DB."""
    id:               str
    email:            str
    profession:       Optional[str]
    updated_at:       str

    