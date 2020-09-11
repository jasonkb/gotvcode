import math
from dataclasses import dataclass, field
from itertools import count
from typing import Any, Dict, Optional, Tuple

from ashnazg.parsers import parse_phone_number


def has_value(val: Any) -> bool:
    if val is None:
        return False

    if isinstance(val, float) and math.isnan(val):
        return False

    return True


counter = count()


@dataclass
class User:
    id: int
    email: Optional[str] = None
    phone: Optional[str] = None

    def __hash__(self) -> int:
        return hash((self.email, self.phone))


users: Dict[Tuple[Optional[str], Optional[str]], User] = {}


def find_or_create_user(email: Optional[str] = None, phone: Optional[str] = None):
    user_email = email.strip().lower() if has_value(email) else None
    user_phone = parse_phone_number(phone) if has_value(phone) else None

    user = users.get((user_email, user_phone))
    if not user:
        user = User(id=next(counter), email=user_email, phone=user_phone)

        users[(user_email, user_phone)] = user

    return user
