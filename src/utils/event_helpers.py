from typing import List, Dict
from src.api.schemas import Event


def validate_events(raw_events: List[Dict]) -> List[Event]:
    """
    Валидирует список словарей через Pydantic Event.
    """
    return [Event(**e) for e in raw_events]
