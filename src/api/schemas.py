from datetime import datetime

from pydantic import BaseModel


class Event(BaseModel):
    user_id: int
    event_type: str
    page: str
    timestamp: datetime
