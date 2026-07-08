from datetime import datetime, timezone

from pydantic import BaseModel, field_serializer, field_validator


class Event(BaseModel):
    user_id: int
    event_type: str
    page: str
    timestamp: datetime

    @field_validator("timestamp")
    @classmethod
    def _to_utc(cls, ts: datetime) -> datetime:
        if ts.tzinfo is None:
            # policy: bare timestamps are treated as UTC.
            # To reject instead: raise ValueError("timestamp must be timezone-aware")
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc)

    @field_serializer("timestamp")
    def _ser_ts(self, ts: datetime) -> str:
        return ts.strftime("%Y-%m-%d %H:%M:%S")


class PurgeUserCommand(BaseModel):
    user_id: int
