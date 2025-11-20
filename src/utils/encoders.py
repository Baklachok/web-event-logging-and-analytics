from datetime import datetime


def serialize_event(event: dict) -> dict:
    """Преобразует datetime в ISO строку для JSON"""
    return {
        k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in event.items()
    }
