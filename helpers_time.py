# helpers_time.py
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

_TZ = ZoneInfo("Asia/Tashkent")

def cutoff_utc(hours: int) -> datetime:
    """Порог в UTC с учётом Ташкента."""
    return (datetime.now(_TZ) - timedelta(hours=hours)).astimezone(timezone.utc)
