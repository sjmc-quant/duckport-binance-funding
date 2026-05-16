import asyncio
from datetime import datetime, timedelta, timezone


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


async def async_sleep_until(target: datetime) -> None:
    delay = (target - now_utc()).total_seconds()
    if delay > 0:
        await asyncio.sleep(delay)


def seconds_until_next_funding_poll(offset_seconds: int = 60) -> int:
    now = now_utc()
    next_hour = ((now.hour // 8) + 1) * 8
    if next_hour >= 24:
        target = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        target = now.replace(hour=next_hour, minute=0, second=0, microsecond=0)
    target += timedelta(seconds=offset_seconds)
    return max(1, int((target - now).total_seconds()))
