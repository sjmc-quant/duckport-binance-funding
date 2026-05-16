import asyncio
from datetime import datetime, timezone


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


async def async_sleep_until(target: datetime) -> None:
    delay = (target - now_utc()).total_seconds()
    if delay > 0:
        await asyncio.sleep(delay)
