import asyncio
from typing import Awaitable, Callable, TypeVar

import aiohttp

from binance_funding.config import RETRY_TIMES
from binance_funding.utils.log_kit import logger

T = TypeVar("T")


def create_aiohttp_session(timeout_seconds: int = 20) -> aiohttp.ClientSession:
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    connector = aiohttp.TCPConnector(ssl=False)
    return aiohttp.ClientSession(timeout=timeout, connector=connector)


async def async_retry_getter(func: Callable[..., Awaitable[T]], *args, **kwargs) -> T:
    last_error = None
    for attempt in range(RETRY_TIMES + 1):
        try:
            return await func(*args, **kwargs)
        except Exception as exc:
            last_error = exc
            if attempt >= RETRY_TIMES:
                break
            delay = min(30, 2 ** attempt)
            logger.warning(f"{func.__name__} failed ({exc}); retrying in {delay}s")
            await asyncio.sleep(delay)
    raise last_error
