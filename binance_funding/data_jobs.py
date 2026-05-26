import asyncio
import gc
import threading
from datetime import date, datetime, timedelta, timezone

import pandas as pd

from binance_funding.api.restful import BinanceFundingRestApi
from binance_funding.component.funding import (
    FUNDING_COLUMNS,
    normalize_funding_frame,
    parse_rest_funding_history,
)
from binance_funding.config import (
    FUNDING_POLL_DELAY_SECONDS,
    FUNDING_REQUEST_INTERVAL_SECONDS,
    FUNDING_RETRY_SLEEP_SECONDS,
    GENESIS_TIME,
    RETRY_TIMES,
    START_DATE,
)
from binance_funding.duckport_client import DuckportClient
from binance_funding.utils import (
    async_sleep_until,
    create_aiohttp_session,
)
from binance_funding.utils.log_kit import divider, logger


def _utc_timestamp(value) -> pd.Timestamp:
    ts = pd.to_datetime(value)
    if ts.tzinfo is None:
        return ts.tz_localize(timezone.utc)
    return ts.tz_convert(timezone.utc)


def _month_start(value: date) -> date:
    return date(value.year, value.month, 1)


def _add_month(value: date) -> date:
    if value.month == 12:
        return date(value.year + 1, 1, 1)
    return date(value.year, value.month + 1, 1)


def _format_ms(value: int) -> pd.Timestamp:
    return pd.to_datetime(value, unit="ms", utc=True)


def _watermark_for_checked_window(end_time) -> pd.Timestamp:
    end = _utc_timestamp(end_time)
    return (end - pd.Timedelta(milliseconds=1)).floor("h")


def current_hour_watermark(now=None) -> pd.Timestamp:
    ts = _utc_timestamp(now or pd.Timestamp.now(tz="UTC"))
    return ts.floor("h")


def iter_month_windows(
    start_time,
    end_time: pd.Timestamp | None = None,
) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    end = _utc_timestamp(end_time or pd.Timestamp.now(tz="UTC"))
    start = _utc_timestamp(start_time)
    if start >= end:
        return []
    windows = []
    month = _month_start(start.date())
    first_next_month = pd.Timestamp(_add_month(month), tz=timezone.utc)
    first_stop = min(first_next_month, end)
    windows.append((start, first_stop))
    month = _add_month(month)
    while True:
        month_start = pd.Timestamp(month, tz=timezone.utc)
        if month_start >= end:
            break
        next_month = _add_month(month)
        stop = min(pd.Timestamp(next_month, tz=timezone.utc), end)
        windows.append((month_start, stop))
        month = next_month
    return windows


class FundingFetcher:
    def __init__(self, api: BinanceFundingRestApi):
        self.api = api
        self.funding_rate_requests = 0

    async def _funding_rate_history(self, start_ms: int, end_ms: int) -> list[dict]:
        await asyncio.sleep(max(0, FUNDING_REQUEST_INTERVAL_SECONDS))
        last_error = None
        for attempt in range(1, RETRY_TIMES + 2):
            self.funding_rate_requests += 1
            try:
                return await self.api.funding_rate_history(
                    startTime=start_ms,
                    endTime=end_ms,
                    limit=1000,
                )
            except Exception as exc:
                last_error = exc
                if attempt > RETRY_TIMES:
                    break
                logger.warning(
                    f"funding_rate_history failed ({exc}); "
                    f"fundingRate_requests={self.funding_rate_requests}; "
                    f"retrying in {FUNDING_RETRY_SLEEP_SECONDS}s"
                )
                await asyncio.sleep(FUNDING_RETRY_SLEEP_SECONDS)
        raise last_error

    async def funding_history(
        self,
        start_time: pd.Timestamp,
        end_time: pd.Timestamp,
    ) -> pd.DataFrame:
        start_ms = int(_utc_timestamp(start_time).timestamp() * 1000)
        end_ms = int(_utc_timestamp(end_time).timestamp() * 1000) - 1
        if end_ms < start_ms:
            return pd.DataFrame(columns=FUNDING_COLUMNS)

        frames = []
        while start_ms <= end_ms:
            rows = await self._funding_rate_history(start_ms, end_ms)
            if not rows:
                break
            frame = parse_rest_funding_history(rows)
            if not frame.empty:
                frame = frame[frame["symbol"].str.endswith("USDT")]

            last_time = max(int(row["fundingTime"]) for row in rows)
            next_start = last_time + 1
            if len(rows) < 1000:
                if not frame.empty:
                    frames.append(frame)
                break

            if next_start <= start_ms:
                raise RuntimeError(
                    "fundingRate pagination cannot advance without risking skipped rows"
                )

            last_timestamp = _format_ms(last_time)
            safe_frame = frame[frame["funding_time"] < last_timestamp] if not frame.empty else frame
            if not safe_frame.empty:
                frames.append(safe_frame)

            exact_rows = await self._funding_rate_history(last_time, last_time)
            if len(exact_rows) >= 1000:
                raise RuntimeError(
                    f"fundingRate returned >=1000 rows for one fundingTime={last_time}; "
                    "all-market pagination cannot safely continue"
                )
            exact_frame = parse_rest_funding_history(exact_rows)
            if not exact_frame.empty:
                exact_frame = exact_frame[exact_frame["symbol"].str.endswith("USDT")]
            if not exact_frame.empty:
                frames.append(exact_frame)

            start_ms = next_start

        if not frames:
            return pd.DataFrame(columns=FUNDING_COLUMNS)
        return normalize_funding_frame(pd.concat(frames, ignore_index=True))


class FundingDataJobs:
    def __init__(self, client: DuckportClient, *, auto_start: bool = True):
        self._client = client
        self._has_duck_time = False
        self.duck_time = self._init_duck_time()
        logger.info(f"Funding duck_time loaded: {self.duck_time}")
        if auto_start:
            self.start()

    def _init_duck_time(self) -> pd.Timestamp:
        dt = self._client.read_duck_time()
        if dt and dt != GENESIS_TIME:
            self._has_duck_time = True
            return _utc_timestamp(dt)
        return pd.Timestamp(START_DATE, tz=timezone.utc)

    def _refresh_duck_time(self) -> None:
        dt = self._client.read_duck_time()
        if dt and dt != GENESIS_TIME:
            self._has_duck_time = True
            self.duck_time = max(self.duck_time, _utc_timestamp(dt))

    def start(self) -> None:
        threading.Thread(target=self._run_poll_loop, daemon=True).start()

    def _run_poll_loop(self):
        async def run():
            await self._run_startup_history_until_success()
            while True:
                next_time = self.next_poll_time(
                    datetime.now(timezone.utc),
                    FUNDING_POLL_DELAY_SECONDS,
                )
                logger.info(f"Next funding REST poll at {next_time.isoformat()}")
                await async_sleep_until(next_time)
                try:
                    await self.catch_up_rest()
                except Exception as exc:
                    logger.error(f"Funding REST poll failed: {exc}")
                    gc.collect()

        asyncio.run(run())

    async def _run_startup_history_until_success(self) -> None:
        while True:
            try:
                await self.update_startup_history()
                return
            except Exception as exc:
                logger.error(f"Funding startup history update failed: {exc}; retrying in 30s")
                gc.collect()
                await asyncio.sleep(30)

    @staticmethod
    def next_poll_time(now: datetime, delay_seconds: int) -> datetime:
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        base = now.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
        next_hour = base + timedelta(hours=1)
        return next_hour + timedelta(seconds=max(0, delay_seconds))

    def _query_start_time(self) -> pd.Timestamp:
        start = self.duck_time.floor("h") + pd.Timedelta(seconds=1)
        start = max(start, pd.Timestamp(START_DATE, tz=timezone.utc))
        return _utc_timestamp(start)

    async def update_startup_history(self) -> None:
        self._refresh_duck_time()
        start_time = self._query_start_time() if self._has_duck_time else pd.Timestamp(START_DATE, tz=timezone.utc)
        end_time = pd.Timestamp.now(tz="UTC")
        divider(f"Funding startup history {start_time} -> {end_time}", display_time=False)
        async with create_aiohttp_session(30) as session:
            fetcher = FundingFetcher(BinanceFundingRestApi(session))
            total_rows = 0
            for window_start, stop_time in iter_month_windows(start_time, end_time):
                request_count_before = fetcher.funding_rate_requests
                divider(
                    f"Funding history {window_start:%Y-%m-%d} -> {stop_time:%Y-%m-%d %H:%M:%S}",
                    display_time=False,
                )
                frame = await fetcher.funding_history(window_start, stop_time)
                request_count = fetcher.funding_rate_requests - request_count_before
                if frame.empty:
                    checked_time = _watermark_for_checked_window(stop_time)
                    checked_time = max(self.duck_time, checked_time)
                    self._client.replace_duck_time(checked_time.strftime("%Y-%m-%d %H:%M:%S"))
                    self.duck_time = max(self.duck_time, checked_time)
                    self._has_duck_time = True
                    logger.info(
                        f"No funding rows for {window_start:%Y-%m}; "
                        f"fundingRate_requests={request_count}; duck_time={self.duck_time}"
                    )
                    continue
                max_time = frame["funding_time"].max()
                self._client.write_funding_rates(frame, max_time)
                self.duck_time = max(self.duck_time, _utc_timestamp(max_time))
                self._has_duck_time = True
                total_rows += len(frame)
                logger.info(
                    f"Funding startup history wrote {len(frame):,} rows for "
                    f"{window_start:%Y-%m}; fundingRate_requests={request_count}; "
                    f"duck_time={self.duck_time}"
                )
        logger.info(f"Funding startup history complete: {total_rows:,} rows")

    async def catch_up_rest(self):
        self._refresh_duck_time()
        start_time = self._query_start_time()
        end_time = pd.Timestamp.now(tz="UTC")
        divider(f"Funding REST poll {start_time} -> {end_time}", display_time=False)

        async with create_aiohttp_session(30) as session:
            fetcher = FundingFetcher(BinanceFundingRestApi(session))
            frame = await fetcher.funding_history(start_time, end_time)

        if frame.empty:
            checked_time = current_hour_watermark(end_time)
            checked_time = max(self.duck_time, checked_time)
            self._client.replace_duck_time(checked_time.strftime("%Y-%m-%d %H:%M:%S"))
            self.duck_time = max(self.duck_time, checked_time)
            self._has_duck_time = True
            logger.info(f"REST funding poll produced no rows; duck_time={self.duck_time}")
            return

        max_time = frame["funding_time"].max()
        self._client.write_funding_rates(frame, max_time)
        self.duck_time = max(self.duck_time, _utc_timestamp(max_time))
        logger.info(f"REST funding poll wrote {len(frame)} rows; duck_time={self.duck_time}")
