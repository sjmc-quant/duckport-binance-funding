import asyncio
import gc
import threading
from datetime import timedelta, timezone

import pandas as pd

from binance_funding.api.restful import BinanceFundingRestApi
from binance_funding.api.ws import MarkPriceWebsocket
from binance_funding.component.funding import (
    parse_mark_price_events,
    parse_rest_funding_history,
    usdt_perpetual_symbols,
    ws_funding_frame,
)
from binance_funding.config import (
    FETCH_CONCURRENCY,
    FUNDING_REST_BOUNDARY_DAYS,
    GENESIS_TIME,
    START_DATE,
    WS_FLUSH_SECONDS,
    WS_SAMPLE_BEFORE_FUNDING_SECONDS,
)
from binance_funding.duckport_client import DuckportClient
from binance_funding.utils import (
    async_retry_getter,
    create_aiohttp_session,
)
from binance_funding.utils.log_kit import divider, logger


class FundingFetcher:
    def __init__(self, api: BinanceFundingRestApi):
        self.api = api

    async def symbols(self) -> list[str]:
        info = await async_retry_getter(self.api.exchange_info)
        return usdt_perpetual_symbols(info)

    async def funding_history_for_symbol(
        self,
        symbol: str,
        start_time: pd.Timestamp,
        end_time: pd.Timestamp,
        interval_hours: int = 8,
    ) -> pd.DataFrame:
        start_ms = int(start_time.timestamp() * 1000)
        end_ms = int(end_time.timestamp() * 1000)
        frames = []
        while start_ms <= end_ms:
            rows = await async_retry_getter(
                self.api.funding_rate_history,
                symbol=symbol,
                startTime=start_ms,
                endTime=end_ms,
                limit=1000,
            )
            if not rows:
                break
            frame = parse_rest_funding_history(rows, interval_hours)
            if not frame.empty:
                frames.append(frame)
            last_time = max(int(row["fundingTime"]) for row in rows)
            next_start = last_time + 1
            if next_start <= start_ms or len(rows) < 1000:
                break
            start_ms = next_start
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)


class FundingDataJobs:
    def __init__(self, client: DuckportClient):
        self._client = client
        self.duck_time = self._init_duck_time()
        self.symbols: set[str] = set()
        self._ws_lock = threading.Lock()
        self._pending_ws_rows: list[dict] = []
        self._emitted_ws_keys: set[tuple[str, pd.Timestamp]] = set()
        logger.info(f"Funding duck_time loaded: {self.duck_time}")
        self.start()

    def _init_duck_time(self) -> pd.Timestamp:
        dt = self._client.read_duck_time()
        if dt and dt != GENESIS_TIME:
            return dt
        return pd.to_datetime(START_DATE).tz_localize(timezone.utc)

    def start(self) -> None:
        threading.Thread(target=self._run_ws_loop, daemon=True).start()

    def _run_ws_loop(self):
        async def run():
            while True:
                try:
                    await self.run_ws()
                except Exception as exc:
                    logger.error(f"Funding websocket failed: {exc}; running REST compensation and retrying in 10s")
                    try:
                        await self.catch_up_rest()
                    except Exception as rest_exc:
                        logger.error(f"REST compensation after WS failure failed: {rest_exc}")
                    gc.collect()
                    await asyncio.sleep(10)

        asyncio.run(run())

    async def _fetch_symbols(self, fetcher: FundingFetcher) -> list[str]:
        symbols = await fetcher.symbols()
        self.symbols = set(symbols)
        logger.info(f"Fetched {len(symbols)} USDT perpetual symbols")
        return symbols

    async def catch_up_rest(self):
        boundary = self.duck_time - timedelta(days=FUNDING_REST_BOUNDARY_DAYS)
        start_time = max(boundary, pd.to_datetime(START_DATE).tz_localize(timezone.utc))
        end_time = pd.Timestamp.now(tz="UTC")
        divider(f"Funding REST catch-up {start_time} -> {end_time}", display_time=False)

        async with create_aiohttp_session(30) as session:
            fetcher = FundingFetcher(BinanceFundingRestApi(session))
            symbols = await self._fetch_symbols(fetcher)
            sem = asyncio.Semaphore(FETCH_CONCURRENCY)

            async def one(symbol: str):
                async with sem:
                    try:
                        return await fetcher.funding_history_for_symbol(symbol, start_time, end_time)
                    except Exception as exc:
                        logger.warning(f"funding history failed for {symbol}: {exc}")
                        return pd.DataFrame()

            frames = []
            for coro in asyncio.as_completed([one(symbol) for symbol in symbols]):
                frame = await coro
                if not frame.empty:
                    frames.append(frame)

        if not frames:
            logger.info("REST funding catch-up produced no rows")
            return
        df = pd.concat(frames, ignore_index=True)
        max_time = df["funding_time"].max()
        df = df[df["funding_time"] <= end_time]
        self._client.write_funding_rates(df, max_time)
        self.duck_time = max(self.duck_time, pd.to_datetime(max_time).tz_convert(timezone.utc))
        logger.info(f"REST funding catch-up wrote {len(df)} rows; duck_time={self.duck_time}")

    async def run_ws(self):
        async with create_aiohttp_session(20) as session:
            fetcher = FundingFetcher(BinanceFundingRestApi(session))
            await self._fetch_symbols(fetcher)

        await asyncio.gather(
            self._listen_all_market_ws(),
            self._ws_flush_loop(),
        )

    async def _listen_all_market_ws(self):
        socket = MarkPriceWebsocket()
        async for message in socket.listen():
            rows = parse_mark_price_events(message)
            for row in rows:
                if row["symbol"] not in self.symbols:
                    continue
                if not self._should_sample_ws_row(row):
                    continue
                key = (row["symbol"], row["next_funding_time"])
                with self._ws_lock:
                    if key in self._emitted_ws_keys:
                        continue
                    self._emitted_ws_keys.add(key)
                    self._pending_ws_rows.append(row)

    @staticmethod
    def _should_sample_ws_row(row: dict) -> bool:
        delta = (row["next_funding_time"] - row["event_time"]).total_seconds()
        return 0 <= delta <= WS_SAMPLE_BEFORE_FUNDING_SECONDS

    async def _ws_flush_loop(self):
        while True:
            await asyncio.sleep(max(1, WS_FLUSH_SECONDS))
            with self._ws_lock:
                rows = list(self._pending_ws_rows)
                self._pending_ws_rows.clear()
            frame = ws_funding_frame(rows)
            if frame.empty:
                continue
            max_time = frame["funding_time"].max()
            self._client.write_funding_rates(frame, max_time)
            self.duck_time = max(self.duck_time, pd.to_datetime(max_time).tz_convert(timezone.utc))
            logger.info(f"Flushed {len(frame)} pre-settlement WS funding rows")
