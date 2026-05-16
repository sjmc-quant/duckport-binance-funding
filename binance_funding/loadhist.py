"""Historical funding-rate bulk loader for binance-funding."""

from __future__ import annotations

import asyncio
import os
import re
import zipfile
from datetime import date, datetime, timedelta, timezone
from glob import glob
from hashlib import sha256
from xml.etree import ElementTree as ET

import aiohttp
import duckdb
import pandas as pd

try:
    from tqdm import tqdm
except ModuleNotFoundError:
    class tqdm:
        def __init__(self, iterable=None, total=None, **kwargs):
            self.iterable = iterable

        def __iter__(self):
            return iter(self.iterable or [])

        def update(self, n=1):
            return None

        def close(self):
            return None

from binance_funding.api.restful import BinanceFundingRestApi
from binance_funding.component.funding import FUNDING_COLUMNS, normalize_funding_frame, parse_rest_funding_history
from binance_funding.config import (
    BASE_URL,
    DUCKPORT_ADDR,
    DUCKPORT_SCHEMA,
    FUNDING_REST_BOUNDARY_DAYS,
    FUNDING_TABLE,
    PARQUET_DIR,
    RESOURCE_PATH,
    S3_LIST_URL,
    START_DATE,
    create_download_semaphore,
    proxy,
)
from binance_funding.duckport_client import DuckportClient
from binance_funding.utils import async_retry_getter, create_aiohttp_session
from binance_funding.utils.log_kit import divider, logger

BOUNDARY_DAYS = int(os.getenv("LOADHIST_BOUNDARY_DAYS", str(FUNDING_REST_BOUNDARY_DAYS)))


def _xml_children(root, name: str):
    ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
    return root.findall(f"{ns}{name}") or root.findall(name)


def _xml_child_text(root, name: str) -> str | None:
    ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
    node = root.find(f"{ns}{name}")
    if node is None:
        node = root.find(name)
    return node.text if node is not None else None


def _month_from_key(key: str) -> str | None:
    match = re.search(r"fundingRate-(\d{4}-\d{2})\.zip", key)
    return match.group(1) if match else None


def _month_bounds() -> tuple[str, str]:
    start_month = START_DATE.strftime("%Y-%m")
    current_month = date.today().replace(day=1).strftime("%Y-%m")
    return start_month, current_month


def _sql_path(path: str) -> str:
    return os.path.abspath(path).replace("\\", "/").replace("'", "''")


def _local_download_dir(symbol: str) -> str:
    return os.path.join(RESOURCE_PATH, "futures", "um", "monthly", "fundingRate", symbol)


def _parquet_dir() -> str:
    return os.path.join(PARQUET_DIR, FUNDING_TABLE)


async def _s3_list(prefix: str) -> ET.Element:
    params = {"delimiter": "/", "prefix": prefix}
    async with create_aiohttp_session(30) as session:
        async with session.get(S3_LIST_URL, params=params, proxy=proxy) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise RuntimeError(f"S3 list failed {resp.status}: {text[:200]}")
            return ET.fromstring(text.encode("utf-8"))


def list_usdt_funding_symbols() -> list[str]:
    async def run():
        root = await _s3_list("data/futures/um/monthly/fundingRate/")
        symbols = []
        for item in _xml_children(root, "CommonPrefixes"):
            prefix = _xml_child_text(item, "Prefix")
            if not prefix:
                continue
            symbol = prefix.rstrip("/").split("/")[-1]
            if symbol.endswith("USDT"):
                symbols.append(symbol)
        return sorted(set(symbols))

    return asyncio.run(run())


def build_monthly_download_list(symbols: list[str]) -> list[dict]:
    start_month, current_month = _month_bounds()

    async def list_symbol(symbol: str):
        root = await _s3_list(f"data/futures/um/monthly/fundingRate/{symbol}/")
        result = []
        for item in _xml_children(root, "Contents"):
            key = _xml_child_text(item, "Key")
            if not key or not key.endswith(".zip.CHECKSUM"):
                continue
            month = _month_from_key(key)
            if month is None or month < start_month or month >= current_month:
                continue
            modified = _xml_child_text(item, "LastModified") or ""
            try:
                last_modified = datetime.strptime(modified, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()
            except ValueError:
                last_modified = 0
            result.append(
                {
                    "symbol": symbol,
                    "key": key,
                    "last_modified": last_modified,
                    "local_path": _local_download_dir(symbol),
                }
            )
        return result

    async def run():
        sem = asyncio.Semaphore(20)

        async def guarded(symbol: str):
            async with sem:
                try:
                    return await list_symbol(symbol)
                except Exception as exc:
                    logger.warning(f"S3 list failed for {symbol}: {exc}")
                    return []

        tasks = [guarded(symbol) for symbol in symbols]
        out = []
        for coro in asyncio.as_completed(tasks):
            out.extend(await coro)
        return out

    return asyncio.run(run())


async def _download_one(session: aiohttp.ClientSession, param: dict, sem, errors: set[str], pbar) -> None:
    key = param["key"]
    os.makedirs(param["local_path"], exist_ok=True)
    checksum_url = f"{BASE_URL}{key}"
    checksum_path = os.path.join(param["local_path"], os.path.basename(key))
    zip_path = checksum_path[:-9]

    if os.path.exists(checksum_path) and os.path.exists(zip_path):
        if os.path.getmtime(checksum_path) >= param["last_modified"]:
            pbar.update(1)
            return

    async with sem:
        for attempt in range(6):
            try:
                async with session.get(checksum_url, proxy=proxy) as resp:
                    checksum_body = await resp.read()
                    if resp.status != 200:
                        raise RuntimeError(f"checksum HTTP {resp.status}")
                with open(checksum_path, "wb") as f:
                    f.write(checksum_body)

                async with session.get(checksum_url[:-9], proxy=proxy) as resp:
                    zip_body = await resp.read()
                    if resp.status != 200:
                        raise RuntimeError(f"zip HTTP {resp.status}")
                with open(zip_path, "wb") as f:
                    f.write(zip_body)

                correct_sum = checksum_body.decode("utf-8").split(" ")[0].strip()
                if sha256(zip_body).hexdigest().lower() != correct_sum:
                    raise RuntimeError("checksum mismatch")
                pbar.update(1)
                return
            except Exception as exc:
                if attempt == 5:
                    errors.add(f"{zip_path}: {exc}")
                await asyncio.sleep(min(15, 2 ** attempt))


def download_files(download_list: list[dict]) -> set[str]:
    errors: set[str] = set()
    pbar = tqdm(total=len(download_list), ncols=80, mininterval=0.5)

    async def run():
        sem = create_download_semaphore()
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
            tasks = [_download_one(session, param, sem, errors, pbar) for param in download_list]
            await asyncio.gather(*tasks)

    try:
        asyncio.run(run())
    finally:
        pbar.close()
    return errors


def _zip_to_frame(path: str) -> pd.DataFrame:
    symbol = os.path.basename(path).split("-fundingRate-")[0]
    with zipfile.ZipFile(path) as zf:
        csv_names = [name for name in zf.namelist() if name.endswith(".csv")]
        if not csv_names:
            return pd.DataFrame(columns=FUNDING_COLUMNS)
        with zf.open(csv_names[0]) as f:
            raw = pd.read_csv(f)
    if raw.empty:
        return pd.DataFrame(columns=FUNDING_COLUMNS)
    frame = pd.DataFrame(
        {
            "funding_time": pd.to_datetime(raw["calc_time"].astype("int64"), unit="ms", utc=True),
            "symbol": symbol,
            "funding_rate": pd.to_numeric(raw["last_funding_rate"], errors="coerce"),
            "mark_price": pd.NA,
            "funding_interval_hours": pd.to_numeric(
                raw["funding_interval_hours"]
                if "funding_interval_hours" in raw
                else pd.Series([8] * len(raw), index=raw.index),
                errors="coerce",
            ).fillna(8).astype("int64"),
            "source": "hist",
        }
    )
    return normalize_funding_frame(frame)


def convert_zips_to_parquet() -> None:
    os.makedirs(_parquet_dir(), exist_ok=True)
    files = sorted(glob(os.path.join(RESOURCE_PATH, "futures", "um", "monthly", "fundingRate", "*", "*.zip")))
    converted = skipped = failed = 0
    for fp in tqdm(files, desc="convert funding zips"):
        month = _month_from_key(fp)
        if not month:
            continue
        symbol = os.path.basename(fp).split("-fundingRate-")[0]
        out_path = os.path.join(_parquet_dir(), f"{symbol}_{month}.parquet")
        marker = out_path + ".ok"
        if os.path.exists(out_path) and os.path.exists(marker):
            try:
                if float(open(marker).read().strip()) == os.path.getmtime(fp):
                    skipped += 1
                    continue
            except Exception:
                pass
        try:
            frame = _zip_to_frame(fp)
            if frame.empty:
                continue
            frame = frame.copy()
            frame["funding_time"] = frame["funding_time"].dt.tz_localize(None)
            tmp = out_path + ".tmp"
            frame.to_parquet(tmp, index=False)
            os.replace(tmp, out_path)
            with open(marker, "w") as f:
                f.write(str(os.path.getmtime(fp)))
            converted += 1
        except Exception as exc:
            failed += 1
            logger.warning(f"Failed to convert {fp}: {exc}")
    logger.info(f"Parquet conversion done: converted={converted}, skipped={skipped}, failed={failed}")


def save_to_duckport(client: DuckportClient) -> None:
    existing_dt = client.read_duck_time()
    ed = existing_dt.replace(tzinfo=None) if existing_dt else None
    boundary = ed - timedelta(days=BOUNDARY_DAYS) if ed else None
    files = sorted(glob(os.path.join(_parquet_dir(), "*.parquet")))
    if not files:
        logger.warning("No funding parquet files to import")
        return

    logger.info(f"funding: duck_time={ed} boundary={boundary} files={len(files)}")
    conn = duckdb.connect()
    new_max = None
    total_rows = 0
    try:
        for fp in files:
            fp_esc = _sql_path(fp)
            row = conn.execute(f"SELECT max(funding_time) FROM read_parquet('{fp_esc}')").fetchone()
            if row is None or row[0] is None:
                continue
            fm = row[0].replace(tzinfo=None)
            if boundary is not None and fm < boundary:
                continue
            where = f"WHERE funding_time >= '{boundary:%Y-%m-%d %H:%M:%S}'" if boundary else ""
            arrow = conn.execute(f"""
                SELECT funding_time, symbol, funding_rate, mark_price,
                       funding_interval_hours, source
                FROM read_parquet('{fp_esc}')
                {where}
                ORDER BY funding_time, symbol
            """).fetch_arrow_table()
            if len(arrow) == 0:
                continue
            client.bulk_write_funding_rates(arrow, "", sync_duck_time=False)
            total_rows += len(arrow)
            if new_max is None or fm > new_max:
                new_max = fm
            logger.info(f"{os.path.basename(fp)}: +{len(arrow):,} rows")
    finally:
        conn.close()

    if new_max is not None:
        client.replace_duck_time(new_max.strftime("%Y-%m-%d %H:%M:%S"))
        logger.info(f"Funding parquet import done: {total_rows:,} rows, duck_time -> {new_max}")
    else:
        logger.info(f"No new parquet rows, duck_time remains {ed}")


async def _exchange_symbols(api: BinanceFundingRestApi) -> list[str]:
    from binance_funding.component.funding import usdt_perpetual_symbols

    info = await async_retry_getter(api.exchange_info)
    return usdt_perpetual_symbols(info)


async def _rest_backfill_symbol(api: BinanceFundingRestApi, symbol: str, start_time, end_time) -> pd.DataFrame:
    start_ms = int(start_time.timestamp() * 1000)
    end_ms = int(end_time.timestamp() * 1000)
    frames = []
    while start_ms <= end_ms:
        rows = await async_retry_getter(
            api.funding_rate_history,
            symbol=symbol,
            startTime=start_ms,
            endTime=end_ms,
            limit=1000,
        )
        if not rows:
            break
        frames.append(parse_rest_funding_history(rows, 8))
        last_time = max(int(row["fundingTime"]) for row in rows)
        if len(rows) < 1000 or last_time + 1 <= start_ms:
            break
        start_ms = last_time + 1
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=FUNDING_COLUMNS)


def rest_backfill_current_gap(client: DuckportClient) -> None:
    duck_time = client.read_duck_time()
    start_time = duck_time - timedelta(days=BOUNDARY_DAYS) if duck_time else pd.Timestamp(START_DATE).tz_localize(timezone.utc)
    end_time = pd.Timestamp.now(tz="UTC")

    async def run():
        async with create_aiohttp_session(30) as session:
            api = BinanceFundingRestApi(session)
            symbols = await _exchange_symbols(api)
            sem = asyncio.Semaphore(10)

            async def guarded(symbol: str):
                async with sem:
                    try:
                        return await _rest_backfill_symbol(api, symbol, start_time, end_time)
                    except Exception as exc:
                        logger.warning(f"REST backfill failed for {symbol}: {exc}")
                        return pd.DataFrame(columns=FUNDING_COLUMNS)

            frames = []
            for coro in asyncio.as_completed([guarded(symbol) for symbol in symbols]):
                frame = await coro
                if not frame.empty:
                    frames.append(frame)
            return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=FUNDING_COLUMNS)

    frame = asyncio.run(run())
    if frame.empty:
        logger.info("REST current-gap backfill produced no rows")
        return
    max_time = frame["funding_time"].max()
    client.write_funding_rates(frame, max_time)
    logger.info(f"REST current-gap backfill wrote {len(frame):,} rows through {max_time}")


def main():
    divider("loadhist funding")
    client = DuckportClient(DUCKPORT_ADDR, DUCKPORT_SCHEMA)
    client.ping()
    client.init_schema(start_date=START_DATE)

    logger.info("Step 1: list symbols")
    symbols = list_usdt_funding_symbols()
    logger.info(f"Found {len(symbols)} USDT funding symbols")

    logger.info("Step 2: build download list")
    download_list = build_monthly_download_list(symbols)
    logger.info(f"Monthly funding files to check: {len(download_list)}")

    logger.info("Step 3: download")
    errors = download_files(download_list)
    if errors:
        logger.warning(f"Download completed with {len(errors)} errors")
        for err in sorted(errors)[:20]:
            logger.warning(err)

    logger.info("Step 4: convert to parquet")
    convert_zips_to_parquet()

    logger.info("Step 5: import parquet to duckport")
    save_to_duckport(client)

    logger.info("Step 6: REST current-month/gap backfill")
    rest_backfill_current_gap(client)

    client.close()
    logger.info(f"loadhist complete. Parquet files retained in {os.path.abspath(_parquet_dir())}")


if __name__ == "__main__":
    main()
