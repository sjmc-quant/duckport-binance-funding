"""Environment-driven configuration for binance-funding."""

import os
import pathlib
from datetime import date, datetime, timezone

from dotenv import load_dotenv

_env_file = os.getenv("INGESTOR_ENV_FILE", "config.env")
load_dotenv(_env_file, override=False)

_server_env = os.getenv(
    "DUCKPORT_SERVER_ENV",
    str(pathlib.Path.home() / ".duckport" / "server.env"),
)
load_dotenv(_server_env, override=False)
load_dotenv("/opt/duckport/server.env", override=False)


def _derive_duckport_addr() -> str:
    raw = os.getenv("DUCKPORT_LISTEN_ADDR", "0.0.0.0:50051")
    host, _, port = raw.rpartition(":")
    host = "localhost" if host in ("0.0.0.0", "", "*") else host
    return f"{host}:{port}"


def _parse_start_date() -> date:
    raw = os.getenv("START_DATE", "2009-01-03")
    return datetime.strptime(raw, "%Y-%m-%d").date()


DUCKPORT_ADDR = _derive_duckport_addr()
DUCKPORT_SCHEMA = "data"

FUNDING_MARKET = "usdt_perp_funding"
FUNDING_TABLE = "usdt_perp_funding"

DATA_SOURCES_STR = os.getenv("DATA_SOURCES", FUNDING_MARKET)
DATA_SOURCES = {s.strip() for s in DATA_SOURCES_STR.split(",") if s.strip()}

GENESIS_TIME = datetime(2009, 1, 3, tzinfo=timezone.utc)
START_DATE = _parse_start_date()

CONCURRENCY = max(1, int(os.getenv("CONCURRENCY", "2")))
FETCH_CONCURRENCY = min(CONCURRENCY, 10)
DOWNLOAD_CONCURRENCY = min(CONCURRENCY, 8)
WS_GROUP_SIZE = int(os.getenv("WS_GROUP_SIZE", "100"))

RESOURCE_PATH = os.getenv("RESOURCE_PATH", "/data/duckport/hist")
PARQUET_DIR = os.getenv("PARQUET_DIR", "/data/duckport/pqt")
BASE_URL = "https://data.binance.vision/"
S3_LIST_URL = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"

PROXY_URL = os.getenv("PROXY_URL", "")
proxy = PROXY_URL or None

ENABLE_WS = os.getenv("ENABLE_WS", "true").lower() == "true"
WS_FLUSH_SECONDS = int(os.getenv("WS_FLUSH_SECONDS", "10"))
WS_SAMPLE_BEFORE_FUNDING_SECONDS = int(os.getenv("WS_SAMPLE_BEFORE_FUNDING_SECONDS", "60"))
FUNDING_REST_BOUNDARY_DAYS = int(os.getenv("FUNDING_REST_BOUNDARY_DAYS", "3"))

DUCKPORT_APPEND_MAX_MESSAGE_BYTES = int(
    os.getenv("DUCKPORT_APPEND_MAX_MESSAGE_BYTES", str(48 * 1024 * 1024))
)
RETRY_TIMES = int(os.getenv("RETRY_TIMES", "5"))


def create_download_semaphore():
    import asyncio

    return asyncio.Semaphore(value=DOWNLOAD_CONCURRENCY)
