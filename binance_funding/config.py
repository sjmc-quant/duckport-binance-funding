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

FUNDING_TABLE = "usdt_perp_funding"

GENESIS_TIME = datetime(2009, 1, 3, tzinfo=timezone.utc)
START_DATE = _parse_start_date()

PROXY_URL = os.getenv("PROXY_URL", "")
proxy = PROXY_URL or None

FUNDING_POLL_DELAY_SECONDS = int(os.getenv("FUNDING_POLL_DELAY_SECONDS", "5"))
FUNDING_REQUEST_INTERVAL_SECONDS = float(os.getenv("FUNDING_REQUEST_INTERVAL_SECONDS", "0.2"))
FUNDING_RETRY_SLEEP_SECONDS = int(os.getenv("FUNDING_RETRY_SLEEP_SECONDS", "75"))

DUCKPORT_APPEND_MAX_MESSAGE_BYTES = int(
    os.getenv("DUCKPORT_APPEND_MAX_MESSAGE_BYTES", str(48 * 1024 * 1024))
)
RETRY_TIMES = int(os.getenv("RETRY_TIMES", "5"))
