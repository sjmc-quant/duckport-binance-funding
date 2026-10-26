import json
import math
import os
import threading
from contextlib import nullcontext
from datetime import timezone
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.flight as flight

from binance_funding.component.funding import (
    FUNDING_COLUMNS,
)
from binance_funding.config import (
    DUCKPORT_APPEND_MAX_MESSAGE_BYTES,
    FUNDING_TABLE,
)
from binance_funding.utils.log_kit import logger


def _normalize_flight_location(addr: str) -> str:
    if addr.startswith("grpc://") or addr.startswith("grpc+tls://"):
        return addr
    return f"grpc://{addr}"


def _resolve_token(token: Optional[str]) -> str:
    if token is not None:
        return token
    return os.getenv("DUCKPORT_TOKEN", "")


class _BearerMiddleware(flight.ClientMiddleware):
    def __init__(self, token: str) -> None:
        self._value = f"Bearer {token}".encode()

    def sending_headers(self):
        return {b"authorization": self._value}


class _BearerFactory(flight.ClientMiddlewareFactory):
    def __init__(self, token: str) -> None:
        self._token = token

    def start_call(self, info):
        return _BearerMiddleware(self._token)


def _open_flight_client(location: str, token: str, **kwargs):
    if token:
        kwargs["middleware"] = [_BearerFactory(token)]
    return flight.FlightClient(location, **kwargs)


class DuckportClient:
    FUNDING_COL_DEFS = [
        "funding_time TIMESTAMP",
        "symbol VARCHAR",
        "funding_rate DOUBLE",
        "mark_price DOUBLE",
    ]
    FUNDING_ARROW_SCHEMA = pa.schema([
        ("funding_time", pa.timestamp("ns")),
        ("symbol", pa.string()),
        ("funding_rate", pa.float64()),
        ("mark_price", pa.float64()),
    ])

    _KEEPALIVE_OPTIONS = [
        ("grpc.keepalive_time_ms", 30_000),
        ("grpc.keepalive_timeout_ms", 10_000),
        ("grpc.keepalive_permit_without_calls", 1),
        ("grpc.http2.max_pings_without_data", 0),
    ]

    def __init__(self, addr: str = "localhost:50051", schema: str = "data", token: Optional[str] = None):
        self.location = _normalize_flight_location(addr)
        self._token = _resolve_token(token)
        self.schema = schema
        self.ingestor = (
            os.getenv("INGESTOR_NAME")
            or os.getenv("INGESTOR_INSTANCE")
            or os.getenv("INGESTOR_EXEC")
            or "binance-funding"
        )
        self.max_lag_seconds = int(os.getenv("WATERMARK_MAX_LAG_SECONDS", str(9 * 3600)))
        self.append_max_message_bytes = int(
            os.getenv("DUCKPORT_APPEND_MAX_MESSAGE_BYTES", str(DUCKPORT_APPEND_MAX_MESSAGE_BYTES))
        )
        self._write_lock = threading.Lock()
        self.client = self._connect()
        logger.info(
            f"DuckportClient connected: {self.location}, schema={schema}, "
            f"append_max_message_bytes={self.append_max_message_bytes}"
        )

    @staticmethod
    def _sql_str(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    def _reconnect(self):
        try:
            self.client.close()
        except Exception:
            pass
        self.client = self._connect()
        logger.warning(f"DuckportClient reconnected: {self.location}")

    def _connect(self):
        return _open_flight_client(
            self.location,
            self._token,
            generic_options=self._KEEPALIVE_OPTIONS,
        )

    def _append_max_rows_per_message(self, data: pa.Table, max_rows: Optional[int] = None) -> int:
        if len(data) == 0:
            return 1
        bytes_per_row = max(1, math.ceil(data.nbytes / len(data)))
        rows = max(1, self.append_max_message_bytes // bytes_per_row)
        return min(rows, max_rows) if max_rows is not None else rows

    def _write_context(self):
        lock = getattr(self, "_write_lock", None)
        return lock if lock is not None else nullcontext()

    def ping(self) -> dict:
        action = flight.Action("duckport.ping", b"")
        results = list(self.client.do_action(action))
        return json.loads(results[0].body.to_pybytes())

    def execute(self, sql: str) -> dict:
        body = json.dumps({"sql": sql}).encode("utf-8")
        action = flight.Action("duckport.execute", body)
        try:
            results = list(self.client.do_action(action))
        except flight.FlightUnavailableError:
            self._reconnect()
            results = list(self.client.do_action(action))
        return json.loads(results[0].body.to_pybytes())

    def execute_transaction(self, statements: list[str]) -> dict:
        body = json.dumps({"statements": statements}).encode("utf-8")
        action = flight.Action("duckport.execute_transaction", body)
        try:
            results = list(self.client.do_action(action))
        except flight.FlightUnavailableError:
            self._reconnect()
            results = list(self.client.do_action(action))
        return json.loads(results[0].body.to_pybytes())

    def append(self, schema: str, table: str, data: pa.Table) -> dict:
        descriptor = flight.FlightDescriptor.for_path("duckport.append", schema, table)
        try:
            writer, reader = self.client.do_put(descriptor, data.schema)
        except flight.FlightUnavailableError:
            self._reconnect()
            writer, reader = self.client.do_put(descriptor, data.schema)
        max_chunksize = self._append_max_rows_per_message(data)
        writer.write_table(data, max_chunksize=max_chunksize)
        writer.done_writing()
        buf = reader.read()
        writer.close()
        return json.loads(bytes(buf))

    def read_table(self, schema: str, table: str) -> pa.Table:
        ticket_json = json.dumps({"schema": schema, "table": table}).encode("utf-8")
        try:
            reader = self.client.do_get(flight.Ticket(ticket_json))
        except flight.FlightUnavailableError:
            self._reconnect()
            reader = self.client.do_get(flight.Ticket(ticket_json))
        return reader.read_all()

    def init_schema(self, start_date=None):
        s = self.schema
        start_ts = f"'{start_date} 00:00:00'" if start_date else "NULL"
        funding_cols = ", ".join(self.FUNDING_COL_DEFS)
        stmts = [
            f"CREATE SCHEMA IF NOT EXISTS {s}",
            f"CREATE TABLE IF NOT EXISTS {s}.config_dict (key VARCHAR PRIMARY KEY, value VARCHAR)",
        ]
        stmts += [
            # No PRIMARY KEY: DuckDB 1.5 never reclaims deleted rows of indexed tables,
            # and the composite key is never used by queries. Writes dedupe via anti-join.
            f"CREATE TABLE IF NOT EXISTS {s}.{FUNDING_TABLE} ({funding_cols})",
            f"ALTER TABLE IF EXISTS {s}.{FUNDING_TABLE} DROP COLUMN IF EXISTS funding_interval_hours",
            f"ALTER TABLE IF EXISTS {s}.{FUNDING_TABLE} DROP COLUMN IF EXISTS source",
            f"DROP TABLE IF EXISTS _staging_{FUNDING_TABLE}",
            f"CREATE TABLE IF NOT EXISTS _staging_{FUNDING_TABLE} ({funding_cols})",
            # Tells `duckport migrate nopk` this table's writer no longer needs a key.
            f"INSERT OR REPLACE INTO {s}.config_dict (key, value) "
            f"VALUES ('write_mode:{FUNDING_TABLE}', 'anti_join')",
            f"INSERT INTO {s}.watermark (table_name, ingestor, max_lag_seconds, time_column, start_time, duck_time, updated_at) "
            f"VALUES ('{FUNDING_TABLE}', {self._sql_str(self.ingestor)}, {self.max_lag_seconds}, 'funding_time', {start_ts}, NULL, CURRENT_TIMESTAMP) "
            f"ON CONFLICT (table_name) DO UPDATE SET "
            f"ingestor = excluded.ingestor, max_lag_seconds = excluded.max_lag_seconds, "
            f"time_column = excluded.time_column",
        ]
        self.execute_transaction(stmts)
        logger.info(f"Schema '{s}' initialised for {FUNDING_TABLE}")

    @staticmethod
    def _strip_tz(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        out = df.copy()
        for col in columns:
            if col in out and pd.api.types.is_datetime64_any_dtype(out[col]):
                out[col] = pd.to_datetime(out[col], utc=True).dt.tz_localize(None)
        return out

    def _funding_arrow(self, df: pd.DataFrame) -> pa.Table:
        sub = df[FUNDING_COLUMNS].copy()
        sub = self._strip_tz(sub, ["funding_time"])
        sub["symbol"] = sub["symbol"].astype(str)
        sub["funding_rate"] = pd.to_numeric(sub["funding_rate"], errors="coerce")
        sub["mark_price"] = pd.to_numeric(sub["mark_price"], errors="coerce")
        return pa.Table.from_pandas(sub, schema=self.FUNDING_ARROW_SCHEMA, preserve_index=False)

    @staticmethod
    def _funding_insert_sql(target: str, staging: str, data: pa.Table) -> Optional[str]:
        # Insert-only semantics: existing (funding_time, symbol) rows are kept as-is.
        # Matches the kline ingestor so all duckport ingestors share one idempotent write
        # contract. ``data`` must be what was appended (see _prepare), so its bounds are
        # naive UTC like the stored column. DISTINCT ON drops duplicates inside the batch;
        # the constant bounds let zonemaps limit the existing-key scan to the batch's own
        # time range. Rows without funding_time are not written (the old key rejected them).
        lo = pc.min(data["funding_time"]).as_py() if len(data) else None
        hi = pc.max(data["funding_time"]).as_py() if len(data) else None
        if lo is None or hi is None:
            return None
        return (
            f"INSERT INTO {target} "
            f"SELECT s.* FROM (SELECT DISTINCT ON (funding_time, symbol) * FROM {staging} "
            f"WHERE funding_time IS NOT NULL) s "
            f"ANTI JOIN (SELECT funding_time, symbol FROM {target} "
            f"WHERE funding_time BETWEEN TIMESTAMP '{lo:%Y-%m-%d %H:%M:%S.%f}' "
            f"AND TIMESTAMP '{hi:%Y-%m-%d %H:%M:%S.%f}') t "
            f"ON s.funding_time = t.funding_time AND s.symbol = t.symbol"
        )

    @staticmethod
    def _prepare(data: pa.Table) -> pa.Table:
        """Naive-UTC funding_time, sorted by time (tight zonemaps)."""
        col = data["funding_time"]
        if pa.types.is_timestamp(col.type) and col.type.tz is not None:
            idx = data.schema.get_field_index("funding_time")
            data = data.set_column(idx, "funding_time", pc.cast(col, pa.timestamp(col.type.unit)))
        return data.sort_by([("funding_time", "ascending"), ("symbol", "ascending")])

    def write_funding_rates(self, df: pd.DataFrame, current_time) -> dict:
        if df.empty:
            return {"total_rows": 0}
        s = self.schema
        staging = f"_staging_{FUNDING_TABLE}"
        arrow = self._prepare(self._funding_arrow(df))
        ts = pd.to_datetime(current_time).strftime("%Y-%m-%d %H:%M:%S")
        with self._write_context():
            self.execute(f"TRUNCATE {staging}")
            append_resp = self.append("main", staging, arrow)
            merge = self._funding_insert_sql(f"{s}.{FUNDING_TABLE}", staging, arrow)
            tx_resp = self.execute_transaction(([merge] if merge else []) + [
                f"INSERT INTO {s}.watermark (table_name, ingestor, max_lag_seconds, time_column, start_time, duck_time, updated_at) "
                f"VALUES ('{FUNDING_TABLE}', {self._sql_str(self.ingestor)}, {self.max_lag_seconds}, 'funding_time', NULL, '{ts}', CURRENT_TIMESTAMP) "
                f"ON CONFLICT (table_name) DO UPDATE SET "
                f"ingestor = excluded.ingestor, max_lag_seconds = excluded.max_lag_seconds, "
                f"duck_time = excluded.duck_time, updated_at = excluded.updated_at",
                f"TRUNCATE {staging}",
            ])
        logger.info(
            f"write_funding_rates: staged={append_resp.get('rows_appended', '?')}, "
            f"tx={tx_resp.get('rows_affected')}"
        )
        return tx_resp

    def bulk_write_funding_rates(
        self,
        arrow_table: pa.Table,
        duck_time_str: str,
        chunk_size: int = 200_000,
        *,
        sync_duck_time: bool = True,
    ) -> dict:
        s = self.schema
        staging = f"_staging_{FUNDING_TABLE}"
        arrow_table = self._prepare(arrow_table)
        total = len(arrow_table)
        row_chunk_size = self._append_max_rows_per_message(arrow_table, chunk_size)
        with self._write_context():
            for offset in range(0, total, row_chunk_size):
                length = min(row_chunk_size, total - offset)
                chunk = arrow_table.slice(offset, length)
                merge = self._funding_insert_sql(f"{s}.{FUNDING_TABLE}", staging, chunk)
                if merge is None:
                    continue
                self.execute(f"TRUNCATE {staging}")
                self.append("main", staging, chunk)
                self.execute_transaction([merge, f"TRUNCATE {staging}"])
                logger.info(
                    f"bulk_write_funding_rates: chunk {offset}-{offset + length} / {total} "
                    f"(rows_per_chunk={row_chunk_size})"
                )
            if sync_duck_time:
                self.replace_duck_time(duck_time_str)
        return {"total_rows": total}

    def replace_duck_time(self, duck_time_str: str) -> None:
        s = self.schema
        self.execute_transaction([
            f"INSERT INTO {s}.watermark (table_name, ingestor, max_lag_seconds, time_column, start_time, duck_time, updated_at) "
            f"VALUES ('{FUNDING_TABLE}', {self._sql_str(self.ingestor)}, {self.max_lag_seconds}, 'funding_time', NULL, '{duck_time_str}', CURRENT_TIMESTAMP) "
            f"ON CONFLICT (table_name) DO UPDATE SET "
            f"ingestor = excluded.ingestor, max_lag_seconds = excluded.max_lag_seconds, "
            f"duck_time = excluded.duck_time, updated_at = excluded.updated_at",
        ])

    def read_duck_time(self) -> Optional[pd.Timestamp]:
        try:
            table = self.read_table(self.schema, "watermark")
            df = table.to_pandas()
            row = df[df["table_name"] == FUNDING_TABLE]
            if row.empty:
                return None
            val = row["duck_time"].iloc[0]
            if pd.isna(val):
                return None
            ts = pd.to_datetime(val)
            if ts.tzinfo is None:
                return ts.tz_localize(timezone.utc)
            return ts.tz_convert(timezone.utc)
        except Exception as exc:
            logger.warning(f"read_duck_time({FUNDING_TABLE}): {exc}")
            return None

    def read_watermark_row(self) -> dict[str, Optional[pd.Timestamp]]:
        try:
            table = self.read_table(self.schema, "watermark")
            df = table.to_pandas()
            row = df[df["table_name"] == FUNDING_TABLE]
            if row.empty:
                return {"duck_time": None, "start_time": None}

            def _ts(col):
                val = row[col].iloc[0]
                if pd.isna(val):
                    return None
                ts = pd.to_datetime(val)
                return ts.tz_localize(timezone.utc) if ts.tzinfo is None else ts.tz_convert(timezone.utc)

            return {"duck_time": _ts("duck_time"), "start_time": _ts("start_time")}
        except Exception as exc:
            logger.warning(f"read_watermark_row: {exc}")
            return {"duck_time": None, "start_time": None}

    def close(self):
        self.client.close()
        logger.info("DuckportClient closed")
