import unittest

from binance_funding.config import FUNDING_TABLE
from binance_funding.duckport_client import DuckportClient


class FakeDuckportClient(DuckportClient):
    def __init__(self):
        self.schema = "data"
        self.ingestor = "binance-funding"
        self.max_lag_seconds = 9 * 3600
        self.transactions = []

    def execute_transaction(self, statements):
        self.transactions.append(statements)
        return {"ok": True}


class DuckportSchemaTests(unittest.TestCase):
    def test_init_schema_creates_required_tables_and_registers_watermark(self):
        client = FakeDuckportClient()
        client.init_schema(start_date="2024-01-01")
        sql = "\n".join(client.transactions[0])

        self.assertIn(f"CREATE TABLE IF NOT EXISTS data.{FUNDING_TABLE}", sql)
        self.assertIn("funding_time TIMESTAMP, symbol VARCHAR, funding_rate DOUBLE, mark_price DOUBLE", sql)
        self.assertIn(f"CREATE TABLE IF NOT EXISTS _staging_{FUNDING_TABLE}", sql)
        self.assertNotIn("CREATE TABLE IF NOT EXISTS data.watermark", sql)
        self.assertNotIn("ALTER TABLE data.watermark", sql)
        self.assertIn("DROP COLUMN IF EXISTS funding_interval_hours", sql)
        self.assertIn("DROP COLUMN IF EXISTS source", sql)
        self.assertNotIn("funding_snapshot", sql)
        self.assertNotIn("funding_info", sql)
        self.assertIn(
            "VALUES ('usdt_perp_funding', 'binance-funding', 32400, 'funding_time', '2024-01-01 00:00:00'",
            sql,
        )
        self.assertNotIn("PRIMARY KEY (funding_time, symbol)", sql)
        self.assertIn(f"('write_mode:{FUNDING_TABLE}', 'anti_join')", sql)
        self.assertNotIn(f"DROP TABLE IF EXISTS data.{FUNDING_TABLE}", sql)

    def test_insert_sql_dedupes_with_anti_join_within_batch_time_range(self):
        import pandas as pd
        import pyarrow as pa

        batch = pa.Table.from_pandas(pd.DataFrame({
            "funding_time": pd.to_datetime(["2024-01-01 08:00", "2024-01-01 00:00"]),
            "symbol": ["BTCUSDT", "ETHUSDT"],
            "funding_rate": [0.0001, 0.0002],
            "mark_price": [1.0, 2.0],
        }), preserve_index=False)
        sql = DuckportClient._funding_insert_sql("data.t", "_staging_t", batch)
        self.assertIn("DISTINCT ON (funding_time, symbol)", sql)
        self.assertIn("ANTI JOIN", sql)
        self.assertIn("BETWEEN TIMESTAMP '2024-01-01 00:00:00.000000' AND TIMESTAMP '2024-01-01 08:00:00.000000'", sql)
        self.assertNotIn("ON CONFLICT", sql)
        self.assertIsNone(DuckportClient._funding_insert_sql("data.t", "_staging_t", batch.slice(0, 0)))

    def test_insert_sql_skips_null_times_and_normalizes_time_zones(self):
        import pandas as pd
        import pyarrow as pa

        nulls = pa.table({
            "funding_time": pa.array([None, None], pa.timestamp("ns")),
            "symbol": ["BTCUSDT", "ETHUSDT"],
            "funding_rate": [0.1, 0.2],
            "mark_price": [1.0, 2.0],
        })
        self.assertIsNone(DuckportClient._funding_insert_sql("data.t", "_staging_t", nulls))

        local = pa.Table.from_pandas(pd.DataFrame({
            "funding_time": pd.to_datetime(["2024-01-01 16:00"]).tz_localize("Asia/Shanghai"),
            "symbol": ["BTCUSDT"], "funding_rate": [0.1], "mark_price": [1.0],
        }), preserve_index=False)
        prepared = DuckportClient._prepare(local)
        self.assertIsNone(prepared["funding_time"].type.tz)
        sql = DuckportClient._funding_insert_sql("data.t", "_staging_t", prepared)
        # 16:00 in Shanghai is 08:00 UTC
        self.assertIn("BETWEEN TIMESTAMP '2024-01-01 08:00:00.000000' AND TIMESTAMP '2024-01-01 08:00:00.000000'", sql)
        self.assertIn("WHERE funding_time IS NOT NULL", sql)


if __name__ == "__main__":
    unittest.main()
