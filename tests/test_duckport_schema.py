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
        self.assertIn("PRIMARY KEY (funding_time, symbol)", sql)
        self.assertNotIn(f"DROP TABLE IF EXISTS data.{FUNDING_TABLE}", sql)


if __name__ == "__main__":
    unittest.main()
