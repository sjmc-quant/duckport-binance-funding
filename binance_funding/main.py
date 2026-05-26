#!/usr/bin/env python3
import os
import platform
import signal
import sys

from binance_funding.utils.log_kit import divider, logger

os.environ["TZ"] = "UTC"


def main():
    if platform.system() == "Windows":
        import asyncio

        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    from binance_funding.config import (
        DUCKPORT_ADDR,
        DUCKPORT_SCHEMA,
        START_DATE,
    )
    from binance_funding.data_jobs import FundingDataJobs
    from binance_funding.duckport_client import DuckportClient

    divider("Binance Funding starting")

    client = DuckportClient(addr=DUCKPORT_ADDR, schema=DUCKPORT_SCHEMA)

    def signal_handler(signum, frame):
        logger.critical("Received exit signal, shutting down...")
        client.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        info = client.ping()
        logger.info(f"duckport-rs server: {info}")
    except Exception as exc:
        logger.error(f"Cannot reach duckport-rs at {DUCKPORT_ADDR}: {exc}")
        sys.exit(1)

    client.init_schema(start_date=START_DATE)
    watermark = client.read_watermark_row()
    logger.info(f"Funding watermark: {watermark}")

    FundingDataJobs(client)

    try:
        signal.pause()
    except AttributeError:
        import threading

        threading.Event().wait()


if __name__ == "__main__":
    main()
