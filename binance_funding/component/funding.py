from __future__ import annotations

import os
import re
from collections.abc import Iterable
from typing import Any

import pandas as pd


FUNDING_COLUMNS = [
    "funding_time",
    "symbol",
    "funding_rate",
    "mark_price",
    "funding_interval_hours",
    "source",
]

def chunked(items: list[str], size: int) -> Iterable[list[str]]:
    for idx in range(0, len(items), size):
        yield items[idx:idx + size]


def normalize_funding_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=FUNDING_COLUMNS)
    out = df[FUNDING_COLUMNS].copy()
    out["symbol"] = out["symbol"].astype(str)
    out["funding_time"] = pd.to_datetime(out["funding_time"], utc=True)
    out["funding_rate"] = pd.to_numeric(out["funding_rate"], errors="coerce")
    out["mark_price"] = pd.to_numeric(out["mark_price"], errors="coerce")
    out["funding_interval_hours"] = pd.to_numeric(
        out["funding_interval_hours"], errors="coerce"
    ).fillna(8).astype("int64")
    out["source"] = out["source"].astype(str)
    out = out.dropna(subset=["funding_time", "symbol", "funding_rate"])
    out = out.drop_duplicates(subset=["funding_time", "symbol"], keep="last")
    out = out.sort_values(["funding_time", "symbol"]).reset_index(drop=True)
    return out


def _series(df: pd.DataFrame, name: str, default):
    if name in df:
        return df[name]
    return pd.Series([default] * len(df), index=df.index)


def symbol_from_funding_path(path: str) -> str:
    name = os.path.basename(path)
    match = re.match(r"(.+?)-fundingRate-\d{4}-\d{2}\.csv$", name)
    if match:
        return match.group(1)
    match = re.match(r"(.+?)-fundingRate-\d{4}-\d{2}\.zip$", name)
    if match:
        return match.group(1)
    return os.path.basename(os.path.dirname(path)).upper()


def parse_funding_csv(path: str, symbol: str | None = None) -> pd.DataFrame:
    sym = symbol or symbol_from_funding_path(path)
    raw = pd.read_csv(path)
    if raw.empty:
        return pd.DataFrame(columns=FUNDING_COLUMNS)
    df = pd.DataFrame(
        {
            "funding_time": pd.to_datetime(raw["calc_time"].astype("int64"), unit="ms", utc=True),
            "symbol": sym,
            "funding_rate": pd.to_numeric(raw["last_funding_rate"], errors="coerce"),
            "mark_price": pd.NA,
            "funding_interval_hours": pd.to_numeric(
                _series(raw, "funding_interval_hours", 8), errors="coerce"
            ).fillna(8).astype("int64"),
            "source": "hist",
        }
    )
    return normalize_funding_frame(df)


def parse_rest_funding_history(rows: list[dict[str, Any]], interval_hours: int = 8) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=FUNDING_COLUMNS)
    df = pd.DataFrame.from_records(rows)
    out = pd.DataFrame(
        {
            "funding_time": pd.to_datetime(df["fundingTime"].astype("int64"), unit="ms", utc=True),
            "symbol": df["symbol"].astype(str),
            "funding_rate": pd.to_numeric(df["fundingRate"], errors="coerce"),
            "mark_price": pd.to_numeric(_series(df, "markPrice", pd.NA), errors="coerce"),
            "funding_interval_hours": interval_hours,
            "source": "rest",
        }
    )
    return normalize_funding_frame(out)


def parse_mark_price_event(message: dict[str, Any]) -> dict[str, Any] | None:
    data = message.get("data", message)
    if data.get("e") != "markPriceUpdate":
        return None
    return {
        "symbol": str(data["s"]),
        "event_time": pd.to_datetime(int(data["E"]), unit="ms", utc=True),
        "mark_price": float(data["p"]),
        "last_funding_rate": float(data["r"]),
        "next_funding_time": pd.to_datetime(int(data["T"]), unit="ms", utc=True),
    }


def ws_funding_frame(rows: list[dict[str, Any]], interval_hours: int = 8) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=FUNDING_COLUMNS)
    df = pd.DataFrame.from_records(rows)
    out = pd.DataFrame(
        {
            "funding_time": pd.to_datetime(df["next_funding_time"], utc=True),
            "symbol": df["symbol"].astype(str),
            "funding_rate": pd.to_numeric(df["last_funding_rate"], errors="coerce"),
            "mark_price": pd.to_numeric(df["mark_price"], errors="coerce"),
            "funding_interval_hours": interval_hours,
            "source": "ws",
        }
    )
    return normalize_funding_frame(out)


def usdt_perpetual_symbols(exchange_info: dict[str, Any]) -> list[str]:
    symbols = []
    for info in exchange_info.get("symbols", []):
        if info.get("quoteAsset") != "USDT":
            continue
        if info.get("contractType") != "PERPETUAL":
            continue
        if info.get("status") != "TRADING":
            continue
        symbols.append(info["symbol"])
    return sorted(symbols)
