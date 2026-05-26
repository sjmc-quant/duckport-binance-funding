from __future__ import annotations

from typing import Any

import pandas as pd


FUNDING_COLUMNS = [
    "funding_time",
    "symbol",
    "funding_rate",
    "mark_price",
]


def normalize_funding_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=FUNDING_COLUMNS)
    for col in FUNDING_COLUMNS:
        if col not in df:
            df[col] = pd.NA
    out = df[FUNDING_COLUMNS].copy()
    out["funding_time"] = pd.to_datetime(out["funding_time"], utc=True).dt.floor("h")
    out["funding_rate"] = pd.to_numeric(out["funding_rate"], errors="coerce")
    out["mark_price"] = pd.to_numeric(out["mark_price"], errors="coerce")
    out = out.dropna(subset=["funding_time", "symbol", "funding_rate"])
    out["symbol"] = out["symbol"].astype(str)
    out = out.drop_duplicates(subset=["funding_time", "symbol"], keep="last")
    out = out.sort_values(["funding_time", "symbol"]).reset_index(drop=True)
    return out


def _series(df: pd.DataFrame, name: str, default):
    if name in df:
        return df[name]
    return pd.Series([default] * len(df), index=df.index)


def parse_rest_funding_history(rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=FUNDING_COLUMNS)
    df = pd.DataFrame.from_records(rows)
    out = pd.DataFrame(
        {
            "funding_time": pd.to_datetime(df["fundingTime"].astype("int64"), unit="ms", utc=True),
            "symbol": df["symbol"].astype(str),
            "funding_rate": pd.to_numeric(df["fundingRate"], errors="coerce"),
            "mark_price": pd.to_numeric(_series(df, "markPrice", pd.NA), errors="coerce"),
        }
    )
    return normalize_funding_frame(out)
