from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo
import yfinance as yf
import pandas as pd

LOCAL_TZ = ZoneInfo("America/Chicago")

def fetch(symbol: str, start: str, end: str, interval: str = "5m") -> pd.DataFrame:
    h = yf.Ticker(symbol).history(
        start=start,
        end=end,
        interval=interval,
        auto_adjust=False,
        prepost=False,
    )
    if h is None or h.empty:
        return pd.DataFrame()

    idx = pd.to_datetime(h.index, errors="coerce")
    # yfinance often returns tz-aware index (UTC or US/Eastern). Normalize to CT.
    if getattr(idx, "tz", None) is None:
        idx = idx.tz_localize(LOCAL_TZ)
    else:
        idx = idx.tz_convert(LOCAL_TZ)

    h = h.copy()
    h.index = idx
    h = h.sort_index()
    return h

if __name__ == "__main__":
    symbol = "LYV"
    run_date = "2026-03-02"
    start = run_date
    end = "2026-03-03"

    h = fetch(symbol, start, end, interval="5m")

    print("rows:", len(h))
    if not h.empty:
        print("index tz:", h.index.tz)
        print("first ts:", h.index[0])
        print("last ts:", h.index[-1])
        print(h[["Open", "High", "Low", "Close"]].head(3))
        print(h[["Open", "High", "Low", "Close"]].tail(3))