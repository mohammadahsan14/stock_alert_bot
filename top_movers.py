# top_movers.py
from __future__ import annotations

import os
import time
from io import StringIO
from datetime import datetime, timedelta

import pandas as pd
import requests
import yfinance as yf

TOP_N = 50  # default top movers to track

OUTPUT_DIR = "outputs"
os.makedirs(OUTPUT_DIR, exist_ok=True)
TICKER_CACHE_PATH = os.path.join(OUTPUT_DIR, "sp500_tickers_cache.csv")
TICKER_CACHE_MAX_AGE_HOURS = 24


def _cache_is_fresh(path: str, max_age_hours: int) -> bool:
    if not os.path.exists(path):
        return False
    mtime = datetime.fromtimestamp(os.path.getmtime(path))
    return (datetime.now() - mtime) <= timedelta(hours=max_age_hours)


def fetch_sp500_tickers() -> list[str]:
    """
    Robust ticker fetch:
      1) use local cache if fresh
      2) try Wikipedia and refresh cache
      3) fallback to old cache if exists
      4) final fallback to small safe list
    """
    # 1) fresh cache
    if _cache_is_fresh(TICKER_CACHE_PATH, TICKER_CACHE_MAX_AGE_HOURS):
        try:
            df = pd.read_csv(TICKER_CACHE_PATH)
            tickers = df["Symbol"].astype(str).tolist()
            tickers = [t.replace(".", "-") for t in tickers]
            print(f"✅ Loaded {len(tickers)} S&P 500 tickers (cache)")
            return tickers
        except Exception:
            pass

    # 2) Wikipedia fetch
    try:
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
            )
        }
        r = requests.get(url, headers=headers, timeout=12)
        r.raise_for_status()

        tables = pd.read_html(StringIO(r.text))
        table = tables[0]
        tickers = table["Symbol"].astype(str).tolist()
        tickers = [t.replace(".", "-") for t in tickers]  # BRK.B → BRK-B

        # save cache
        try:
            table[["Symbol"]].to_csv(TICKER_CACHE_PATH, index=False)
        except Exception:
            pass

        print(f"✅ Fetched {len(tickers)} S&P 500 tickers (Wikipedia)")
        return tickers

    except Exception as e:
        print("⚠️ Failed to fetch S&P 500 tickers from Wikipedia:", e)

    # 3) old cache fallback
    if os.path.exists(TICKER_CACHE_PATH):
        try:
            df = pd.read_csv(TICKER_CACHE_PATH)
            tickers = df["Symbol"].astype(str).tolist()
            tickers = [t.replace(".", "-") for t in tickers]
            print(f"✅ Loaded {len(tickers)} S&P 500 tickers (stale cache fallback)")
            return tickers
        except Exception:
            pass

    # 4) final fallback
    return ["AAPL", "MSFT", "AMZN", "GOOGL", "META", "NVDA", "TSLA"]


def _get_ticker_df(download_df: pd.DataFrame, symbol: str, batch_len: int) -> pd.DataFrame:
    """
    yfinance can return:
      - Single ticker: columns = Open/High/Low/Close/Adj Close/Volume
      - Multi ticker: columns can be MultiIndex or grouped by ticker.
    This helper returns a clean OHLCV df for a single symbol or empty df if missing.
    """
    if download_df is None or download_df.empty:
        return pd.DataFrame()

    # Single ticker request often returns a flat df
    if batch_len == 1 and all(col in download_df.columns for col in ["Open", "High", "Low", "Close"]):
        return download_df.copy()

    # MultiIndex case: try both shapes
    if isinstance(download_df.columns, pd.MultiIndex):
        # try (symbol, field)
        try:
            part = download_df[symbol].copy()
            if "Close" in part.columns:
                return part
        except Exception:
            pass

        # try (field, symbol) -> swap levels
        try:
            swapped = download_df.swaplevel(axis=1)
            part = swapped[symbol].copy()
            if "Close" in part.columns:
                return part
        except Exception:
            pass

    # Non-multiindex but still batched sometimes
    try:
        part = download_df[symbol].copy()
        if isinstance(part, pd.DataFrame) and "Close" in part.columns:
            return part
    except Exception:
        pass

    return pd.DataFrame()


def _safe_last_close_series(ticker_df: pd.DataFrame) -> pd.Series:
    if ticker_df is None or ticker_df.empty or "Close" not in ticker_df.columns:
        return pd.Series(dtype="float64")
    s = pd.to_numeric(ticker_df["Close"], errors="coerce").dropna()
    return s


def calculate_top_movers(tickers: list[str], top_n: int = TOP_N) -> list[dict]:
    """
    Computes movers using last two trading closes (not calendar days).
    Also computes 5-day and 20-day changes when enough history exists.
    """
    all_movers: list[dict] = []
    batch_size = 50
    max_retries = 3

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        attempt = 0
        df = None

        while attempt < max_retries:
            try:
                df = yf.download(
                    batch,
                    period="30d",
                    group_by="ticker",
                    progress=False,
                    threads=True,
                    auto_adjust=False,
                )
                break
            except Exception as e:
                attempt += 1
                print(f"⚠️ Batch fetch failed (attempt {attempt}/{max_retries}): {e}")
                time.sleep(1.0)

        if df is None or df.empty:
            print(f"❌ Skipping batch after {max_retries} failed attempts")
            continue

        for symbol in batch:
            try:
                ticker_df = _get_ticker_df(df, symbol, batch_len=len(batch))
                close = _safe_last_close_series(ticker_df)
                if close.empty or len(close) < 2:
                    continue

                # last two trading closes
                close_today = float(close.iloc[-1])
                close_prev = float(close.iloc[-2])
                if close_prev == 0:
                    continue

                pct_change_day = ((close_today - close_prev) / close_prev) * 100

                # 5 trading days back (if available)
                if len(close) >= 6:
                    close_5 = float(close.iloc[-6])
                    pct_change_week = ((close_today - close_5) / close_5) * 100 if close_5 else 0.0
                else:
                    pct_change_week = 0.0

                # 20 trading days back (if available)
                if len(close) >= 21:
                    close_20 = float(close.iloc[-21])
                    pct_change_month = ((close_today - close_20) / close_20) * 100 if close_20 else 0.0
                else:
                    pct_change_month = 0.0

                # vol proxy
                day_vol = abs(pct_change_day) / 100.0
                week_vol = abs(pct_change_week) / 100.0

                day_trade = "✅ Preferable" if pct_change_day > 1 else "⚠️ Moderate"
                week_trade = "✅ Preferable" if pct_change_week > 2 else "⚠️ Moderate"
                month_trade = "✅ Preferable" if pct_change_month > 5 else "⚠️ Moderate"

                if day_vol > 0.03 or week_vol > 0.05:
                    risk = "High"
                elif day_vol > 0.015:
                    risk = "Medium"
                else:
                    risk = "Low"

                if pct_change_day > 1:
                    mover_signal = "✅ CAN CONSIDER BUY"
                elif pct_change_day < -1:
                    mover_signal = "❌ AVOID / WATCH"
                else:
                    mover_signal = "⚠️ NEUTRAL"

                all_movers.append({
                    "symbol": symbol,
                    "current": close_today,
                    "pct_change": pct_change_day,
                    "risk": risk,
                    "mover_signal": mover_signal,
                    "day_trade": day_trade,
                    "week_trade": week_trade,
                    "month_trade": month_trade,
                    # Helpful debug fields (won't break your main.py)
                    # "as_of_close": str(close.index[-1].date()) if hasattr(close.index[-1], "date") else "",
                })

            except Exception as e:
                print(f"⚠️ Failed {symbol}: {e}")
                continue

    # Sort by absolute daily move
    all_movers.sort(key=lambda x: abs(x.get("pct_change", 0.0)), reverse=True)
    return all_movers[:top_n]