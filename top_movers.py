# top_movers.py
import yfinance as yf
import pandas as pd
import requests
from io import StringIO
import time

TOP_N = 50  # default top movers to track

def fetch_sp500_tickers():
    try:
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0 Safari/537.36"
        }
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        tables = pd.read_html(StringIO(r.text))
        table = tables[0]
        tickers = table["Symbol"].tolist()
        tickers = [t.replace(".", "-") for t in tickers]  # BRK.B → BRK-B
        print(f"✅ Fetched {len(tickers)} S&P 500 tickers")
        return tickers
    except Exception as e:
        print("⚠️ Failed to fetch S&P 500 tickers:", e)
        return ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META"]


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
    # Shape A: columns like (symbol, 'Close') if group_by='ticker'
    # Shape B: columns like ('Close', symbol) if group_by='column'
    if isinstance(download_df.columns, pd.MultiIndex):
        # try (symbol, field)
        try:
            part = download_df[symbol].copy()
            # expected columns now like Open/High/Low/Close...
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

    # If not MultiIndex but still batched, sometimes yfinance returns dict-like columns
    try:
        part = download_df[symbol].copy()
        if isinstance(part, pd.DataFrame) and "Close" in part.columns:
            return part
    except Exception:
        pass

    return pd.DataFrame()


def calculate_top_movers(tickers, top_n=TOP_N):
    all_movers = []
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
                    auto_adjust=False
                )
                break
            except Exception as e:
                attempt += 1
                print(f"⚠️ Batch fetch failed (attempt {attempt}/{max_retries}): {e}")
                time.sleep(1)

        if df is None or df.empty:
            print(f"❌ Skipping batch after {max_retries} failed attempts")
            continue

        for symbol in batch:
            try:
                ticker_df = _get_ticker_df(df, symbol, batch_len=len(batch))
                if ticker_df.empty or "Close" not in ticker_df.columns:
                    continue

                # Drop NaNs and ensure enough rows
                ticker_df = ticker_df.dropna(subset=["Close"])
                if len(ticker_df) < 5:
                    continue

                # indices for week/month lookbacks
                idx_20 = min(len(ticker_df) - 1, 20)
                idx_5 = min(len(ticker_df) - 1, 5)

                close_today = float(ticker_df["Close"].iloc[-1])
                close_yesterday = float(ticker_df["Close"].iloc[-2])
                close_5days = float(ticker_df["Close"].iloc[-(idx_5 + 1)])
                close_20days = float(ticker_df["Close"].iloc[-(idx_20 + 1)])

                # Guard divide by zero
                if close_yesterday == 0 or close_5days == 0 or close_20days == 0:
                    continue

                pct_change_day = ((close_today - close_yesterday) / close_yesterday) * 100
                pct_change_week = ((close_today - close_5days) / close_5days) * 100
                pct_change_month = ((close_today - close_20days) / close_20days) * 100

                day_vol = abs(close_today - close_yesterday) / close_yesterday
                weekly_vol = abs(close_today - close_5days) / close_5days

                day_trade = "✅ Preferable" if pct_change_day > 1 else "⚠️ Moderate"
                week_trade = "✅ Preferable" if pct_change_week > 2 else "⚠️ Moderate"
                month_trade = "✅ Preferable" if pct_change_month > 5 else "⚠️ Moderate"

                if day_vol > 0.03 or weekly_vol > 0.05:
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
                    "month_trade": month_trade
                })

            except Exception as e:
                print(f"⚠️ Failed {symbol}: {e}")
                continue

    all_movers.sort(key=lambda x: abs(x["pct_change"]), reverse=True)
    return all_movers[:top_n]