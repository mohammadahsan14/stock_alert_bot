import yfinance as yf
import pandas as pd
import requests
from io import StringIO
import time

TOP_N = 20  # default top movers to track

# -----------------------------
# Fetch S&P 500 tickers safely
# -----------------------------
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
        tickers = table['Symbol'].tolist()
        tickers = [t.replace('.', '-') for t in tickers]  # BRK.B → BRK-B
        print(f"✅ Fetched {len(tickers)} S&P 500 tickers")
        return tickers
    except Exception as e:
        print("⚠️ Failed to fetch S&P 500 tickers:", e)
        return ["AAPL","MSFT","GOOGL","AMZN","TSLA","META"]

# -----------------------------
# Calculate Top Movers using batch download with retry
# -----------------------------
def calculate_top_movers(tickers, top_n=TOP_N):
    all_movers = []
    batch_size = 50  # split tickers into smaller chunks
    max_retries = 3

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i+batch_size]
        attempt = 0
        while attempt < max_retries:
            try:
                df = yf.download(batch, period="30d", group_by="ticker", progress=False, threads=True)
                break
            except Exception as e:
                attempt += 1
                print(f"⚠️ Batch fetch failed (attempt {attempt}/{max_retries}): {e}")
                time.sleep(1)
        else:
            print(f"❌ Skipping batch {batch} after {max_retries} failed attempts")
            continue

        for symbol in batch:
            try:
                # Handle single ticker batch differently
                ticker_df = df[symbol] if len(batch) > 1 else df
                if ticker_df.empty or len(ticker_df) < 5:
                    continue

                # Indices for day/week/month
                idx_20 = min(len(ticker_df)-1, 20)
                idx_5 = min(len(ticker_df)-1, 5)

                close_today = ticker_df['Close'].iloc[-1].item()
                close_yesterday = ticker_df['Close'].iloc[-2].item()
                close_5days = ticker_df['Close'].iloc[-(idx_5+1)].item()
                close_20days = ticker_df['Close'].iloc[-(idx_20+1)].item()

                pct_change_day = ((close_today - close_yesterday)/close_yesterday)*100
                pct_change_week = ((close_today - close_5days)/close_5days)*100
                pct_change_month = ((close_today - close_20days)/close_20days)*100

                day_vol = abs(close_today - close_yesterday)/close_yesterday
                weekly_vol = abs(close_today - close_5days)/close_5days

                # Multi-horizon decisions
                day_trade = "✅ Preferable" if pct_change_day>1 else "⚠️ Moderate"
                week_trade = "✅ Preferable" if pct_change_week>2 else "⚠️ Moderate"
                month_trade = "✅ Preferable" if pct_change_month>5 else "⚠️ Moderate"

                # Risk assessment
                if day_vol>0.03 or weekly_vol>0.05:
                    risk="High"
                elif day_vol>0.015:
                    risk="Medium"
                else:
                    risk="Low"

                # Base decision
                if pct_change_day > 1:
                    decision="✅ CAN CONSIDER BUY"
                elif pct_change_day < -1:
                    decision="❌ AVOID / WATCH"
                else:
                    decision="⚠️ NEUTRAL"

                all_movers.append({
                    "symbol": symbol,
                    "current": close_today,
                    "pct_change": pct_change_day,
                    "risk": risk,
                    "decision": decision,
                    "day_trade": day_trade,
                    "week_trade": week_trade,
                    "month_trade": month_trade
                })

            except Exception as e:
                print(f"⚠️ Failed {symbol}: {e}")
                continue

    # Sort by absolute daily change
    all_movers.sort(key=lambda x: abs(x['pct_change']), reverse=True)
    return all_movers[:top_n]