import pandas as pd
import yfinance as yf

def get_market_direction():
    indices = ["SPY", "QQQ"]
    result = []

    for idx in indices:
        try:
            df = yf.download(
                tickers=idx,
                period="5d",          # give a little buffer for holidays/weekends
                progress=False,
                threads=False,
                auto_adjust=False,
            )

            if df is None or df.empty or "Close" not in df.columns:
                result.append({"name": idx, "symbol": idx, "change": None, "direction": "unknown"})
                continue

            close = pd.to_numeric(df["Close"], errors="coerce").dropna()
            if len(close) < 2:
                result.append({"name": idx, "symbol": idx, "change": None, "direction": "unknown"})
                continue

            current = float(close.iloc[-1])
            previous = float(close.iloc[-2])
            if previous == 0:
                result.append({"name": idx, "symbol": idx, "change": None, "direction": "unknown"})
                continue

            percent_change = ((current - previous) / previous) * 100.0
            change = round(percent_change, 2)

            # simple direction label
            if change > 0.2:
                direction = "up"
            elif change < -0.2:
                direction = "down"
            else:
                direction = "flat"

            result.append({
                "name": idx,
                "symbol": idx,
                "change": change,
                "direction": direction,
            })

        except Exception as e:
            print(f"⚠️ Failed to fetch market index {idx}: {e}")
            result.append({"name": idx, "symbol": idx, "change": None, "direction": "unknown"})

    return result