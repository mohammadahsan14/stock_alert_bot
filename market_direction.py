import yfinance as yf

def get_market_direction():
    indices = ["SPY", "QQQ"]
    result = []

    for idx in indices:
        try:
            df = yf.download(idx, period="2d", progress=False)
            if len(df) < 2:
                continue
            current = df['Close'].iloc[-1].item()
            previous = df['Close'].iloc[-2].item()
            percent_change = ((current - previous) / previous) * 100
            result.append({
                "name": idx,
                "symbol": idx,
                "change": round(percent_change, 2)
            })
        except Exception as e:
            print(f"⚠️ Failed to fetch market index {idx}: {e}")
            continue

    return result