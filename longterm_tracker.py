import pandas as pd
import yfinance as yf
from datetime import datetime

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
SCAN_FILE = BASE_DIR / "outputs/local/longterm/longterm_scan.csv"
TRACK_FILE = BASE_DIR / "outputs/local/longterm/longterm_tracker.csv"


def track_targets():
    if not SCAN_FILE.exists():
        print("No scan file found.")
        return

    df = pd.read_csv(SCAN_FILE)

    if df.empty:
        print("No data to track.")
        return

    records = []

    for _, row in df.iterrows():
        symbol = row["symbol"]
        entry = row.get("current")
        t1 = row.get("target_1")
        t2 = row.get("target_2")
        t3 = row.get("target_3")

        try:
            hist = yf.Ticker(symbol).history(period="30d", auto_adjust=False)
        except Exception:
            continue

        if hist is None or hist.empty:
            continue

        highs = hist["High"].dropna()

        hit_t1 = any(highs >= t1) if pd.notna(t1) else False
        hit_t2 = any(highs >= t2) if pd.notna(t2) else False
        hit_t3 = any(highs >= t3) if pd.notna(t3) else False

        best_high = highs.max() if not highs.empty else None
        best_return = ((best_high - entry) / entry) * 100 if entry and best_high else None

        records.append({
            "date_checked": datetime.now().strftime("%Y-%m-%d"),
            "symbol": symbol,
            "entry_price": entry,
            "target_1": t1,
            "target_2": t2,
            "target_3": t3,
            "hit_target_1": hit_t1,
            "hit_target_2": hit_t2,
            "hit_target_3": hit_t3,
            "best_high": round(best_high, 2) if best_high else None,
            "best_return_pct": round(best_return, 2) if best_return else None,
        })

    out_df = pd.DataFrame(records)

    if TRACK_FILE.exists():
        existing = pd.read_csv(TRACK_FILE)
        out_df = pd.concat([existing, out_df], ignore_index=True)

    out_df.to_csv(TRACK_FILE, index=False)

print(f"📊 Tracker updated: {TRACK_FILE}")

if __name__ == "__main__":
        track_targets()