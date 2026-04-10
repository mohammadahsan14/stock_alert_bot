from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd
import yfinance as yf

from datetime import datetime

from top_movers import fetch_sp500_tickers
from price_action import compute_price_action_summary
from email_sender import send_email as _send_email
import os


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "outputs" / "local" / "longterm"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
PORTFOLIO_FILE = OUTPUT_DIR / "longterm_portfolio.csv"
APP_ENV = os.getenv("APP_ENV", "local")
IS_LOCAL = APP_ENV == "local"

SENDER_EMAIL = os.getenv("SENDER_EMAIL", "")
RECEIVER_EMAIL = os.getenv("RECEIVER_EMAIL", "")
LOCAL_RECEIVER_EMAIL = os.getenv("LOCAL_RECEIVER_EMAIL", "")
EMAIL_SUBJECT_PREFIX_LOCAL = os.getenv("EMAIL_SUBJECT_PREFIX_LOCAL", "[LOCAL TEST]")
EMAIL_SUBJECT_PREFIX_PROD = os.getenv("EMAIL_SUBJECT_PREFIX_PROD", "[PROD]")

EMAIL_SUBJECT_PREFIX = EMAIL_SUBJECT_PREFIX_LOCAL if IS_LOCAL else EMAIL_SUBJECT_PREFIX_PROD
EFFECTIVE_RECEIVER_EMAIL = (LOCAL_RECEIVER_EMAIL or RECEIVER_EMAIL) if IS_LOCAL else RECEIVER_EMAIL

MAX_POSITIONS = 3
MAX_PER_POSITION_PCT = 0.40


def _safe_float(x) -> Optional[float]:
    try:
        if x is None or pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None

def detect_candle_pattern(row: dict) -> str:
    body_pct = _safe_float(row.get("body_pct")) or 0.0
    upper_wick_pct = _safe_float(row.get("upper_wick_pct")) or 0.0
    lower_wick_pct = _safe_float(row.get("lower_wick_pct")) or 0.0
    candle_bias = str(row.get("candle_bias") or "unknown")

    if body_pct <= 0.01 and upper_wick_pct > 0 and lower_wick_pct > 0:
        return "Doji"

    if lower_wick_pct > (body_pct * 2.0) and candle_bias == "bullish":
        return "Hammer"

    if upper_wick_pct > (body_pct * 2.0) and candle_bias == "bearish":
        return "Shooting Star"

    if candle_bias == "bullish":
        return "Bullish Candle"
    if candle_bias == "bearish":
        return "Bearish Candle"
    return "Neutral Candle"

def build_longterm_plan(row: dict) -> dict:
    current = _safe_float(row.get("current"))
    market_cap = _safe_float(row.get("market_cap"))
    revenue_growth = _safe_float(row.get("revenue_growth"))
    earnings_growth = _safe_float(row.get("earnings_growth"))
    near_support = bool(row.get("near_support"))
    near_resistance = bool(row.get("near_resistance"))
    longterm_score = int(row.get("longterm_score", 0) or 0)

    if current is None or current <= 0:
        return {
            "long_target_price": None,
            "review_below": None,
            "holding_horizon": "unknown",
        }

    growth_strength = (
        (revenue_growth is not None and revenue_growth > 0.10) and
        (earnings_growth is not None and earnings_growth > 0.10)
    )

    if growth_strength and market_cap is not None and market_cap >= 100_000_000_000:
        target_mult = 1.20
        horizon = "12-18 months"
    elif growth_strength:
        target_mult = 1.25
        horizon = "6-12 months"
    elif market_cap is not None and market_cap >= 100_000_000_000:
        target_mult = 1.12
        horizon = "12-24 months"
    else:
        target_mult = 1.10
        horizon = "6-12 months"

    if near_resistance:
        target_mult = max(1.08, target_mult - 0.03)

    if near_support and longterm_score >= 10:
        target_mult += 0.02

    review_below = current * 0.92
    if longterm_score >= 10:
        review_below = current * 0.90

    return {
        "long_target_price": round(current * target_mult, 2),
        "review_below": round(review_below, 2),
        "holding_horizon": horizon,
    }

def build_multi_targets(row: dict) -> dict:
    current = _safe_float(row.get("current"))
    main_target = _safe_float(row.get("long_target_price"))

    if current is None or current <= 0 or main_target is None:
        return {
            "target_1": None,
            "target_2": None,
            "target_3": None,
        }

    # distance to main target
    move_pct = (main_target - current) / current

    # target 1 = 40% of move (early trim)
    t1 = current * (1 + move_pct * 0.4)

    # target 2 = your original
    t2 = main_target

    # target 3 = extension (runner)
    t3 = current * (1 + move_pct * 1.3)

    return {
        "target_1": round(t1, 2),
        "target_2": round(t2, 2),
        "target_3": round(t3, 2),
    }


def build_entry_plan(row: dict) -> dict:
    current = _safe_float(row.get("current"))
    review_below = _safe_float(row.get("review_below"))
    range_zone = str(row.get("range_zone") or "")
    candle_pattern = str(row.get("candle_pattern") or "")
    longterm_label = str(row.get("longterm_label") or "")

    if current is None or current <= 0:
        return {
            "entry_zone": "",
            "add_zone": "",
            "exit_rule": "",
        }

    # default buffers
    entry_low = current * 0.98
    entry_high = current * 1.01

    add_low = current * 0.92
    add_high = current * 0.97

    # if already near support, allow tighter / cleaner entry
    if range_zone == "near_support":
        entry_low = current * 0.99
        entry_high = current * 1.02
        add_low = current * 0.94
        add_high = current * 0.98

    # bullish candle / hammer = allow slightly more aggressive entry
    if candle_pattern in ["Hammer", "Bullish Candle"]:
        entry_low = current * 0.995
        entry_high = current * 1.02

    # HOLD names should be more conservative
    if longterm_label == "HOLD":
        entry_low = current * 0.97
        entry_high = current * 1.00
        add_low = current * 0.90
        add_high = current * 0.95

    if review_below is not None and review_below > 0:
        exit_rule = f"Below {review_below:.2f}"
    else:
        exit_rule = f"Below {(current * 0.90):.2f}"

    return {
        "entry_zone": f"{entry_low:.2f} - {entry_high:.2f}",
        "add_zone": f"{add_low:.2f} - {add_high:.2f}",
        "exit_rule": exit_rule,
    }

def decide_longterm_action(row: dict) -> dict:
    current = _safe_float(row.get("current"))
    target = _safe_float(row.get("long_target_price"))
    review_below = _safe_float(row.get("review_below"))

    score = int(row.get("longterm_score", 0) or 0)
    label = str(row.get("longterm_label") or "")
    entry_signal = str(row.get("entry_signal") or "")
    range_zone = str(row.get("range_zone") or "")
    candle_pattern = str(row.get("candle_pattern") or "")
    candle_bias = str(row.get("candle_bias") or "")

    if current is None or current <= 0:
        return {"final_action": "NO_ACTION", "action_confidence": 0}

    confidence = 0

    if label == "ACCUMULATE":
        confidence += 4
    elif label == "HOLD":
        confidence += 2

    if score >= 12:
        confidence += 3
    elif score >= 10:
        confidence += 2
    elif score >= 8:
        confidence += 1

    if range_zone == "near_support":
        confidence += 2
    elif range_zone == "mid_range":
        confidence -= 1
    elif range_zone == "near_resistance":
        confidence -= 2

    if entry_signal == "STRONG BUY":
        confidence += 2
    elif entry_signal == "BUY":
        confidence += 1

    if candle_pattern == "Hammer":
        confidence += 1
    elif candle_pattern == "Shooting Star":
        confidence -= 1

    if candle_bias == "bullish":
        confidence += 1
    elif candle_bias == "bearish":
        confidence -= 1

    confidence = max(1, min(confidence, 10))

    # final action
    if review_below is not None and current <= review_below:
        return {"final_action": "EXIT", "action_confidence": max(confidence, 8)}

    if target is not None and current >= target:
        return {"final_action": "TRIM", "action_confidence": max(confidence, 8)}

    if label == "ACCUMULATE" and range_zone == "near_support":
        if entry_signal == "STRONG BUY":
            return {"final_action": "BUY_NOW", "action_confidence": confidence}
        if entry_signal == "BUY":
            return {"final_action": "BUY_NOW", "action_confidence": confidence}

    if label == "ACCUMULATE" and range_zone == "mid_range":
        return {"final_action": "WAIT_PULLBACK", "action_confidence": confidence}

    if label == "ACCUMULATE" and range_zone == "near_resistance":
        return {"final_action": "WAIT", "action_confidence": confidence}

    if label == "HOLD":
        return {"final_action": "HOLD", "action_confidence": confidence}

    return {"final_action": "NO_ACTION", "action_confidence": confidence}

def add_allocations(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    out = df.copy()
    top = filter_buy_ready(out).head(4).copy()

    out["alloc_5k"] = 0.0
    out["alloc_10k"] = 0.0

    if top.empty:
        return out

    if len(top) == 1:
        weights = [1.0]
    elif len(top) == 2:
        weights = [0.6, 0.4]
    elif len(top) == 3:
        weights = [0.4, 0.35, 0.25]
    else:
        weights = [0.35, 0.25, 0.20, 0.20]

    for idx, w in zip(top.index, weights):
        out.at[idx, "alloc_5k"] = round(5000 * w, 2)
        out.at[idx, "alloc_10k"] = round(10000 * w, 2)

    return out

def filter_buy_ready(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    return df[
        (df["longterm_label"] == "ACCUMULATE") &
        (df["range_zone"] == "near_support")
    ].copy()



def add_to_portfolio(symbol: str, buy_price: float, target: float, review_below: float, qty: int = 1):
    import pandas as pd
    from datetime import datetime

    row = {
        "symbol": symbol,
        "buy_price": buy_price,
        "qty": qty,
        "buy_date": datetime.now().strftime("%Y-%m-%d"),
        "target": target,
        "review_below": review_below,
        "status": "HOLD",
    }

    if PORTFOLIO_FILE.exists():
        df = pd.read_csv(PORTFOLIO_FILE)
        existing_symbols = df["symbol"].astype(str).str.upper().str.strip().tolist()

        if str(symbol).upper().strip() in existing_symbols:
            print(f"⚠️ {symbol} already in portfolio — skipping")
            return

        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    else:
        df = pd.DataFrame([row])

    df.to_csv(PORTFOLIO_FILE, index=False)
    print(f"✅ Added {symbol} to portfolio")


def add_top_picks_to_portfolio(df: pd.DataFrame, total_capital: float = 5000):
    if df is None or df.empty:
        print("No data to add.")
        return

    df = filter_buy_ready(df)

    if df.empty:
        print("No BUY-ready stocks.")
        return

    picks = df.head(MAX_POSITIONS)

    per_stock_cap = min(
        total_capital / len(picks),
        total_capital * MAX_PER_POSITION_PCT
    )

    for _, row in picks.iterrows():
        symbol = row.get("symbol")
        price = _safe_float(row.get("current"))
        target = _safe_float(row.get("long_target_price"))
        review = _safe_float(row.get("review_below"))

        if not symbol or price is None or price <= 0:
            continue

        qty = int(per_stock_cap / price)

        if qty <= 0:
            continue

        add_to_portfolio(
            symbol=symbol,
            buy_price=price,
            target=target,
            review_below=review,
            qty=qty
        )

    print(f"✅ Added BUY-ready picks (max {MAX_POSITIONS})")


def track_portfolio() -> pd.DataFrame:
    if not PORTFOLIO_FILE.exists():
        print("No long-term portfolio file found.")
        return pd.DataFrame()

    df = pd.read_csv(PORTFOLIO_FILE)
    if df.empty:
        print("Portfolio is empty.")
        return df

    current_prices = []
    pnl_dollars = []
    pnl_pct = []
    statuses = []

    for _, row in df.iterrows():
        sym = str(row.get("symbol", "")).upper().strip()
        buy_price = _safe_float(row.get("buy_price"))
        qty = _safe_float(row.get("qty"))
        target = _safe_float(row.get("target"))
        review_below = _safe_float(row.get("review_below"))

        current = None
        try:
            hist = yf.Ticker(sym).history(period="5d", auto_adjust=False)
            if hist is not None and not hist.empty and "Close" in hist.columns:
                current = float(pd.to_numeric(hist["Close"], errors="coerce").dropna().iloc[-1])
        except Exception:
            current = None

        current_prices.append(current if current is not None else pd.NA)

        if current is not None and buy_price is not None and qty is not None:
            pnl_val = (current - buy_price) * qty
            pnl_pct_val = ((current - buy_price) / buy_price) * 100.0 if buy_price > 0 else None
        else:
            pnl_val = None
            pnl_pct_val = None

        pnl_dollars.append(round(pnl_val, 2) if pnl_val is not None else pd.NA)
        pnl_pct.append(round(pnl_pct_val, 2) if pnl_pct_val is not None else pd.NA)

        status = "HOLD"

        if current is not None:
            if target is not None and current >= target:
                status = "TARGET HIT"

            elif review_below is not None and current <= review_below:
                status = "REVIEW"

            elif buy_price is not None:
                drop_pct = ((current - buy_price) / buy_price) * 100

                if -5 <= drop_pct <= -2:
                    status = "ADD"  # sweet dip zone

                elif drop_pct < -5:
                    status = "WATCH"  # deeper dip, caution

        statuses.append(status)

    df["current_price"] = current_prices
    df["pnl_dollars"] = pnl_dollars
    df["pnl_pct"] = pnl_pct
    df["status"] = statuses
    df["last_checked"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df.to_csv(PORTFOLIO_FILE, index=False)
    print(f"Updated portfolio: {PORTFOLIO_FILE}")
    return df

def update_millionaire_tracker(monthly_add: float, cash_balance: float = 0.0):
    df = track_portfolio()

    if df is None or df.empty:
        print("No portfolio data")
        return

    # holdings value
    holdings_value = (df["current_price"] * df["qty"]).sum()

    # total portfolio = holdings + cash
    portfolio_value = float(holdings_value) + float(cash_balance)

    file = OUTPUT_DIR / "millionaire_tracker.csv"

    if file.exists():
        existing = pd.read_csv(file)
        total_invested = float(existing["total_invested"].iloc[-1]) + monthly_add
    else:
        # 🔥 IMPORTANT: your real starting capital
        initial_capital = 7391.92
        total_invested = initial_capital + monthly_add

    profit_pct = ((portfolio_value - total_invested) / total_invested) * 100 if total_invested > 0 else 0.0
    progress = (portfolio_value / 1_000_000) * 100

    row = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "total_invested": round(total_invested, 2),
        "holdings_value": round(holdings_value, 2),
        "cash_balance": round(float(cash_balance), 2),
        "portfolio_value": round(portfolio_value, 2),
        "monthly_add": round(monthly_add, 2),
        "profit_pct": round(profit_pct, 2),
        "progress_to_1m": round(progress, 4),
    }

    if file.exists():
        existing = pd.read_csv(file)
        existing = pd.concat([existing, pd.DataFrame([row])], ignore_index=True)
    else:
        existing = pd.DataFrame([row])

    existing.to_csv(file, index=False)

    print(f"💰 Progress to $1M: {row['progress_to_1m']}%")
    print(f"📁 Saved: {file}")


def build_portfolio_email(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return "<h2>📦 Long-Term Portfolio</h2><p>No holdings found.</p>"

    def _fmt_money(x):
        try:
            if pd.isna(x):
                return ""
            return f"{float(x):.2f}"
        except Exception:
            return ""

    def _fmt_pct(x):
        try:
            if pd.isna(x):
                return ""
            return f"{float(x):+.2f}%"
        except Exception:
            return ""

    def row_html(r):
        sym = str(r.get("symbol", "")).upper().strip()
        buy_price = _fmt_money(r.get("buy_price"))
        current_price = _fmt_money(r.get("current_price"))
        qty = str(r.get("qty", ""))
        target = _fmt_money(r.get("target"))
        review_below = _fmt_money(r.get("review_below"))
        pnl_dollars = _fmt_money(r.get("pnl_dollars"))
        pnl_pct = _fmt_pct(r.get("pnl_pct"))
        status = str(r.get("status", ""))
        buy_date = str(r.get("buy_date", ""))
        last_checked = str(r.get("last_checked", ""))

        return f"""
        <tr>
          <td><b>{sym}</b></td>
          <td>{buy_date}</td>
          <td>{qty}</td>
          <td>{buy_price}</td>
          <td>{current_price}</td>
          <td>{target}</td>
          <td>{review_below}</td>
          <td>{pnl_dollars}</td>
          <td>{pnl_pct}</td>
          <td>{status}</td>
          <td>{last_checked}</td>
        </tr>
        """

    rows = "\n".join([row_html(r) for _, r in df.iterrows()])

    return f"""
       
    <h2>📦 Long-Term Portfolio</h2>
    <p>Tracked holdings with current price, P/L, target, and review levels.</p>
    <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;font-family:Arial;font-size:13px;">
      <tr style="background:#eee;">
        <th>Symbol</th>
        <th>Buy Date</th>
        <th>Qty</th>
        <th>Buy Price</th>
        <th>Current</th>
        <th>Target</th>
        <th>Review Below</th>
        <th>P/L $</th>
        <th>P/L %</th>
        <th>Status</th>
        <th>Last Checked</th>
      </tr>
      {rows}
    </table>
    """
def send_portfolio_alerts() -> bool:
    df = track_portfolio()

    if df is None or df.empty:
        print("No holdings found for alerts.")
        return False

    alert_df = df[
        df["status"].astype(str).str.strip().isin(["ADD", "REVIEW", "TARGET HIT"])
    ].copy()

    if alert_df.empty:
        print("No actionable alerts.")
        return False

    html = build_portfolio_email(alert_df)
    return send_email("🚨 Portfolio Alerts", html)


def send_email(subject: str, html_body: str) -> bool:
    print("DEBUG: EMAIL_PREVIEW =", os.getenv("EMAIL_PREVIEW"))

    if os.getenv("EMAIL_PREVIEW", "0") == "1":
        preview_file = OUTPUT_DIR / "email_preview_longterm.html"
        preview_file.write_text(html_body, encoding="utf-8")
        print(f"✅ Email preview saved: {preview_file}")
        return True

    final_subject = f"{EMAIL_SUBJECT_PREFIX} {subject}"
    return _send_email(
        subject=final_subject,
        html_body=html_body,
        to_email=EFFECTIVE_RECEIVER_EMAIL,
        from_email=SENDER_EMAIL,
        attachment_path=None,
    )

def build_longterm_email(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return "<h2>📈 Long-Term Scan</h2><p>No rows found.</p>"

    top = filter_buy_ready(df).head(10).copy()

    def row_html(r):
        return f"""
        <tr>
          <td><b>{r.get('symbol', '')}</b></td>
          <td>{r.get('longterm_score', '')}</td>
          <td>{r.get('longterm_label', '')}</td>
          <td>{round(float(r.get('current', 0) or 0), 2) if pd.notna(r.get('current')) else ''}</td>
          <td>{round(float(r.get('long_target_price', 0) or 0), 2) if pd.notna(r.get('long_target_price')) else ''}</td>
          <td>{round(float(r.get('review_below', 0) or 0), 2) if pd.notna(r.get('review_below')) else ''}</td>
          <td>{r.get('holding_horizon', '')}</td>
          <td>{r.get('entry_signal', '')}</td>
          <td>{r.get('entry_zone', '')}</td>
          <td>{r.get('add_zone', '')}</td>
          <td>{r.get('exit_rule', '')}</td>
          <td>{r.get('final_action', '')}</td>
          <td>{r.get('action_confidence', '')}/10</td>
          <td>{r.get('range_zone', '')}</td>
          <td>{r.get('target_1', '')}</td>
          <td>{r.get('target_2', '')}</td>
          <td>{r.get('target_3', '')}</td>
          <td>{r.get('candle_pattern', '')}</td>
          <td>{round(float(r.get('alloc_5k', 0) or 0), 2) if pd.notna(r.get('alloc_5k')) and float(r.get('alloc_5k') or 0) > 0 else ''}</td>
          <td>{round(float(r.get('alloc_10k', 0) or 0), 2) if pd.notna(r.get('alloc_10k')) and float(r.get('alloc_10k') or 0) > 0 else ''}</td>
          <td>{r.get('longterm_reasons', '')}</td>
        </tr>
        """

    rows = "\n".join([row_html(r) for _, r in top.iterrows()])

    return f"""
    <h2>📈 Long-Term Scan</h2>
    <p>Top long-term candidates based on trend, fundamentals, relative strength, and price action.</p>
    <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;font-family:Arial;font-size:13px;">
    <tr style="background:#eee;">
      <th>Symbol</th>
      <th>Score</th>
      <th>Label</th>
      <th>Current</th>
      <th>Long Target</th>
      <th>Review Below</th>
      <th>Holding Horizon</th>
      <th>Entry Signal</th>
      <th>Entry Zone</th>
      <th>Add Zone</th>
      <th>Exit Rule</th>
      <th>Final Action</th>
      <th>Confidence</th>
      <th>Range Zone</th>
      <th>Target 1</th>
      <th>Target 2</th>
      <th>Target 3</th>
      <th>Candle Pattern</th>
      <th>$5K Alloc</th>
      <th>$10K Alloc</th>
      <th>Reasons</th>
    </tr>
      {rows}
    </table>
    """



def get_market_snapshot() -> dict:
    out = {"spy_change_6m_pct": None}
    try:
        spy = yf.Ticker("SPY").history(period="6mo", auto_adjust=False)
        if spy is not None and not spy.empty and len(spy) >= 2:
            start = float(spy["Close"].iloc[0])
            end = float(spy["Close"].iloc[-1])
            if start > 0:
                out["spy_change_6m_pct"] = ((end - start) / start) * 100.0
    except Exception:
        pass
    return out


def get_stock_snapshot(symbol: str) -> dict:
    t = yf.Ticker(symbol)

    info = {}
    try:
        info = t.info or {}
    except Exception:
        info = {}

    hist = pd.DataFrame()
    try:
        hist = t.history(period="1y", auto_adjust=False)
    except Exception:
        hist = pd.DataFrame()

    current = sma50 = sma200 = change_6m_pct = None

    if hist is not None and not hist.empty and "Close" in hist.columns:
        close = pd.to_numeric(hist["Close"], errors="coerce").dropna()
        if not close.empty:
            current = float(close.iloc[-1])
            if len(close) >= 50:
                sma50 = float(close.tail(50).mean())
            if len(close) >= 200:
                sma200 = float(close.tail(200).mean())

            if len(close) >= 120:
                start_6m = float(close.iloc[-120])
                end_6m = float(close.iloc[-1])
                if start_6m > 0:
                    change_6m_pct = ((end_6m - start_6m) / start_6m) * 100.0

    revenue_growth = _safe_float(info.get("revenueGrowth"))
    earnings_growth = _safe_float(info.get("earningsGrowth"))
    debt_to_equity = _safe_float(info.get("debtToEquity"))
    market_cap = _safe_float(info.get("marketCap"))

    return {
        "symbol": symbol,
        "current": current,
        "sma50": sma50,
        "sma200": sma200,
        "change_6m_pct": change_6m_pct,
        "revenue_growth": revenue_growth,
        "earnings_growth": earnings_growth,
        "debt_to_equity": debt_to_equity,
        "market_cap": market_cap,
    }


def score_longterm(row: dict, spy_change_6m_pct: Optional[float]) -> tuple[int, str]:
    score = 0
    reasons = []

    current = _safe_float(row.get("current"))
    sma50 = _safe_float(row.get("sma50"))
    sma200 = _safe_float(row.get("sma200"))
    change_6m_pct = _safe_float(row.get("change_6m_pct"))
    revenue_growth = _safe_float(row.get("revenue_growth"))
    earnings_growth = _safe_float(row.get("earnings_growth"))
    debt_to_equity = _safe_float(row.get("debt_to_equity"))
    market_cap = _safe_float(row.get("market_cap"))

    candle_bias = str(row.get("candle_bias") or "")
    range_zone = str(row.get("range_zone") or "")
    near_support = bool(row.get("near_support"))
    near_resistance = bool(row.get("near_resistance"))

    if current and sma50 and current > sma50:
        score += 2
        reasons.append("Price above SMA50")

    if sma50 and sma200 and sma50 > sma200:
        score += 3
        reasons.append("SMA50 above SMA200")

    if revenue_growth is not None and revenue_growth > 0.10:
        score += 2
        reasons.append("Revenue growth > 10%")

    if earnings_growth is not None and earnings_growth > 0.10:
        score += 2
        reasons.append("Earnings growth > 10%")

    if debt_to_equity is not None and debt_to_equity < 100:
        score += 1
        reasons.append("Debt-to-equity healthy")

    if market_cap is not None and market_cap >= 10_000_000_000:
        score += 1
        reasons.append("Large-cap stability")

    if change_6m_pct is not None and spy_change_6m_pct is not None:
        if change_6m_pct > spy_change_6m_pct:
            score += 2
            reasons.append("Outperforming SPY over 6 months")

    if near_support:
        score += 1
        reasons.append("Near support zone")

    if near_resistance:
        score -= 1
        reasons.append("Near resistance zone")

    if candle_bias == "bullish":
        score += 1
        reasons.append("Bullish recent candle")
    elif candle_bias == "bearish":
        score -= 1
        reasons.append("Bearish recent candle")

    if range_zone == "mid_range":
        score -= 1
        reasons.append("Mid-range entry")

    return score, " | ".join(reasons)


def run_longterm_scan(limit: int = 50) -> pd.DataFrame:
    tickers = fetch_sp500_tickers()[:limit]
    market = get_market_snapshot()
    spy_change_6m_pct = market.get("spy_change_6m_pct")

    rows = []

    for sym in tickers:
        snap = get_stock_snapshot(sym)
        current = _safe_float(snap.get("current")) or 0.0
        pa = compute_price_action_summary(sym, current)

        row = {
            **snap,
            **pa,
        }

        score, reason_text = score_longterm(row, spy_change_6m_pct)
        row["longterm_score"] = score
        row["longterm_label"] = "ACCUMULATE" if score >= 10 else "HOLD" if score >= 6 else "AVOID"
        row["longterm_reasons"] = reason_text

        row["candle_pattern"] = detect_candle_pattern(row)

        plan = build_longterm_plan(row)
        row["long_target_price"] = plan.get("long_target_price")
        targets = build_multi_targets(row)
        row["target_1"] = targets.get("target_1")
        row["target_2"] = targets.get("target_2")
        row["target_3"] = targets.get("target_3")
        row["review_below"] = plan.get("review_below")
        row["holding_horizon"] = plan.get("holding_horizon")

        entry_plan = build_entry_plan(row)
        row["entry_zone"] = entry_plan.get("entry_zone")
        row["add_zone"] = entry_plan.get("add_zone")
        row["exit_rule"] = entry_plan.get("exit_rule")

        if row["longterm_label"] != "ACCUMULATE":
            row["entry_signal"] = "WAIT"

        elif row["range_zone"] == "near_support":
            if row["candle_pattern"] == "Hammer":
                row["entry_signal"] = "STRONG BUY"
            else:
                row["entry_signal"] = "BUY"

        else:
            row["entry_signal"] = "WAIT"

        action_plan = decide_longterm_action(row)
        row["final_action"] = action_plan.get("final_action")
        row["action_confidence"] = action_plan.get("action_confidence")

        rows.append(row)

    df = pd.DataFrame(rows)

    out_csv = OUTPUT_DIR / "longterm_scan.csv"

    if not df.empty:
        df = df.sort_values(by=["longterm_score", "change_6m_pct"], ascending=False)
        df = add_allocations(df)

    df.to_csv(out_csv, index=False)

    send_manual = os.getenv("SEND_LONGTERM_EMAIL", "0") == "1"
    if send_manual and not df.empty:
        html = build_longterm_email(df)
        send_email("📈 Long-Term Scan", html)

    print(f"Saved: {out_csv}")
    return df


if __name__ == "__main__":
    df = run_longterm_scan(limit=50)
    print(df[[
        "symbol",
        "longterm_score",
        "longterm_label",
        "current",
        "long_target_price",
        "review_below",
        "holding_horizon",
        "entry_signal",
        "entry_zone",
        "add_zone",
        "exit_rule",
        "final_action",
        "action_confidence",
        "alloc_5k",
        "alloc_10k",
        "range_zone",
        "candle_bias",
        "candle_pattern",
    ]].head(20).to_string(index=False))