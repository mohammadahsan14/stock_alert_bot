# main.py
import os
import re
import argparse
from typing import List, Tuple
from datetime import datetime, time, timedelta

import pandas as pd
import yfinance as yf
from openpyxl import load_workbook
from openpyxl.styles import PatternFill, Font, Alignment
from email.message import EmailMessage
import smtplib

from top_movers import fetch_sp500_tickers, calculate_top_movers
from scoring_engine import get_predictive_score
from news_fetcher import fetch_news_links
from price_category import get_price_category
from config import (
    SENDER_EMAIL, APP_PASSWORD, RECEIVER_EMAIL,
    TOP_N, SCORE_COLORS, SCORE_HIGH, SCORE_MEDIUM,
    EXPECTED_UPSIDE_HIGH, EXPECTED_UPSIDE_MEDIUM, EXPECTED_DOWN,
)

# -----------------------------
# Defaults (clean + minimal)
# -----------------------------
OUTPUT_DIR = "outputs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def out_path(filename: str) -> str:
    """Return a safe file path inside outputs/."""
    return os.path.join(OUTPUT_DIR, filename)

#  Logs now live inside outputs
DAILY_LOG_CSV = out_path("daily_stock_log.csv")        # stores ONLY picks sent
PERF_LOG_CSV  = out_path("performance_log.csv")        # appended daily after post-market

EXCEL_MAX_ROWS = 10        # SUPPORTING_DATA rows
TRADE_MAX_PICKS = 3        # EMAIL + DAILY_PICKS rows

SUDDEN_MOVER_PCT_THRESHOLD = 3.0
POST_MARKET_START = time(15, 10)  # 3:10 PM CST buffer (market closes 3:00 PM CST)

# -----------------------------
# Reliability gates (NO TRADE DAY logic)
# -----------------------------
MIN_STRONG_BUY_PICKS = 1
MIN_CONFIDENCE_TO_TRADE = 6
MAX_ALLOWED_VOLATILITY = 6.0      # if biggest mover is > 6% premarket, skip day
MARKET_DOWNSHIFT_BLOCK = True     # if market down, require stronger evidence

# -----------------------------
# Earnings filter (gap-risk protection)
# -----------------------------
SKIP_EARNINGS_STOCKS = True
EARNINGS_LOOKAHEAD_DAYS = 3

# Price buckets (used for email grouping only)
PRICE_BUCKETS = [
    ("Ultra Penny ($)", "Ultra Penny Stocks"),
    ("Penny ($)", "Penny Stocks"),
    ("Mid ($$)", "Mid Price Stocks"),
    ("Mid-High ($$$)", "Mid-High Stocks"),
    ("High ($$$$)", "High Price Stocks"),
    ("Unknown", "Unknown"),
]

# -----------------------------
# Map score to decision
# -----------------------------
def map_score_to_decision(score: int) -> str:
    if score >= SCORE_HIGH:
        return "Strong Buy"
    elif score >= SCORE_MEDIUM:
        return "Moderate"
    return "Not Advisable"

# -----------------------------
# Normalize hex color for openpyxl
# -----------------------------
def normalize_color(color: str) -> str:
    if not color:
        color = "#FFFFFF"
    color = color.lstrip("#")
    if len(color) == 6:
        color = "FF" + color
    return color.upper()

# -----------------------------
# Extract headline / url from HTML link string
# -----------------------------
def extract_headline_from_html(news_html: str) -> str:
    if not news_html:
        return ""
    news_html = str(news_html).replace("\uFFFC", "").strip()
    m = re.search(r'>(.*?)</a>', news_html)
    return (m.group(1).strip() if m else re.sub(r"<.*?>", "", news_html).strip())

def extract_url_from_html(news_html: str) -> str:
    if not news_html:
        return ""
    m = re.search(r'href="([^"]+)"', str(news_html))
    return m.group(1).strip() if m else ""

# -----------------------------
# Simple sentiment -> flag
# -----------------------------
POS_WORDS = {"beat", "strong", "growth", "surge", "upgrade", "raises", "record", "profit", "wins", "bull"}
NEG_WORDS = {"miss", "drop", "loss", "cuts", "downgrade", "falls", "weak", "lawsuit", "plunge", "bear"}

def news_flag_from_headlines(headlines: List[str]) -> str:
    if not headlines:
        return "🟡"
    score = 0
    for h in headlines:
        t = (h or "").lower()
        if any(w in t for w in POS_WORDS):
            score += 1
        if any(w in t for w in NEG_WORDS):
            score -= 1
    if score >= 1:
        return "🟢"
    if score <= -1:
        return "🔴"
    return "🟡"

# -----------------------------
# Market direction
# -----------------------------
def get_market_direction() -> str:
    try:
        sp500 = yf.Ticker("^GSPC").history(period="2d")["Close"]
        return "up" if sp500.iloc[-1] > sp500.iloc[-2] else "down"
    except Exception:
        return "up"

# -----------------------------
# Confidence model (simple + stable)
# -----------------------------
def compute_confidence(score_val: int, pct_change: float, market_trend: str, news_flag: str) -> int:
    base = score_val / 10.0  # 0..10
    vol_adj = max(0.7, 1 - abs(pct_change) / 10.0)  # floor 0.7
    market_adj = 1.05 if market_trend == "up" else 0.95
    news_adj = 1.05 if news_flag == "🟢" else (0.95 if news_flag == "🔴" else 1.0)
    conf = int(round(base * vol_adj * market_adj * news_adj))
    return min(max(conf, 1), 10)

# -----------------------------
# Earnings soon? (gap-risk)
# -----------------------------
def has_earnings_soon(symbol: str, now: datetime, lookahead_days: int = 3) -> bool:
    try:
        t = yf.Ticker(symbol)
        cal = t.calendar
        if cal is None or cal.empty:
            return False

        dt = None
        if "Earnings Date" in cal.index:
            vals = cal.loc["Earnings Date"].values
            if len(vals) > 0:
                dt = vals[0]

        if dt is None:
            for v in cal.values.flatten().tolist():
                if hasattr(v, "to_pydatetime"):
                    dt = v.to_pydatetime()
                    break

        if dt is None or not hasattr(dt, "date"):
            return False

        start = now.date()
        end = (now + timedelta(days=lookahead_days)).date()
        return start <= dt.date() <= end
    except Exception:
        return False

# -----------------------------
# Trade Plan column
# -----------------------------
def assign_trade_plan(risk: str, pct_change: float, market_trend: str, score_val: int) -> str:
    if abs(pct_change) >= 3.0:
        return "Intraday"
    if score_val >= SCORE_HIGH and market_trend == "up" and risk in ("Low", "Medium"):
        return "Swing (3–10 days)"
    return "Intraday"

# -----------------------------
# NO TRADE DAY logic
# -----------------------------
def should_skip_day(df: pd.DataFrame, market_trend: str) -> Tuple[bool, str]:
    if df is None or df.empty:
        return True, "Empty dataset"

    if "current" not in df.columns or df["current"].isna().any():
        return True, "Missing/invalid prices"

    if "pct_change" in df.columns:
        extreme = df["pct_change"].abs().max()
        if pd.notna(extreme) and extreme >= MAX_ALLOWED_VOLATILITY:
            return True, f"Market too volatile today (max mover {extreme:.2f}%)"

    if "decision" in df.columns and "confidence" in df.columns:
        tradeable = df[(df["decision"] == "Strong Buy") & (df["confidence"] >= MIN_CONFIDENCE_TO_TRADE)]
        if len(tradeable) < MIN_STRONG_BUY_PICKS:
            return True, "No Strong Buy picks meeting confidence threshold"

    if MARKET_DOWNSHIFT_BLOCK and market_trend == "down":
        tradeable = df[(df["decision"] == "Strong Buy") & (df["confidence"] >= MIN_CONFIDENCE_TO_TRADE)]
        if len(tradeable) < 2:
            return True, "Market trend down + not enough high-confidence picks"

    return False, ""

# -----------------------------
# Excel styling
# -----------------------------
def style_excel_sheet(sheet):
    header_font = Font(bold=True, color="FFFFFF")
    header_fill = PatternFill(
        start_color=normalize_color("#2F5597"),
        end_color=normalize_color("#2F5597"),
        fill_type="solid",
    )
    center = Alignment(horizontal="center", vertical="center", wrap_text=True)

    for cell in sheet[1]:
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = center

    sheet.freeze_panes = "A2"

    for col in sheet.columns:
        max_length = 0
        col_letter = col[0].column_letter
        for cell in col:
            if cell.value is None:
                continue
            max_length = max(max_length, len(str(cell.value)))
        sheet.column_dimensions[col_letter].width = min(max_length + 2, 55)

    headers = {str(c.value).strip(): idx + 1 for idx, c in enumerate(sheet[1]) if c.value}
    score_label_col = headers.get("score_label")
    decision_col = headers.get("decision")

    for r in range(2, sheet.max_row + 1):
        try:
            if score_label_col:
                cell = sheet.cell(row=r, column=score_label_col)
                label = str(cell.value or "")
                color = SCORE_COLORS.get(label, "#FFFFFF")
                cell.fill = PatternFill(
                    start_color=normalize_color(color),
                    end_color=normalize_color(color),
                    fill_type="solid",
                )
                cell.alignment = center

            if decision_col:
                dcell = sheet.cell(row=r, column=decision_col)
                dval = str(dcell.value or "")
                if dval == "Strong Buy":
                    c = "#92D050"
                elif dval == "Moderate":
                    c = "#FFF2CC"
                else:
                    c = "#F4CCCC"
                dcell.fill = PatternFill(
                    start_color=normalize_color(c),
                    end_color=normalize_color(c),
                    fill_type="solid",
                )
                dcell.alignment = center
        except Exception:
            pass

# -----------------------------
# Email HTML (top picks; includes predicted price + plan)
# -----------------------------
def build_email_html_top_picks(df: pd.DataFrame, run_date: str) -> str:
    if df.empty:
        return f"<h2>📊 Daily Picks – Pre Market ({run_date})</h2><p>No picks available today.</p>"

    html = f"<h2>📊 Daily Picks – Pre Market ({run_date})</h2>"
    html += "<p>Ranked by <b>confidence</b> then <b>score</b>. Full details are in Excel.</p>"

    picks = df.sort_values(by=["confidence", "score"], ascending=False).head(TRADE_MAX_PICKS).copy()

    for cat_code, cat_name in PRICE_BUCKETS:
        cat_df = picks[picks["price_category"] == cat_code]
        if cat_df.empty:
            continue

        html += f"<h3>{cat_name}</h3>"
        html += "<table style='border-collapse:collapse;font-family:Arial;width:100%;margin-bottom:18px;'>"
        html += """
        <tr style='background:#f2f2f2;'>
          <th style='padding:6px;border:1px solid #ddd;'>Symbol</th>
          <th style='padding:6px;border:1px solid #ddd;'>Price</th>
          <th style='padding:6px;border:1px solid #ddd;'>Predicted</th>
          <th style='padding:6px;border:1px solid #ddd;'>Plan</th>
          <th style='padding:6px;border:1px solid #ddd;'>Decision</th>
          <th style='padding:6px;border:1px solid #ddd;'>Score</th>
          <th style='padding:6px;border:1px solid #ddd;'>Conf</th>
          <th style='padding:6px;border:1px solid #ddd;'>News</th>
        </tr>
        """
        for _, row in cat_df.iterrows():
            label = row.get("score_label", "")
            score_color = SCORE_COLORS.get(label, "#FFFFFF")
            html += f"""
            <tr>
              <td style='padding:6px;border:1px solid #ddd;'>{row.get('symbol','')}</td>
              <td style='padding:6px;border:1px solid #ddd;'>{row.get('current',0):.2f}</td>
              <td style='padding:6px;border:1px solid #ddd;'>{row.get('predicted_price',0):.2f}</td>
              <td style='padding:6px;border:1px solid #ddd;text-align:center;'>{row.get('trade_plan','')}</td>
              <td style='padding:6px;border:1px solid #ddd;text-align:center;'>{row.get('decision','')}</td>
              <td style='padding:6px;border:1px solid #ddd;background:{score_color};text-align:center;'>{label} ({int(row.get('score',0))})</td>
              <td style='padding:6px;border:1px solid #ddd;text-align:center;'>{int(row.get('confidence',0))}</td>
              <td style='padding:6px;border:1px solid #ddd;text-align:center;font-size:16px;'>{row.get('news_flag','🟡')}</td>
            </tr>
            """
        html += "</table>"

    return html

# -----------------------------
# Send email
# -----------------------------
def send_email(subject: str, html_body: str, attachment_path: str = None):
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = SENDER_EMAIL
    msg["To"] = RECEIVER_EMAIL
    msg.add_alternative(html_body, subtype="html")

    if attachment_path:
        try:
            with open(attachment_path, "rb") as f:
                msg.add_attachment(
                    f.read(),
                    maintype="application",
                    subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    filename=os.path.basename(attachment_path),
                )
        except Exception as e:
            print("⚠️ Attachment failed:", e)

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(SENDER_EMAIL, APP_PASSWORD)
            smtp.send_message(msg)
        print("✅ Email sent:", subject)
    except Exception as e:
        print("❌ Email failed:", e)

# -----------------------------
# Post-market evaluation (reads picks log)
# -----------------------------
def evaluate_post_market_from_log(log_csv: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(log_csv)
    except Exception:
        return pd.DataFrame()

    if df.empty or "symbol" not in df.columns:
        return df

    close_prices = []
    for symbol in df["symbol"].tolist():
        try:
            data = yf.Ticker(symbol).history(period="1d")
            close = float(data["Close"].iloc[-1]) if not data.empty else float("nan")
        except Exception:
            close = float("nan")
        close_prices.append(close)

    df["close_price"] = close_prices
    df["actual_change_pct"] = (df["close_price"] - df["current"]) / df["current"] * 100

    outcomes = []
    for _, row in df.iterrows():
        predicted = row.get("decision", "Not Advisable")
        actual = row.get("actual_change_pct", 0)

        if predicted in ["Strong Buy", "Moderate"] and actual > 0:
            outcomes.append("✅ Correct")
        elif predicted == "Not Advisable" and actual <= 0:
            outcomes.append("✅ Correct")
        else:
            outcomes.append("❌ Incorrect")
    df["outcome"] = outcomes
    return df

# -----------------------------
# Midday alert (sudden movers)
# -----------------------------
def build_midday_alert(df: pd.DataFrame, run_date: str) -> str:
    if df.empty:
        return ""

    html = f"<h2>⚡ Sudden Movers Alert ({run_date})</h2>"
    html += "<table style='border-collapse:collapse;font-family:Arial;width:100%;'>"
    html += """
    <tr style='background:#f2f2f2;'>
      <th style='padding:6px;border:1px solid #ddd;'>Symbol</th>
      <th style='padding:6px;border:1px solid #ddd;'>% Change</th>
      <th style='padding:6px;border:1px solid #ddd;'>Decision</th>
      <th style='padding:6px;border:1px solid #ddd;'>Score</th>
      <th style='padding:6px;border:1px solid #ddd;'>Conf</th>
      <th style='padding:6px;border:1px solid #ddd;'>Main News</th>
    </tr>
    """
    for _, row in df.iterrows():
        link = row.get("main_news_link", "")
        title = row.get("main_news_title", "")
        link_html = f'<a href="{link}" target="_blank">{title}</a>' if link else (title or "—")

        html += f"""
        <tr>
          <td style='padding:6px;border:1px solid #ddd;'>{row.get('symbol')}</td>
          <td style='padding:6px;border:1px solid #ddd;text-align:center;'>{row.get('pct_change',0):.2f}%</td>
          <td style='padding:6px;border:1px solid #ddd;text-align:center;'>{row.get('decision')}</td>
          <td style='padding:6px;border:1px solid #ddd;text-align:center;'>{row.get('score')}</td>
          <td style='padding:6px;border:1px solid #ddd;text-align:center;'>{row.get('confidence')}</td>
          <td style='padding:6px;border:1px solid #ddd;'>{link_html}</td>
        </tr>
        """
    html += "</table>"
    return html

# -----------------------------
# Weekly dashboard (last 7 days)
# -----------------------------
def build_weekly_dashboard_html(perf_csv: str, now: datetime) -> str:
    try:
        df = pd.read_csv(perf_csv)
    except Exception:
        return "<p>No performance log found yet.</p>"

    if df.empty or "run_date" not in df.columns:
        return "<p>Performance log is empty.</p>"

    df["run_date"] = pd.to_datetime(df["run_date"], errors="coerce")
    df = df.dropna(subset=["run_date"])

    last7 = df[df["run_date"] >= (now - pd.Timedelta(days=7))].copy()
    if last7.empty:
        return "<p>No trades in last 7 days.</p>"

    total = len(last7)
    correct = int((last7["outcome"] == "✅ Correct").sum()) if "outcome" in last7.columns else 0
    rate = (correct / total * 100) if total else 0
    avg_move = last7["actual_change_pct"].mean() if "actual_change_pct" in last7.columns else 0

    best = last7.sort_values("actual_change_pct", ascending=False).head(3)[["symbol", "actual_change_pct"]]
    worst = last7.sort_values("actual_change_pct", ascending=True).head(3)[["symbol", "actual_change_pct"]]

    def rows(df2):
        out = ""
        for _, r in df2.iterrows():
            out += f"<li>{r['symbol']}: {r['actual_change_pct']:.2f}%</li>"
        return out or "<li>—</li>"

    return f"""
    <h2>📅 Weekly Dashboard (Last 7 Days)</h2>
    <p>
      Trades evaluated: <b>{total}</b><br>
      Win rate: <b>{rate:.2f}%</b><br>
      Avg move: <b>{avg_move:.2f}%</b>
    </p>
    <h3>Top 3 Winners</h3>
    <ul>{rows(best)}</ul>
    <h3>Top 3 Losers</h3>
    <ul>{rows(worst)}</ul>
    """

# -----------------------------
# PRE-MARKET
# -----------------------------
def run_premarket(now: datetime):
    market_trend = get_market_direction()

    tickers = fetch_sp500_tickers()
    movers = calculate_top_movers(tickers, TOP_N)
    df = pd.DataFrame(movers)
    print("movers len:", len(movers))
    print(df.head(3))
    print(df.columns)

    if df.empty:
        raise RuntimeError("Top movers returned empty dataset. Refusing to run.")

    # Score a bit more than we keep, then cut down
    df = df.head(max(EXCEL_MAX_ROWS, 20)).copy()

    # Ensure required columns
    if "pct_change" not in df.columns:
        df["pct_change"] = 0.0
    df["pct_change"] = pd.to_numeric(df["pct_change"], errors="coerce").fillna(0.0)

    if "current" not in df.columns:
        if "price" in df.columns:
            df["current"] = df["price"]
        else:
            raise RuntimeError("Missing 'current' price column from top_movers output.")
    df["current"] = pd.to_numeric(df["current"], errors="coerce").fillna(0.0)

    scores, labels, confs = [], [], []
    main_titles, main_links, flags = [], [], []
    decisions, predicted_prices, targets, stops, categories = [], [], [], [], []
    earnings_risks, trade_plans = [], []

    for _, row in df.iterrows():
        sym = row["symbol"]
        current = float(row["current"])
        risk = str(row.get("risk", "Medium"))
        pct = float(row.get("pct_change", 0.0))

        score_val, score_label = get_predictive_score(sym)
        score_val = int(score_val)
        decision = map_score_to_decision(score_val)

        factor = EXPECTED_UPSIDE_HIGH if score_val >= SCORE_HIGH else (
            EXPECTED_UPSIDE_MEDIUM if score_val >= SCORE_MEDIUM else EXPECTED_DOWN
        )

        # News
        news_items = fetch_news_links(sym, max_articles=3)
        main_item = news_items[0] if news_items else ""
        title = extract_headline_from_html(main_item)
        link = extract_url_from_html(main_item)
        headlines = [extract_headline_from_html(x) for x in news_items if x]
        flag = news_flag_from_headlines(headlines)

        # Earnings risk
        erisk = has_earnings_soon(sym, now, EARNINGS_LOOKAHEAD_DAYS)
        if SKIP_EARNINGS_STOCKS and erisk:
            decision = "Not Advisable"

        conf = compute_confidence(score_val, pct, market_trend, flag)

        # Trade plan
        tplan = assign_trade_plan(risk=risk, pct_change=pct, market_trend=market_trend, score_val=score_val)

        scores.append(score_val)
        labels.append(score_label)
        confs.append(conf)
        main_titles.append(title)
        main_links.append(link)
        flags.append(flag)
        decisions.append(decision)

        predicted = current * factor
        predicted_prices.append(predicted)
        targets.append(predicted)
        stops.append(current * 0.97)

        categories.append(get_price_category(current) if current > 0 else "Unknown")
        earnings_risks.append("YES" if erisk else "NO")
        trade_plans.append(tplan)

    # attach computed columns
    df["score"] = scores
    df["score_label"] = labels
    df["confidence"] = confs
    df["decision"] = decisions
    df["predicted_price"] = predicted_prices
    df["target_price"] = targets
    df["stop_loss"] = stops
    df["price_category"] = categories
    df["news_flag"] = flags
    df["main_news_title"] = main_titles
    df["main_news_link"] = main_links
    df["earnings_risk"] = earnings_risks
    df["trade_plan"] = trade_plans
    df["run_date"] = now.strftime("%Y-%m-%d")

    # Reduce supporting set
    df = df.sort_values(by=["confidence", "score"], ascending=False).head(max(EXCEL_MAX_ROWS, TRADE_MAX_PICKS)).copy()

    # Picks sent (Strong Buy + confidence gate)
    daily_picks = (
        df[(df["decision"] == "Strong Buy") & (df["confidence"] >= MIN_CONFIDENCE_TO_TRADE)]
        .sort_values(by=["confidence", "score"], ascending=False)
        .head(TRADE_MAX_PICKS)
        .copy()
    )
    if daily_picks.empty:
        daily_picks = df.sort_values(by=["confidence", "score"], ascending=False).head(min(TRADE_MAX_PICKS, len(df))).copy()

    # NO TRADE DAY check (after scoring)
    skip, reason = should_skip_day(df, market_trend)
    if daily_picks.empty:
        daily_picks = df.sort_values(by=["confidence", "score"], ascending=False).head(TRADE_MAX_PICKS).copy()

    # Save daily log = PICKS ONLY (for evaluation)
    daily_picks.to_csv(DAILY_LOG_CSV, index=False)

    # Excel (3 sheets only)
    daily_cols = [
        "run_date", "symbol", "price_category",
        "current", "predicted_price", "target_price", "stop_loss",
        "trade_plan", "earnings_risk",
        "decision", "score", "score_label", "confidence",
        "news_flag", "main_news_title", "main_news_link"
    ]
    support_cols = [
        "run_date", "symbol", "price_category", "current", "pct_change",
        "predicted_price", "trade_plan", "earnings_risk",
        "decision", "score", "score_label", "confidence",
        "news_flag", "main_news_title", "main_news_link"
    ]

    daily_picks_out = daily_picks[[c for c in daily_cols if c in daily_picks.columns]]
    supporting = df[[c for c in support_cols if c in df.columns]]

    perf = pd.DataFrame([{
        "run_date": now.strftime("%Y-%m-%d"),
        "picks_sent": len(daily_picks_out),
        "note": "Post-market updates win-rate (based on picks sent)."
    }])

    pre_excel = f"stock_watchlist_{now.strftime('%Y%m%d')}.xlsx"
    with pd.ExcelWriter(pre_excel, engine="openpyxl") as writer:
        daily_picks_out.to_excel(writer, sheet_name="DAILY_PICKS", index=False)
        supporting.to_excel(writer, sheet_name="SUPPORTING_DATA", index=False)
        perf.to_excel(writer, sheet_name="MODEL_PERFORMANCE", index=False)

    wb = load_workbook(pre_excel)
    for s in wb.sheetnames:
        style_excel_sheet(wb[s])
    wb.save(pre_excel)

    # If NO TRADE DAY, email that instead of picks
    if skip:
        html = f"""
        <h2>🛑 NO TRADE DAY – Pre Market ({now.strftime('%Y-%m-%d')})</h2>
        <p><b>Reason:</b> {reason}</p>
        <p>Excel is attached for review, but no picks were sent for trading today.</p>
        """
        send_email(f"🛑 NO TRADE DAY – Pre Market ({now.strftime('%Y-%m-%d')})", html, attachment_path=pre_excel)
        return

    # Normal picks email
    email_html = build_email_html_top_picks(daily_picks, now.strftime("%Y-%m-%d"))
    print("daily_picks cols:", daily_picks.columns.tolist())
    print(daily_picks[["symbol", "current", "predicted_price"]].head())
    send_email(f"📈 Daily Stock Alert – Pre Market ({now.strftime('%Y-%m-%d')})", email_html, attachment_path=pre_excel)

# -----------------------------
# MIDDAY
# -----------------------------
def run_midday(now: datetime):
    tickers = fetch_sp500_tickers()
    movers = calculate_top_movers(tickers, TOP_N)
    df = pd.DataFrame(movers)
    if df.empty or "pct_change" not in df.columns:
        return

    df["pct_change"] = pd.to_numeric(df["pct_change"], errors="coerce").fillna(0.0)
    df = df[df["pct_change"].abs() >= SUDDEN_MOVER_PCT_THRESHOLD].copy()
    if df.empty:
        return

    market_trend = get_market_direction()

    scores, labels, confs, decisions = [], [], [], []
    titles, links = [], []

    for _, row in df.iterrows():
        sym = row["symbol"]
        score_val, score_label = get_predictive_score(sym)
        score_val = int(score_val)
        decision = map_score_to_decision(score_val)

        news_items = fetch_news_links(sym, max_articles=1)
        main_item = news_items[0] if news_items else ""
        title = extract_headline_from_html(main_item)
        link = extract_url_from_html(main_item)

        flag = news_flag_from_headlines([title])
        conf = compute_confidence(score_val, float(row.get("pct_change", 0.0)), market_trend, flag)

        scores.append(score_val)
        labels.append(score_label)
        decisions.append(decision)
        confs.append(conf)
        titles.append(title)
        links.append(link)

    df["score"] = scores
    df["score_label"] = labels
    df["decision"] = decisions
    df["confidence"] = confs
    df["main_news_title"] = titles
    df["main_news_link"] = links

    df = df[df["confidence"] >= 7].sort_values(by=["confidence", "score"], ascending=False)
    if df.empty:
        return

    html = build_midday_alert(df, now.strftime("%Y-%m-%d"))
    send_email(f"⚡ Sudden Movers Alert ({now.strftime('%Y-%m-%d')})", html)

# -----------------------------
# POST-MARKET
# -----------------------------
def run_postmarket(now: datetime):
    if now.time() < POST_MARKET_START:
        print("⏳ Post-market skipped (too early).")
        return

    df = evaluate_post_market_from_log(DAILY_LOG_CSV)
    if df.empty:
        send_email(
            f"📊 Post-Market Summary ({now.strftime('%Y-%m-%d')})",
            "<p>No picks log found / empty picks log — cannot evaluate today.</p>",
        )
        return

    correct = int((df["outcome"] == "✅ Correct").sum())
    total = len(df)
    rate = (correct / total * 100) if total else 0

    summary_html = f"""
    <h2>📊 Post-Market Summary ({now.strftime('%Y-%m-%d')})</h2>
    <p><b>Picks evaluated:</b> {total}<br>
       <b>Correct:</b> {correct}<br>
       <b>Incorrect:</b> {total - correct}<br>
       <b>Success Rate:</b> {rate:.2f}%</p>
    """

    post_excel = f"post_market_{now.strftime('%Y%m%d')}.xlsx"
    df.to_excel(post_excel, index=False)
    wb = load_workbook(post_excel)
    for s in wb.sheetnames:
        style_excel_sheet(wb[s])
    wb.save(post_excel)

    send_email(f"📊 Post-Market Stock Summary ({now.strftime('%Y-%m-%d')})", summary_html, attachment_path=post_excel)

    # Append performance log (row-per-pick)
    out = df.copy()
    if "run_date" not in out.columns:
        out["run_date"] = now.strftime("%Y-%m-%d")

    keep = ["run_date", "symbol", "decision", "score", "confidence", "current", "close_price", "actual_change_pct", "outcome"]
    out = out[[c for c in keep if c in out.columns]]

    try:
        if os.path.exists(PERF_LOG_CSV):
            prev = pd.read_csv(PERF_LOG_CSV)
            merged = pd.concat([prev, out], ignore_index=True)
            merged.to_csv(PERF_LOG_CSV, index=False)
        else:
            out.to_csv(PERF_LOG_CSV, index=False)
    except Exception as e:
        print("⚠️ Perf log append failed:", e)

    # Weekly dashboard email every Friday
    if now.weekday() == 4:
        dash = build_weekly_dashboard_html(PERF_LOG_CSV, now)
        send_email(f"📅 Weekly Trading Dashboard ({now.strftime('%Y-%m-%d')})", dash)

# -----------------------------
# Main
# -----------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["premarket", "midday", "postmarket"], default="premarket")
    args = parser.parse_args()

    now = datetime.now()

    if args.mode == "premarket":
        run_premarket(now)
    elif args.mode == "midday":
        run_midday(now)
    else:
        run_postmarket(now)

if __name__ == "__main__":
    main()