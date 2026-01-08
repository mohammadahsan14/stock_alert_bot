# main.py

from top_movers import fetch_sp500_tickers, calculate_top_movers
from market_direction import get_market_direction
from scoring_engine import get_predictive_score
from news_fetcher import fetch_news_links
from config import (
    SENDER_EMAIL, APP_PASSWORD, RECEIVER_EMAIL, TOP_N,
    RISK_COLORS, TRADE_COLORS, SCORE_COLORS,
    SCORE_HIGH, SCORE_MEDIUM,
    EXPECTED_UPSIDE_HIGH, EXPECTED_UPSIDE_MEDIUM, EXPECTED_DOWN
)
import pandas as pd
import smtplib
from email.message import EmailMessage
from datetime import datetime, timedelta
import random

# -----------------------------
# MAIN FUNCTION
# -----------------------------
def main():
    # -----------------------------
    # Fetch tickers
    # -----------------------------
    try:
        tickers = fetch_sp500_tickers()
    except Exception:
        tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META"]

    # -----------------------------
    # Top movers
    # -----------------------------
    try:
        movers = calculate_top_movers(tickers, TOP_N)
    except Exception:
        movers = []

    df = pd.DataFrame(movers) if movers else pd.DataFrame()

    # -----------------------------
    # Market direction
    # -----------------------------
    try:
        market = get_market_direction()
    except Exception:
        market = []

    market_bias = "NEUTRAL"
    for m in market:
        change = m.get("change")
        if change is None:
            continue
        if change < -0.5:
            market_bias = "BEARISH"
            break
        elif change > 0.5:
            market_bias = "BULLISH"

    # Adjust for bearish market
    if not df.empty and market_bias == "BEARISH":
        df.loc[df.get("decision") == "✅ CAN CONSIDER BUY", "decision"] = "⚠️ NEUTRAL"
        df.loc[df.get("day_trade") == "✅ Preferable", "day_trade"] = "⚠️ Moderate"

    # Buyable or fallback watchlist
    buyable_df = df[df.get('decision', '').str.contains("CAN CONSIDER BUY")] if not df.empty else pd.DataFrame()
    target_df = buyable_df if not buyable_df.empty else df.sample(n=min(len(df), 10), random_state=42) if not df.empty else pd.DataFrame()

    if target_df.empty:
        # Dummy fallback watchlist
        sample_stocks = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
        target_df = pd.DataFrame([{
            "symbol": s,
            "current": random.uniform(100, 500),
            "pct_change": random.uniform(-2, 2),
            "risk": random.choice(["Low","Medium","High"]),
            "decision": "Watch",
            "day_trade": "⚠️ Moderate"
        } for s in sample_stocks])

    # Sample 10–15 stocks
    n_stocks = min(len(target_df), 15)
    target_df = target_df.sample(n=n_stocks, random_state=42).reset_index(drop=True)

    # Assign timeframe & buy dates
    today = datetime.today()
    timeframes = [("Day", today + timedelta(days=1)),
                  ("Week", today + timedelta(days=7)),
                  ("Long-term", today + timedelta(days=30))]
    target_df['timeframe'] = [timeframes[i % 3][0] for i in range(len(target_df))]
    target_df['buy_date'] = [timeframes[i % 3][1].strftime("%Y-%m-%d") for i in range(len(target_df))]

    # -----------------------------
    # Predictive score, expected price, reason
    # -----------------------------
    scores, score_labels, expected_prices, reasons = [], [], [], []
    for _, row in target_df.iterrows():
        try:
            score_val, score_label = get_predictive_score(row['symbol'])
        except Exception:
            score_val, score_label = 0, "Red"

        scores.append(score_val)
        score_labels.append(score_label)

        # Expected price calculation
        if score_val >= SCORE_HIGH:
            factor = EXPECTED_UPSIDE_HIGH
        elif score_val >= SCORE_MEDIUM:
            factor = EXPECTED_UPSIDE_MEDIUM
        else:
            factor = EXPECTED_DOWN
        expected_prices.append(row['current'] * factor)

        # Fetch news
        news_links = fetch_news_links(row['symbol'])
        reason_text = f"{'Strong' if score_val>=SCORE_HIGH else 'Moderate' if score_val>=SCORE_MEDIUM else 'Low'} predictive score for {row['symbol']}. "
        reason_text += " ".join(news_links)
        reasons.append(reason_text)

    target_df['score'] = scores
    target_df['score_label'] = score_labels
    target_df['expected_price'] = expected_prices
    target_df['reason'] = reasons

    # -----------------------------
    # Build HTML table
    # -----------------------------
    html = """
    <h2 style="font-family: Arial, sans-serif;">📈 Stock Watchlist</h2>
    <table style="border-collapse: collapse; font-family: Arial, sans-serif; width: 100%;">
    <tr style="background-color:#f2f2f2;">
    <th>Symbol</th><th>Price</th><th>Change %</th><th>Risk</th><th>Decision</th>
    <th>Timeframe</th><th>Buy Date</th><th>Score</th><th>Expected Price</th><th>Reason</th>
    </tr>
    """
    for i, row in target_df.iterrows():
        arrow = "▲" if row['pct_change'] > 0 else "▼"
        change_color = "#28a745" if row['pct_change'] > 0 else "#dc3545"
        score_color = SCORE_COLORS.get(row['score_label'], "#fff")
        row_bg = "#ffffff" if i % 2 == 0 else "#f9f9f9"

        html += f"""
        <tr style="background-color:{row_bg};">
            <td>{row['symbol']}</td>
            <td>{row['current']:.2f}</td>
            <td style="color:{change_color}">{arrow} {abs(row['pct_change']):.2f}%</td>
            <td style="background-color:{RISK_COLORS.get(row['risk'], '#fff')}; text-align:center">{row['risk']}</td>
            <td style="background-color:{TRADE_COLORS.get('✅ Preferable', '#fff')}; text-align:center">{row['decision']}</td>
            <td>{row['timeframe']}</td>
            <td>{row['buy_date']}</td>
            <td style="background-color:{score_color}; text-align:center">{row['score_label']} ({row['score']})</td>
            <td>{row['expected_price']:.2f}</td>
            <td style="max-width:300px; word-wrap:break-word;">{row['reason']}</td>
        </tr>
        """
    html += "</table>"

    # -----------------------------
    # Email content
    # -----------------------------
    bias_color = "#28a745" if market_bias=="BULLISH" else "#dc3545" if market_bias=="BEARISH" else "#ffc107"
    html_content = f"""
    <html>
    <body>
        <h2>📊 Market Bias: <span style="color:{bias_color}">{market_bias}</span></h2>
        {html}
    </body>
    </html>
    """

    # -----------------------------
    # Send Email
    # -----------------------------
    msg = EmailMessage()
    msg["Subject"] = "📈 Daily Stock Alert – Pre Market"
    msg["From"] = SENDER_EMAIL
    msg["To"] = RECEIVER_EMAIL
    msg.add_alternative(html_content, subtype='html')

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(SENDER_EMAIL, APP_PASSWORD)
            smtp.send_message(msg)
            print("✅ Email sent successfully")
    except Exception as e:
        print("❌ Email failed:", e)


if __name__ == "__main__":
    main()