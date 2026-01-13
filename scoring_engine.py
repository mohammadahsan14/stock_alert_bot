# scoring_engine.py
import yfinance as yf
import pandas as pd
import numpy as np

from news_fetcher import fetch_news_links
from config import SCORE_HIGH, SCORE_MEDIUM


# -----------------------------
# RSI helper (Wilder's RSI)
# -----------------------------
def compute_rsi(series: pd.Series, period: int = 14) -> float:
    series = pd.to_numeric(series, errors="coerce").dropna()
    if series.empty or len(series) < period + 2:
        return 50.0

    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    # Wilder's smoothing (EMA with alpha=1/period)
    avg_gain = gain.ewm(alpha=1/period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))

    last = rsi.iloc[-1]
    return float(last) if last == last else 50.0


# -----------------------------
# Market scoring (SPY proxy)
# -----------------------------
def score_market() -> tuple[int, dict]:
    """
    Uses SPY as market proxy.
    Returns: (score, facts)
    """
    try:
        spy = yf.Ticker("SPY").history(period="2d", auto_adjust=False)["Close"]
        if spy.empty or len(spy) < 2:
            return 8, {"market_change_pct": None, "market_trend": "unknown"}

        pct = float((spy.iloc[-1] - spy.iloc[-2]) / spy.iloc[-2] * 100)

        if pct > 0.5:
            return 15, {"market_change_pct": pct, "market_trend": "up_strong"}
        elif pct > -0.5:
            return 8, {"market_change_pct": pct, "market_trend": "flat"}
        else:
            return 0, {"market_change_pct": pct, "market_trend": "down"}
    except Exception:
        return 8, {"market_change_pct": None, "market_trend": "unknown"}


# -----------------------------
# Technical scoring
# -----------------------------
def score_technicals(symbol: str) -> tuple[int, dict]:
    """
    Returns: (score, facts)
    """
    try:
        df = yf.Ticker(symbol).history(period="6mo", auto_adjust=False)
        if df is None or df.empty or len(df) < 60:
            return 0, {"tech_data_ok": False}

        df = df.dropna(subset=["Close", "Volume"]).copy()
        if len(df) < 60:
            return 0, {"tech_data_ok": False}

        close = df["Close"]
        rsi = compute_rsi(close)

        sma_50 = float(close.rolling(50).mean().iloc[-1])
        sma_200 = float(close.rolling(200).mean().iloc[-1]) if len(close) >= 200 else float(close.mean())

        last = float(close.iloc[-1])
        above_50sma = last > sma_50
        above_200sma = last > sma_200

        vol_ma20 = float(df["Volume"].rolling(20).mean().iloc[-1])
        volume_spike = float(df["Volume"].iloc[-1]) > 1.5 * vol_ma20 if vol_ma20 else False

        score = 0

        # RSI
        if 45 <= rsi <= 65:
            score += 10
        elif rsi < 30:
            score -= 8  # don't punish too hard

        # Trend alignment
        if above_50sma:
            score += 6
        if above_200sma:
            score += 6

        # Participation
        if volume_spike:
            score += 8

        score = int(max(score, 0))
        return score, {
            "tech_data_ok": True,
            "rsi": rsi,
            "above_50sma": above_50sma,
            "above_200sma": above_200sma,
            "volume_spike": volume_spike,
        }
    except Exception:
        return 0, {"tech_data_ok": False}


# -----------------------------
# Fundamental scoring
# -----------------------------
def score_fundamentals(symbol: str) -> tuple[int, dict]:
    """
    Returns: (score, facts)
    """
    try:
        t = yf.Ticker(symbol)
        info = t.info or {}

        eps_growth = float(info.get("earningsQuarterlyGrowth") or 0)
        revenue_growth = float(info.get("revenueGrowth") or 0)

        debt_to_equity = info.get("debtToEquity")
        debt_known = debt_to_equity is not None

        debt_ok = False
        if debt_known:
            try:
                debt_ok = float(debt_to_equity) < 1
            except Exception:
                debt_ok = False

        market_cap = float(info.get("marketCap") or 0)

        score = 0
        if eps_growth > 0.15:
            score += 10
        if revenue_growth > 0.10:
            score += 10
        if debt_known and debt_ok:
            score += 5
        if market_cap > 5_000_000_000:
            score += 5

        return int(score), {
            "eps_growth": eps_growth,
            "revenue_growth": revenue_growth,
            "debt_known": debt_known,
            "debt_ok": debt_ok,
            "market_cap": market_cap,
        }
    except Exception:
        return 0, {}


# -----------------------------
# News scoring (less gameable)
# -----------------------------
def score_news(symbol: str) -> tuple[int, dict]:
    """
    Returns: (score, facts)
    """
    try:
        news_links = fetch_news_links(symbol, max_articles=3) or []
        n = len([x for x in news_links if x and "No news" not in str(x)])

        if n == 0:
            return 0, {"has_news": False, "news_count": 0}

        # Simple tiered scoring
        if n == 1:
            score = 5
        elif n == 2:
            score = 8
        else:
            score = 10

        return score, {"has_news": True, "news_count": n}
    except Exception:
        return 0, {"has_news": False, "news_count": 0}


# -----------------------------
# Reasons builder
# -----------------------------
def generate_reasoning(facts: dict) -> str:
    reasons = []

    pct = facts.get("market_change_pct")
    if pct is not None:
        if pct > 0.5:
            reasons.append(f"Market supportive (SPY +{pct:.2f}%)")
        elif pct < -0.5:
            reasons.append(f"Market weak (SPY {pct:.2f}%)")
        else:
            reasons.append(f"Market flat (SPY {pct:.2f}%)")

    if facts.get("tech_data_ok"):
        rsi = facts.get("rsi", 50)
        if 45 <= rsi <= 65:
            reasons.append(f"RSI healthy ({rsi:.0f})")
        elif rsi < 30:
            reasons.append(f"RSI oversold ({rsi:.0f})")

        if facts.get("above_50sma"):
            reasons.append("Above 50SMA")
        if facts.get("above_200sma"):
            reasons.append("Above 200SMA")
        if facts.get("volume_spike"):
            reasons.append("Volume spike")
    else:
        reasons.append("Technical data limited")

    if facts.get("eps_growth", 0) > 0.15:
        reasons.append("EPS growth strong")
    if facts.get("revenue_growth", 0) > 0.10:
        reasons.append("Revenue growth strong")

    if facts.get("debt_known") and facts.get("debt_ok"):
        reasons.append("Debt manageable")

    if facts.get("market_cap", 0) > 5_000_000_000:
        reasons.append("Large-cap stability")

    if facts.get("has_news"):
        reasons.append(f"Recent news ({facts.get('news_count', 1)} articles)")

    return " | ".join(reasons) if reasons else "No strong positive indicators yet."


# -----------------------------
# Main scoring API
# -----------------------------
def get_predictive_score(symbol: str) -> tuple[int, str]:
    """
    Backward compatible. Returns (score, label).
    """
    score, label, _ = get_predictive_score_with_reasons(symbol)
    return score, label


def get_predictive_score_with_reasons(symbol: str) -> tuple[int, str, str]:
    """
    Returns (score, label, reasons).
    Adds small quality penalties to prevent overconfidence with missing data.
    """
    try:
        market_score, market_facts = score_market()
        tech_score, tech_facts = score_technicals(symbol)
        fund_score, fund_facts = score_fundamentals(symbol)
        news_score, news_facts = score_news(symbol)

        total = market_score + tech_score + fund_score + news_score

        # Quality penalties (prevents fake Strong Buys on partial data)
        if not tech_facts.get("tech_data_ok"):
            total -= 10
        if not fund_facts:
            total -= 5

        total = int(max(0, min(total, 100)))

        if total >= SCORE_HIGH:
            label = "Green"
        elif total >= SCORE_MEDIUM:
            label = "Yellow"
        else:
            label = "Red"

        facts = {}
        facts.update(market_facts)
        facts.update(tech_facts)
        facts.update(fund_facts)
        facts.update(news_facts)

        reasons = generate_reasoning(facts)
        return total, label, reasons
    except Exception:
        return 0, "Red", "Scoring failed due to data error."