# scoring_engine.py
import yfinance as yf
from news_fetcher import fetch_news_links
from config import SCORE_HIGH, SCORE_MEDIUM
import pandas as pd
import numpy as np

# -----------------------------
# Market scoring
# -----------------------------
def score_market(symbol):
    try:
        spy = yf.Ticker("SPY").history(period="2d")['Close']
        percent_change = (spy.iloc[-1] - spy.iloc[-2]) / spy.iloc[-2] * 100
        if percent_change > 0.5:
            return 15
        elif percent_change > -0.5:
            return 8
        else:
            return 0
    except:
        return 8  # fallback

# -----------------------------
# Technical scoring
# -----------------------------
def score_technicals(symbol):
    try:
        df = yf.Ticker(symbol).history(period="60d")
        if df.empty or len(df) < 20:
            return 0

        close = df['Close']
        rsi = compute_rsi(close)
        sma_50 = close.rolling(50).mean().iloc[-1]
        sma_200 = close.rolling(200).mean().iloc[-1] if len(close) >= 200 else close.mean()
        above_50sma = close.iloc[-1] > sma_50
        above_200sma = close.iloc[-1] > sma_200
        volume_spike = df['Volume'].iloc[-1] > 1.5 * df['Volume'].rolling(20).mean().iloc[-1]

        score = 0
        if 45 <= rsi <= 65:
            score += 10
        elif rsi < 30:
            score -= 10
        if above_50sma:
            score += 5
        if above_200sma:
            score += 5
        if volume_spike:
            score += 10

        return max(score, 0)
    except:
        return 0

def compute_rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = -delta.clip(upper=0).rolling(period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.iloc[-1] if not np.isnan(rsi.iloc[-1]) else 50

# -----------------------------
# Fundamental scoring
# -----------------------------
def score_fundamentals(symbol):
    try:
        t = yf.Ticker(symbol)
        info = t.info
        eps_growth = info.get('earningsQuarterlyGrowth', 0) or 0
        revenue_growth = info.get('revenueGrowth', 0) or 0
        debt_ok = info.get('debtToEquity', 0) < 1
        market_cap = info.get('marketCap', 0) or 0

        score = 0
        if eps_growth > 0.15:
            score += 10
        if revenue_growth > 0.10:
            score += 10
        if debt_ok:
            score += 5
        if market_cap > 5_000_000_000:
            score += 5
        return score
    except:
        return 0

# -----------------------------
# News scoring
# -----------------------------
def score_news(symbol):
    news_links = fetch_news_links(symbol)
    if news_links and news_links[0] != "No news available":
        return 15
    return 0

# -----------------------------
# Final predictive score
# -----------------------------
def get_predictive_score(symbol):
    try:
        market_score = score_market(symbol)
        tech_score = score_technicals(symbol)
        fund_score = score_fundamentals(symbol)
        news_score = score_news(symbol)

        total = market_score + tech_score + fund_score + news_score
        total = min(total, 100)  # cap at 100

        if total >= SCORE_HIGH:
            label = "Green"
        elif total >= SCORE_MEDIUM:
            label = "Yellow"
        else:
            label = "Red"

        return total, label
    except:
        return 0, "Red"