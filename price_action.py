from __future__ import annotations

from typing import Optional
import pandas as pd
import yfinance as yf


def _safe_float(x) -> Optional[float]:
    try:
        if x is None or pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None


def load_recent_price_action(symbol: str, period: str = "10d", interval: str = "5m") -> pd.DataFrame:
    try:
        df = yf.Ticker(symbol).history(period=period, interval=interval, auto_adjust=False, prepost=False)
        if df is None or df.empty:
            return pd.DataFrame()
        return df.copy()
    except Exception:
        return pd.DataFrame()


def classify_last_candle(df: pd.DataFrame) -> dict:
    if df is None or df.empty:
        return {
            "candle_bias": "unknown",
            "body_pct": None,
            "upper_wick_pct": None,
            "lower_wick_pct": None,
        }

    last = df.iloc[-1]

    o = _safe_float(last.get("Open"))
    h = _safe_float(last.get("High"))
    l = _safe_float(last.get("Low"))
    c = _safe_float(last.get("Close"))

    if None in (o, h, l, c) or o == 0:
        return {
            "candle_bias": "unknown",
            "body_pct": None,
            "upper_wick_pct": None,
            "lower_wick_pct": None,
        }

    body = abs(c - o)
    body_pct = (body / o) * 100.0

    upper_wick = h - max(o, c)
    lower_wick = min(o, c) - l

    upper_wick_pct = (upper_wick / o) * 100.0
    lower_wick_pct = (lower_wick / o) * 100.0

    if c > o:
        bias = "bullish"
    elif c < o:
        bias = "bearish"
    else:
        bias = "neutral"

    return {
        "candle_bias": bias,
        "body_pct": round(body_pct, 4),
        "upper_wick_pct": round(upper_wick_pct, 4),
        "lower_wick_pct": round(lower_wick_pct, 4),
    }


def compute_range_position(df: pd.DataFrame, current_price: float, lookback: int = 20) -> dict:
    if df is None or df.empty or current_price is None or current_price <= 0:
        return {
            "range_low": None,
            "range_high": None,
            "range_position_pct": None,
            "range_zone": "unknown",
            "near_support": False,
            "near_resistance": False,
        }

    x = df.tail(lookback).copy()
    if x.empty:
        return {
            "range_low": None,
            "range_high": None,
            "range_position_pct": None,
            "range_zone": "unknown",
            "near_support": False,
            "near_resistance": False,
        }

    lows = pd.to_numeric(x["Low"], errors="coerce").dropna()
    highs = pd.to_numeric(x["High"], errors="coerce").dropna()

    if lows.empty or highs.empty:
        return {
            "range_low": None,
            "range_high": None,
            "range_position_pct": None,
            "range_zone": "unknown",
            "near_support": False,
            "near_resistance": False,
        }

    range_low = float(lows.min())
    range_high = float(highs.max())
    span = range_high - range_low

    if span <= 0:
        return {
            "range_low": round(range_low, 4),
            "range_high": round(range_high, 4),
            "range_position_pct": None,
            "range_zone": "unknown",
            "near_support": False,
            "near_resistance": False,
        }

    pos_pct = ((current_price - range_low) / span) * 100.0

    if pos_pct <= 30:
        zone = "near_support"
    elif pos_pct >= 70:
        zone = "near_resistance"
    else:
        zone = "mid_range"

    return {
        "range_low": round(range_low, 4),
        "range_high": round(range_high, 4),
        "range_position_pct": round(pos_pct, 2),
        "range_zone": zone,
        "near_support": pos_pct <= 30,
        "near_resistance": pos_pct >= 70,
    }


def compute_price_action_summary(symbol: str, current_price: float) -> dict:
    df = load_recent_price_action(symbol)
    candle = classify_last_candle(df)
    rng = compute_range_position(df, current_price=current_price)

    return {
        **candle,
        **rng,
    }