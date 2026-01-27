# forecast_engine.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple
import pandas as pd
import yfinance as yf


@dataclass
class ForecastResult:
    predicted_price: float
    target_price: float
    stop_loss: float
    atr: float
    trend: str  # "up" | "down" | "sideways"
    reason: str


def _safe_float(x, default: float = 0.0) -> float:
    try:
        v = float(x)
        if pd.isna(v):
            return default
        return v
    except Exception:
        return default


def _fetch_history(symbol: str, lookback: str) -> pd.DataFrame:
    """
    yfinance can be flaky; keep this hardened and simple.
    """
    try:
        df = yf.Ticker(symbol).history(period=lookback, auto_adjust=False)
        if isinstance(df, pd.DataFrame):
            return df
    except Exception:
        pass
    return pd.DataFrame()


def compute_atr(df: pd.DataFrame, period: int = 14) -> float:
    """
    ATR via SMA(TrueRange). Stable, low-risk.
    """
    if df is None or df.empty:
        return 0.0

    df = df.dropna(subset=["High", "Low", "Close"]).copy()
    if len(df) < period + 2:
        return 0.0

    high = pd.to_numeric(df["High"], errors="coerce")
    low = pd.to_numeric(df["Low"], errors="coerce")
    close = pd.to_numeric(df["Close"], errors="coerce")
    prev_close = close.shift(1)

    tr = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1
    ).max(axis=1)

    atr = tr.rolling(period).mean().iloc[-1]
    return _safe_float(atr, 0.0)


def compute_trend(df: pd.DataFrame) -> str:
    """
    Trend via SMA20/SMA50 and last vs SMA20.
    """
    if df is None or df.empty:
        return "sideways"

    df = df.dropna(subset=["Close"]).copy()
    if len(df) < 55:
        return "sideways"

    close = pd.to_numeric(df["Close"], errors="coerce").dropna()
    if len(close) < 55:
        return "sideways"

    sma20 = close.rolling(20).mean().iloc[-1]
    sma50 = close.rolling(50).mean().iloc[-1]
    last = close.iloc[-1]

    if pd.isna(sma20) or pd.isna(sma50) or pd.isna(last):
        return "sideways"

    if last > sma20 and sma20 > sma50:
        return "up"
    if last < sma20 and sma20 < sma50:
        return "down"
    return "sideways"


def _score_to_multipliers(score: int) -> Tuple[float, float]:
    """
    score -> (target_ATR_multiple, stop_ATR_multiple)
    """
    if score >= 70:
        return 2.2, 1.2
    if score >= 50:
        return 2.0, 1.15
    if score >= 25:
        return 1.2, 1.0
    return 0.6, 0.9


def forecast_price_levels(
    symbol: str,
    current: float,
    score: int,
    lookback: str = "3mo",
    max_up_pct: float = 0.15,     # safety clamp
    max_down_pct: float = 0.10,   # safety clamp
    min_stop_pct: float = 0.02,   # ensures stop isn't too tight/useless
    min_atr_pct: float = 0.002,   # 0.2% of price (below this ATR is "too small")
    max_atr_pct: float = 0.12,    # 12% of price (above this ATR is "too wild")
) -> ForecastResult:
    """
    ATR + trend bias forecast (monitoring-safe).
    Adds sanity checks so we don't generate nonsense.
    """
    current = _safe_float(current, 0.0)
    if current <= 0:
        return ForecastResult(0.0, 0.0, 0.0, 0.0, "sideways", "Invalid current price")

    # Fetch history (hardened)
    df = _fetch_history(symbol, lookback=lookback)
    if df is None or df.empty or len(df) < 20:
        stop = max(0.01, current * (1 - max(max_down_pct, min_stop_pct)))
        return ForecastResult(
            predicted_price=current,
            target_price=current,
            stop_loss=stop,
            atr=0.0,
            trend="sideways",
            reason="Not enough history; fallback used"
        )

    df = df.dropna(subset=["High", "Low", "Close"]).copy()
    trend = compute_trend(df)
    atr = compute_atr(df, period=14)

    if atr <= 0:
        stop = max(0.01, current * (1 - max(max_down_pct, min_stop_pct)))
        return ForecastResult(
            predicted_price=current,
            target_price=current,
            stop_loss=stop,
            atr=0.0,
            trend=trend,
            reason="ATR unavailable; fallback stop used"
        )

    # ATR sanity checks
    atr_pct = atr / current if current else 0.0
    if atr_pct < min_atr_pct:
        # ATR too tiny => targets/stops become meaningless
        stop = max(0.01, current * (1 - max(max_down_pct, min_stop_pct)))
        return ForecastResult(
            predicted_price=current,
            target_price=current,
            stop_loss=stop,
            atr=float(atr),
            trend=trend,
            reason=f"ATR too small ({atr_pct:.2%}); fallback used"
        )

    if atr_pct > max_atr_pct:
        # ATR too huge => clamp hard to prevent nonsense
        # We'll still return a forecast, but the reason makes it obvious.
        atr = current * max_atr_pct
        atr_pct = max_atr_pct

    k, m = _score_to_multipliers(int(score))

    # Trend bias (small nudges)
    if trend == "up":
        k *= 1.10
    elif trend == "down":
        k *= 0.85
        m *= 1.10

    predicted = current + (k * atr)
    target = current + ((k + 0.30) * atr)
    stop = current - (m * atr)

    # Safety clamps
    predicted = min(predicted, current * (1 + max_up_pct))
    target = min(target, current * (1 + max_up_pct))

    # Ensure stop is not too tight (and not beyond max_down clamp)
    # Ensure stop is within [stop_floor, stop_min_dist]
    stop_floor = current * (1 - max_down_pct)  # can't be worse than this
    stop_min_dist = current * (1 - min_stop_pct)  # must be at least this far away

    low = min(stop_floor, stop_min_dist)
    high = max(stop_floor, stop_min_dist)

    stop = max(stop, low)
    stop = min(stop, high)

    # Final sanity
    predicted = max(0.01, _safe_float(predicted, current))
    target = max(0.01, _safe_float(target, current))
    stop = max(0.01, _safe_float(stop, current * 0.97))

    # If somehow stop >= current, force it to min distance
    if stop >= current:
        stop = max(0.01, current * (1 - min_stop_pct))

    reason = (
        f"ATR(14)={atr:.4f} ({atr_pct:.2%}), trend={trend}, "
        f"k={k:.2f}, m={m:.2f}, clamp=+{max_up_pct:.0%}/-{max_down_pct:.0%}, "
        f"min_stop={min_stop_pct:.0%}"
    )

    return ForecastResult(
        predicted_price=float(predicted),
        target_price=float(target),
        stop_loss=float(stop),
        atr=float(atr),
        trend=trend,
        reason=reason
    )