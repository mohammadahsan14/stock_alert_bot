# forecast_engine.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple, Literal
import pandas as pd
import yfinance as yf


Horizon = Literal["swing", "intraday"]


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
    Swing: score -> (target_ATR_multiple, stop_ATR_multiple)
    """
    if score >= 70:
        return 2.2, 1.2
    if score >= 50:
        return 2.0, 1.15
    if score >= 25:
        return 1.2, 1.0
    return 0.6, 0.9


def _intraday_pcts(score: int, trend: str, atr_pct: float) -> tuple[float, float]:
    """
    Intraday targets should be much tighter than swing.
    Return (target_pct, stop_pct) as fractions of current price.

    We blend:
      - a score-based baseline
      - ATR-based cap so we don't demand huge moves intraday
      - slight trend bias
    """
    s = int(score)

    # baseline (roughly: 0.8% to 2.0% target)
    if s >= 70:
        tgt = 0.018
        stp = 0.012
    elif s >= 50:
        tgt = 0.014
        stp = 0.010
    elif s >= 25:
        tgt = 0.010
        stp = 0.009
    else:
        tgt = 0.008
        stp = 0.008

    # ATR-aware cap:
    # If ATR is small, don't set targets bigger than ~1.1 * ATR.
    # If ATR is large, still cap intraday targets (avoid 4–6% intraday nonsense).
    atr_cap = max(0.006, min(0.025, 1.10 * atr_pct))  # between 0.6% and 2.5%
    tgt = min(tgt, atr_cap)

    # Ensure stop is tighter than target, but not absurdly tight
    stp = min(stp, max(0.006, tgt * 0.75))

    # Trend bias
    if trend == "up":
        tgt *= 1.05
    elif trend == "down":
        tgt *= 0.90
        stp *= 1.05

    # Final clamps
    tgt = max(0.006, min(tgt, 0.025))
    stp = max(0.006, min(stp, 0.020))

    return float(tgt), float(stp)


def forecast_price_levels(
    symbol: str,
    current: float,
    score: int,
    lookback: str = "3mo",
    max_up_pct: float = 0.15,     # swing safety clamp
    max_down_pct: float = 0.10,   # swing safety clamp
    min_stop_pct: float = 0.02,   # swing min stop distance
    min_atr_pct: float = 0.002,   # 0.2% of price (below this ATR is "too small")
    max_atr_pct: float = 0.12,    # 12% of price (above this ATR is "too wild")
    horizon: Horizon = "swing",   # NEW: "swing" (default) or "intraday"
) -> ForecastResult:
    """
    ATR + trend bias forecast (monitoring-safe).
    Adds sanity checks so we don't generate nonsense.

    horizon="swing":
      - ATR-multiple targets (your original behavior)

    horizon="intraday":
      - much tighter % targets/stops so same-day "target hit" is realistic
    """
    current = _safe_float(current, 0.0)
    if current <= 0:
        return ForecastResult(0.0, 0.0, 0.0, 0.0, "sideways", "Invalid current price")

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

    atr_pct = atr / current if current else 0.0

    if atr_pct < min_atr_pct:
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
        atr = current * max_atr_pct
        atr_pct = max_atr_pct

    # -------------------------
    # INTRADAY MODE
    # -------------------------
    if horizon == "intraday":
        tgt_pct, stp_pct = _intraday_pcts(int(score), trend, atr_pct)

        predicted = current * (1.0 + max(0.004, min(0.020, tgt_pct * 0.9)))
        target = current * (1.0 + tgt_pct)
        stop = current * (1.0 - stp_pct)

        # intraday clamps (tighter than swing)
        max_up_i = min(0.03, max(0.02, 1.25 * atr_pct))     # usually 2%–3%
        max_dn_i = min(0.02, max(0.015, 1.10 * atr_pct))    # usually 1.5%–2%

        predicted = min(predicted, current * (1 + max_up_i))
        target = min(target, current * (1 + max_up_i))
        stop = max(stop, current * (1 - max_dn_i))

        predicted = max(0.01, _safe_float(predicted, current))
        target = max(0.01, _safe_float(target, current))
        stop = max(0.01, _safe_float(stop, current * (1 - max_dn_i)))

        if stop >= current:
            stop = current * (1 - max_dn_i)

        reason = (
            f"INTRADAY | ATR(14)={atr:.4f} ({atr_pct:.2%}), trend={trend}, "
            f"tgt_pct={tgt_pct:.2%}, stp_pct={stp_pct:.2%}, "
            f"clamp=+{max_up_i:.2%}/-{max_dn_i:.2%}"
        )

        return ForecastResult(
            predicted_price=float(predicted),
            target_price=float(target),
            stop_loss=float(stop),
            atr=float(atr),
            trend=trend,
            reason=reason
        )

    # -------------------------
    # SWING MODE (original)
    # -------------------------
    k, m = _score_to_multipliers(int(score))

    if trend == "up":
        k *= 1.10
    elif trend == "down":
        k *= 0.85
        m *= 1.10

    predicted = current + (k * atr)
    target = current + ((k + 0.30) * atr)
    stop = current - (m * atr)

    predicted = min(predicted, current * (1 + max_up_pct))
    target = min(target, current * (1 + max_up_pct))

    stop_floor = current * (1 - max_down_pct)
    stop_min_dist = current * (1 - min_stop_pct)

    low = min(stop_floor, stop_min_dist)
    high = max(stop_floor, stop_min_dist)

    stop = max(stop, low)
    stop = min(stop, high)

    predicted = max(0.01, _safe_float(predicted, current))
    target = max(0.01, _safe_float(target, current))
    stop = max(0.01, _safe_float(stop, current * 0.97))

    if stop >= current:
        stop = max(0.01, current * (1 - min_stop_pct))

    reason = (
        f"SWING | ATR(14)={atr:.4f} ({atr_pct:.2%}), trend={trend}, "
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