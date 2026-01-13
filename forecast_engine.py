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


def compute_atr(df: pd.DataFrame, period: int = 14) -> float:
    """
    Average True Range (ATR) from OHLC history.
    Uses SMA on True Range; stable and simple.
    """
    if df is None or df.empty:
        return 0.0

    df = df.dropna(subset=["High", "Low", "Close"]).copy()
    if len(df) < period + 2:
        return 0.0

    high = df["High"]
    low = df["Low"]
    close = df["Close"]
    prev_close = close.shift(1)

    tr = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1
    ).max(axis=1)

    atr = tr.rolling(period).mean().iloc[-1]
    return _safe_float(atr, 0.0)


def compute_trend(df: pd.DataFrame) -> str:
    """
    Simple trend detection using SMA20 vs SMA50 and latest close vs SMA20.
    """
    if df is None or df.empty:
        return "sideways"

    df = df.dropna(subset=["Close"]).copy()
    if len(df) < 55:
        return "sideways"

    close = df["Close"]
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
    Convert score -> (target_ATR_multiple, stop_ATR_multiple)
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
    max_up_pct: float = 0.15,   # safety clamp
    max_down_pct: float = 0.10  # safety clamp
) -> ForecastResult:
    """
    Produces predicted/target/stop based on ATR + trend bias.

    - predicted_price: expected level
    - target_price: take-profit level
    - stop_loss: ATR-based stop
    """
    current = _safe_float(current, 0.0)
    if current <= 0:
        return ForecastResult(
            predicted_price=0.0,
            target_price=0.0,
            stop_loss=0.0,
            atr=0.0,
            trend="sideways",
            reason="Invalid current price"
        )

    try:
        df = yf.Ticker(symbol).history(period=lookback, auto_adjust=False)

        if df is None or df.empty or len(df) < 20:
            return ForecastResult(
                predicted_price=current,
                target_price=current,
                stop_loss=max(0.01, current * 0.97),
                atr=0.0,
                trend="sideways",
                reason="Not enough history; fallback used"
            )

        df = df.dropna(subset=["High", "Low", "Close"]).copy()

        atr = compute_atr(df, period=14)
        trend = compute_trend(df)

        if atr <= 0:
            return ForecastResult(
                predicted_price=current,
                target_price=current,
                stop_loss=max(0.01, current * 0.97),
                atr=0.0,
                trend=trend,
                reason="ATR unavailable; fallback stop used"
            )

        k, m = _score_to_multipliers(int(score))

        # trend bias (small nudges)
        if trend == "up":
            k *= 1.10
        elif trend == "down":
            k *= 0.85
            m *= 1.10

        predicted = current + (k * atr)
        target = current + ((k + 0.30) * atr)  # slightly above predicted
        stop = current - (m * atr)

        # Safety clamps (prevents absurd levels due to weird ATR)
        predicted = min(predicted, current * (1 + max_up_pct))
        target = min(target, current * (1 + max_up_pct))
        stop = max(stop, current * (1 - max_down_pct))

        predicted = max(0.01, predicted)
        target = max(0.01, target)
        stop = max(0.01, stop)

        reason = f"ATR(14)={atr:.2f}, trend={trend}, k={k:.2f}, m={m:.2f}, clamp=+{max_up_pct:.0%}/-{max_down_pct:.0%}"

        return ForecastResult(
            predicted_price=float(predicted),
            target_price=float(target),
            stop_loss=float(stop),
            atr=float(atr),
            trend=trend,
            reason=reason
        )

    except Exception as e:
        return ForecastResult(
            predicted_price=current,
            target_price=current,
            stop_loss=max(0.01, current * 0.97),
            atr=0.0,
            trend="sideways",
            reason=f"Forecast error fallback: {e}"
        )