# performance_tracker.py
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Tuple, Optional

import pandas as pd
import yfinance as yf


@dataclass
class PortfolioConfig:
    output_dir: str = "outputs"
    open_file: str = "portfolio_open.csv"
    history_file: str = "trade_history.csv"
    max_open_positions: int = 5
    max_hold_days: int = 7


def _path(cfg: PortfolioConfig, name: str) -> str:
    os.makedirs(cfg.output_dir, exist_ok=True)
    return os.path.join(cfg.output_dir, name)


def load_open_portfolio(cfg: PortfolioConfig) -> pd.DataFrame:
    p = _path(cfg, cfg.open_file)
    if os.path.exists(p):
        df = pd.read_csv(p)
        if not df.empty:
            df["open_date"] = pd.to_datetime(df["open_date"], errors="coerce")
        return df
    return pd.DataFrame(columns=[
        "symbol", "open_date", "entry_price", "target_price", "stop_loss",
        "score", "confidence", "decision", "forecast_trend", "notes"
    ])


def save_open_portfolio(cfg: PortfolioConfig, df: pd.DataFrame) -> None:
    p = _path(cfg, cfg.open_file)
    df2 = df.copy()
    if "open_date" in df2.columns:
        df2["open_date"] = pd.to_datetime(df2["open_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df2.to_csv(p, index=False)


def append_trade_history(cfg: PortfolioConfig, rows: pd.DataFrame) -> None:
    p = _path(cfg, cfg.history_file)
    if rows is None or rows.empty:
        return
    if os.path.exists(p):
        prev = pd.read_csv(p)
        out = pd.concat([prev, rows], ignore_index=True)
    else:
        out = rows.copy()
    out.to_csv(p, index=False)


def _get_close_price(symbol: str) -> Optional[float]:
    try:
        h = yf.Ticker(symbol).history(period="2d")
        if h is None or h.empty:
            return None
        return float(h["Close"].iloc[-1])
    except Exception:
        return None


def add_new_positions_from_picks(
    cfg: PortfolioConfig,
    open_df: pd.DataFrame,
    picks_df: pd.DataFrame,
    run_date: datetime,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Add new positions from today's picks, respecting max_open_positions
    Returns: (updated_open_df, actually_added_df)
    """
    if picks_df is None or picks_df.empty:
        return open_df, pd.DataFrame()

    open_df = open_df.copy()
    picks_df = picks_df.copy()

    # Normalize
    picks_df["symbol"] = picks_df["symbol"].astype(str)
    existing = set(open_df["symbol"].astype(str).tolist()) if not open_df.empty else set()

    capacity = max(cfg.max_open_positions - len(existing), 0)
    if capacity <= 0:
        return open_df, pd.DataFrame()

    # Only add symbols not already open
    candidates = picks_df[~picks_df["symbol"].isin(existing)].copy()
    if candidates.empty:
        return open_df, pd.DataFrame()

    candidates = candidates.head(capacity).copy()

    added_rows = []
    for _, r in candidates.iterrows():
        added_rows.append({
            "symbol": r.get("symbol"),
            "open_date": run_date.strftime("%Y-%m-%d"),
            "entry_price": float(r.get("current", 0.0) or 0.0),
            "target_price": float(r.get("target_price", r.get("predicted_price", 0.0)) or 0.0),
            "stop_loss": float(r.get("stop_loss", 0.0) or 0.0),
            "score": int(r.get("score", 0) or 0),
            "confidence": int(r.get("confidence", 0) or 0),
            "decision": str(r.get("decision", "")),
            "forecast_trend": str(r.get("forecast_trend", "")),
            "notes": "Auto-added from daily picks",
        })

    added_df = pd.DataFrame(added_rows)
    open_df = pd.concat([open_df, added_df], ignore_index=True)
    return open_df, added_df


def update_and_close_positions(
    cfg: PortfolioConfig,
    open_df: pd.DataFrame,
    asof: datetime,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Check close prices and close positions if target/stop/time-exit hit.
    Returns: (remaining_open_df, closed_today_df)
    """
    if open_df is None or open_df.empty:
        return open_df, pd.DataFrame()

    open_df = open_df.copy()
    open_df["open_date"] = pd.to_datetime(open_df["open_date"], errors="coerce")
    open_df["close_price"] = None
    open_df["days_held"] = (asof.date() - open_df["open_date"].dt.date).apply(lambda x: x.days if pd.notna(x) else 0)

    closed_rows = []
    keep_rows = []

    for _, r in open_df.iterrows():
        sym = str(r["symbol"])
        close_price = _get_close_price(sym)
        if close_price is None:
            # cannot evaluate -> keep it open
            keep_rows.append(r.to_dict())
            continue

        entry = float(r.get("entry_price", 0.0) or 0.0)
        target = float(r.get("target_price", 0.0) or 0.0)
        stop = float(r.get("stop_loss", 0.0) or 0.0)
        days_held = int(r.get("days_held", 0) or 0)

        reason = None
        if target > 0 and close_price >= target:
            reason = "TARGET HIT"
        elif stop > 0 and close_price <= stop:
            reason = "STOP HIT"
        elif days_held >= cfg.max_hold_days:
            reason = "TIME EXIT"

        if reason:
            pnl_pct = ((close_price - entry) / entry * 100) if entry else 0.0
            closed_rows.append({
                "symbol": sym,
                "open_date": r.get("open_date").strftime("%Y-%m-%d") if pd.notna(r.get("open_date")) else "",
                "close_date": asof.strftime("%Y-%m-%d"),
                "entry_price": entry,
                "close_price": close_price,
                "pnl_pct": pnl_pct,
                "reason": reason,
                "target_price": target,
                "stop_loss": stop,
                "score": int(r.get("score", 0) or 0),
                "confidence": int(r.get("confidence", 0) or 0),
                "decision": str(r.get("decision", "")),
                "forecast_trend": str(r.get("forecast_trend", "")),
            })
        else:
            rr = r.to_dict()
            rr["close_price"] = close_price
            keep_rows.append(rr)

    remaining = pd.DataFrame(keep_rows)
    closed_df = pd.DataFrame(closed_rows)
    return remaining, closed_df


def portfolio_summary(open_df: pd.DataFrame, closed_df: pd.DataFrame) -> str:
    open_cnt = 0 if open_df is None else len(open_df)
    closed_cnt = 0 if closed_df is None else len(closed_df)
    avg_pnl = 0.0
    if closed_df is not None and not closed_df.empty and "pnl_pct" in closed_df.columns:
        avg_pnl = float(closed_df["pnl_pct"].mean())
    return f"Open positions: {open_cnt} | Closed today: {closed_cnt} | Avg P/L closed today: {avg_pnl:.2f}%"