# performance_tracker.py (UPDATED + hardened)
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Tuple, Optional, Dict, Any

import pandas as pd
import yfinance as yf


# -----------------------------
# Config
# -----------------------------
@dataclass
class PortfolioConfig:
    # Base folder will be env-aware: outputs/<APP_ENV>/portfolio/
    output_dir: str = "outputs"
    open_file: str = "portfolio_open.csv"
    history_file: str = "trade_history.csv"
    max_open_positions: int = 5
    max_hold_days: int = 7


def _env_name() -> str:
    return (os.getenv("APP_ENV") or "prod").strip().lower() or "prod"


def _portfolio_dir(cfg: PortfolioConfig) -> str:
    # outputs/<env>/portfolio
    p = os.path.join(cfg.output_dir, _env_name(), "portfolio")
    os.makedirs(p, exist_ok=True)
    return p


def _path(cfg: PortfolioConfig, name: str) -> str:
    return os.path.join(_portfolio_dir(cfg), name)


OPEN_COLS = [
    "symbol", "open_date", "entry_price", "target_price", "stop_loss",
    "score", "confidence", "decision", "forecast_trend", "notes",
]

HISTORY_COLS = [
    "action",
    "symbol", "open_date", "close_date", "entry_price", "close_price",
    "pnl_pct", "reason", "target_price", "stop_loss",
    "score", "confidence", "decision", "forecast_trend",
]


def _ensure_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    if df is None or not isinstance(df, pd.DataFrame):
        df = pd.DataFrame()
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    return df[cols].copy()


# -----------------------------
# Load / Save
# -----------------------------
def load_open_portfolio(cfg: PortfolioConfig) -> pd.DataFrame:
    p = _path(cfg, cfg.open_file)
    if os.path.exists(p) and os.path.getsize(p) > 0:
        try:
            df = pd.read_csv(p)
        except Exception:
            df = pd.DataFrame()
    else:
        df = pd.DataFrame()

    df = _ensure_cols(df, OPEN_COLS)
    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
    df["open_date"] = pd.to_datetime(df["open_date"], errors="coerce")

    if not df.empty:
        df = df.sort_values(by=["symbol", "open_date"], ascending=[True, True])
        df = df.drop_duplicates(subset=["symbol"], keep="last").reset_index(drop=True)

    return df


def save_open_portfolio(cfg: PortfolioConfig, df: pd.DataFrame) -> None:
    p = _path(cfg, cfg.open_file)

    df2 = _ensure_cols(df, OPEN_COLS)
    df2["symbol"] = df2["symbol"].astype(str).str.upper().str.strip()

    od = pd.to_datetime(df2["open_date"], errors="coerce")
    df2["open_date"] = od.dt.strftime("%Y-%m-%d")

    df2 = df2.drop_duplicates(subset=["symbol"], keep="last")
    df2.to_csv(p, index=False)


# -----------------------------
# Trade history
# -----------------------------
def append_trade_history(cfg: PortfolioConfig, rows: pd.DataFrame) -> None:
    if rows is None or rows.empty:
        return

    p = _path(cfg, cfg.history_file)

    new = _ensure_cols(rows, HISTORY_COLS)
    new["symbol"] = new["symbol"].astype(str).str.upper().str.strip()

    if os.path.exists(p) and os.path.getsize(p) > 0:
        try:
            prev = pd.read_csv(p)
        except Exception:
            prev = pd.DataFrame()
        prev = _ensure_cols(prev, HISTORY_COLS)
        prev["symbol"] = prev["symbol"].astype(str).str.upper().str.strip()
        out = pd.concat([prev, new], ignore_index=True)
    else:
        out = new

    # ✅ Robust dedupe for both OPEN and CLOSE actions
    out["action"] = out["action"].astype(str)
    out["open_date"] = out["open_date"].astype(str)
    out["close_date"] = out["close_date"].astype(str)

    # OPEN: symbol|OPEN|open_date
    # CLOSE: symbol|CLOSE|close_date
    out["dedupe_key"] = out.apply(
        lambda r: f"{r.get('symbol','')}|{r.get('action','')}|"
                  f"{r.get('open_date','') if r.get('action')=='OPEN' else r.get('close_date','')}",
        axis=1
    )

    out = out.drop_duplicates(subset=["dedupe_key"], keep="last").drop(columns=["dedupe_key"])
    out.to_csv(p, index=False)


def append_open_actions(cfg: PortfolioConfig, added_df: pd.DataFrame) -> None:
    """
    Log OPEN actions into trade_history.csv so history is complete.
    """
    if added_df is None or added_df.empty:
        return

    rows = added_df.copy()
    rows["action"] = "OPEN"
    rows["close_date"] = ""
    rows["close_price"] = pd.NA
    rows["pnl_pct"] = pd.NA
    rows["reason"] = "OPEN"

    append_trade_history(cfg, rows)


# -----------------------------
# Price fetch (hardened)
# -----------------------------
def _get_last_close_price(symbol: str) -> Tuple[Optional[float], str]:
    """
    Returns: (close_price, reason)
    reason helps debugging monitoring issues.
    """
    try:
        h = yf.Ticker(symbol).history(period="5d", auto_adjust=False)
        if h is None or not isinstance(h, pd.DataFrame) or h.empty:
            return None, "no_history"

        if "Close" not in h.columns:
            return None, "no_close_column"

        close = pd.to_numeric(h["Close"], errors="coerce").dropna()
        if close.empty:
            return None, "no_close_values"

        return float(close.iloc[-1]), "ok"
    except Exception as e:
        return None, f"error:{e!r}"


# -----------------------------
# Portfolio updates
# -----------------------------
def add_new_positions_from_picks(
    cfg: PortfolioConfig,
    open_df: pd.DataFrame,
    picks_df: pd.DataFrame,
    run_date: datetime,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Add new positions from today's picks, respecting max_open_positions.
    Returns: (updated_open_df, actually_added_df)
    """
    open_df = _ensure_cols(open_df, OPEN_COLS)
    open_df["symbol"] = open_df["symbol"].astype(str).str.upper().str.strip()
    open_df["open_date"] = pd.to_datetime(open_df["open_date"], errors="coerce")

    if picks_df is None or picks_df.empty:
        return open_df, pd.DataFrame(columns=OPEN_COLS)

    picks = picks_df.copy()
    picks["symbol"] = picks["symbol"].astype(str).str.upper().str.strip()

    existing = set(open_df["symbol"].dropna().astype(str).tolist())
    capacity = max(cfg.max_open_positions - len(existing), 0)
    if capacity <= 0:
        return open_df, pd.DataFrame(columns=OPEN_COLS)

    candidates = picks[~picks["symbol"].isin(existing)].copy()
    if candidates.empty:
        return open_df, pd.DataFrame(columns=OPEN_COLS)

    candidates = candidates.head(capacity).copy()

    added_rows: list[Dict[str, Any]] = []
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

    # ✅ build added_df BEFORE using it
    added_df = _ensure_cols(pd.DataFrame(added_rows), OPEN_COLS)
    added_df["symbol"] = added_df["symbol"].astype(str).str.upper().str.strip()
    added_df["open_date"] = pd.to_datetime(added_df["open_date"], errors="coerce")

    # --- safe concat (avoids pandas FutureWarning) ---
    if open_df is None or open_df.empty:
        merged = added_df.copy()
    elif added_df.empty:
        merged = open_df.copy()
    else:
        left = open_df.dropna(axis=1, how="all")
        right = added_df.dropna(axis=1, how="all")
        merged = pd.concat([left, right], ignore_index=True)

    merged = _ensure_cols(merged, OPEN_COLS)
    merged["symbol"] = merged["symbol"].astype(str).str.upper().str.strip()
    merged["open_date"] = pd.to_datetime(merged["open_date"], errors="coerce")

    # de-dupe (idempotent across reruns)
    merged = merged.sort_values(by=["symbol", "open_date"], ascending=[True, True])
    merged = merged.drop_duplicates(subset=["symbol"], keep="last").reset_index(drop=True)

    return merged, added_df


def update_and_close_positions(
    cfg: PortfolioConfig,
    open_df: pd.DataFrame,
    asof: datetime,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Close positions if target/stop/time-exit hit.
    Returns: (remaining_open_df, closed_today_df)
    """
    open_df = _ensure_cols(open_df, OPEN_COLS)
    if open_df.empty:
        return open_df, pd.DataFrame(columns=HISTORY_COLS)

    df = open_df.copy()
    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
    df["open_date"] = pd.to_datetime(df["open_date"], errors="coerce")

    def _days_held(od):
        if pd.isna(od):
            return 0
        return int((asof.date() - od.date()).days)

    df["days_held"] = df["open_date"].apply(_days_held)

    closed_rows = []
    keep_rows = []

    for _, r in df.iterrows():
        sym = str(r["symbol"]).strip().upper()
        close_price, close_reason = _get_last_close_price(sym)

        if close_price is None:
            rr = r.to_dict()
            rr["notes"] = f"{(rr.get('notes') or '')} | close_fetch={close_reason}".strip(" |")
            keep_rows.append(rr)
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
            pnl_pct = 0.0
            if entry and entry > 0:
                pnl_pct = ((close_price - entry) / entry) * 100.0

            closed_rows.append({
                "action": "CLOSE",
                "symbol": sym,
                "open_date": r.get("open_date").strftime("%Y-%m-%d") if pd.notna(r.get("open_date")) else "",
                "close_date": asof.strftime("%Y-%m-%d"),
                "entry_price": entry,
                "close_price": float(close_price),
                "pnl_pct": float(pnl_pct),
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
            rr["notes"] = f"{(rr.get('notes') or '')} | last_close={close_price:.2f}".strip(" |")
            keep_rows.append(rr)

    remaining = _ensure_cols(pd.DataFrame(keep_rows), OPEN_COLS)
    closed_df = _ensure_cols(pd.DataFrame(closed_rows), HISTORY_COLS)

    if not remaining.empty:
        remaining["symbol"] = remaining["symbol"].astype(str).str.upper().str.strip()
        remaining["open_date"] = pd.to_datetime(remaining["open_date"], errors="coerce")
        remaining = remaining.sort_values(by=["symbol", "open_date"], ascending=[True, True])
        remaining = remaining.drop_duplicates(subset=["symbol"], keep="last").reset_index(drop=True)

    return remaining, closed_df


def portfolio_summary(open_df: pd.DataFrame, closed_df: pd.DataFrame) -> str:
    open_cnt = 0 if open_df is None else len(open_df)
    closed_cnt = 0 if closed_df is None else len(closed_df)
    avg_pnl = 0.0
    if closed_df is not None and not closed_df.empty and "pnl_pct" in closed_df.columns:
        avg_pnl = float(pd.to_numeric(closed_df["pnl_pct"], errors="coerce").dropna().mean() or 0.0)
    return f"Open positions: {open_cnt} | Closed today: {closed_cnt} | Avg P/L closed today: {avg_pnl:.2f}%"