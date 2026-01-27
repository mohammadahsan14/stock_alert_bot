# options/option_universe.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
import yfinance as yf

LOCAL_TZ = ZoneInfo("America/Chicago")


@dataclass
class UniverseConfig:
    dte_min: int = 20
    dte_max: int = 120
    strikes_pct: Tuple[float, ...] = (-0.05, -0.025, 0.0, 0.025, 0.05)
    max_contracts_per_underlying: int = 5
    max_underlyings: int = 15
    allow_only_simple_tickers: bool = True  # ✅ new


def _safe_float(x) -> Optional[float]:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return None
        return float(x)
    except Exception:
        return None


def _is_simple_ticker(sym: str) -> bool:
    # allow A-Z 1..6 only for OCC root (no dots/dashes)
    return bool(sym) and sym.isalpha() and sym.upper() == sym and 1 <= len(sym) <= 6


def _occ_symbol(root: str, exp_yyyy_mm_dd: str, cp: str, strike: float) -> str:
    root = (root or "").upper().strip()[:6]
    yy = exp_yyyy_mm_dd[2:4]
    mm = exp_yyyy_mm_dd[5:7]
    dd = exp_yyyy_mm_dd[8:10]
    strike_int = int(round(float(strike) * 1000))
    strike8 = f"{strike_int:08d}"
    return f"{root}{yy}{mm}{dd}{cp.upper()}{strike8}"


def build_occ_list_from_underlyings(
    underlyings: List[str],
    cp: str = "C",  # "C", "P", or "BOTH"
    cfg: UniverseConfig = UniverseConfig(),
) -> List[str]:
    cleaned = []
    for u in underlyings or []:
        s = str(u).strip().upper().replace(" ", "")
        if not s:
            continue
        if cfg.allow_only_simple_tickers and not _is_simple_ticker(s):
            continue
        cleaned.append(s)

    cleaned = sorted(set(cleaned))[: cfg.max_underlyings]

    out: List[str] = []
    today = datetime.now(LOCAL_TZ).date()

    cps: List[str]
    cp_up = (cp or "C").upper().strip()
    if cp_up == "BOTH":
        cps = ["C", "P"]
    elif cp_up in ("C", "P"):
        cps = [cp_up]
    else:
        cps = ["C"]

    for sym in cleaned:
        try:
            t = yf.Ticker(sym)

            hist = t.history(period="2d", auto_adjust=False)
            if hist is None or hist.empty:
                continue
            px = _safe_float(hist["Close"].iloc[-1])
            if px is None or px <= 0:
                continue

            expiries = list(t.options or [])
            if not expiries:
                continue

            good = []
            for e in expiries:
                d = pd.to_datetime(e, errors="coerce")
                if pd.isna(d):
                    continue
                dte = (d.date() - today).days
                if cfg.dte_min <= dte <= cfg.dte_max:
                    good.append((dte, e))

            if not good:
                continue

            good.sort(key=lambda x: x[0])
            expiry = good[0][1]  # nearest expiry in window

            strikes = []
            for pct in cfg.strikes_pct:
                strikes.append(round(px * (1.0 + pct), 0))

            strikes = sorted(set(strikes))[: cfg.max_contracts_per_underlying]

            for strike in strikes:
                for one_cp in cps:
                    out.append(_occ_symbol(sym, expiry, one_cp, strike))

        except Exception:
            continue

    return sorted(set(out))