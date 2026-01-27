from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

import pandas as pd
import yfinance as yf

from options.option_parser import parse_option_symbol


@dataclass
class OptionQuote:
    occ: str
    underlying: str
    expiry: str
    option_type: str
    strike: float

    last_price: Optional[float] = None
    bid: Optional[float] = None
    ask: Optional[float] = None
    mid: Optional[float] = None
    volume: Optional[float] = None
    open_interest: Optional[float] = None
    iv: Optional[float] = None

    underlying_price: Optional[float] = None
    underlying_day_high: Optional[float] = None
    underlying_day_low: Optional[float] = None

    dte_days: Optional[int] = None
    intrinsic_value: Optional[float] = None
    fallback_used: bool = False
    fallback_reason: str = ""

    chain_expiry_used: str = ""
    fetched_at: str = ""


def _safe_float(x) -> Optional[float]:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return None
        return float(x)
    except Exception:
        return None


def _pos_float(x) -> Optional[float]:
    v = _safe_float(x)
    if v is None:
        return None
    return v if v > 0 else None


def _intrinsic_value(opt_type: str, underlying_price: Optional[float], strike: float) -> Optional[float]:
    if underlying_price is None:
        return None
    if opt_type.upper() == "C":
        return max(0.0, float(underlying_price) - float(strike))
    return max(0.0, float(strike) - float(underlying_price))


def _pick_chain_expiry(underlying: str, occ_expiry: str) -> Tuple[Optional[str], str]:
    t = yf.Ticker(underlying)
    expiries = list(t.options or [])
    if not expiries:
        return None, "yahoo_options_empty"

    if occ_expiry in expiries:
        return occ_expiry, "exact"

    try:
        occ_d = pd.to_datetime(occ_expiry).date()
    except Exception:
        return None, "occ_expiry_parse_failed"

    for delta_days in (1, 2, 3):
        cand = (occ_d - timedelta(days=delta_days)).strftime("%Y-%m-%d")
        if cand in expiries:
            return cand, f"shifted_minus_{delta_days}d"

    try:
        exp_dates = []
        for e in expiries:
            d = pd.to_datetime(e, errors="coerce")
            if pd.notna(d):
                exp_dates.append((d.date(), e))
        if not exp_dates:
            return None, "yahoo_expiry_list_unparseable"

        exp_dates.sort(key=lambda x: x[0])
        prior = [e for (d, e) in exp_dates if d <= occ_d]
        if prior:
            best = prior[-1]
            best_d = pd.to_datetime(best).date()
            if (occ_d - best_d).days <= 7:
                return best, "nearest_prior_within_7d"

        return None, "no_reasonable_nearest_expiry"
    except Exception:
        return None, "nearest_expiry_selection_failed"


def _best_strike_row(df: pd.DataFrame, strike: float) -> Optional[pd.Series]:
    if df is None or df.empty or "strike" not in df.columns:
        return None

    df2 = df.copy()
    df2["strike"] = pd.to_numeric(df2["strike"], errors="coerce")
    df2 = df2.dropna(subset=["strike"])
    if df2.empty:
        return None

    exact = df2[df2["strike"] == float(strike)]
    if not exact.empty:
        return exact.iloc[0]

    df2["dist"] = (df2["strike"] - float(strike)).abs()
    df2 = df2.sort_values("dist")
    return df2.iloc[0]


def fetch_option_quote(occ: str) -> OptionQuote:
    info = parse_option_symbol(occ)
    underlying = info["underlying"]
    occ_expiry = info["expiry"].strftime("%Y-%m-%d")
    opt_type = info["option_type"].upper()
    strike = float(info["strike"])

    fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    q = OptionQuote(
        occ=occ,
        underlying=underlying,
        expiry=occ_expiry,
        option_type=opt_type,
        strike=strike,
        fetched_at=fetched_at,
    )

    # DTE
    try:
        today_utc = datetime.now(timezone.utc).date()
        exp_d = pd.to_datetime(occ_expiry).date()
        q.dte_days = max(0, (exp_d - today_utc).days)
    except Exception:
        q.dte_days = None

    # Underlying snapshot
    try:
        uh = yf.Ticker(underlying).history(period="2d", auto_adjust=False)
        if uh is not None and not uh.empty:
            q.underlying_price = _pos_float(uh["Close"].iloc[-1])
            q.underlying_day_high = _pos_float(uh["High"].iloc[-1])
            q.underlying_day_low = _pos_float(uh["Low"].iloc[-1])
    except Exception:
        pass

    q.intrinsic_value = _intrinsic_value(q.option_type, q.underlying_price, q.strike)

    # Resolve expiry
    chain_expiry, why = _pick_chain_expiry(underlying, occ_expiry)
    if not chain_expiry:
        q.fallback_used = True
        q.fallback_reason = why
        return q

    q.chain_expiry_used = chain_expiry

    try:
        chain = yf.Ticker(underlying).option_chain(chain_expiry)
        df = chain.calls if opt_type == "C" else chain.puts

        row = _best_strike_row(df, strike)
        if row is None:
            q.fallback_used = True
            q.fallback_reason = "strike_not_found_in_chain"
            q.expiry = chain_expiry
            return q

        q.last_price = _pos_float(row.get("lastPrice"))
        q.bid = _pos_float(row.get("bid"))
        q.ask = _pos_float(row.get("ask"))

        # ✅ Build mid robustly:
        # 1) bid/ask mid if both exist
        # 2) else use last_price as a practical premium proxy
        if q.bid is not None and q.ask is not None and q.ask >= q.bid:
            q.mid = (q.bid + q.ask) / 2.0
        elif q.last_price is not None:
            q.mid = q.last_price
            q.fallback_used = True
            q.fallback_reason = (q.fallback_reason + "|mid_from_last_price").strip("|")
        else:
            q.mid = None

        q.volume = _pos_float(row.get("volume"))
        q.open_interest = _pos_float(row.get("openInterest"))

        # ✅ IV sanity: ignore obviously-bad tiny IV values
        iv_raw = _pos_float(row.get("impliedVolatility"))
        if iv_raw is not None and iv_raw < 0.05:
            q.iv = None
            q.fallback_used = True
            q.fallback_reason = (q.fallback_reason + "|iv_suspicious_low").strip("|")
        else:
            q.iv = iv_raw

        q.expiry = chain_expiry

        # If still no usable premium info, mark fallback
        if q.mid is None and q.last_price is None and q.bid is None and q.ask is None:
            q.fallback_used = True
            q.fallback_reason = (q.fallback_reason + "|premium_fields_missing_or_illiquid").strip("|")

        return q

    except Exception:
        q.fallback_used = True
        q.fallback_reason = "option_chain_fetch_failed"
        q.expiry = chain_expiry
        return q


if __name__ == "__main__":
    tests = [
        "AAPL260620C00250000",
        "TSLA260620P00400000",
    ]
    for occ in tests:
        print("\nOCC:", occ)
        print(fetch_option_quote(occ))