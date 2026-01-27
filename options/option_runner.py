# options/option_runner.py
from __future__ import annotations

import json
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Optional

import pandas as pd

from options.option_marketdata import fetch_option_quote
from options.option_scoring import score_option_C  # returns OptionDecision object

LOCAL_TZ = ZoneInfo("America/Chicago")


def _as_float(x) -> Optional[float]:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return None
        return float(x)
    except Exception:
        return None


def _as_pos_float(x) -> Optional[float]:
    """Treat 0 or negative values as missing (common in options quotes after-hours)."""
    v = _as_float(x)
    if v is None:
        return None
    return v if v > 0 else None


def run_options(
    occ_list: List[str],
    *,
    max_rows: Optional[int] = None,
    debug_print: bool = False,
) -> pd.DataFrame:
    """
    Runs option marketdata + scoring for a list of OCC symbols and returns a stable-schema dataframe.

    - Never hardcodes any symbols.
    - Skips bad OCC symbols safely (continues with others).
    - Stores metrics as JSON string (Excel/email friendly).
    """
    rows: List[Dict[str, Any]] = []
    run_ts = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")

    occs = [o.strip().upper() for o in (occ_list or []) if str(o).strip()]
    if max_rows is not None:
        occs = occs[: max(0, int(max_rows))]

    for occ in occs:
        try:
            q = fetch_option_quote(occ)
            od = score_option_C(q)  # OptionDecision

            decision = str(getattr(od, "bucket", "") or "").upper().strip()
            score = getattr(od, "score", None)
            conf = getattr(od, "confidence", None)
            reasons = getattr(od, "reasons", None)
            metrics = getattr(od, "metrics", None)

            # reasons may be list[str] or single string
            if isinstance(reasons, list):
                reasons_str = " | ".join([str(x) for x in reasons[:6]])
            else:
                reasons_str = str(reasons or "")

            # metrics: keep as JSON string for safer CSV/Excel/email
            try:
                metrics_json = json.dumps(metrics if isinstance(metrics, dict) else {}, default=str)
            except Exception:
                metrics_json = "{}"

            rows.append({
                "occ": q.occ,
                "underlying": q.underlying,
                "expiry": q.expiry,
                "type": q.option_type,
                "strike": _as_float(q.strike),

                "mid": _as_pos_float(getattr(q, "mid", None)),
                "bid": _as_pos_float(getattr(q, "bid", None)),
                "ask": _as_pos_float(getattr(q, "ask", None)),
                "last": _as_pos_float(getattr(q, "last_price", None)),

                "volume": _as_pos_float(getattr(q, "volume", None)),
                "open_interest": _as_pos_float(getattr(q, "open_interest", None)),
                "iv": _as_pos_float(getattr(q, "iv", None)),

                "underlying_price": _as_pos_float(getattr(q, "underlying_price", None)),
                "dte_days": getattr(q, "dte_days", None),
                "intrinsic_value": _as_float(getattr(q, "intrinsic_value", None)),
                "fallback_used": bool(getattr(q, "fallback_used", False)),
                "fallback_reason": str(getattr(q, "fallback_reason", "") or ""),

                "decision": decision,                  # BUY/WATCH/AVOID
                "score": _as_float(score),             # 0-100
                "confidence": _as_float(conf),         # 1-10
                "reasons": reasons_str,
                "metrics": metrics_json,

                "run_ts": run_ts,
            })

            if debug_print:
                print(f"✅ {occ} -> {decision} score={_as_float(score)} conf={_as_float(conf)}")

        except Exception as e:
            # Do NOT crash the whole run if one OCC breaks
            rows.append({
                "occ": occ,
                "underlying": "",
                "expiry": "",
                "type": "",
                "strike": None,

                "mid": None,
                "bid": None,
                "ask": None,
                "last": None,

                "volume": None,
                "open_interest": None,
                "iv": None,

                "underlying_price": None,
                "dte_days": None,
                "intrinsic_value": None,
                "fallback_used": True,
                "fallback_reason": f"runner_exception:{type(e).__name__}",

                "decision": "AVOID",
                "score": 0.0,
                "confidence": 1.0,
                "reasons": f"Runner error: {e}",
                "metrics": "{}",

                "run_ts": run_ts,
            })
            if debug_print:
                print(f"❌ {occ} failed: {e}")

    df = pd.DataFrame(rows)

    # Sort best-first: BUY > WATCH > AVOID, then score desc
    if not df.empty and "decision" in df.columns:
        rank = {"BUY": 0, "WATCH": 1, "AVOID": 2}
        df["_rank"] = df["decision"].astype(str).str.upper().map(rank).fillna(9)

        if "score" in df.columns:
            df["score"] = pd.to_numeric(df["score"], errors="coerce")

        df = df.sort_values(by=["_rank", "score"], ascending=[True, False]).drop(columns=["_rank"])

    return df


if __name__ == "__main__":
    # Intentionally no hardcoded symbols here.
    # Run via option_mode.py or set OPTIONS_OCC_LIST and run option_mode.
    print("✅ option_runner.py is a library module. Run via: python -m options.option_mode --dry-run")