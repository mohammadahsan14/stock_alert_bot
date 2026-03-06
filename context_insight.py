# ============================================================
# 🔒 LOCKED FILE — DETERMINISTIC INSIGHT BUILDER
# File: context_insight.py
# Version: 2026-03-05_v2
#
# Notes:
# - Generates 1-line deterministic key_insight for email/Excel
# - Uses ONLY pipeline values (no LLM)
# - Flags include: extended move, trade gate, news risk, mover signal
#
# v2 changes:
# - WATCH rows now display "watch only (below trade gate)"
#   instead of "below trade gate" for clearer interpretation.
# - No change to trading logic, scoring, or confidence gating.
# ============================================================

from __future__ import annotations


def _safe_float(v, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _safe_int(v, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def build_key_insight(row: dict) -> str:
    """
    Deterministic 1-line insight summarizing the setup.
    Uses ONLY existing pipeline values (no hallucination risk).

    Intended for email/Excel quick scanning.
    """

    symbol = str(row.get("symbol", "")).upper()
    trend = str(row.get("forecast_trend", "sideways")).lower()
    news_flag = str(row.get("news_flag", "")).strip()
    stance = str(row.get("stance", "WATCH")).upper()
    mover_signal = str(row.get("mover_signal", "")).strip()

    score = _safe_int(row.get("score"))
    confidence = _safe_int(row.get("confidence"))
    vote_score = _safe_int(row.get("vote_score"))

    pct_change = _safe_float(row.get("pct_change"))
    target = _safe_float(row.get("target_price"))
    stop = _safe_float(row.get("stop_loss"))

    conf_gate = _safe_int(row.get("conf_gate"))

    flags = []

    # Detect extended moves
    if abs(pct_change) >= 6:
        flags.append("extended move")

    # Deterministic trade gate check
    # Deterministic trade gate check (WATCH wording tweak)
    if conf_gate > 0 and confidence < conf_gate:
        if stance == "WATCH":
            flags.append("watch only (below trade gate)")
        else:
            flags.append("below trade gate")

    # Detect negative news
    if news_flag == "🔴":
        flags.append("news risk")

    # Detect mover signal
    if mover_signal:
        ms = mover_signal.lower()
        # keep only short tags; avoid repeating the full phrase
        if "avoid" in ms or "watch" in ms:
            flags.append("watch only")
        elif "can consider buy" in ms:
            flags.append("buy ok")
        else:
            flags.append(ms)

    flag_text = ""
    if flags:
        flag_text = f"; flags={', '.join(flags)}"

    return (
        f"{symbol}: stance={stance}; trend={trend}; "
        f"score={score}; conf={confidence}; vote={vote_score}; "
        f"target={target:.2f}; stop={stop:.2f}"
        f"{flag_text}"
    )