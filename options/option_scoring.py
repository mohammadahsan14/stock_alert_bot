# options/option_scoring.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Dict, Any, List

from options.option_marketdata import OptionQuote


@dataclass
class OptionDecision:
    bucket: str          # "BUY", "WATCH", "AVOID"
    score: int           # 0-100
    confidence: int      # 1-10
    reasons: List[str]
    metrics: Dict[str, Any]


def _pct(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None or b == 0:
        return None
    return (a / b) * 100.0


def _nz(x: Optional[float]) -> Optional[float]:
    """Normalize: treat 0/negative as missing for quote fields."""
    if x is None:
        return None
    try:
        v = float(x)
    except Exception:
        return None
    return None if v <= 0 else v


def _clean_iv(iv: Optional[float]) -> Optional[float]:
    """
    Normalize IV:
    - treat tiny values (like 0.00x) as missing (often after-hours garbage)
    - treat crazy large values as missing
    """
    iv = _nz(iv)
    if iv is None:
        return None
    if iv < 0.05 or iv > 5.0:
        return None
    return iv


def score_option_C(q: OptionQuote) -> OptionDecision:
    """
    Strategy C (premium-first):
    - Use option premium + liquidity + spread + IV + moneyness to score.
    - If premium is missing, fallback to intrinsic + underlying snapshot.
    """

    reasons: List[str] = []
    metrics: Dict[str, Any] = {}

    # ---------- Core fields (normalized) ----------
    # Premium: prefer mid, fallback to last_price if mid missing
    mid = _nz(getattr(q, "mid", None))
    last = _nz(getattr(q, "last_price", None))

    # If mid is missing but last exists, use last as premium proxy
    premium = mid if mid is not None else last

    bid = _nz(getattr(q, "bid", None))
    ask = _nz(getattr(q, "ask", None))

    vol = _nz(getattr(q, "volume", None))
    oi = _nz(getattr(q, "open_interest", None))
    iv = _clean_iv(getattr(q, "iv", None))

    dte = getattr(q, "dte_days", None)
    underlying = _nz(getattr(q, "underlying_price", None))
    strike = float(getattr(q, "strike", 0.0) or 0.0)
    opt_type = str(getattr(q, "option_type", "") or "").upper()

    # ---------- Derived ----------
    spread = None
    spread_pct = None
    if bid is not None and ask is not None and ask >= bid:
        spread = ask - bid
        # spread as % of premium (mid preferred, last fallback)
        spread_pct = _pct(spread, premium)

    moneyness_pct = None
    if underlying is not None and strike > 0:
        # For calls: positive = underlying above strike (ITM)
        moneyness_pct = ((underlying - strike) / underlying) * 100.0

    metrics.update({
        "premium_used": premium,
        "mid": mid,
        "last": last,
        "bid": bid,
        "ask": ask,
        "spread": spread,
        "spread_pct_of_premium": spread_pct,
        "volume": vol,
        "open_interest": oi,
        "iv": iv,
        "dte_days": dte,
        "underlying_price": underlying,
        "strike": strike,
        "moneyness_pct": moneyness_pct,
        "intrinsic_value": getattr(q, "intrinsic_value", None),
        "fallback_used": bool(getattr(q, "fallback_used", False)),
        "fallback_reason": str(getattr(q, "fallback_reason", "") or ""),
    })

    # ---------- Premium-first path ----------
    if premium is not None:
        score = 50  # baseline

        # Data-quality penalty: mid estimated from last price (no bid/ask)
        fb_used = bool(getattr(q, "fallback_used", False))
        fb_reason = str(getattr(q, "fallback_reason", "") or "")
        if fb_used and "mid_from_last_price" in fb_reason:
            score -= 6
            reasons.append("Mid estimated from last price (no bid/ask) — conservative penalty")

        # Liquidity: volume + OI
        v = int(vol) if vol is not None else 0

        if v >= 500:
            score += 12
            reasons.append(f"Good volume ({v})")
        elif v >= 200:
            score += 7
            reasons.append(f"Decent volume ({v})")
        elif v >= 50:
            score += 1
            reasons.append(f"Light volume ({v})")
        else:
            score -= 10
            reasons.append(f"Very low volume ({v})")

        # Optional bonus for open interest if present (don’t penalize if missing)
        if oi is not None:
            o = int(oi)
            if o >= 20000:
                score += 8
                reasons.append(f"Very strong open interest ({o})")
            elif o >= 8000:
                score += 5
                reasons.append(f"Strong open interest ({o})")
            elif o >= 2000:
                score += 2
                reasons.append(f"OK open interest ({o})")

        # Spread quality (only score if we have valid bid/ask)
        if spread_pct is not None:
            if spread_pct <= 1.0:
                score += 10
                reasons.append(f"Tight spread ({spread_pct:.2f}% of premium)")
            elif spread_pct <= 3.0:
                score += 4
                reasons.append(f"Acceptable spread ({spread_pct:.2f}% of premium)")
            else:
                score -= 10
                reasons.append(f"Wide spread ({spread_pct:.2f}% of premium)")
        else:
            # After-hours often has no bid/ask; don’t punish, just note it
            reasons.append("Spread unavailable (bid/ask missing)")

        # IV sanity
        if iv is not None:
            if iv <= 0.35:
                score += 6
                reasons.append(f"IV reasonable ({iv:.2f})")
            elif iv <= 0.60:
                score += 1
                reasons.append(f"IV moderate ({iv:.2f})")
            else:
                score -= 6
                reasons.append(f"IV high ({iv:.2f})")
        else:
            reasons.append("IV unavailable/invalid")

        # DTE guardrails
        if isinstance(dte, int):
            if 20 <= dte <= 120:
                score += 6
                reasons.append(f"DTE in sweet spot ({dte} days)")
            elif dte < 10:
                score -= 8
                reasons.append(f"DTE very short ({dte} days)")
            elif dte > 180:
                score -= 3
                reasons.append(f"DTE very long ({dte} days)")

        # Moneyness hint (call vs put)
        if moneyness_pct is not None and underlying is not None:
            if opt_type == "C":
                if -3 <= moneyness_pct <= 3:
                    score += 6
                    reasons.append(f"Near-the-money call (moneyness {moneyness_pct:.2f}%)")
                elif moneyness_pct > 5:
                    score += 2
                    reasons.append(f"ITM call (moneyness {moneyness_pct:.2f}%)")
                elif moneyness_pct < -8:
                    score -= 7
                    reasons.append(f"Far OTM call (moneyness {moneyness_pct:.2f}%)")
            else:  # P
                put_mny = ((strike - underlying) / underlying) * 100.0 if underlying else None
                if put_mny is not None:
                    metrics["put_moneyness_pct"] = put_mny
                    if -3 <= put_mny <= 3:
                        score += 6
                        reasons.append(f"Near-the-money put (moneyness {put_mny:.2f}%)")
                    elif put_mny > 5:
                        score += 2
                        reasons.append(f"ITM put (moneyness {put_mny:.2f}%)")
                    elif put_mny < -8:
                        score -= 7
                        reasons.append(f"Far OTM put (moneyness {put_mny:.2f}%)")

        # Clamp + confidence
        score = max(0, min(score, 100))
        confidence = max(1, min(10, round(score / 10)))

        # Bucket mapping
        if score >= 75:
            bucket = "BUY"
        elif score >= 55:
            bucket = "WATCH"
        else:
            bucket = "AVOID"

        return OptionDecision(bucket=bucket, score=score, confidence=confidence, reasons=reasons, metrics=metrics)

    # ---------- Fallback path (premium missing) ----------
    reasons.append("Premium missing (mid/last/bid/ask) — using fallback scoring")

    score = 35  # cautious baseline
    confidence = 3

    intrinsic = getattr(q, "intrinsic_value", None)
    try:
        intrinsic = float(intrinsic) if intrinsic is not None else None
    except Exception:
        intrinsic = None

    if intrinsic is not None:
        if intrinsic > 0:
            score += 10
            reasons.append(f"Has intrinsic value ({intrinsic:.2f})")
        else:
            reasons.append("No intrinsic value (pure time value)")

    if moneyness_pct is not None:
        if -3 <= moneyness_pct <= 3:
            score += 6
            reasons.append(f"Near-the-money (moneyness {moneyness_pct:.2f}%)")
        elif moneyness_pct < -8:
            score -= 6
            reasons.append(f"Far OTM (moneyness {moneyness_pct:.2f}%)")

    score = max(0, min(score, 100))
    bucket = "WATCH" if score >= 65 else "AVOID"

    return OptionDecision(bucket=bucket, score=score, confidence=confidence, reasons=reasons, metrics=metrics)


if __name__ == "__main__":
    from options.option_marketdata import fetch_option_quote

    tests = [
        "AAPL260620C00250000",
        "TSLA260620P00400000",
    ]

    for occ in tests:
        q = fetch_option_quote(occ)
        d = score_option_C(q)
        print("\nOCC:", occ)
        print("QUOTE:", q)
        print("DECISION:", d.bucket, f"score={d.score}", f"conf={d.confidence}")
        for r in d.reasons[:8]:
            print(" -", r)