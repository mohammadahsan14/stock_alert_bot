# llm/explain.py (LOCKED + STRICT - quick-glance card explain + postmarket coach)
from __future__ import annotations

import os
import re
from .client import llm_text

# -----------------------------
# Strict mode flags
# -----------------------------
STRICT_LLM = os.getenv("STRICT_LLM", "0").strip() == "1"

# Prefer phrase-level bans to avoid accidental matches like "buying pressure" / "sell-off".
BANNED_PHRASES = [
    "you should",
    "you must",
    "i recommend",
    "recommended",
    "recommend",
    "preferable",
    "good opportunity",
    "go for it",
    "enter now",
    "exit now",
    "buy now",
    "sell now",
    "take profit",
    "stop out",
    "strong buy",
]
_BANNED_RE = re.compile(r"|".join(re.escape(p) for p in BANNED_PHRASES), flags=re.IGNORECASE)

# New quick-glance format (exactly 5 lines)
CARD_LINES = [
    "- Entry range:",
    "- Time window:",
    "- Levels:",
    "- P/L:",
    "- Stance reason:",
]

COACH_LINES = [
    "- What happened:",
    "- Likely loss drivers:",
    "- What worked:",
    "- Next-session process tweak:",
    "- Risk control tweak:",
]

_NUM_RE = re.compile(r"(?<![A-Za-z])[-+]?\d+(?:\.\d+)?%?")


# -----------------------------
# Helpers
# -----------------------------
def _contains_banned_language(text: str) -> bool:
    return bool(text) and (_BANNED_RE.search(text) is not None)


def _normalize_text(s: str) -> str:
    s = (s or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    s = s.replace("•", "-")
    return s


def _fallback_block(labels: list[str]) -> str:
    return "\n".join([f"{lab} not provided" for lab in labels])


def _sanitize_banned_language(text: str) -> str:
    """
    Convert advice-y wording into neutral wording (strict-safe),
    without changing any numbers. Covers ALL banned phrases so STRICT mode
    doesn't nuke output.
    """
    if not text:
        return text

    t = text

    replacements = [
        (r"\byou should\b", " "),
        (r"\byou must\b", " "),
        (r"\bi recommend\b", "noted"),
        (r"\brecommended\b", "noted"),
        (r"\brecommend\b", "note"),
        (r"\bpreferable\b", "more favorable"),
        (r"\bgood opportunity\b", "notable setup"),
        (r"\bgo for it\b", "monitor conditions"),
        (r"\bbuy now\b", "enter scenario"),
        (r"\bsell now\b", "exit scenario"),
        (r"\benter now\b", "enter scenario"),
        (r"\bexit now\b", "exit scenario"),
        (r"\btake profit\b", "profit-taking scenario"),
        (r"\bstop out\b", "stop scenario"),
        (r"\bstrong buy\b", "higher-conviction setup"),
    ]
    for pat, rep in replacements:
        t = re.sub(pat, rep, t, flags=re.IGNORECASE)

    # Whole-word neutralization
    t = re.sub(r"\bbuy\b", "enter", t, flags=re.IGNORECASE)
    t = re.sub(r"\bsell\b", "exit", t, flags=re.IGNORECASE)

    t = re.sub(r"\s{2,}", " ", t).strip()
    return t


def _numbers_in_text(s: str) -> set[str]:
    return set(_NUM_RE.findall(s or ""))


def _llm_has_new_numbers(llm_out: str, row: dict) -> bool:
    """
    Soft hallucination guard. If LLM introduces many numbers not present in row,
    fallback to deterministic baseline.
    """
    llm_nums = _numbers_in_text(llm_out)
    if not llm_nums:
        return False

    row_nums = _numbers_in_text(str(row))
    new_nums = [n for n in llm_nums if n not in row_nums]
    return len(new_nums) >= 4


def _pct(a: float, b: float) -> float:
    if a == 0:
        return 0.0
    return (b - a) / a * 100.0


def _fmt_money(x: float | None) -> str:
    if x is None:
        return "not provided"
    try:
        return f"${float(x):.2f}"
    except Exception:
        return "not provided"


def _fmt_pct(x: float | None) -> str:
    if x is None:
        return "not provided"
    try:
        return f"{float(x):.2f}%"
    except Exception:
        return "not provided"


def _entry_range_from_row(row: dict) -> tuple[str, float | None, float | None]:
    """
    Entry range = current ± 0.25 * ATR (if ATR present). Returns:
      (rendered_text, cur, atr)
    """
    cur = row.get("current")
    atr = row.get("forecast_atr")
    try:
        cur_f = float(cur)
    except Exception:
        return "not provided", None, None

    try:
        atr_f = float(atr)
    except Exception:
        atr_f = None

    if atr_f is None or atr_f <= 0:
        return f"{_fmt_money(cur_f)} (cur {_fmt_money(cur_f)})", cur_f, None

    pad = 0.25 * atr_f
    lo = cur_f - pad
    hi = cur_f + pad
    return f"{_fmt_money(lo)}–{_fmt_money(hi)} (cur {_fmt_money(cur_f)})", cur_f, atr_f


def _time_window_bucket(row: dict, cur: float | None, atr: float | None) -> str:
    """
    Buckets:
      Day, 2–3 days, Week, More than week
    Prefer explicit holding_window/horizon if present.
    Else infer from ATR% = atr/current, nudged by confidence.
    """
    hw = (row.get("holding_window") or row.get("horizon") or "").strip().lower()
    if hw:
        if "intra" in hw or hw == "day":
            return "Day"
        if "2" in hw or "3" in hw:
            return "2–3 days"
        if "week" in hw:
            return "Week"
        if "month" in hw or "swing" in hw or "multi" in hw:
            return "More than week"

    if cur is None or atr is None or cur <= 0:
        return "not provided"

    atr_pct = (atr / cur) * 100.0

    if atr_pct >= 6.0:
        bucket = "Day"
    elif atr_pct >= 3.0:
        bucket = "2–3 days"
    elif atr_pct >= 1.5:
        bucket = "Week"
    else:
        bucket = "More than week"

    # confidence nudge
    try:
        conf = int(row.get("confidence"))
    except Exception:
        conf = 5

    order = ["Day", "2–3 days", "Week", "More than week"]
    i = order.index(bucket)
    if conf >= 7:
        i = min(i + 1, len(order) - 1)
    elif conf <= 4:
        i = max(i - 1, 0)

    return order[i]


def _stance_and_reason(row: dict) -> tuple[str, str]:
    """
    Deterministic stance aligned with pipeline decisions.
    Maps:
      Strong Buy -> GO
      Moderate   -> WATCH
      Not Advisable -> AVOID
    Falls back to confidence if unclear.
    """

    decision = str(row.get("decision") or "").strip().lower()
    conf = row.get("confidence")
    score = row.get("score")
    gate = row.get("conf_gate")  # optional

    try:
        conf_i = int(conf) if conf is not None else None
    except Exception:
        conf_i = None

    # --- Explicit decision mapping ---
    if "strong buy" in decision or decision in {"go", "qualified", "pick"}:
        stance = "GO"
    elif "moderate" in decision or "watch" in decision:
        stance = "WATCH"
    elif "not advisable" in decision or "avoid" in decision:
        stance = "AVOID"
    else:
        # fallback to confidence
        if conf_i is not None and conf_i >= 7:
            stance = "GO"
        elif conf_i is not None and conf_i >= 5:
            stance = "WATCH"
        else:
            stance = "AVOID"

    news_flag = row.get("news_flag")
    news_part = f"news {news_flag}" if news_flag not in (None, "", "not provided") else "news not provided"

    if gate is not None and conf_i is not None:
        reason = f"conf {conf_i} vs gate {gate} | {news_part}"
    elif conf_i is not None:
        reason = f"conf {conf_i} | score {score} | {news_part}"
    else:
        reason = f"score {score} | {news_part}"

    return stance, reason


def _pl_block(row: dict) -> str:
    """
    Computes P/L in $ and % and R:R from current/target/stop.
    """
    try:
        cur = float(row.get("current"))
        tgt = float(row.get("target_price"))
        stp = float(row.get("stop_loss"))
    except Exception:
        return "not provided"

    up_d = tgt - cur
    dn_d = cur - stp
    up_p = _pct(cur, tgt)
    dn_p = abs(_pct(cur, stp))
    rr = (up_d / dn_d) if dn_d > 0 else 0.0

    return (
        f"+{_fmt_money(up_d)} ({_fmt_pct(up_p)}) | "
        f"-{_fmt_money(dn_d)} (-{_fmt_pct(dn_p)}) | "
        f"R:R {rr:.2f}"
    )


def _levels_block(row: dict) -> str:
    tgt = row.get("target_price")
    stp = row.get("stop_loss")
    try:
        tgt_f = float(tgt)
        tgt_s = _fmt_money(tgt_f)
    except Exception:
        tgt_s = "not provided"
    try:
        stp_f = float(stp)
        stp_s = _fmt_money(stp_f)
    except Exception:
        stp_s = "not provided"
    return f"target {tgt_s} | stop {stp_s}"

def _baseline_coach_from_stats(run_date: str, prem_s: dict, mid_s: dict, all_s: dict) -> str:
    try:
        evaluated = int(all_s.get("evaluated", 0) or 0)
        wins = int(all_s.get("wins", 0) or 0)
        losses = int(all_s.get("losses", 0) or 0)
        not_hit = int(all_s.get("not_hit", 0) or 0)
        win_rate = float(all_s.get("win_rate", 0.0) or 0.0)
    except Exception:
        evaluated, wins, losses, not_hit, win_rate = 0, 0, 0, 0, 0.0

    if evaluated <= 0:
        return "\n".join([
            "- What happened: No evaluated trades in stats.",
            "- Likely loss drivers: not provided",
            "- What worked: not provided",
            "- Next-session process tweak: Verify that evaluation rows are being written to performance_log.",
            "- Risk control tweak: Keep risk minimal until evaluation data is stable.",
        ])

    stop_rate = (losses / evaluated * 100.0) if evaluated else 0.0
    not_hit_rate = (not_hit / evaluated * 100.0) if evaluated else 0.0

    # Deterministic, neutral language (no “you should”)
    likely_driver = "Many setups did not reach targets within the session window." if not_hit_rate >= 50 else "Mixed follow-through; some setups did not complete."
    worked = "Low stop-hit rate suggests risk containment held." if losses == 0 else "Stops triggered, indicating adverse moves were contained."

    tweak_parts = []
    if not_hit_rate >= 60:
        tweak_parts.append("Tighten time-scaled targets (esp. midday) or skip late recommendations.")
    if stop_rate >= 40:
        tweak_parts.append("Increase selectivity: raise confidence gate for the weaker session or reduce noisy movers.")
    if not tweak_parts:
        tweak_parts.append("Keep parameters stable; collect more samples before changing thresholds.")

    risk_parts = []
    if evaluated < 6:
        risk_parts.append("Small sample size: keep position sizing conservative.")
    risk_parts.append("Cap number of midday trades when time-left is low.")

    return "\n".join([
        f"- What happened: evaluated={evaluated}, wins={wins}, losses={losses}, not_hit={not_hit}, win_rate={win_rate:.2f}%.",
        f"- Likely loss drivers: {likely_driver}",
        f"- What worked: {worked}",
        f"- Next-session process tweak: {' '.join(tweak_parts)}",
        f"- Risk control tweak: {' '.join(risk_parts)}",
    ])

def _baseline_card_from_row(row: dict) -> str:
    """
    Deterministic baseline quick-glance card (exactly 5 lines).
    """
    entry_txt, cur, atr = _entry_range_from_row(row)
    tw = _time_window_bucket(row, cur, atr)
    lv = _levels_block(row)
    pl = _pl_block(row)
    stance, reason = _stance_and_reason(row)

    return "\n".join(
        [
            f"- Entry range: {entry_txt}",
            f"- Time window: {tw}",
            f"- Levels: {lv}",
            f"- P/L: {pl}",
            f"- Stance reason: {stance} | {reason}",
        ]
    )


def _force_exact_lines(text: str, labels: list[str], *, strict: bool) -> str:
    """
    Enforces EXACT output labels and line count.
    In strict mode: sanitize + hard scrub banned phrases (no fallback on bans).
    """
    t = _normalize_text(text)

    if strict:
        t = _sanitize_banned_language(t)
        t = _BANNED_RE.sub("", t)
        t = re.sub(r"\s{2,}", " ", t).strip()

    lines = [ln.strip() for ln in t.split("\n") if ln.strip()]

    if len(lines) == len(labels) and all(lines[i].startswith(labels[i]) for i in range(len(labels))):
        return "\n".join(lines)

    # Salvage
    found: dict[str, str] = {}
    for ln in lines:
        for lab in labels:
            if ln.startswith(lab):
                val = ln[len(lab):].strip()
                found[lab] = val if val else "not provided"
                break

    out = [f"{lab} {found.get(lab, 'not provided')}".rstrip() for lab in labels]
    final = "\n".join(out)

    if strict:
        final = _sanitize_banned_language(final)
        final = _BANNED_RE.sub("", final)
        final = re.sub(r"\s{2,}", " ", final).strip()

    return final


def _run_with_retry(prompt: str, labels: list[str], *, max_tokens: int, row: dict | None = None) -> str:
    """
    One retry in STRICT mode if output still malformed.
    Always enforce hallucination guard.
    """

    raw = llm_text(prompt, max_output_tokens=max_tokens).strip()
    out = _force_exact_lines(raw, labels, strict=STRICT_LLM)

    # 🚨 Always enforce hallucination guard
    if row is not None and _llm_has_new_numbers(out, row):
        return _baseline_card_from_row(row)

    if STRICT_LLM:
        weak = (out == _fallback_block(labels)) or (
            sum("not provided" in ln.lower() for ln in out.splitlines()) >= len(labels) - 2
        )

        if weak:
            raw2 = llm_text(
                prompt
                + "\nFINAL WARNING: Use ONLY DATA values; compute entry range and P/L only from current/ATR/target/stop; keep label format exact.",
                max_output_tokens=max_tokens,
            ).strip()

            out2 = _force_exact_lines(raw2, labels, strict=STRICT_LLM)

            # 🚨 Guard again after retry
            if row is not None and _llm_has_new_numbers(out2, row):
                return _baseline_card_from_row(row)

            weak2 = (out2 == _fallback_block(labels)) or (
                sum("not provided" in ln.lower() for ln in out2.splitlines()) >= len(labels) - 2
            )

            if weak2:
                return _baseline_card_from_row(row)

            return out2

    # Non-strict mode fallback
    if row is not None:
        weak = (out == _fallback_block(labels)) or (
            sum("not provided" in ln.lower() for ln in out.splitlines()) >= len(labels) - 2
        )
        if weak:
            return _baseline_card_from_row(row)

    return out


# -----------------------------
# Premarket / Midday Quick-Glance Explain
# -----------------------------
def explain_trade_plan(row: dict) -> str:
    """
    Produces the exact 5-line quick-glance card.
    LLM is used as a formatter only; deterministic baseline is the source of truth.
    """
    baseline = _baseline_card_from_row(row)

    prompt = f"""
You are a cautious trading assistant. Rewrite the BASELINE into a quick-glance card.

HARD RULES:
- Use ONLY numbers already present in BASELINE (do NOT invent new ones).
- Keep wording neutral (no advice/commands). Avoid "you should", "recommend", "buy now", "sell now".
- Output EXACTLY 5 lines with these labels in this order (no extra lines):
{chr(10).join(CARD_LINES)}

BASELINE:
{baseline}
""".strip()

    # If STRICT is on, sanitizer ensures no banned phrasing survives
    return _run_with_retry(
        prompt,
        CARD_LINES,
        max_tokens=220,
        row={"row": row, "baseline": baseline},  # includes derived numbers for guard
    )


def safe_explain_pick(row: dict) -> str:
    try:
        return explain_trade_plan(row)
    except Exception:
        return _baseline_card_from_row(row)


# -----------------------------
# Postmarket Coach (unchanged)
# -----------------------------
def postmarket_coach(run_date: str, prem_s: dict, mid_s: dict, all_s: dict) -> str:
    prompt = f"""
You are a cautious performance reviewer writing neutral, non-advisory feedback.

TASK:
Using ONLY these STATS, summarize what happened and suggest process improvements for the next session.

HARD RULES:
- Use ONLY numbers present in STATS. Do NOT invent any numbers.
- If something is unclear, write exactly: "not provided".
- NO advice/commands. Avoid "you should", "recommend", "best trade".
- Use neutral wording: "pattern", "likely driver", "process tweak", "risk control adjustment".
- Keep it under 160 words total.
- Output EXACTLY 5 lines with these labels in this order:
{chr(10).join(COACH_LINES)}

STATS:
run_date: {run_date}
premarket: {prem_s}
midday: {mid_s}
combined: {all_s}
""".strip()
    return _run_with_retry(prompt, COACH_LINES, max_tokens=280)

def _is_weak_block(text: str, labels: list[str]) -> bool:
    """
    Weak if:
    - empty
    - fallback block
    - too many 'not provided'
    - wrong labels/line count
    """
    t = _normalize_text(text)
    if not t:
        return True

    # Exact fallback string check
    if t.strip() == _fallback_block(labels).strip():
        return True

    lines = [ln.strip() for ln in t.split("\n") if ln.strip()]
    if len(lines) != len(labels):
        return True

    if not all(lines[i].startswith(labels[i]) for i in range(len(labels))):
        return True

    np_count = sum("not provided" in ln.lower() for ln in lines)
    return np_count >= len(labels) - 2

def safe_postmarket_coach(run_date: str, prem_s: dict, mid_s: dict, all_s: dict) -> str:
    try:
        out = postmarket_coach(run_date, prem_s, mid_s, all_s)

        # ✅ If LLM returns weak/empty/not-provided block, use deterministic coach
        if _is_weak_block(out, COACH_LINES):
            return _baseline_coach_from_stats(run_date, prem_s, mid_s, all_s)

        return out

    except Exception as e:
        # ✅ Hard failure: deterministic fallback first
        try:
            return _baseline_coach_from_stats(run_date, prem_s, mid_s, all_s)
        except Exception:
            fb = _fallback_block(COACH_LINES)
            return f"{fb}\n(LLM unavailable: {type(e).__name__})"