# llm/explain.py (LOCKED + STRICT - quick-glance card explain + postmarket coach + insights)
from __future__ import annotations

import os
import re
from .client import llm_text

# -----------------------------
# Strict mode flags
# -----------------------------
STRICT_LLM = os.getenv("STRICT_LLM", "0").strip() == "1"

# Prefer phrase-level bans to avoid accidental matches like "buying pressure" / "sell-off".
BANNED_PHRASES = sorted(
    [
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
        "best trade",
        "guaranteed",
        "high probability",
        "can't miss",
        "must trade",
    ],
    key=len,
    reverse=True,
)

_BANNED_RE = re.compile(r"|".join(re.escape(p) for p in BANNED_PHRASES), flags=re.IGNORECASE)

# New quick-glance format (exactly 5 lines)
CARD_LINES = [
    "- Entry range:",
    "- Time window:",
    "- Levels:",
    "- P/L:",
    "- Stance reason:",
]

# Insights format (exactly 4 lines)
INSIGHT_LINES = [
    "- Why on list:",
    "- Main risks:",
    "- What would improve stance:",
    "- What to watch:",
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

    # Whole-word neutralization (avoid touching "buying", "sell-off")
    def _neutralize_trade_words(match):
        word = match.group(0).lower()
        if word == "buy":
            return "enter"
        if word == "sell":
            return "exit"
        return word

    t = re.sub(r"\b(buy|sell)\b", _neutralize_trade_words, t, flags=re.IGNORECASE)
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

    if "strong buy" in decision or decision in {"go", "qualified", "pick"}:
        stance = "GO"
    elif "moderate" in decision or "watch" in decision:
        stance = "WATCH"
    elif "not advisable" in decision or "avoid" in decision:
        stance = "AVOID"
    else:
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
        tgt_s = _fmt_money(float(tgt))
    except Exception:
        tgt_s = "not provided"
    try:
        stp_s = _fmt_money(float(stp))
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

    likely_driver = (
        "Many setups did not reach targets within the session window."
        if not_hit_rate >= 50
        else "Mixed follow-through; some setups did not complete."
    )
    worked = (
        "Low stop-hit rate suggests risk containment held."
        if losses == 0
        else "Stops triggered, indicating adverse moves were contained."
    )

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
    entry_txt, cur, atr = _entry_range_from_row(row)
    tw = _time_window_bucket(row, cur, atr)
    lv = _levels_block(row)
    pl = _pl_block(row)
    stance, reason = _stance_and_reason(row)

    return "\n".join([
        f"- Entry range: {entry_txt}",
        f"- Time window: {tw}",
        f"- Levels: {lv}",
        f"- P/L: {pl}",
        f"- Stance reason: {stance} | {reason}",
    ])


def _baseline_insights_from_row(row: dict) -> str:
    """
    Deterministic baseline insights (exactly 4 lines). Intentionally light on numbers.
    """
    sym = str(row.get("symbol") or "").upper().strip()
    trend = str(row.get("forecast_trend") or "not provided")
    news = str(row.get("news_flag") or "not provided")
    reasons = str(row.get("reasons") or "").strip()

    reasons_short = reasons[:160] + ("…" if len(reasons) > 160 else "")
    why = f"{sym}: trend={trend}; news={news}; reasons={reasons_short or 'not provided'}"
    risks = "Volatility / gap risk; market regime risk; news headline reversal risk."
    improve = "Higher confidence vs gate; cleaner price action vs key levels; better market breadth."
    watch = "Open behavior, first 15–30m range, and follow-through vs target/stop."

    return "\n".join([
        f"- Why on list: {why if why.strip() else 'not provided'}",
        f"- Main risks: {risks}",
        f"- What would improve stance: {improve}",
        f"- What to watch: {watch}",
    ])

def _scrub_trailing_artifacts(s: str) -> str:
    # remove artifacts like "1:6" or " 2:4"
    return re.sub(r"\s*\b\d+:\d+\b\s*$", "", s).strip()

def _force_exact_lines(text: str, labels: list[str], *, strict: bool) -> str:
    """
    Enforces EXACT output labels and line count.
    In strict mode: sanitize + hard scrub banned phrases.
    """
    t = _normalize_text(text)

    if strict:
        t = _sanitize_banned_language(t)
        t = _BANNED_RE.sub("", t)
        t = re.sub(r"\s{2,}", " ", t).strip()

    lines = [_scrub_trailing_artifacts(ln.strip()) for ln in t.split("\n") if ln.strip()]
    if len(lines) == len(labels) and all(lines[i].startswith(labels[i]) for i in range(len(labels))):
        return "\n".join(lines)

    found: dict[str, str] = {}
    for ln in lines:
        for lab in labels:
            if ln.startswith(lab):
                val = ln[len(lab):].strip()
                found[lab] = val if val else "not provided"
                break

    out = [f"{lab} {found.get(lab, 'not provided')}".rstrip() for lab in labels]
    final = "\n".join([_scrub_trailing_artifacts(x) for x in out])

    if strict:
        final = _sanitize_banned_language(final)
        final = _BANNED_RE.sub("", final)
        final = re.sub(r"\s{2,}", " ", final).strip()

    return final


def _is_weak_block(text: str, labels: list[str]) -> bool:
    t = _normalize_text(text)
    if not t:
        return True
    if t.strip() == _fallback_block(labels).strip():
        return True

    lines = [_scrub_trailing_artifacts(ln.strip()) for ln in t.split("\n") if ln.strip()]
    if len(lines) != len(labels):
        return True
    if not all(lines[i].startswith(labels[i]) for i in range(len(labels))):
        return True

    np_count = sum("not provided" in ln.lower() for ln in lines)
    return np_count >= len(labels) - 2


def _run_with_retry(
    prompt: str,
    labels: list[str],
    *,
    max_tokens: int,
    row: dict | None = None,
    fallback_fn=None,
) -> str:
    """
    One retry in STRICT mode if output still malformed.
    Always enforce hallucination guard.
    fallback_fn(row) should return a deterministic block matching 'labels'.
    """
    raw = llm_text(prompt, max_output_tokens=max_tokens).strip()
    out = _force_exact_lines(raw, labels, strict=STRICT_LLM)

    if row is not None and _llm_has_new_numbers(out, row):
        return fallback_fn(row) if fallback_fn else _fallback_block(labels)

    if STRICT_LLM and _is_weak_block(out, labels):
        raw2 = llm_text(
            prompt + "\nFINAL WARNING: Use ONLY DATA values; keep label format exact; no invented numbers.",
            max_output_tokens=max_tokens,
        ).strip()

        out2 = _force_exact_lines(raw2, labels, strict=STRICT_LLM)
        if row is not None and _llm_has_new_numbers(out2, row):
            return fallback_fn(row) if fallback_fn else _fallback_block(labels)
        if _is_weak_block(out2, labels):
            return fallback_fn(row) if fallback_fn else _fallback_block(labels)
        return out2

    if _is_weak_block(out, labels):
        return fallback_fn(row) if (row is not None and fallback_fn) else _fallback_block(labels)

    return out


# -----------------------------
# Premarket / Midday Quick-Glance Plan Card
# -----------------------------
def explain_trade_plan(row: dict) -> str:
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

    return _run_with_retry(
        prompt,
        CARD_LINES,
        max_tokens=220,
        row={**row, "baseline": baseline},
        fallback_fn=_baseline_card_from_row,
    )


def safe_explain_pick(row: dict) -> str:
    try:
        return explain_trade_plan(row)
    except Exception:
        return _baseline_card_from_row(row)


# -----------------------------
# Premarket / Midday LLM Insights (non-redundant with plan card)
# -----------------------------
def explain_insights(row: dict) -> str:
    baseline = _baseline_insights_from_row(row)

    prompt = f"""
You are a cautious trading assistant. Rewrite BASELINE into insights.

HARD RULES:
- No financial advice/commands.
- Use ONLY info from BASELINE (no new facts).
- Output EXACTLY 4 lines with these labels in order:
{chr(10).join(INSIGHT_LINES)}

BASELINE:
{baseline}
""".strip()

    return _run_with_retry(
        prompt,
        INSIGHT_LINES,
        max_tokens=180,
        row={**row, "baseline": baseline},
        fallback_fn=_baseline_insights_from_row,
    )


def safe_explain_insights(row: dict) -> str:
    try:
        return explain_insights(row)
    except Exception:
        return _baseline_insights_from_row(row)


# -----------------------------
# Postmarket Coach
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

    return _run_with_retry(
        prompt,
        COACH_LINES,
        max_tokens=280,
        row={"run_date": run_date, "premarket": prem_s, "midday": mid_s, "combined": all_s},
        fallback_fn=lambda _row: _baseline_coach_from_stats(run_date, prem_s, mid_s, all_s),
    )


def safe_postmarket_coach(run_date: str, prem_s: dict, mid_s: dict, all_s: dict) -> str:
    try:
        out = postmarket_coach(run_date, prem_s, mid_s, all_s)
        if _is_weak_block(out, COACH_LINES):
            return _baseline_coach_from_stats(run_date, prem_s, mid_s, all_s)
        return out
    except Exception as e:
        try:
            return _baseline_coach_from_stats(run_date, prem_s, mid_s, all_s)
        except Exception:
            fb = _fallback_block(COACH_LINES)
            return f"{fb}\n(LLM unavailable: {type(e).__name__})"