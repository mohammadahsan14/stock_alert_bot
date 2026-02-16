# premarket_runner.py (FINAL LOCK VERSION + FINAL_VIEW + HIGH-VOTE FALLBACK + ENRICHED RAW + DETERMINISTIC PLAN_CARD + LLM OPTIONAL)
from __future__ import annotations

from llm.explain import safe_explain_pick

import os
import re
import html as _html
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Optional

import pandas as pd
import yfinance as yf
from openpyxl import load_workbook
from openpyxl.styles import PatternFill, Font, Alignment

from config import (
    APP_ENV,
    IS_LOCAL,
    SENDER_EMAIL,
    RECEIVER_EMAIL,
    LOCAL_RECEIVER_EMAIL,
    EMAIL_SUBJECT_PREFIX_LOCAL,
    EMAIL_SUBJECT_PREFIX_PROD,
    TOP_N,
    TRADE_MAX_PICKS,
    SCORE_COLORS,
    SCORE_HIGH,
    SCORE_MEDIUM,
    MIN_CONFIDENCE_TO_TRADE,
    MAX_PRICE,
    ELITE_SCORE_OVERRIDE,
    ELITE_CONF_OVERRIDE,
)

from email_sender import send_email as _send_email
from top_movers import fetch_sp500_tickers, calculate_top_movers
from scoring_engine import get_predictive_score_with_reasons
from forecast_engine import forecast_price_levels
from news_fetcher import fetch_news_links
from price_category import get_price_category

from performance_tracker import (
    PortfolioConfig,
    load_open_portfolio,
    save_open_portfolio,
    add_new_positions_from_picks,
    append_open_actions,
)

import warnings
warnings.filterwarnings(
    "ignore",
    category=FutureWarning,
    message="The behavior of DataFrame concatenation with empty or all-NA entries is deprecated"
)

LOCAL_TZ = ZoneInfo("America/Chicago")

POS_WORDS = {"beat", "strong", "growth", "surge", "upgrade", "raises", "record", "profit", "wins", "bull"}
NEG_WORDS = {"miss", "drop", "loss", "cuts", "downgrade", "falls", "weak", "lawsuit", "plunge", "bear"}

# LLM flags
PREMARKET_LLM_ENABLED = os.getenv("LLM_ENABLED", "1") == "1"
PREMARKET_LLM_TOP_N = int(os.getenv("PREMARKET_LLM_TOP_N", "8"))

# FINAL view behavior
FINAL_VIEW_TOP_N = int(os.getenv("FINAL_VIEW_TOP_N", "3"))
CONF_GATE = int(os.getenv("CONF_GATE", str(MIN_CONFIDENCE_TO_TRADE)))  # used only for stance reason text


# -----------------------------
# LLM helpers
# -----------------------------
def _llm_num_or_np(x) -> float | str:
    """Return a positive float, else 'not provided' (prevents fake 0.0 values)."""
    try:
        v = float(pd.to_numeric(x, errors="coerce"))
        return v if v > 0 else "not provided"
    except Exception:
        return "not provided"


def _ensure_llm_col(dfx: pd.DataFrame) -> pd.DataFrame:
    if dfx is None:
        return pd.DataFrame()
    if "llm_explanation" not in dfx.columns:
        dfx = dfx.copy()
        dfx["llm_explanation"] = ""
    return dfx


def _apply_llm_explanations(dfx: pd.DataFrame, *, horizon: str, top_n: int | None = None) -> pd.DataFrame:
    """
    Fill llm_explanation for top N rows of this dataframe (or all rows if top_n is None).
    Uses ONLY existing row data (no made-up numbers).
    """
    dfx = _ensure_llm_col(dfx)

    if (not PREMARKET_LLM_ENABLED) or dfx is None or dfx.empty:
        return dfx

    n = len(dfx) if top_n is None else min(int(top_n), len(dfx))
    explain_df = dfx.head(n).copy()

    expl: List[str] = []
    for _, rr in explain_df.iterrows():
        payload = {
            "symbol": str(rr.get("symbol", "")).upper().strip(),
            "decision": str(rr.get("decision", "")),
            "score": int(pd.to_numeric(rr.get("score"), errors="coerce") or 0),
            "confidence": int(pd.to_numeric(rr.get("confidence"), errors="coerce") or 0),
            "pct_change": float(pd.to_numeric(rr.get("pct_change"), errors="coerce") or 0.0),

            "current": _llm_num_or_np(rr.get("current")),
            "predicted_price": _llm_num_or_np(rr.get("predicted_price")),
            "target_price": _llm_num_or_np(rr.get("target_price")),
            "stop_loss": _llm_num_or_np(rr.get("stop_loss")),
            "forecast_atr": _llm_num_or_np(rr.get("forecast_atr")),

            "forecast_trend": str(rr.get("forecast_trend") or ""),
            "forecast_reason": str(rr.get("forecast_reason") or ""),
            "news_flag": str(rr.get("news_flag") or ""),
            "main_news_title": str(rr.get("main_news_title") or ""),
            "reasons": str(rr.get("reasons") or ""),

            "horizon": horizon,
            "position_size_usd": float(os.getenv("DEFAULT_POSITION_SIZE_USD", "0") or 0) or "not provided",
            "holding_window": str(rr.get("holding_window") or rr.get("horizon") or "intraday"),
            "conf_gate": CONF_GATE,
        }
        expl.append(str(safe_explain_pick(payload) or "").strip())

    explain_df["llm_explanation"] = expl
    dfx.loc[explain_df.index, "llm_explanation"] = explain_df["llm_explanation"].astype(str)
    return dfx


# -----------------------------
# Output helpers (env-aware)
# -----------------------------
def env_base_dir() -> Path:
    base = Path(__file__).resolve().parent / "outputs" / APP_ENV
    base.mkdir(parents=True, exist_ok=True)
    return base


def logs_dir() -> Path:
    p = env_base_dir() / "logs"
    p.mkdir(parents=True, exist_ok=True)
    return p


def run_dir(now: datetime, mode: str) -> Path:
    day = now.strftime("%Y%m%d")
    p = env_base_dir() / "runs" / day / mode
    p.mkdir(parents=True, exist_ok=True)
    return p


def out_path(filename: str, *, now: datetime | None = None, mode: str | None = None, kind: str = "runs") -> str:
    if kind == "logs":
        return str(logs_dir() / filename)
    if now is None or mode is None:
        return str(env_base_dir() / filename)
    return str(run_dir(now, mode) / filename)


def ensure_csv_exists(path: str, header_cols: list[str]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if (not p.exists()) or p.stat().st_size == 0:
        pd.DataFrame(columns=header_cols).to_csv(path, index=False)


def make_run_id(now: datetime) -> str:
    return now.strftime("%Y%m%d_%H%M%S")


def _premarket_email_marker(now: datetime) -> Path:
    d = run_dir(now, "premarket")
    run_date = now.strftime("%Y-%m-%d")
    return d / f"email_sent_{run_date}.txt"


# -----------------------------
# Email routing (env-aware)
# -----------------------------
EMAIL_SUBJECT_PREFIX = EMAIL_SUBJECT_PREFIX_LOCAL if IS_LOCAL else EMAIL_SUBJECT_PREFIX_PROD
EFFECTIVE_RECEIVER_EMAIL = (LOCAL_RECEIVER_EMAIL or RECEIVER_EMAIL) if IS_LOCAL else RECEIVER_EMAIL


def send_email(subject: str, html_body: str, attachment_path: str | None = None) -> bool:
    final_subject = f"{EMAIL_SUBJECT_PREFIX} {subject}"
    return _send_email(
        subject=final_subject,
        html_body=html_body,
        to_email=EFFECTIVE_RECEIVER_EMAIL,
        from_email=SENDER_EMAIL,
        attachment_path=attachment_path,
    )


# -----------------------------
# Styling
# -----------------------------
def normalize_color(color: str) -> str:
    if not color:
        color = "#FFFFFF"
    color = color.lstrip("#")
    if len(color) == 6:
        color = "FF" + color
    return color.upper()


def style_excel_sheet(sheet):
    header_font = Font(bold=True, color="FFFFFF")
    header_fill = PatternFill(
        start_color=normalize_color("#2F5597"),
        end_color=normalize_color("#2F5597"),
        fill_type="solid",
    )
    center = Alignment(horizontal="center", vertical="center", wrap_text=True)

    if sheet.max_row < 1 or sheet.max_column < 1:
        return

    for cell in sheet[1]:
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = center

    sheet.freeze_panes = "A2"

    for col in sheet.columns:
        max_length = 0
        col_letter = col[0].column_letter
        for cell in col:
            if cell.value is None:
                continue
            max_length = max(max_length, len(str(cell.value)))
        sheet.column_dimensions[col_letter].width = min(max_length + 2, 55)

    headers = {str(c.value).strip(): idx + 1 for idx, c in enumerate(sheet[1]) if c.value}
    score_label_col = headers.get("score_label")
    decision_col = headers.get("decision")

    for r in range(2, sheet.max_row + 1):
        try:
            if score_label_col:
                cell = sheet.cell(row=r, column=score_label_col)
                label = str(cell.value or "")
                color = SCORE_COLORS.get(label, "#FFFFFF")
                cell.fill = PatternFill(
                    start_color=normalize_color(color),
                    end_color=normalize_color(color),
                    fill_type="solid",
                )
                cell.alignment = center

            if decision_col:
                dcell = sheet.cell(row=r, column=decision_col)
                dval = str(dcell.value or "")
                if dval == "Strong Buy":
                    c = "#92D050"
                elif dval == "Moderate":
                    c = "#FFF2CC"
                else:
                    c = "#F4CCCC"
                dcell.fill = PatternFill(
                    start_color=normalize_color(c),
                    end_color=normalize_color(c),
                    fill_type="solid",
                )
                dcell.alignment = center
        except Exception:
            pass


# -----------------------------
# Helpers
# -----------------------------
def map_score_to_decision(score: int) -> str:
    if score >= SCORE_HIGH:
        return "Strong Buy"
    if score >= SCORE_MEDIUM:
        return "Moderate"
    return "Not Advisable"


def extract_headline_from_html(news_html: str) -> str:
    if not news_html:
        return ""
    news_html = str(news_html).replace("\uFFFC", "").strip()
    m = re.search(r">(.*?)</a>", news_html)
    return (m.group(1).strip() if m else re.sub(r"<.*?>", "", news_html).strip())


def extract_url_from_html(news_html: str) -> str:
    if not news_html:
        return ""
    m = re.search(r'href="([^"]+)"', str(news_html))
    return m.group(1).strip() if m else ""


def news_flag_from_headlines(headlines: List[str]) -> str:
    if not headlines:
        return "🟡"
    score = 0
    for h in headlines:
        t = (h or "").lower()
        if any(w in t for w in POS_WORDS):
            score += 1
        if any(w in t for w in NEG_WORDS):
            score -= 1
    if score >= 1:
        return "🟢"
    if score <= -1:
        return "🔴"
    return "🟡"


def get_market_snapshot() -> dict:
    out = {"trend": "up", "spy_gap_pct": 0.0, "vix": None}
    try:
        spy = yf.Ticker("SPY").history(period="2d", auto_adjust=False)
        if not spy.empty and len(spy) >= 2:
            prev_close = float(spy["Close"].iloc[-2])
            last_close = float(spy["Close"].iloc[-1])
            out["trend"] = "up" if last_close > prev_close else "down"
            out["spy_gap_pct"] = ((last_close - prev_close) / prev_close) * 100.0
    except Exception:
        pass
    try:
        vix = yf.Ticker("^VIX").history(period="1d", auto_adjust=False)
        if not vix.empty:
            out["vix"] = float(vix["Close"].iloc[-1])
    except Exception:
        pass
    return out


def compute_confidence(score_val: int, pct_change: float, market_trend: str, news_flag: str) -> int:
    score_val = max(0, min(int(score_val), 100))
    base = round((score_val / 100.0) * 10.0)

    move = min(abs(float(pct_change or 0.0)), 8.0)
    vol_penalty = 2 if move >= 6 else (1 if move >= 4 else 0)

    market_bonus = 1 if market_trend == "up" else 0
    news_bonus = 1 if news_flag == "🟢" else (-1 if news_flag == "🔴" else 0)

    conf = int(base + market_bonus + news_bonus - vol_penalty)
    return max(1, min(conf, 10))


def _safe_float(x) -> Optional[float]:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return None
        return float(x)
    except Exception:
        return None


def _infer_intraday_target_stop(row: pd.Series) -> tuple[Optional[float], Optional[float]]:
    entry = _safe_float(row.get("current"))
    tgt = _safe_float(row.get("target_price"))
    stp = _safe_float(row.get("stop_loss"))

    if tgt is None:
        tgt = _safe_float(row.get("predicted_price"))

    if stp is None and entry is not None and entry > 0:
        conf_raw = pd.to_numeric(row.get("confidence"), errors="coerce")
        conf = int(conf_raw) if pd.notna(conf_raw) else 5
        stp_pct = 0.012 if conf >= 7 else (0.015 if conf >= 6 else 0.02)
        stp = entry * (1.0 - stp_pct)

    if tgt is None and entry is not None and entry > 0:
        conf_raw = pd.to_numeric(row.get("confidence"), errors="coerce")
        conf = int(conf_raw) if pd.notna(conf_raw) else 5
        tgt_pct = 0.015 if conf >= 7 else (0.012 if conf >= 6 else 0.01)
        tgt = entry * (1.0 + tgt_pct)

    return tgt, stp


# -----------------------------
# Deterministic Plan Card (LOCKED FORMAT)
# -----------------------------
def _entry_range(cur: Optional[float], atr: Optional[float]) -> tuple[Optional[float], Optional[float]]:
    if cur is None or cur <= 0:
        return None, None
    if atr is None or atr <= 0:
        lo = cur * 0.9925
        hi = cur * 1.0075
        return lo, hi
    lo = cur - 0.35 * atr
    hi = cur + 0.35 * atr
    return lo, hi


def _time_window_bucket(conf: int, trend: str) -> str:
    trend = (trend or "").lower()
    if conf >= 7 and trend == "up":
        return "2–3 days"
    if conf >= 6:
        return "1–2 days"
    return "day"


def _plan_card_row(r: pd.Series) -> str:
    cur = _safe_float(r.get("current"))
    tgt = _safe_float(r.get("target_price"))
    stp = _safe_float(r.get("stop_loss"))
    atr = _safe_float(r.get("forecast_atr"))
    conf = int(pd.to_numeric(r.get("confidence"), errors="coerce") or 0)
    trend = str(r.get("forecast_trend") or "")

    lo, hi = _entry_range(cur, atr)
    tw = _time_window_bucket(conf, trend)

    def m(x):
        return "" if x is None else f"{x:.2f}"

    pl_up = pl_dn = rr = None
    up_pct = dn_pct = None

    if cur and tgt and stp and cur > 0:
        pl_up = tgt - cur
        pl_dn = stp - cur
        up_pct = (pl_up / cur) * 100.0
        dn_pct = (pl_dn / cur) * 100.0
        risk = abs(pl_dn)
        rr = (pl_up / risk) if risk > 0 else None

    return "\n".join([
        f"• Entry range: ${m(lo)}–${m(hi)} (cur ${m(cur)})" if (lo is not None and hi is not None) else f"• Entry range: not provided (cur ${m(cur)})",
        f"• Time window: {tw}",
        f"• Levels: target ${m(tgt)} | stop ${m(stp)}",
        f"• P/L: +${m(pl_up)} ({m(up_pct)}%) | ${m(pl_dn)} ({m(dn_pct)}%) | R:R {m(rr)}" if rr is not None else "• P/L: not provided",
        f"• Stance reason: conf {r.get('confidence')} vs gate {CONF_GATE} | news {r.get('news_flag')}",
    ])


# -----------------------------
# NEW: RAW enrichment + High-vote + FINAL_VIEW
# -----------------------------
def _enrich_raw_movers(raw_df: pd.DataFrame, all_scored_df: pd.DataFrame) -> pd.DataFrame:
    """RAW_MOVERS typically lacks target/stop/atr/trend. Enrich by joining ALL_SCORED on symbol."""
    raw_df = raw_df.copy() if raw_df is not None else pd.DataFrame()
    if raw_df.empty:
        return _ensure_llm_col(raw_df)

    if "symbol" not in raw_df.columns or all_scored_df is None or all_scored_df.empty:
        return _ensure_llm_col(raw_df)

    keep_cols = [
        "symbol",
        "predicted_price", "target_price", "stop_loss",
        "forecast_trend", "forecast_atr", "forecast_reason",
        "score", "score_label", "confidence",
        "news_flag", "main_news_title", "main_news_link",
        "reasons", "decision",
    ]
    enrich = all_scored_df.copy()
    for c in keep_cols:
        if c not in enrich.columns:
            enrich[c] = pd.NA
    enrich = enrich[keep_cols].drop_duplicates(subset=["symbol"])

    out = raw_df.merge(enrich, on="symbol", how="left", suffixes=("", "_y"))
    out = _ensure_llm_col(out)
    return out


def _vote_score(rr: pd.Series) -> int:
    """Deterministic 'high-vote' score. No LLM involved."""
    score = 0
    conf = pd.to_numeric(rr.get("confidence"), errors="coerce")
    scr = pd.to_numeric(rr.get("score"), errors="coerce")
    pct = pd.to_numeric(rr.get("pct_change"), errors="coerce")
    trend = str(rr.get("forecast_trend") or "").lower()
    news = str(rr.get("news_flag") or "")

    if pd.notna(conf) and int(conf) >= 6:
        score += 3
    if pd.notna(scr) and float(scr) >= 60:
        score += 2
    if pd.notna(pct) and abs(float(pct)) >= 5.0:
        score += 1
    if trend == "up":
        score += 1

    mover_signal = str(rr.get("mover_signal") or "")
    if "❌" in mover_signal:
        score -= 2

    if news == "🔴":
        score -= 2
    elif news == "🟡":
        score -= 1

    return int(score)


def _build_final_view(
    *,
    picks_df: pd.DataFrame,
    monitor_df: pd.DataFrame,
    all_scored_df: pd.DataFrame,
    raw_enriched_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    FINAL_VIEW:
      - If PICKS exist -> GO list (top picks)
      - Else -> WATCH list (top 3 high-vote from combined sources)
    """
    cols_min = [
        "symbol", "current", "pct_change",
        "target_price", "stop_loss", "predicted_price",
        "forecast_trend", "forecast_atr", "forecast_reason",
        "score", "score_label", "confidence",
        "news_flag", "main_news_title", "main_news_link",
        "reasons", "decision", "llm_explanation",
    ]

    def _ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy() if df is not None else pd.DataFrame()
        for c in cols_min:
            if c not in df.columns:
                df[c] = pd.NA
        return df

    picks_df = _ensure_cols(picks_df)
    monitor_df = _ensure_cols(monitor_df)
    all_scored_df = _ensure_cols(all_scored_df)
    raw_enriched_df = _ensure_cols(raw_enriched_df)

    if picks_df is not None and not picks_df.empty:
        final = picks_df.copy()
        final["stance"] = "GO"
        final["stance_reason"] = final.apply(
            lambda r: f"conf {r.get('confidence')} vs gate {CONF_GATE} | news {r.get('news_flag')}",
            axis=1
        )
        return final.reset_index(drop=True)

    combined = pd.concat([monitor_df, all_scored_df, raw_enriched_df], ignore_index=True)
    combined = combined.dropna(subset=["symbol"]).copy()
    combined["symbol"] = combined["symbol"].astype(str).str.upper().str.strip()
    combined = combined.drop_duplicates(subset=["symbol"])

    combined["vote_score"] = combined.apply(_vote_score, axis=1)
    combined = combined.sort_values(by=["vote_score", "confidence", "score"], ascending=False)

    final = combined.head(FINAL_VIEW_TOP_N).copy()
    final["stance"] = "WATCH"
    final["stance_reason"] = final.apply(
        lambda r: f"conf {r.get('confidence')} vs gate {CONF_GATE} | vote {r.get('vote_score')} | news {r.get('news_flag')}",
        axis=1
    )
    return final.reset_index(drop=True)


# -----------------------------
# Logs
# -----------------------------
DAILY_LOG_CSV = out_path("daily_stock_log.csv", kind="logs")
RECO_LOG_CSV = out_path("recommendations_log.csv", kind="logs")

ensure_csv_exists(RECO_LOG_CSV, [
    "run_ts", "run_date", "mode", "app_env",
    "symbol", "current", "pct_change",
    "decision", "score", "score_label", "confidence",
    "predicted_price", "target_price", "stop_loss",
    "forecast_trend", "forecast_atr",
    "news_flag", "main_news_title", "main_news_link",
    "reasons",
])

ensure_csv_exists(DAILY_LOG_CSV, [
    "run_date", "mode", "symbol", "price_category",
    "current", "predicted_price", "target_price", "stop_loss",
    "forecast_trend", "forecast_atr", "forecast_reason",
    "trade_plan", "earnings_risk",
    "decision", "score", "score_label", "confidence",
    "reasons", "llm_explanation", "news_flag", "main_news_title", "main_news_link",
])


def append_recommendations_log(df_reco: pd.DataFrame, now: datetime, mode: str) -> None:
    if df_reco is None or df_reco.empty:
        return

    out = df_reco.copy()
    out.insert(0, "run_ts", now.strftime("%Y-%m-%d %H:%M:%S"))
    out.insert(1, "run_date", now.strftime("%Y-%m-%d"))
    out.insert(2, "mode", mode)
    out.insert(3, "app_env", APP_ENV)

    cols_keep = [
        "run_ts", "run_date", "mode", "app_env",
        "symbol", "current", "pct_change",
        "decision", "score", "score_label", "confidence",
        "predicted_price", "target_price", "stop_loss",
        "forecast_trend", "forecast_atr",
        "news_flag", "main_news_title", "main_news_link",
        "reasons",
    ]
    for c in cols_keep:
        if c not in out.columns:
            out[c] = pd.NA

    for c in ["current", "pct_change", "predicted_price", "target_price", "stop_loss", "forecast_atr"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    out = out[cols_keep]
    file_exists = os.path.exists(RECO_LOG_CSV) and os.path.getsize(RECO_LOG_CSV) > 0
    out.to_csv(RECO_LOG_CSV, mode="a", header=not file_exists, index=False)


def append_daily_log(df_rows: pd.DataFrame, run_date: str, mode: str) -> None:
    """
    Source-of-truth log for postmarket evaluation.
    Writes FINAL_VIEW rows (GO + WATCH) with stable schema.
    """
    if df_rows is None or df_rows.empty:
        return

    cols = [
        "run_date", "mode", "symbol", "price_category",
        "current", "predicted_price", "target_price", "stop_loss",
        "forecast_trend", "forecast_atr", "forecast_reason",
        "trade_plan", "earnings_risk",
        "decision", "score", "score_label", "confidence",
        "reasons", "llm_explanation", "news_flag", "main_news_title", "main_news_link",
    ]

    out = df_rows.copy()
    out["run_date"] = run_date
    out["mode"] = mode

    for c in cols:
        if c not in out.columns:
            out[c] = pd.NA
    out = out[cols]

    for c in ["current", "predicted_price", "target_price", "stop_loss", "forecast_atr"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    existing = (
        pd.read_csv(DAILY_LOG_CSV)
        if (os.path.exists(DAILY_LOG_CSV) and os.path.getsize(DAILY_LOG_CSV) > 0)
        else pd.DataFrame(columns=cols)
    )
    existing = existing.reindex(columns=cols)

    merged = pd.concat([existing, out], ignore_index=True)
    merged["symbol"] = merged["symbol"].astype(str).str.upper().str.strip()
    merged = merged.drop_duplicates(subset=["run_date", "mode", "symbol"], keep="last")
    merged.to_csv(DAILY_LOG_CSV, index=False)


# -----------------------------
# Excel writer
# -----------------------------
def write_premarket_excel(
    excel_path: str,
    *,
    final_view_df: pd.DataFrame,
    picks_df: pd.DataFrame,
    candidates_df: pd.DataFrame,
    monitor_df: pd.DataFrame,
    all_scored_df: pd.DataFrame,
    raw_movers_df: pd.DataFrame,
    rf: Path,
) -> None:
    try:
        excel_path_p = Path(excel_path)
        excel_path_p.parent.mkdir(parents=True, exist_ok=True)

        with pd.ExcelWriter(excel_path, engine="openpyxl") as xw:
            (final_view_df if final_view_df is not None else pd.DataFrame()).to_excel(xw, sheet_name="FINAL_VIEW", index=False)
            (picks_df if picks_df is not None else pd.DataFrame()).to_excel(xw, sheet_name="PICKS", index=False)
            (candidates_df if candidates_df is not None else pd.DataFrame()).to_excel(xw, sheet_name="CANDIDATES", index=False)
            (monitor_df if monitor_df is not None else pd.DataFrame()).to_excel(xw, sheet_name="MONITOR_TOP20", index=False)
            (all_scored_df if all_scored_df is not None else pd.DataFrame()).to_excel(xw, sheet_name="ALL_SCORED", index=False)
            (raw_movers_df if raw_movers_df is not None else pd.DataFrame()).to_excel(xw, sheet_name="RAW_MOVERS", index=False)

        wb = load_workbook(excel_path)
        for s in wb.sheetnames:
            style_excel_sheet(wb[s])
        wb.save(excel_path)

    except Exception as e:
        (Path(rf) / "excel_write_error.txt").write_text(repr(e), encoding="utf-8")


# -----------------------------
# Premarket runner
# -----------------------------
def run_premarket(now: datetime | None = None) -> None:
    now = now or datetime.now(LOCAL_TZ)
    mode = "premarket"
    rf = run_dir(now, mode)
    run_date = now.strftime("%Y-%m-%d")

    marker = _premarket_email_marker(now)
    if marker.exists():
        print("📩 Premarket email already sent for this run_date — skipping resend.")
        return

    tickers = fetch_sp500_tickers()
    movers = calculate_top_movers(tickers, top_n=TOP_N)
    df_raw = pd.DataFrame(movers)

    excel_path = out_path(
        f"premarket_{now.strftime('%Y%m%d')}_{make_run_id(now)}.xlsx",
        now=now, mode=mode, kind="runs"
    )

    if df_raw.empty:
        empty = _ensure_llm_col(pd.DataFrame())
        write_premarket_excel(
            excel_path,
            final_view_df=empty,
            picks_df=empty,
            candidates_df=empty,
            monitor_df=empty,
            all_scored_df=empty,
            raw_movers_df=empty,
            rf=Path(rf),
        )
        send_email(f"🌅 Premarket Picks ({run_date})", "<p>No movers returned.</p>", attachment_path=excel_path)
        return

    df_raw["pct_change"] = pd.to_numeric(df_raw.get("pct_change"), errors="coerce").fillna(0.0)
    df_raw["current"] = pd.to_numeric(df_raw.get("current"), errors="coerce").fillna(0.0)

    snapshot = get_market_snapshot()
    market_trend = snapshot.get("trend", "up")

    rows: List[Dict[str, Any]] = []
    for _, r in df_raw.iterrows():
        sym = str(r.get("symbol", "")).upper().strip()
        current = float(r.get("current", 0.0) or 0.0)
        pct_change = float(r.get("pct_change", 0.0) or 0.0)

        score_val, score_label, reasons = get_predictive_score_with_reasons(sym)
        score_val = int(score_val)
        decision = map_score_to_decision(score_val)

        news_items = fetch_news_links(sym, max_articles=1)
        main_item = news_items[0] if news_items else ""
        title = extract_headline_from_html(main_item)
        link = extract_url_from_html(main_item)
        flag = news_flag_from_headlines([title])

        conf = compute_confidence(score_val, pct_change, market_trend, flag)

        f = forecast_price_levels(sym, current=current, score=score_val, horizon="intraday")
        price_cat = get_price_category(current)

        rows.append({
            "symbol": sym,
            "price_category": price_cat,
            "current": current,
            "pct_change": pct_change,
            "predicted_price": getattr(f, "predicted_price", pd.NA),
            "target_price": getattr(f, "target_price", pd.NA),
            "stop_loss": getattr(f, "stop_loss", pd.NA),
            "forecast_trend": getattr(f, "trend", ""),
            "forecast_atr": getattr(f, "atr", pd.NA),
            "forecast_reason": getattr(f, "reason", ""),
            "trade_plan": "Enter near current; target-hit win tracking uses intraday high vs target.",
            "earnings_risk": "",
            "decision": decision,
            "score": score_val,
            "score_label": score_label,
            "confidence": int(conf),
            "reasons": reasons,
            "llm_explanation": "",
            "news_flag": flag,
            "main_news_title": title,
            "main_news_link": link,
        })

    out_df = pd.DataFrame(rows)

    for c in ["current", "pct_change", "predicted_price", "target_price", "stop_loss", "forecast_atr"]:
        if c in out_df.columns:
            out_df[c] = pd.to_numeric(out_df[c], errors="coerce")

    all_scored_df = out_df.sort_values(by=["confidence", "score"], ascending=False).copy()
    monitor_df = all_scored_df.head(20).copy()

    candidates_df = out_df.copy()
    candidates_df = candidates_df[candidates_df["confidence"] >= MIN_CONFIDENCE_TO_TRADE].copy()
    candidates_df = candidates_df[
        (candidates_df["current"] <= MAX_PRICE) |
        ((candidates_df["score"] >= ELITE_SCORE_OVERRIDE) & (candidates_df["confidence"] >= ELITE_CONF_OVERRIDE))
    ].copy()
    candidates_df = candidates_df.sort_values(by=["confidence", "score"], ascending=False)

    picks_df = candidates_df.head(int(TRADE_MAX_PICKS)).copy().reset_index(drop=True)

    # Ensure picks have target/stop
    if not picks_df.empty:
        tgt_fix, stp_fix = [], []
        for _, r in picks_df.iterrows():
            tgt, stp = _infer_intraday_target_stop(r)
            tgt_fix.append(tgt if tgt is not None else pd.NA)
            stp_fix.append(stp if stp is not None else pd.NA)
        picks_df["target_price"] = pd.to_numeric(pd.Series(tgt_fix, index=picks_df.index), errors="coerce")
        picks_df["stop_loss"] = pd.to_numeric(pd.Series(stp_fix, index=picks_df.index), errors="coerce")

    # Enrich raw movers and build FINAL_VIEW
    raw_enriched_df = _enrich_raw_movers(df_raw, all_scored_df)
    final_view_df = _build_final_view(
        picks_df=picks_df,
        monitor_df=monitor_df,
        all_scored_df=all_scored_df,
        raw_enriched_df=raw_enriched_df,
    )

    # FINAL_VIEW: enforce levels + deterministic plan_card
    if not final_view_df.empty:
        tgt_fix, stp_fix = [], []
        for _, r in final_view_df.iterrows():
            tgt, stp = _infer_intraday_target_stop(r)
            tgt_fix.append(tgt if tgt is not None else pd.NA)
            stp_fix.append(stp if stp is not None else pd.NA)

        final_view_df["target_price"] = pd.to_numeric(pd.Series(tgt_fix, index=final_view_df.index), errors="coerce")
        final_view_df["stop_loss"] = pd.to_numeric(pd.Series(stp_fix, index=final_view_df.index), errors="coerce")
        final_view_df["plan_card"] = final_view_df.apply(_plan_card_row, axis=1)
    else:
        final_view_df = final_view_df.copy()
        final_view_df["plan_card"] = ""

    # -----------------------------
    # LLM explanations (OPTIONAL)
    #   - Keep TOP-N behavior for big tables (cost control)
    #   - ALWAYS fill FINAL_VIEW (small list)
    # -----------------------------
    picks_df = _apply_llm_explanations(picks_df, horizon="premarket_picks", top_n=PREMARKET_LLM_TOP_N)
    monitor_df = _apply_llm_explanations(monitor_df, horizon="premarket_monitor", top_n=PREMARKET_LLM_TOP_N)
    candidates_df = _apply_llm_explanations(candidates_df, horizon="premarket_candidates", top_n=PREMARKET_LLM_TOP_N)
    all_scored_df = _apply_llm_explanations(all_scored_df, horizon="premarket_all_scored", top_n=PREMARKET_LLM_TOP_N)
    final_view_df = _apply_llm_explanations(final_view_df, horizon="premarket_final_view", top_n=None)
    raw_enriched_df = _apply_llm_explanations(raw_enriched_df, horizon="premarket_raw_enriched", top_n=PREMARKET_LLM_TOP_N)

    # RECO log (optional legacy)
    append_recommendations_log(candidates_df, now, mode="premarket")

    # DAILY log (source-of-truth for postmarket)
    append_daily_log(final_view_df, run_date, mode="premarket")

    # Portfolio open add (best-effort) only when real PICKS exist
    if not picks_df.empty:
        try:
            cfg = PortfolioConfig()
            open_df = load_open_portfolio(cfg)
            for c in ["symbol", "current", "target_price", "stop_loss", "score", "confidence", "decision", "forecast_trend"]:
                if c not in picks_df.columns:
                    picks_df[c] = pd.NA
            updated_open, added_df = add_new_positions_from_picks(cfg, open_df, picks_df, now)
            save_open_portfolio(cfg, updated_open)
            append_open_actions(cfg, added_df)
        except Exception as e:
            (Path(rf) / "portfolio_error.txt").write_text(repr(e), encoding="utf-8")

    # Excel
    write_premarket_excel(
        excel_path,
        final_view_df=final_view_df,
        picks_df=picks_df,
        candidates_df=candidates_df,
        monitor_df=monitor_df,
        all_scored_df=all_scored_df,
        raw_movers_df=raw_enriched_df,
        rf=Path(rf),
    )

    # -----------------------------
    # Email (always shows FINAL_VIEW + deterministic PLAN_CARD)
    # -----------------------------
    def _fmt_money(v) -> str:
        try:
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return ""
            return f"{float(v):.2f}"
        except Exception:
            return ""

    def row_html(rr):
        sym = _html.escape(str(rr.get("symbol", "")))
        cur = _fmt_money(rr.get("current"))
        tgt = _fmt_money(rr.get("target_price"))
        stp = _fmt_money(rr.get("stop_loss"))
        score = _html.escape(str(rr.get("score", "")))
        conf = _html.escape(str(rr.get("confidence", "")))
        stance = _html.escape(str(rr.get("stance", rr.get("decision", ""))))
        title = _html.escape(str(rr.get("main_news_title") or ""))
        link = str(rr.get("main_news_link") or "").strip() or "#"
        reasons = _html.escape(str(rr.get("reasons") or ""))[:350]

        # ✅ locked deterministic card
        plan = _html.escape(str(rr.get("plan_card") or ""))[:900].replace("\n", "<br>")

        return f"""
        <tr>
          <td><b>{sym}</b></td>
          <td>{cur}</td>
          <td>{tgt}</td>
          <td>{stp}</td>
          <td>{score}</td>
          <td>{conf}</td>
          <td>{stance}</td>
          <td><a href="{link}" target="_blank">{title}</a></td>
          <td style="color:#444;">{reasons}</td>
          <td style="color:#333;white-space:normal;">{plan}</td>
        </tr>
        """

    rows_html = "\n".join([row_html(rr) for _, rr in final_view_df.iterrows()])
    mode_note = "Qualified Picks (GO)" if (picks_df is not None and not picks_df.empty) else "High Vote Watchlist (WATCH)"

    html = f"""
    <h2>🌅 Premarket ({run_date})</h2>
    <p>
      <b>Market trend:</b> {_html.escape(str(snapshot.get("trend")))} |
      <b>SPY gap:</b> {snapshot.get("spy_gap_pct", 0.0):.2f}% |
      <b>VIX:</b> {snapshot.get("vix")}
    </p>
    <p><b>Mode:</b> {mode_note}</p>
    <p>Filters: confidence ≥ {MIN_CONFIDENCE_TO_TRADE}, price ≤ ${MAX_PRICE} (elite override allowed).</p>

    <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;font-family:Arial;font-size:13px;">
      <tr style="background:#eee;">
        <th>Symbol</th><th>Price</th><th>Target</th><th>Stop</th><th>Score</th><th>Conf</th><th>Stance</th><th>Headline</th><th>Reasons</th><th>Plan Card (Locked)</th>
      </tr>
      {rows_html}
    </table>

    <p><b>Attachment:</b> Excel included with sheets: FINAL_VIEW, PICKS, CANDIDATES, MONITOR_TOP20, ALL_SCORED, RAW_MOVERS.</p>
    """

    if send_email(f"🌅 Premarket ({run_date})", html, attachment_path=excel_path):
        marker.write_text("sent\n", encoding="utf-8")

    print(f"✅ Premarket complete | final_view={len(final_view_df)} | picks={len(picks_df)} | excel={excel_path}")


if __name__ == "__main__":
    run_premarket()