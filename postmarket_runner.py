# postmarket_runner.py (BULLETPROOF - Weekly Excel attach + perf log hardening + debug artifacts)
from __future__ import annotations

import os
import re
import html as _html
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Optional, Tuple, Dict
import pandas as pd
from pandas.api.types import is_datetime64tz_dtype
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
    SCORE_COLORS,
    INTRADAY_INTERVAL,
    CONSERVATIVE_SAME_BAR_POLICY,
    EVAL_FALLBACK_STRICT,
    MARKET_OPEN_CT,
    MARKET_CLOSE_CT,
    POST_MARKET_START_CT,
)

from email_sender import send_email as _send_email
from performance_tracker import (
    PortfolioConfig,
    load_open_portfolio,
    save_open_portfolio,
    append_trade_history,
    update_and_close_positions,
    portfolio_summary,
)

import warnings
warnings.filterwarnings(
    "ignore",
    category=FutureWarning,
    message="The behavior of DataFrame concatenation with empty or all-NA entries is deprecated"
)


LOCAL_TZ = ZoneInfo("America/Chicago")
POST_MARKET_START = POST_MARKET_START_CT

# Debug artifacts (set POSTMARKET_DEBUG=1 to enable extra CSVs)
POSTMARKET_DEBUG = os.getenv("POSTMARKET_DEBUG", "0") == "1"


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


def make_run_id(now: datetime) -> str:
    return now.strftime("%Y%m%d_%H%M%S")


def ensure_csv_exists(path: str, header_cols: list[str]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if (not p.exists()) or p.stat().st_size == 0:
        pd.DataFrame(columns=header_cols).to_csv(path, index=False)


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


def send_postmarket_email_once(now: datetime, subject: str, html_body: str, attachment_path: str | None = None) -> bool:
    sent_flag = Path(run_dir(now, "postmarket")) / f"email_sent_{now.strftime('%Y%m%d')}.txt"
    if sent_flag.exists():
        print("📩 Postmarket email already sent for this run_date — skipping resend.")
        return False
    ok = send_email(subject, html_body, attachment_path)
    sent_flag.write_text(
        f"sent_ts={now.strftime('%Y-%m-%d %H:%M:%S')}\nrun_id={make_run_id(now)}\n",
        encoding="utf-8",
    )
    return ok


# -----------------------------
# Logs
# -----------------------------
DAILY_LOG_CSV = out_path("daily_stock_log.csv", kind="logs")
RECO_LOG_CSV = out_path("recommendations_log.csv", kind="logs")
PERF_LOG_CSV = out_path("performance_log.csv", kind="logs")

PERF_COLS = [
    "run_date",
    "source_mode",
    "instrument_type",
    "symbol",
    "decision",
    "score",
    "confidence",
    "entry_price",
    "target_price",
    "stop_loss",
    "reco_ts",
    "session_start",
    "session_end",
    "day_high_after_reco",
    "day_low_after_reco",
    "close_price",
    "actual_change_pct_close",
    "target_hit",
    "stop_hit",
    "first_hit",
    "first_hit_time",
    "hit_latency_minutes",
    "target_overshoot_pct",
    "best_exit_price_after_target",
    "best_exit_time_after_target",
    "best_exit_from_entry_pct",
    "best_exit_from_target_pct",
    "best_exit_latency_minutes",
    "outcome",
]
ensure_csv_exists(PERF_LOG_CSV, PERF_COLS)


# -----------------------------
# Excel styling
# -----------------------------
def normalize_color(color: str) -> str:
    if not color:
        color = "#FFFFFF"
    color = color.lstrip("#")
    if len(color) == 6:
        color = "FF" + color
    return color.upper()


def style_excel_sheet(sheet):
    if sheet.max_row < 1 or sheet.max_column < 1:
        return

    header_font = Font(bold=True, color="FFFFFF")
    header_fill = PatternFill(
        start_color=normalize_color("#2F5597"),
        end_color=normalize_color("#2F5597"),
        fill_type="solid",
    )
    center = Alignment(horizontal="center", vertical="center", wrap_text=True)

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


# -----------------------------
# Helpers
# -----------------------------
def _safe_float(x) -> Optional[float]:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return None
        return float(x)
    except Exception:
        return None


def _ensure_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy() if df is not None else pd.DataFrame()
    for c in cols:
        if c not in out.columns:
            out[c] = pd.NA
    return out


def _parse_ts_maybe(val: object) -> Optional[datetime]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        if isinstance(val, datetime):
            dt = val
        else:
            dt = pd.to_datetime(str(val), errors="coerce").to_pydatetime()
        if dt is None:
            return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=LOCAL_TZ)
        return dt.astimezone(LOCAL_TZ)
    except Exception:
        return None


def _session_bounds(run_date: str) -> Tuple[datetime, datetime]:
    d = pd.to_datetime(run_date, errors="coerce")
    if pd.isna(d):
        d = pd.Timestamp(datetime.now(LOCAL_TZ).date())
    day = d.date()
    start_dt = datetime.combine(day, MARKET_OPEN_CT, tzinfo=LOCAL_TZ)
    end_dt = datetime.combine(day, MARKET_CLOSE_CT, tzinfo=LOCAL_TZ)
    return start_dt, end_dt


def _classify_instrument(symbol: str, row: Optional[pd.Series] = None) -> str:
    try:
        if row is not None:
            it = str(row.get("instrument_type") or row.get("asset_type") or "").strip().lower()
            if it in {"option", "options"}:
                return "options"
            if it in {"stock", "equity", "shares"}:
                return "stock"
    except Exception:
        pass

    s = (symbol or "").upper().strip()

    if re.fullmatch(r"[A-Z]{1,6}\d{6}[CP]\d{8}", s):
        return "options"

    if re.search(r"\d{6,8}[CP]\d+", s):
        return "options"

    return "stock"


def _intraday_after_reco(symbol: str, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    interval = INTRADAY_INTERVAL if INTRADAY_INTERVAL else "5m"
    try:
        start_date = start_dt.date()
        end_date = (end_dt.date() + timedelta(days=1))

        h = yf.Ticker(symbol).history(
            start=str(start_date),
            end=str(end_date),
            interval=interval,
            auto_adjust=False,
            prepost=False,
        )
        if h is None or h.empty:
            return pd.DataFrame()

        try:
            idx = pd.to_datetime(h.index, errors="coerce")
            if getattr(idx, "tz", None) is None:
                idx = idx.tz_localize(LOCAL_TZ)
            else:
                idx = idx.tz_convert(LOCAL_TZ)
            h.index = idx
            h = h[~h.index.isna()].copy()
        except Exception:
            pass

        h = h.sort_index()
        h = h[(h.index >= start_dt) & (h.index <= end_dt)].copy()
        return h

    except Exception:
        return pd.DataFrame()


def _get_close_for_date(symbol: str, run_date: str) -> Optional[float]:
    """
    Returns the CLOSE price for the given run_date (local trading day).
    Uses 7d window and picks the row whose date matches run_date.
    """
    try:
        h = yf.Ticker(symbol).history(period="7d", auto_adjust=False)
        if h is None or h.empty or "Close" not in h.columns:
            return None

        idx = pd.to_datetime(h.index, errors="coerce")
        if getattr(idx, "tz", None) is not None:
            idx = idx.tz_convert(LOCAL_TZ)
        dates = idx.date

        target_day = pd.to_datetime(run_date, errors="coerce")
        if pd.isna(target_day):
            return None
        target_day = target_day.date()

        mask = dates == target_day
        if not mask.any():
            return None

        close = pd.to_numeric(h.loc[mask, "Close"], errors="coerce").dropna()
        if close.empty:
            return None
        return float(close.iloc[-1])
    except Exception:
        return None


def _compute_outcome_from_hits(target_hit: bool, stop_hit: bool) -> str:
    if target_hit:
        return "🏆 Target Hit"
    if stop_hit:
        return "🛑 Stop Hit"
    return "⏳ Not Hit"


def _summarize_eval(df: pd.DataFrame) -> Dict[str, float]:
    if df is None or df.empty:
        return {"evaluated": 0, "wins": 0, "losses": 0, "not_hit": 0, "win_rate": 0.0}

    eval_df = df[df["outcome"].isin(["🏆 Target Hit", "🛑 Stop Hit", "⏳ Not Hit"])].copy()
    total = int(len(eval_df))
    wins = int((eval_df["outcome"] == "🏆 Target Hit").sum()) if total else 0
    losses = int((eval_df["outcome"] == "🛑 Stop Hit").sum()) if total else 0
    not_hit = int((eval_df["outcome"] == "⏳ Not Hit").sum()) if total else 0
    rate = (wins / total * 100.0) if total else 0.0
    return {"evaluated": total, "wins": wins, "losses": losses, "not_hit": not_hit, "win_rate": rate}


def _summarize_eval_by_instrument(df: pd.DataFrame) -> Dict[str, Dict[str, float]]:
    out = {}
    if df is None or df.empty:
        return {"stock": _summarize_eval(pd.DataFrame()), "options": _summarize_eval(pd.DataFrame())}
    for k in ["stock", "options"]:
        out[k] = _summarize_eval(df[df.get("instrument_type", "") == k].copy())
    return out


# -----------------------------
# Load sources
# -----------------------------
def load_premarket_today(daily_log_csv: str, run_date: str) -> pd.DataFrame:
    if (not os.path.exists(daily_log_csv)) or os.path.getsize(daily_log_csv) == 0:
        return pd.DataFrame()

    df = pd.read_csv(daily_log_csv)
    if df.empty or "run_date" not in df.columns:
        return pd.DataFrame()

    df = df[df["run_date"].astype(str) == str(run_date)].copy()
    if df.empty:
        return pd.DataFrame()

    if "mode" in df.columns:
        df = df[df["mode"].astype(str).str.lower().isin(["premarket", ""])].copy()

    df["source_mode"] = "premarket"
    return df


def load_midday_today(reco_log_csv: str, run_date: str) -> pd.DataFrame:
    if (not os.path.exists(reco_log_csv)) or os.path.getsize(reco_log_csv) == 0:
        return pd.DataFrame()

    df = pd.read_csv(reco_log_csv)
    if df.empty or "run_date" not in df.columns:
        return pd.DataFrame()

    df = df[df["run_date"].astype(str) == str(run_date)].copy()
    if df.empty:
        return pd.DataFrame()

    if "mode" in df.columns:
        df = df[df["mode"].astype(str).str.lower() == "midday"].copy()
    if df.empty:
        return pd.DataFrame()

    if "run_ts" in df.columns:
        df["run_ts"] = pd.to_datetime(df["run_ts"], errors="coerce")
        df = df.sort_values("run_ts").drop_duplicates(subset=["symbol"], keep="last")

    df["source_mode"] = "midday"
    return df


# -----------------------------
# Evaluation (TARGET HIT logic + analytics)
# -----------------------------
def evaluate_rows(df_in: pd.DataFrame, run_date: str) -> pd.DataFrame:
    if df_in is None or df_in.empty:
        return pd.DataFrame()

    df = df_in.copy()
    df = _ensure_cols(
        df,
        ["symbol", "decision", "score", "confidence", "current", "target_price", "stop_loss", "source_mode", "instrument_type"],
    )
    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
    df["decision"] = df["decision"].astype(str)
    df["source_mode"] = df["source_mode"].astype(str)

    df["instrument_type"] = [
        _classify_instrument(sym, row=r) for sym, (_, r) in zip(df["symbol"].tolist(), df.iterrows())
    ]

    df["entry_price"] = pd.to_numeric(df["current"], errors="coerce")

    session_start, session_end = _session_bounds(run_date)

    reco_ts_list = []
    for _, r in df.iterrows():
        rt = None
        if "run_ts" in df.columns:
            rt = _parse_ts_maybe(r.get("run_ts"))
        reco_ts_list.append(rt if rt is not None else session_start)

    df["reco_ts"] = reco_ts_list
    df["session_start"] = session_start.strftime("%Y-%m-%d %H:%M:%S")
    df["session_end"] = session_end.strftime("%Y-%m-%d %H:%M:%S")

    day_highs, day_lows = [], []
    closes, pct_close = [], []
    target_hits, stop_hits = [], []
    first_hits, first_hit_times = [], []
    hit_latency_minutes = []
    target_overshoot_pct = []

    best_exit_price_after_target = []
    best_exit_time_after_target = []
    best_exit_from_entry_pct = []
    best_exit_from_target_pct = []
    best_exit_latency_minutes = []

    outcomes = []

    for _, r in df.iterrows():
        sym = str(r.get("symbol", "")).upper().strip()
        entry = _safe_float(r.get("entry_price"))
        target = _safe_float(r.get("target_price"))
        stop = _safe_float(r.get("stop_loss"))

        reco_dt = r.get("reco_ts")
        if isinstance(reco_dt, str):
            reco_dt = _parse_ts_maybe(reco_dt)
        if reco_dt is None:
            reco_dt = session_start
        if reco_dt.tzinfo is None:
            reco_dt = reco_dt.replace(tzinfo=LOCAL_TZ)

        # Clamp reco_dt into the session window
        if reco_dt < session_start:
            reco_dt = session_start

        if reco_dt > session_end:
            outcomes.append("⛔ Skipped (Late Recommendation)")
            day_highs.append(pd.NA)
            day_lows.append(pd.NA)
            closes.append(_get_close_for_date(sym, run_date))
            pct_close.append(pd.NA)
            target_hits.append(False)
            stop_hits.append(False)
            first_hits.append("SKIP")
            first_hit_times.append("reco_after_close")
            hit_latency_minutes.append(pd.NA)
            target_overshoot_pct.append(pd.NA)
            best_exit_price_after_target.append(pd.NA)
            best_exit_time_after_target.append("")
            best_exit_from_entry_pct.append(pd.NA)
            best_exit_from_target_pct.append(pd.NA)
            best_exit_latency_minutes.append(pd.NA)
            continue

        close_price = _get_close_for_date(sym, run_date)
        closes.append(close_price)

        if close_price is None or entry is None or entry <= 0:
            pct_close.append(pd.NA)
        else:
            pct_close.append(((close_price - entry) / entry) * 100.0)

        intraday = _intraday_after_reco(sym, reco_dt, session_end)

        th = False
        sh = False
        fh = ""
        fht_str = ""
        latency = pd.NA
        overshoot = pd.NA

        best_px = pd.NA
        best_ts_str = ""
        best_from_entry = pd.NA
        best_from_target = pd.NA
        best_latency = pd.NA

        if intraday is None or intraday.empty:
            day_highs.append(pd.NA)
            day_lows.append(pd.NA)

            if not EVAL_FALLBACK_STRICT:
                if close_price is not None and target is not None and target > 0 and close_price >= target:
                    th = True
                    fh = "TARGET"
                    fht_str = "close_fallback"
                elif close_price is not None and stop is not None and stop > 0 and close_price <= stop:
                    sh = True
                    fh = "STOP"
                    fht_str = "close_fallback"

            target_hits.append(th)
            stop_hits.append(sh)
            first_hits.append(fh)
            first_hit_times.append(fht_str)
            hit_latency_minutes.append(latency)
            target_overshoot_pct.append(overshoot)
            best_exit_price_after_target.append(best_px)
            best_exit_time_after_target.append(best_ts_str)
            best_exit_from_entry_pct.append(best_from_entry)
            best_exit_from_target_pct.append(best_from_target)
            best_exit_latency_minutes.append(best_latency)
            outcomes.append(_compute_outcome_from_hits(th, sh))
            continue

        hi = pd.to_numeric(intraday.get("High"), errors="coerce").dropna() if "High" in intraday.columns else pd.Series(dtype=float)
        lo = pd.to_numeric(intraday.get("Low"), errors="coerce").dropna() if "Low" in intraday.columns else pd.Series(dtype=float)

        max_high = float(hi.max()) if not hi.empty else None
        min_low = float(lo.min()) if not lo.empty else None

        day_highs.append(max_high if max_high is not None else pd.NA)
        day_lows.append(min_low if min_low is not None else pd.NA)

        has_target = target is not None and target > 0
        has_stop = stop is not None and stop > 0

        if not has_target and not has_stop:
            target_hits.append(False)
            stop_hits.append(False)
            first_hits.append("")
            first_hit_times.append("")
            hit_latency_minutes.append(pd.NA)
            target_overshoot_pct.append(pd.NA)
            best_exit_price_after_target.append(pd.NA)
            best_exit_time_after_target.append("")
            best_exit_from_entry_pct.append(pd.NA)
            best_exit_from_target_pct.append(pd.NA)
            best_exit_latency_minutes.append(pd.NA)
            outcomes.append("⏳ Not Hit")
            continue

        first_hit_dt: Optional[datetime] = None

        for ts, bar in intraday.iterrows():
            bar_high = _safe_float(bar.get("High"))
            bar_low = _safe_float(bar.get("Low"))

            hit_target_now = has_target and (bar_high is not None) and (bar_high >= target)
            hit_stop_now = has_stop and (bar_low is not None) and (bar_low <= stop)

            if hit_target_now and hit_stop_now:
                if str(CONSERVATIVE_SAME_BAR_POLICY).strip().lower() == "target_first":
                    th = True
                    fh = "TARGET"
                else:
                    sh = True
                    fh = "STOP"
                first_hit_dt = ts if isinstance(ts, datetime) else None
                break

            if hit_target_now:
                th = True
                fh = "TARGET"
                first_hit_dt = ts if isinstance(ts, datetime) else None
                break

            if hit_stop_now:
                sh = True
                fh = "STOP"
                first_hit_dt = ts if isinstance(ts, datetime) else None
                break

        if first_hit_dt is not None:
            try:
                first_hit_dt = first_hit_dt.astimezone(LOCAL_TZ)
                fht_str = first_hit_dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                fht_str = str(first_hit_dt)

        if first_hit_dt is not None and isinstance(reco_dt, datetime):
            try:
                latency = round((first_hit_dt - reco_dt).total_seconds() / 60.0, 2)
                if latency < 0:
                    latency = 0.0
            except Exception:
                latency = pd.NA

        if th and has_target and max_high is not None and target and target > 0:
            overshoot = round(((max_high - target) / target) * 100.0, 3)

        if th and first_hit_dt is not None and "High" in intraday.columns:
            after = intraday[intraday.index >= first_hit_dt].copy()
            if not after.empty:
                after_hi = pd.to_numeric(after["High"], errors="coerce").dropna()
                if not after_hi.empty:
                    best_val = float(after_hi.max())
                    best_px = best_val
                    try:
                        best_time = after_hi.idxmax()
                        if isinstance(best_time, datetime):
                            best_time = best_time.astimezone(LOCAL_TZ)
                            best_ts_str = best_time.strftime("%Y-%m-%d %H:%M:%S")
                            if isinstance(reco_dt, datetime):
                                best_latency = round((best_time - reco_dt).total_seconds() / 60.0, 2)
                                if best_latency < 0:
                                    best_latency = 0.0
                    except Exception:
                        best_ts_str = ""

                    if entry is not None and entry > 0:
                        best_from_entry = round(((best_val - entry) / entry) * 100.0, 3)
                    if target is not None and target > 0:
                        best_from_target = round(((best_val - target) / target) * 100.0, 3)

        target_hits.append(th)
        stop_hits.append(sh)
        first_hits.append(fh)
        first_hit_times.append(fht_str)
        hit_latency_minutes.append(latency)
        target_overshoot_pct.append(overshoot)
        best_exit_price_after_target.append(best_px)
        best_exit_time_after_target.append(best_ts_str)
        best_exit_from_entry_pct.append(best_from_entry)
        best_exit_from_target_pct.append(best_from_target)
        best_exit_latency_minutes.append(best_latency)
        outcomes.append(_compute_outcome_from_hits(th, sh))

    df["target_price"] = pd.to_numeric(df["target_price"], errors="coerce")
    df["stop_loss"] = pd.to_numeric(df["stop_loss"], errors="coerce")

    df["day_high_after_reco"] = day_highs
    df["day_low_after_reco"] = day_lows
    df["close_price"] = closes
    df["actual_change_pct_close"] = pct_close

    df["target_hit"] = target_hits
    df["stop_hit"] = stop_hits
    df["first_hit"] = first_hits
    df["first_hit_time"] = first_hit_times
    df["hit_latency_minutes"] = hit_latency_minutes
    df["target_overshoot_pct"] = target_overshoot_pct
    df["best_exit_price_after_target"] = best_exit_price_after_target
    df["best_exit_time_after_target"] = best_exit_time_after_target
    df["best_exit_from_entry_pct"] = best_exit_from_entry_pct
    df["best_exit_from_target_pct"] = best_exit_from_target_pct
    df["best_exit_latency_minutes"] = best_exit_latency_minutes

    df["outcome"] = outcomes
    df["run_date"] = run_date

    cols_order = PERF_COLS[:]
    df = _ensure_cols(df, cols_order)
    return df[cols_order]


def append_perf_log(out_df: pd.DataFrame, now: Optional[datetime] = None) -> None:
    """
    Bulletproof perf append:
    - backfills entry_price from current if missing
    - writes optional debug artifacts before/after dropna
    """
    try:
        if out_df is None or out_df.empty:
            return

        now = now or datetime.now(LOCAL_TZ)
        rf = run_dir(now, "postmarket")

        # Backfill entry_price if missing (from current BEFORE slicing)
        out = out_df.copy()
        out = _ensure_cols(out, PERF_COLS + ["current"])

        out["entry_price"] = pd.to_numeric(out.get("entry_price"), errors="coerce")
        cur = pd.to_numeric(out.get("current"), errors="coerce")
        out["entry_price"] = out["entry_price"].fillna(cur)

        out = _ensure_cols(out, PERF_COLS)[PERF_COLS]

        # Required columns to persist a row
        required = ["run_date", "source_mode", "symbol", "decision", "score", "confidence", "entry_price"]
        for c in required:
            if c in out.columns:
                out[c] = out[c].replace("", pd.NA)

        if POSTMARKET_DEBUG:
            out.to_csv(Path(rf) / "perf_debug_before_dropna.csv", index=False)

        out2 = out.dropna(subset=[c for c in required if c in out.columns])

        if POSTMARKET_DEBUG:
            out2.to_csv(Path(rf) / "perf_debug_after_dropna.csv", index=False)

        if out2.empty:
            (Path(rf) / "perf_append_skipped.txt").write_text(
                "append_perf_log skipped: all rows dropped by required fields.\n"
                f"required={required}\n",
                encoding="utf-8",
            )
            return

        file_exists = os.path.exists(PERF_LOG_CSV) and os.path.getsize(PERF_LOG_CSV) > 0
        if file_exists:
            prev = pd.read_csv(PERF_LOG_CSV)
            prev = _ensure_cols(prev, PERF_COLS)[PERF_COLS]
            frames = [df for df in (prev, out2) if df is not None and not df.empty]
            merged = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        else:
            merged = out2.copy()

        merged = merged.drop_duplicates(subset=["run_date", "source_mode", "symbol"], keep="last")
        merged.to_csv(PERF_LOG_CSV, index=False)

    except Exception as e:
        print("⚠️ PERF log append failed:", e)


def build_weekly_dashboard_html(perf_log_csv: str, now: datetime) -> str:
    try:
        if not os.path.exists(perf_log_csv) or os.path.getsize(perf_log_csv) == 0:
            return "<h2>📅 Weekly Dashboard</h2><p>No performance log yet.</p>"

        df = pd.read_csv(perf_log_csv)
        if df.empty or "run_date" not in df.columns:
            return "<h2>📅 Weekly Dashboard</h2><p>Performance log empty/invalid.</p>"

        # ✅ normalize legacy + fill blanks
        df = normalize_perf_df(df)

        # clean source_mode BEFORE d7
        if "source_mode" in df.columns:
            df["source_mode"] = df["source_mode"].astype(str).str.strip().str.lower()
            df["source_mode"] = df["source_mode"].replace({"nan": "", "none": ""})
            df["source_mode"] = df["source_mode"].replace("", pd.NA).fillna("unknown")

        week_ago = (now - timedelta(days=7)).replace(tzinfo=None)
        d7 = df[df["run_date"] >= week_ago].copy()

        # --- source_mode cleanup (so Premarket/Midday filters work) ---
        if "source_mode" in df.columns:
            df["source_mode"] = df["source_mode"].astype(str).str.strip().str.lower()
            df["source_mode"] = df["source_mode"].replace({"nan": ""})
            df["source_mode"] = df["source_mode"].replace("", pd.NA)
            df["source_mode"] = df["source_mode"].fillna("unknown")
        else:
            df["source_mode"] = "unknown"

        # --- backfill target_hit/stop_hit from outcome (legacy consistency) ---
        if "outcome" in df.columns:
            if "target_hit" in df.columns:
                df.loc[df["outcome"] == "🏆 Target Hit", "target_hit"] = True
            if "stop_hit" in df.columns:
                df.loc[df["outcome"] == "🛑 Stop Hit", "stop_hit"] = True

        # ✅ filter AFTER cleanup
        d7 = df[df["run_date"] >= week_ago].copy()
        if d7.empty:
            return "<h2>📅 Weekly Dashboard</h2><p>No rows in last 7 days.</p>"

        all_s = _summarize_eval(d7)
        prem_s = _summarize_eval(d7[d7["source_mode"] == "premarket"].copy())
        mid_s  = _summarize_eval(d7[d7["source_mode"] == "midday"].copy())

        all_by = _summarize_eval_by_instrument(d7)
        prem_by = _summarize_eval_by_instrument(d7[d7["source_mode"] == "premarket"].copy())
        mid_by  = _summarize_eval_by_instrument(d7[d7["source_mode"] == "midday"].copy())

        return f"""
        <h2>📅 Weekly Trading Dashboard ({now.strftime('%Y-%m-%d')})</h2>

        <p><b>ALL (7d):</b> evaluated={all_s["evaluated"]}, wins={all_s["wins"]}, losses={all_s["losses"]}, not_hit={all_s["not_hit"]}, win_rate={all_s["win_rate"]:.2f}%</p>
        <p><b>Premarket (7d):</b> evaluated={prem_s["evaluated"]}, win_rate={prem_s["win_rate"]:.2f}%<br>
           <b>Midday (7d):</b> evaluated={mid_s["evaluated"]}, win_rate={mid_s["win_rate"]:.2f}%</p>

        <h3>📦 Instrument Buckets (7d)</h3>
        <p><b>ALL - Stock:</b> eval={all_by["stock"]["evaluated"]}, win_rate={all_by["stock"]["win_rate"]:.2f}% |
           <b>Options:</b> eval={all_by["options"]["evaluated"]}, win_rate={all_by["options"]["win_rate"]:.2f}%</p>
        <p><b>Premarket - Stock:</b> eval={prem_by["stock"]["evaluated"]}, win_rate={prem_by["stock"]["win_rate"]:.2f}% |
           <b>Options:</b> eval={prem_by["options"]["evaluated"]}, win_rate={prem_by["options"]["win_rate"]:.2f}%</p>
        <p><b>Midday - Stock:</b> eval={mid_by["stock"]["evaluated"]}, win_rate={mid_by["stock"]["win_rate"]:.2f}% |
           <b>Options:</b> eval={mid_by["options"]["evaluated"]}, win_rate={mid_by["options"]["win_rate"]:.2f}%</p>

        <p>Win definition: target price hit at any time after recommendation (intraday high ≥ target).</p>
        """
    except Exception:
        return "<h2>📅 Weekly Dashboard</h2><p>Dashboard generation failed.</p>"
        return "<h2>📅 Weekly Dashboard</h2><p>Dashboard generation failed.</p>"


def _excel_safe_df(df: pd.DataFrame, local_tz: str = "America/Chicago") -> pd.DataFrame:
    if df is None or df.empty:
        return df

    out = df.copy()

    for col in out.columns:
        if is_datetime64tz_dtype(out[col]):
            out[col] = out[col].dt.tz_convert(local_tz).dt.tz_localize(None)

    def _strip_tz(v):
        if isinstance(v, pd.Timestamp) and v.tz is not None:
            return v.tz_convert(local_tz).tz_localize(None)
        return v

    obj_cols = out.select_dtypes(include=["object"]).columns
    for col in obj_cols:
        sample = out[col].dropna().head(50).tolist()
        if any(isinstance(v, pd.Timestamp) and v.tz is not None for v in sample):
            out[col] = out[col].apply(_strip_tz)

    return out


# -----------------------------
# Portfolio wrapper (unchanged)
# -----------------------------
def portfolio_update_and_close(run_dt: datetime) -> Tuple[str, pd.DataFrame]:
    cfg = PortfolioConfig()
    open_df = load_open_portfolio(cfg)
    remaining, closed_df = update_and_close_positions(cfg, open_df, run_dt)
    save_open_portfolio(cfg, remaining)

    if closed_df is not None and not closed_df.empty:
        closed_hist = closed_df.copy()
        closed_hist["action"] = "CLOSE"
        append_trade_history(cfg, closed_hist)

    summary = portfolio_summary(remaining, closed_df)
    return summary, closed_df

def normalize_perf_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    df = df.copy()
    df = _ensure_cols(df, PERF_COLS)

    # run_date -> tz-naive date-only
    df["run_date"] = pd.to_datetime(df["run_date"], errors="coerce").dt.normalize()

    # source_mode cleanup
    if "source_mode" in df.columns:
        df["source_mode"] = df["source_mode"].astype(str).str.strip().str.lower()
        df["source_mode"] = df["source_mode"].replace({"nan": "", "none": ""})
        df["source_mode"] = df["source_mode"].replace("", pd.NA).fillna("unknown")

    # fill instrument_type if missing
    if "instrument_type" in df.columns and "symbol" in df.columns:
        df["instrument_type"] = df["instrument_type"].replace("", pd.NA)
        mask = df["instrument_type"].isna()
        df.loc[mask, "instrument_type"] = [
            _classify_instrument(sym) for sym in df.loc[mask, "symbol"].astype(str).tolist()
        ]

    # backfill session bounds + reco_ts if missing
    for i, r in df.iterrows():
        if pd.isna(r.get("run_date")):
            continue
        rd = r["run_date"].date().strftime("%Y-%m-%d")
        ss, se = _session_bounds(rd)

        if not str(r.get("session_start") or "").strip():
            df.at[i, "session_start"] = ss.strftime("%Y-%m-%d %H:%M:%S")
        if not str(r.get("session_end") or "").strip():
            df.at[i, "session_end"] = se.strftime("%Y-%m-%d %H:%M:%S")
        if not str(r.get("reco_ts") or "").strip():
            df.at[i, "reco_ts"] = ss.strftime("%Y-%m-%d %H:%M:%S")

    # legacy outcome mapping
    legacy_map = {"✅ Correct": "🏆 Target Hit", "❌ Incorrect": "🛑 Stop Hit"}
    df["outcome"] = df["outcome"].replace(legacy_map)

    # booleans normalize
    for b in ["target_hit", "stop_hit"]:
        if b in df.columns:
            s = df[b].astype(str).str.strip().str.upper()
            df[b] = s.map({"TRUE": True, "FALSE": False}).fillna(False).astype(bool)

    # backfill booleans from outcome (fixes your CSV)
    if "outcome" in df.columns:
        if "target_hit" in df.columns:
            df.loc[df["outcome"] == "🏆 Target Hit", "target_hit"] = True
        if "stop_hit" in df.columns:
            df.loc[df["outcome"] == "🛑 Stop Hit", "stop_hit"] = True

    return df
# -----------------------------
# Weekly Excel builder (ALWAYS attaches)
# -----------------------------
def build_weekly_excel(perf_log_csv: str, now: datetime) -> Tuple[str, int, int]:
    """
    Always produces an excel file (even if perf log empty).
    Returns: (path, rows_last_7d, rows_total)
    """
    weekly_excel = out_path(
        f"weekly_dashboard_{now.strftime('%Y%m%d')}_{make_run_id(now)}.xlsx",
        now=now,
        mode="postmarket",
        kind="runs",
    )

    if os.path.exists(perf_log_csv) and os.path.getsize(perf_log_csv) > 0:
        wdf = pd.read_csv(perf_log_csv)
    else:
        wdf = pd.DataFrame(columns=PERF_COLS)

    wdf = normalize_perf_df(wdf)  # ✅ normalize legacy + fill blanks

    if "run_date" in wdf.columns:
        cutoff = (now - timedelta(days=7)).date()  # date-only
        wdf7 = wdf[wdf["run_date"].dt.date >= cutoff].copy()
    else:
        wdf7 = pd.DataFrame(columns=PERF_COLS)

    with pd.ExcelWriter(weekly_excel, engine="openpyxl") as writer:
        _excel_safe_df(wdf7).to_excel(writer, sheet_name="LAST_7_DAYS", index=False)
        _excel_safe_df(wdf).to_excel(writer, sheet_name="ALL_HISTORY", index=False)

    wb = load_workbook(weekly_excel)
    for s in wb.sheetnames:
        style_excel_sheet(wb[s])
    wb.save(weekly_excel)

    return weekly_excel, int(len(wdf7)), int(len(wdf))


# -----------------------------
# Postmarket runner
# -----------------------------
def run_postmarket(now: datetime | None = None) -> None:
    now = now or datetime.now(LOCAL_TZ)

    if now.time() < POST_MARKET_START:
        print("⏳ Post-market skipped (too early).")
        if IS_LOCAL:
            send_email("🧪 Postmarket Test (too early)", "<p>Skipped because too early.</p>")
        return

    run_date = now.strftime("%Y-%m-%d")
    rf = run_dir(now, "postmarket")

    psummary_text = ""
    try:
        psummary_text, _closed = portfolio_update_and_close(now)
    except Exception as e:
        psummary_text = f"Portfolio update failed: {e}"

    prem = load_premarket_today(DAILY_LOG_CSV, run_date)
    mid = load_midday_today(RECO_LOG_CSV, run_date)

    prem_eval = evaluate_rows(prem, run_date) if not prem.empty else pd.DataFrame()
    mid_eval = evaluate_rows(mid, run_date) if not mid.empty else pd.DataFrame()

    all_eval = (
        pd.concat([prem_eval, mid_eval], ignore_index=True)
        if (not prem_eval.empty or not mid_eval.empty)
        else pd.DataFrame()
    )

    if POSTMARKET_DEBUG:
        if prem is not None:
            prem.to_csv(Path(rf) / "debug_premarket_source.csv", index=False)
        if mid is not None:
            mid.to_csv(Path(rf) / "debug_midday_source.csv", index=False)
        if prem_eval is not None and not prem_eval.empty:
            prem_eval.to_csv(Path(rf) / "debug_premarket_eval.csv", index=False)
        if mid_eval is not None and not mid_eval.empty:
            mid_eval.to_csv(Path(rf) / "debug_midday_eval.csv", index=False)

    if all_eval.empty:
        html = f"""
            <h2>📊 Post-Market Summary ({run_date})</h2>
            <p>No premarket picks (daily log) AND no midday recommendations (reco log) found for today.</p>
        """
        if psummary_text:
            html += f"<h3>📁 Portfolio Summary</h3><pre>{_html.escape(psummary_text)}</pre>"

        html += f"""
        <hr>
        <p><b>Debug:</b><br>
        daily_log_exists={os.path.exists(DAILY_LOG_CSV)} size={os.path.getsize(DAILY_LOG_CSV) if os.path.exists(DAILY_LOG_CSV) else 0}<br>
        reco_log_exists={os.path.exists(RECO_LOG_CSV)} size={os.path.getsize(RECO_LOG_CSV) if os.path.exists(RECO_LOG_CSV) else 0}<br>
        prem_rows_today={len(prem)} | mid_rows_today={len(mid)}
        </p>
        """

        send_postmarket_email_once(now, f"📊 Post-Market Summary ({run_date})", html)
        return

    prem_s = _summarize_eval(prem_eval)
    mid_s = _summarize_eval(mid_eval)
    all_s = _summarize_eval(all_eval)

    prem_by = _summarize_eval_by_instrument(prem_eval)
    mid_by = _summarize_eval_by_instrument(mid_eval)
    all_by = _summarize_eval_by_instrument(all_eval)

    summary_html = f"""
        <h2>📊 Post-Market Summary ({run_date})</h2>
        <p><b>Win definition:</b> target hit anytime after recommendation (intraday high ≥ target). Stop is a loss only if it occurs before target.</p>

        <h3>✅ Premarket</h3>
        <p>
          evaluated: {prem_s["evaluated"]}<br>
          wins (target hit): {prem_s["wins"]}<br>
          losses (stop hit): {prem_s["losses"]}<br>
          not hit: {prem_s["not_hit"]}<br>
          win rate: {prem_s["win_rate"]:.2f}%
        </p>
        <p><b>Premarket buckets:</b>
          Stock win_rate={prem_by["stock"]["win_rate"]:.2f}% (n={prem_by["stock"]["evaluated"]}) |
          Options win_rate={prem_by["options"]["win_rate"]:.2f}% (n={prem_by["options"]["evaluated"]})
        </p>

        <h3>⚡ Midday</h3>
        <p>
          evaluated: {mid_s["evaluated"]}<br>
          wins (target hit): {mid_s["wins"]}<br>
          losses (stop hit): {mid_s["losses"]}<br>
          not hit: {mid_s["not_hit"]}<br>
          win rate: {mid_s["win_rate"]:.2f}%
        </p>
        <p><b>Midday buckets:</b>
          Stock win_rate={mid_by["stock"]["win_rate"]:.2f}% (n={mid_by["stock"]["evaluated"]}) |
          Options win_rate={mid_by["options"]["win_rate"]:.2f}% (n={mid_by["options"]["evaluated"]})
        </p>

        <h3>📌 Combined</h3>
        <p>
          evaluated: {all_s["evaluated"]}<br>
          wins (target hit): {all_s["wins"]}<br>
          losses (stop hit): {all_s["losses"]}<br>
          not hit: {all_s["not_hit"]}<br>
          win rate: {all_s["win_rate"]:.2f}%
        </p>
        <p><b>Combined buckets:</b>
          Stock win_rate={all_by["stock"]["win_rate"]:.2f}% (n={all_by["stock"]["evaluated"]}) |
          Options win_rate={all_by["options"]["win_rate"]:.2f}% (n={all_by["options"]["evaluated"]})
        </p>
    """

    if psummary_text:
        summary_html += f"<h3>📁 Portfolio Summary</h3><pre>{_html.escape(psummary_text)}</pre>"

    post_excel = out_path(
        f"post_market_report_{now.strftime('%Y%m%d')}_{make_run_id(now)}.xlsx",
        now=now,
        mode="postmarket",
        kind="runs",
    )

    summary_df = pd.DataFrame([
        {"bucket": "premarket", "evaluated": prem_s["evaluated"], "wins": prem_s["wins"], "losses": prem_s["losses"], "not_hit": prem_s["not_hit"], "win_rate_pct": round(prem_s["win_rate"], 2)},
        {"bucket": "midday", "evaluated": mid_s["evaluated"], "wins": mid_s["wins"], "losses": mid_s["losses"], "not_hit": mid_s["not_hit"], "win_rate_pct": round(mid_s["win_rate"], 2)},
        {"bucket": "combined", "evaluated": all_s["evaluated"], "wins": all_s["wins"], "losses": all_s["losses"], "not_hit": all_s["not_hit"], "win_rate_pct": round(all_s["win_rate"], 2)},
        {"bucket": "combined_stock", "evaluated": all_by["stock"]["evaluated"], "wins": all_by["stock"]["wins"], "losses": all_by["stock"]["losses"], "not_hit": all_by["stock"]["not_hit"], "win_rate_pct": round(all_by["stock"]["win_rate"], 2)},
        {"bucket": "combined_options", "evaluated": all_by["options"]["evaluated"], "wins": all_by["options"]["wins"], "losses": all_by["options"]["losses"], "not_hit": all_by["options"]["not_hit"], "win_rate_pct": round(all_by["options"]["win_rate"], 2)},
    ])

    with pd.ExcelWriter(post_excel, engine="openpyxl") as writer:
        _excel_safe_df(summary_df).to_excel(writer, sheet_name="SUMMARY", index=False)
        if not prem_eval.empty:
            _excel_safe_df(prem_eval).to_excel(writer, sheet_name="PREMARKET_EVAL", index=False)
        if not mid_eval.empty:
            _excel_safe_df(mid_eval).to_excel(writer, sheet_name="MIDDAY_EVAL", index=False)
        _excel_safe_df(all_eval).to_excel(writer, sheet_name="ALL_EVAL", index=False)

    wb = load_workbook(post_excel)
    for s in wb.sheetnames:
        style_excel_sheet(wb[s])
    wb.save(post_excel)

    send_postmarket_email_once(
        now,
        f"📊 Post-Market Report (Target-Hit + Analytics) ({run_date})",
        summary_html,
        attachment_path=post_excel,
    )

    # IMPORTANT: append perf BEFORE weekly dashboard reads it
    append_perf_log(all_eval, now=now)

    if now.weekday() == 4:
        dash = build_weekly_dashboard_html(PERF_LOG_CSV, now)

        weekly_excel, rows_7d, rows_total = build_weekly_excel(PERF_LOG_CSV, now)

        dash += f"""
        <hr>
        <p><b>Debug:</b><br>
        perf_log_path: {PERF_LOG_CSV}<br>
        perf_log_exists: {os.path.exists(PERF_LOG_CSV)}<br>
        perf_log_size_bytes: {os.path.getsize(PERF_LOG_CSV) if os.path.exists(PERF_LOG_CSV) else 0}<br>
        perf_rows_total: {rows_total}<br>
        perf_rows_last_7d: {rows_7d}<br>
        weekly_excel: {weekly_excel}
        </p>
        """

        if psummary_text:
            dash += f"<h3>📁 Current Portfolio</h3><pre>{_html.escape(psummary_text)}</pre>"

        send_email(f"📅 Weekly Trading Dashboard ({run_date})", dash, attachment_path=weekly_excel)

    print(
        f"✅ Postmarket complete | "
        f"premarket_eval={prem_s['evaluated']} mid_eval={mid_s['evaluated']} "
        f"combined_eval={all_s['evaluated']} win_rate={all_s['win_rate']:.2f}% | "
        f"excel={post_excel}"
    )


if __name__ == "__main__":
    run_postmarket()