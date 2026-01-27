# midday_runner.py (FINAL - robust target/stop sanity + safe midday scaling + debug artifacts)
from __future__ import annotations

import os
import re
import html as _html
from datetime import datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import List, Optional

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
    SCORE_HIGH,
    SCORE_MEDIUM,
    MAX_PRICE,
    ELITE_SCORE_OVERRIDE,
    ELITE_CONF_OVERRIDE,
)

from email_sender import send_email as _send_email
from top_movers import fetch_sp500_tickers, calculate_top_movers
from scoring_engine import get_predictive_score_with_reasons
from news_fetcher import fetch_news_links
from forecast_engine import forecast_price_levels  # pred/target/stop

LOCAL_TZ = ZoneInfo("America/Chicago")

# Market session times (Central)
SESSION_START = time(8, 30)   # 8:30am CT
SESSION_END   = time(15, 0)   # 3:00pm CT

# Debug artifacts (set to "1" to enable)
MIDDAY_DEBUG = os.getenv("MIDDAY_DEBUG", "0") == "1"

# Market words
POS_WORDS = {"beat", "strong", "growth", "surge", "upgrade", "raises", "record", "profit", "wins", "bull"}
NEG_WORDS = {"miss", "drop", "loss", "cuts", "downgrade", "falls", "weak", "lawsuit", "plunge", "bear"}

SUDDEN_MOVER_PCT_THRESHOLD = float(os.getenv("SUDDEN_MOVER_PCT_THRESHOLD", "2.0"))
MIDDAY_MIN_CONFIDENCE = int(os.getenv("MIDDAY_MIN_CONFIDENCE", "5"))

# Guardrails for forecast sanity (long-only)
FORECAST_TARGET_MAX_MULT = float(os.getenv("FORECAST_TARGET_MAX_MULT", "1.20"))  # target <= 20% above entry
FORECAST_STOP_MIN_MULT   = float(os.getenv("FORECAST_STOP_MIN_MULT", "0.80"))    # stop  >= 80% of entry


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
# Excel styling
# -----------------------------
def normalize_color(color: str) -> str:
    if not color:
        color = "#FFFFFF"
    color = color.lstrip("#")
    if len(color) == 6:
        color = "FF" + color
    return color.upper()


def style_excel_sheet(sheet) -> None:
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


def write_midday_excel(
    excel_path: str,
    *,
    pass_df: pd.DataFrame,
    all_df: pd.DataFrame,
    threshold_df: pd.DataFrame,
    rf: Path,
) -> None:
    try:
        p = Path(excel_path)
        p.parent.mkdir(parents=True, exist_ok=True)

        with pd.ExcelWriter(excel_path, engine="openpyxl") as xw:
            (pass_df if pass_df is not None else pd.DataFrame()).to_excel(xw, sheet_name="PASS", index=False)
            (all_df if all_df is not None else pd.DataFrame()).to_excel(xw, sheet_name="ALL_CANDIDATES", index=False)
            (threshold_df if threshold_df is not None else pd.DataFrame()).to_excel(xw, sheet_name="AFTER_THRESHOLD", index=False)

        wb = load_workbook(excel_path)
        for s in wb.sheetnames:
            style_excel_sheet(wb[s])
        wb.save(excel_path)

    except Exception as e:
        (Path(rf) / "midday_excel_error.txt").write_text(repr(e), encoding="utf-8")


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
    base = round((score_val / 100.0) * 10.0)  # 0..10

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


def _default_target_stop(entry: float, conf: int) -> tuple[float, float]:
    conf = int(conf) if conf is not None else 5
    tgt_pct = 0.015 if conf >= 7 else (0.012 if conf >= 6 else 0.01)
    stp_pct = 0.012 if conf >= 7 else (0.015 if conf >= 6 else 0.02)
    return entry * (1.0 + tgt_pct), entry * (1.0 - stp_pct)


def _sanitize_forecast_levels(entry: float, conf: int, pred: Optional[float], tgt: Optional[float], stp: Optional[float]) -> tuple[Optional[float], float, float]:
    if entry is None or entry <= 0:
        return pred, tgt if tgt is not None else pd.NA, stp if stp is not None else pd.NA

    if tgt is not None and tgt <= entry:
        tgt = None
    if stp is not None and stp >= entry:
        stp = None

    if tgt is not None and tgt > entry * FORECAST_TARGET_MAX_MULT:
        tgt = None
    if stp is not None and stp < entry * FORECAST_STOP_MIN_MULT:
        stp = None

    if tgt is None or stp is None:
        dt, ds = _default_target_stop(entry, conf)
        if tgt is None:
            tgt = dt
        if stp is None:
            stp = ds

    return pred, float(tgt), float(stp)


def _time_scaled_target_stop(entry: float, base_target: float, base_stop: float, now: datetime) -> tuple[float, float]:
    if entry <= 0:
        return base_target, base_stop

    sess_end_dt = datetime.combine(now.date(), SESSION_END, tzinfo=LOCAL_TZ)
    sess_start_dt = datetime.combine(now.date(), SESSION_START, tzinfo=LOCAL_TZ)

    minutes_left = max(0.0, (sess_end_dt - now).total_seconds() / 60.0)
    total_minutes = max(1.0, (sess_end_dt - sess_start_dt).total_seconds() / 60.0)
    frac_left = min(1.0, minutes_left / total_minutes)

    tgt_pct = max(0.0, (base_target / entry) - 1.0)
    stp_pct = max(0.0, 1.0 - (base_stop / entry)) if base_stop and base_stop > 0 else 0.0

    tgt_pct_scaled = tgt_pct * frac_left
    stp_pct_scaled = stp_pct * frac_left

    tgt_pct_scaled = min(max(tgt_pct_scaled, 0.005), 0.04)
    stp_pct_scaled = min(max(stp_pct_scaled, 0.004), 0.03)

    new_target = entry * (1.0 + tgt_pct_scaled)
    new_stop   = entry * (1.0 - stp_pct_scaled)
    return float(new_target), float(new_stop)


def _infer_intraday_target_stop(row: pd.Series) -> tuple[Optional[float], Optional[float]]:
    entry = _safe_float(row.get("current") if "current" in row else row.get("entry_price"))
    tgt = _safe_float(row.get("target_price"))
    stp = _safe_float(row.get("stop_loss"))

    if tgt is None:
        tgt = _safe_float(row.get("predicted_price"))

    if entry is not None and entry > 0:
        if tgt is not None and tgt <= entry:
            tgt = None
        if stp is not None and stp >= entry:
            stp = None

        conf = int(pd.to_numeric(row.get("confidence"), errors="coerce") or 5)
        if tgt is None or stp is None:
            dt, ds = _default_target_stop(entry, conf)
            if tgt is None:
                tgt = dt
            if stp is None:
                stp = ds

    return tgt, stp


def build_midday_alert(df: pd.DataFrame, run_date: str) -> str:
    if df is None or df.empty:
        return f"<h2>⚡ Midday Sudden Movers ({run_date})</h2><p>No candidates passed filters.</p>"

    cols = [
        "symbol", "current", "pct_change",
        "predicted_price", "target_price", "stop_loss",
        "score", "confidence", "decision",
        "main_news_title", "main_news_link", "reasons",
    ]
    df2 = df.copy()
    for c in cols:
        if c not in df2.columns:
            df2[c] = ""

    def _fmt_money(x):
        try:
            if pd.isna(x):
                return ""
            return f"{float(x):.2f}"
        except Exception:
            return ""

    def _fmt_pct(x):
        try:
            if pd.isna(x):
                return ""
            return f"{float(x):.2f}%"
        except Exception:
            return ""

    def row_html(r):
        sym = _html.escape(str(r["symbol"]))
        cur = _fmt_money(r["current"])
        pct = _fmt_pct(r["pct_change"])

        pred = _fmt_money(r.get("predicted_price"))
        tgt = _fmt_money(r.get("target_price"))
        stp = _fmt_money(r.get("stop_loss"))

        score = str(r["score"])
        conf = str(r["confidence"])
        dec = _html.escape(str(r["decision"]))
        title = _html.escape(str(r["main_news_title"] or ""))
        link = str(r["main_news_link"] or "").strip() or "#"
        reasons = _html.escape(str(r["reasons"] or ""))[:600]
        return f"""
        <tr>
          <td><b>{sym}</b></td>
          <td>{cur}</td>
          <td>{pct}</td>
          <td>{pred}</td>
          <td>{tgt}</td>
          <td>{stp}</td>
          <td>{score}</td>
          <td>{conf}</td>
          <td>{dec}</td>
          <td><a href="{link}" target="_blank">{title}</a></td>
          <td style="color:#444;">{reasons}</td>
        </tr>
        """

    rows = "\n".join([row_html(r) for _, r in df2.iterrows()])

    return f"""
    <h2>⚡ Midday Sudden Movers ({run_date})</h2>
    <p>Filters: abs(move)≥{SUDDEN_MOVER_PCT_THRESHOLD}%, conf≥{MIDDAY_MIN_CONFIDENCE}, price≤${MAX_PRICE} (elite override allowed).</p>
    <p><b>Win rule (postmarket):</b> target hit anytime after recommendation (intraday high ≥ target).</p>
    <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;font-family:Arial;font-size:13px;">
      <tr style="background:#eee;">
        <th>Symbol</th><th>Price</th><th>%</th><th>Pred</th><th>Target</th><th>Stop</th>
        <th>Score</th><th>Conf</th><th>Decision</th><th>Headline</th><th>Reasons</th>
      </tr>
      {rows}
    </table>
    """


# -----------------------------
# Logs
# -----------------------------
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

    for c in ["predicted_price", "target_price", "stop_loss"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    out = out[cols_keep]
    file_exists = os.path.exists(RECO_LOG_CSV) and os.path.getsize(RECO_LOG_CSV) > 0
    out.to_csv(RECO_LOG_CSV, mode="a", header=not file_exists, index=False)


# -----------------------------
# DAILY Picks log (for postmarket evaluation)
# -----------------------------
DAILY_LOG_CSV = out_path("daily_stock_log.csv", kind="logs")
ensure_csv_exists(DAILY_LOG_CSV, [
    "run_date", "mode", "symbol", "price_category",
    "current", "predicted_price", "target_price", "stop_loss",
    "forecast_trend", "forecast_atr", "forecast_reason",
    "trade_plan", "earnings_risk",
    "decision", "score", "score_label", "confidence",
    "reasons", "news_flag", "main_news_title", "main_news_link",
])


# -----------------------------
# Midday runner
# -----------------------------
def run_midday(now: datetime | None = None) -> None:
    now = now or datetime.now(LOCAL_TZ)

    # ✅ Guard: only run during market session window
    # Skip only in non-local environments (prod / DigitalOcean)
    if now.time() < SESSION_START and not IS_LOCAL:
        print("⛔ Midday runner skipped: market not open yet.")
        return

    if now.time() >= SESSION_END and not IS_LOCAL:
        print("⛔ Midday runner skipped: market already closed.")
        return


    mode = "midday"
    rf = run_dir(now, mode)
    run_date = now.strftime("%Y-%m-%d")

    empty_note = Path(rf) / "empty_midday.txt"

    tickers = fetch_sp500_tickers()
    movers = calculate_top_movers(tickers, top_n=TOP_N)
    raw_df = pd.DataFrame(movers)

    excel_path = out_path(
        f"midday_{now.strftime('%Y%m%d')}_{make_run_id(now)}.xlsx",
        now=now, mode=mode, kind="runs"
    )

    if raw_df.empty or "pct_change" not in raw_df.columns:
        raw_df.to_csv(Path(rf) / "midday_raw_movers.csv", index=False)
        write_midday_excel(
            excel_path,
            pass_df=pd.DataFrame(),
            all_df=pd.DataFrame(),
            threshold_df=pd.DataFrame(),
            rf=Path(rf),
        )
        html = f"<h2>⚡ Midday Sudden Movers ({run_date})</h2><p>No movers returned.</p>"
        send_email(f"⚡ Sudden Movers Alert ({run_date})", html, attachment_path=excel_path)
        return

    raw_df["pct_change"] = pd.to_numeric(raw_df["pct_change"], errors="coerce").fillna(0.0)
    raw_df["current"] = pd.to_numeric(raw_df.get("current"), errors="coerce").fillna(10**9)

    thr_df = raw_df[raw_df["pct_change"].abs() >= SUDDEN_MOVER_PCT_THRESHOLD].copy()
    thr_df.to_csv(Path(rf) / "midday_after_threshold.csv", index=False)

    if thr_df.empty:
        empty_note.write_text(
            f"No movers exceeded threshold {SUDDEN_MOVER_PCT_THRESHOLD}%. raw_movers={len(raw_df)}",
            encoding="utf-8",
        )
        write_midday_excel(
            excel_path,
            pass_df=pd.DataFrame(),
            all_df=pd.DataFrame(),
            threshold_df=thr_df,
            rf=Path(rf),
        )
        html = f"""
        <h2>⚡ Midday Sudden Movers ({run_date})</h2>
        <p>No movers exceeded threshold {SUDDEN_MOVER_PCT_THRESHOLD}%.</p>
        <p><b>Attachment:</b> Excel included with sheets: PASS (empty), ALL_CANDIDATES, AFTER_THRESHOLD.</p>
        """
        send_email(f"⚡ Sudden Movers Alert ({run_date})", html, attachment_path=excel_path)
        return

    snapshot = get_market_snapshot()
    market_trend = snapshot.get("trend", "up")

    scores, labels, reasons_list, confs, decisions = [], [], [], [], []
    titles, links, flags = [], [], []
    preds, tgts, stps, ftrends, fatrs = [], [], [], [], []

    dbg_rows = []

    for _, row in thr_df.iterrows():
        sym = str(row.get("symbol", "")).upper().strip()
        current = float(row.get("current", 0.0) or 0.0)
        pct_change = float(row.get("pct_change", 0.0) or 0.0)

        score_val, score_label, reasons = get_predictive_score_with_reasons(sym)
        score_val = int(score_val)
        decision = map_score_to_decision(score_val)

        news_items = fetch_news_links(sym, max_articles=1)
        main_item = news_items[0] if news_items else ""
        title = extract_headline_from_html(main_item)
        link = extract_url_from_html(main_item)
        flag = news_flag_from_headlines([title])

        conf = compute_confidence(score_val, pct_change, market_trend, flag)

        f = forecast_price_levels(sym, current=current, score=score_val)

        raw_pred = _safe_float(getattr(f, "predicted_price", None))
        raw_tgt = _safe_float(getattr(f, "target_price", None))
        raw_stp = _safe_float(getattr(f, "stop_loss", None))

        pred_s, tgt_s, stp_s = _sanitize_forecast_levels(current, int(conf), raw_pred, raw_tgt, raw_stp)

        scores.append(score_val)
        labels.append(score_label)
        reasons_list.append(reasons)
        decisions.append(decision)
        confs.append(int(conf))
        titles.append(title)
        links.append(link)
        flags.append(flag)

        preds.append(pred_s if pred_s is not None else pd.NA)
        tgts.append(tgt_s if tgt_s is not None else pd.NA)
        stps.append(stp_s if stp_s is not None else pd.NA)
        ftrends.append(str(getattr(f, "trend", "")))
        fatrs.append(float(getattr(f, "atr", pd.NA)) if getattr(f, "atr", None) is not None else pd.NA)

        if MIDDAY_DEBUG:
            dbg_rows.append({
                "symbol": sym,
                "current": current,
                "pct_change": pct_change,
                "confidence": int(conf),
                "raw_target": raw_tgt,
                "raw_stop": raw_stp,
                "san_target": tgt_s,
                "san_stop": stp_s,
            })

    all_df = thr_df.copy()
    all_df["score"] = scores
    all_df["score_label"] = labels
    all_df["reasons"] = reasons_list
    all_df["decision"] = decisions
    all_df["confidence"] = confs
    all_df["main_news_title"] = titles
    all_df["main_news_link"] = links
    all_df["news_flag"] = flags

    all_df["predicted_price"] = pd.to_numeric(pd.Series(preds, index=all_df.index), errors="coerce")
    all_df["target_price"] = pd.to_numeric(pd.Series(tgts, index=all_df.index), errors="coerce")
    all_df["stop_loss"] = pd.to_numeric(pd.Series(stps, index=all_df.index), errors="coerce")
    all_df["forecast_trend"] = ftrends
    all_df["forecast_atr"] = fatrs

    all_df.sort_values(by=["confidence", "score"], ascending=False).to_csv(Path(rf) / "midday_candidates_all.csv", index=False)

    pass_df = all_df[all_df["confidence"] >= MIDDAY_MIN_CONFIDENCE].copy()
    pass_df = pass_df[
        (pass_df["current"] <= MAX_PRICE) |
        ((pass_df["score"] >= ELITE_SCORE_OVERRIDE) & (pass_df["confidence"] >= ELITE_CONF_OVERRIDE))
    ].copy()
    pass_df = pass_df.sort_values(by=["confidence", "score"], ascending=False)
    pass_df.to_csv(Path(rf) / "midday_candidates_pass.csv", index=False)

    # -----------------------------
    # Midday target/stop scaling
    # -----------------------------
    if not pass_df.empty:
        tgt_scaled, stp_scaled = [], []
        for _, r in pass_df.iterrows():
            entry = _safe_float(r.get("current"))
            base_tgt = _safe_float(r.get("target_price"))
            base_stp = _safe_float(r.get("stop_loss"))

            if entry is not None and entry > 0 and base_tgt is not None and base_stp is not None:
                new_tgt, new_stp = _time_scaled_target_stop(entry, base_tgt, base_stp, now)
                tgt_scaled.append(new_tgt)
                stp_scaled.append(new_stp)
            else:
                tgt_scaled.append(base_tgt if base_tgt is not None else pd.NA)
                stp_scaled.append(base_stp if base_stp is not None else pd.NA)

        # ✅ FIX: align by index to prevent symbol-mixups
        pass_df["target_price"] = pd.to_numeric(pd.Series(tgt_scaled, index=pass_df.index), errors="coerce")
        pass_df["stop_loss"] = pd.to_numeric(pd.Series(stp_scaled, index=pass_df.index), errors="coerce")

    # Hard sanity check before sending/logging
    if not pass_df.empty:
        cur_s = pd.to_numeric(pass_df["current"], errors="coerce")
        tgt_s = pd.to_numeric(pass_df["target_price"], errors="coerce")
        stp_s = pd.to_numeric(pass_df["stop_loss"], errors="coerce")
        bad = pass_df[(tgt_s <= cur_s) | (stp_s >= cur_s)]
        if not bad.empty:
            bad.to_csv(Path(rf) / "bad_targets.csv", index=False)

    if MIDDAY_DEBUG and dbg_rows:
        pd.DataFrame(dbg_rows).to_csv(Path(rf) / "debug_forecast_sanity.csv", index=False)

    append_recommendations_log(pass_df, now, mode="midday")

    # DAILY log (PASS only)
    daily_cols = [
        "run_date", "mode", "symbol", "price_category",
        "current", "predicted_price", "target_price", "stop_loss",
        "forecast_trend", "forecast_atr", "forecast_reason",
        "trade_plan", "earnings_risk",
        "decision", "score", "score_label", "confidence",
        "reasons", "news_flag", "main_news_title", "main_news_link",
    ]

    picks_log = pass_df.copy()
    picks_log["run_date"] = run_date
    picks_log["mode"] = "midday"

    tgt_fix, stp_fix = [], []
    for _, r in picks_log.iterrows():
        tgt, stp = _infer_intraday_target_stop(r)
        tgt_fix.append(tgt if tgt is not None else pd.NA)
        stp_fix.append(stp if stp is not None else pd.NA)

    # ✅ FIX: align by index to prevent symbol-mixups
    picks_log["target_price"] = pd.to_numeric(pd.Series(tgt_fix, index=picks_log.index), errors="coerce")
    picks_log["stop_loss"] = pd.to_numeric(pd.Series(stp_fix, index=picks_log.index), errors="coerce")

    if "price_category" not in picks_log.columns:
        picks_log["price_category"] = ""
    if "forecast_reason" not in picks_log.columns:
        picks_log["forecast_reason"] = ""
    if "trade_plan" not in picks_log.columns:
        picks_log["trade_plan"] = "Midday sudden mover; target-hit win tracking uses intraday high vs target."
    if "earnings_risk" not in picks_log.columns:
        picks_log["earnings_risk"] = ""

    for c in daily_cols:
        if c not in picks_log.columns:
            picks_log[c] = ""

    picks_log = picks_log[daily_cols]

    existing = (
        pd.read_csv(DAILY_LOG_CSV)
        if (os.path.exists(DAILY_LOG_CSV) and os.path.getsize(DAILY_LOG_CSV) > 0)
        else pd.DataFrame(columns=daily_cols)
    )

    if "mode" not in existing.columns:
        existing["mode"] = ""

    merged = pd.concat([existing, picks_log], ignore_index=True)
    merged = merged.drop_duplicates(subset=["run_date", "mode", "symbol"], keep="last")
    merged.to_csv(DAILY_LOG_CSV, index=False)

    write_midday_excel(
        excel_path,
        pass_df=pass_df,
        all_df=all_df,
        threshold_df=thr_df,
        rf=Path(rf),
    )

    if pass_df.empty:
        empty_note.write_text(
            f"No candidates passed gates. conf>={MIDDAY_MIN_CONFIDENCE}, price<=${MAX_PRICE} unless elite.",
            encoding="utf-8",
        )
        html = f"""
        <h2>⚡ Midday Sudden Movers ({run_date})</h2>
        <p>No candidates passed gates.</p>
        <p><b>Attachment:</b> Excel included with sheets: PASS (empty), ALL_CANDIDATES, AFTER_THRESHOLD.</p>
        """
        send_email(f"⚡ Sudden Movers Alert ({run_date})", html, attachment_path=excel_path)
        return

    html = build_midday_alert(pass_df, run_date)
    send_email(f"⚡ Sudden Movers Alert ({run_date})", html, attachment_path=excel_path)
    print(f"✅ Midday complete | sent={len(pass_df)} | threshold_rows={len(thr_df)}")


if __name__ == "__main__":
    run_midday()