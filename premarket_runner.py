# premarket_runner.py (FINAL LOCK VERSION)
from __future__ import annotations

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
    # outputs/prod/runs/YYYYMMDD/premarket/email_sent_YYYY-MM-DD.txt
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


def _normalize_run_date_col(df: pd.DataFrame, col: str = "run_date") -> pd.DataFrame:
    """Force YYYY-MM-DD strings to avoid mixed '2026-01-23 00:00:00' vs '2026-01-23' issues."""
    if df is None or df.empty or col not in df.columns:
        return df
    s = pd.to_datetime(df[col], errors="coerce")
    df[col] = s.dt.date.astype(str)
    return df


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
    "reasons", "news_flag", "main_news_title", "main_news_link",
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


# -----------------------------
# Excel writer
# -----------------------------
def write_premarket_excel(
    excel_path: str,
    *,
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
    # Prevent duplicate emails for same run_date
    marker = _premarket_email_marker(now)
    if marker.exists():
        print("📩 Premarket email already sent for this run_date — skipping resend.")
        return

    tickers = fetch_sp500_tickers()
    movers = calculate_top_movers(tickers, top_n=TOP_N)
    df = pd.DataFrame(movers)

    excel_path = out_path(
        f"premarket_{now.strftime('%Y%m%d')}_{make_run_id(now)}.xlsx",
        now=now, mode=mode, kind="runs"
    )

    if df.empty:
        write_premarket_excel(
            excel_path,
            picks_df=pd.DataFrame(),
            candidates_df=pd.DataFrame(),
            monitor_df=pd.DataFrame(),
            all_scored_df=pd.DataFrame(),
            raw_movers_df=df,
            rf=Path(rf),
        )
        send_email(f"🌅 Premarket Picks ({run_date})", "<p>No movers returned.</p>", attachment_path=excel_path)
        return

    df["pct_change"] = pd.to_numeric(df.get("pct_change"), errors="coerce").fillna(0.0)
    df["current"] = pd.to_numeric(df.get("current"), errors="coerce").fillna(0.0)

    snapshot = get_market_snapshot()
    market_trend = snapshot.get("trend", "up")

    rows: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
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

    if not picks_df.empty:
        tgt_fix, stp_fix = [], []
        for _, r in picks_df.iterrows():
            tgt, stp = _infer_intraday_target_stop(r)
            tgt_fix.append(tgt if tgt is not None else pd.NA)
            stp_fix.append(stp if stp is not None else pd.NA)
        picks_df["target_price"] = pd.to_numeric(pd.Series(tgt_fix), errors="coerce")
        picks_df["stop_loss"] = pd.to_numeric(pd.Series(stp_fix), errors="coerce")

    daily_cols = [
        "run_date", "mode", "symbol", "price_category",
        "current", "predicted_price", "target_price", "stop_loss",
        "forecast_trend", "forecast_atr", "forecast_reason",
        "trade_plan", "earnings_risk",
        "decision", "score", "score_label", "confidence",
        "reasons", "news_flag", "main_news_title", "main_news_link",
    ]

    picks_log = picks_df.copy()
    picks_log["run_date"] = run_date
    picks_log["mode"] = "premarket"

    # Ensure schema (without forcing "" into numeric columns yet)
    picks_log = picks_log.reindex(columns=daily_cols)

    for c in ["current", "predicted_price", "target_price", "stop_loss"]:
        if c in picks_log.columns:
            picks_log[c] = pd.to_numeric(picks_log[c], errors="coerce")

    # Final defense: ensure target/stop exist
    if not picks_log.empty:
        tgt_fix, stp_fix = [], []
        for _, r in picks_log.iterrows():
            tgt, stp = _infer_intraday_target_stop(r)
            tgt_fix.append(tgt if tgt is not None else pd.NA)
            stp_fix.append(stp if stp is not None else pd.NA)
        picks_log["target_price"] = pd.to_numeric(pd.Series(tgt_fix), errors="coerce")
        picks_log["stop_loss"] = pd.to_numeric(pd.Series(stp_fix), errors="coerce")

    # Normalize run_date (prevents mixed formats)
    picks_log = _normalize_run_date_col(picks_log, "run_date")

    # Load existing (stable schema)
    existing = (
        pd.read_csv(DAILY_LOG_CSV)
        if (os.path.exists(DAILY_LOG_CSV) and os.path.getsize(DAILY_LOG_CSV) > 0)
        else pd.DataFrame(columns=daily_cols)
    )
    existing = existing.reindex(columns=daily_cols)
    existing = _normalize_run_date_col(existing, "run_date")

    # Normalize keys (strings), keep symbol clean
    for k in ["run_date", "mode", "symbol"]:
        existing[k] = existing[k].astype(str)
        picks_log[k] = picks_log[k].astype(str)
    picks_log["symbol"] = picks_log["symbol"].str.upper().str.strip()

    # Concat only non-empty frames (reduces FutureWarning)
    frames = [f for f in [existing, picks_log] if f is not None and not f.empty]
    merged = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=daily_cols)

    if not merged.empty:
        merged = merged.drop_duplicates(subset=["run_date", "mode", "symbol"], keep="last")

    merged.to_csv(DAILY_LOG_CSV, index=False)

    # RECO log
    append_recommendations_log(candidates_df, now, mode="premarket")

    # Portfolio open add (best-effort)
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
        picks_df=picks_df,
        candidates_df=candidates_df,
        monitor_df=monitor_df,
        all_scored_df=all_scored_df,
        raw_movers_df=df,
        rf=Path(rf),
    )

    # Email
    if picks_df.empty:
        html = f"""
        <h2>🌅 Premarket Picks ({run_date})</h2>
        <p>No picks passed filters.</p>
        <p><b>Monitor Mode:</b> See Excel sheets <b>MONITOR_TOP20</b> and <b>ALL_SCORED</b>.</p>
        <p><b>Attachment:</b> Excel included.</p>
        """
        if send_email(f"🌅 Premarket Picks ({run_date})", html, attachment_path=excel_path):
            marker.write_text("sent\n", encoding="utf-8")
        return

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
        dec = _html.escape(str(rr.get("decision", "")))
        title = _html.escape(str(rr.get("main_news_title") or ""))
        link = str(rr.get("main_news_link") or "").strip() or "#"
        reasons = _html.escape(str(rr.get("reasons") or ""))[:600]
        return f"""
        <tr>
          <td><b>{sym}</b></td>
          <td>{cur}</td>
          <td>{tgt}</td>
          <td>{stp}</td>
          <td>{score}</td>
          <td>{conf}</td>
          <td>{dec}</td>
          <td><a href="{link}" target="_blank">{title}</a></td>
          <td style="color:#444;">{reasons}</td>
        </tr>
        """

    rows_html = "\n".join([row_html(rr) for _, rr in picks_df.iterrows()])

    html = f"""
    <h2>🌅 Premarket Picks ({run_date})</h2>
    <p>
      <b>Market trend:</b> {_html.escape(str(snapshot.get("trend")))} |
      <b>SPY gap:</b> {snapshot.get("spy_gap_pct", 0.0):.2f}% |
      <b>VIX:</b> {snapshot.get("vix")}
    </p>
    <p>Filters: confidence ≥ {MIN_CONFIDENCE_TO_TRADE}, price ≤ ${MAX_PRICE} (elite override allowed).</p>
    <p><b>Win rule (postmarket):</b> target hit anytime after recommendation (intraday high ≥ target).</p>

    <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;font-family:Arial;font-size:13px;">
      <tr style="background:#eee;">
        <th>Symbol</th><th>Price</th><th>Target</th><th>Stop</th><th>Score</th><th>Conf</th><th>Decision</th><th>Headline</th><th>Reasons</th>
      </tr>
      {rows_html}
    </table>

    <p><b>Attachment:</b> Excel included with sheets: PICKS, CANDIDATES, MONITOR_TOP20, ALL_SCORED, RAW_MOVERS.</p>
    """
    if send_email(f"🌅 Premarket Picks ({run_date})", html, attachment_path=excel_path):
        marker.write_text("sent\n", encoding="utf-8")
    print(f"✅ Premarket complete | picks={len(picks_df)} | excel={excel_path}")


if __name__ == "__main__":
    run_premarket()