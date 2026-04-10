from pathlib import Path
from datetime import datetime
import io
import base64

import pandas as pd
import yfinance as yf
from flask import Flask, request, render_template_string

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

app = Flask(__name__)

# --------------------------------------------------
# Paths
# --------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent

DAILY_LOG_CANDIDATES = [
    BASE_DIR / "daily_stock_log.csv",
    BASE_DIR / "outputs" / "local" / "logs" / "daily_stock_log.csv",
    BASE_DIR / "outputs" / "prod" / "logs" / "daily_stock_log.csv",
]

PERF_LOG_CANDIDATES = [
    BASE_DIR / "outputs" / "local" / "logs" / "performance_log.csv",
    BASE_DIR / "outputs" / "prod" / "logs" / "performance_log.csv",
]


# --------------------------------------------------
# Generic helpers
# --------------------------------------------------
def find_existing_file(candidates):
    for path in candidates:
        if path.exists():
            return path
    return None


def safe_float(val):
    try:
        if pd.isna(val):
            return None
        return float(val)
    except Exception:
        return None


def fmt_money(val, fallback="Pending"):
    num = safe_float(val)
    if num is None:
        return fallback
    return f"${num:,.2f}"


def fmt_pct(val, fallback="Pending"):
    num = safe_float(val)
    if num is None:
        return fallback
    return f"{num:.2f}%"


def fmt_plain(val, fallback="Pending"):
    if val is None:
        return fallback
    try:
        if pd.isna(val):
            return fallback
    except Exception:
        pass
    text = str(val).strip()
    if not text or text.lower() in {"nan", "none"}:
        return fallback
    return text


def stance_badge_class(stance: str) -> str:
    s = (stance or "").strip().upper()
    if "GO" in s or "BUY" in s or "STRONG BUY" in s:
        return "badge-go"
    if "WATCH" in s or "HOLD" in s:
        return "badge-watch"
    if "SELL" in s or "AVOID" in s or "STOP" in s:
        return "badge-stop"
    return "badge-neutral"


def calc_distance_pct(current_val, level_val):
    current_num = safe_float(current_val)
    level_num = safe_float(level_val)
    if current_num is None or level_num is None or current_num == 0:
        return "Pending"
    pct = ((level_num - current_num) / current_num) * 100.0
    return f"{pct:.2f}%"


# --------------------------------------------------
# Data loading
# --------------------------------------------------
def load_daily_data():
    path = find_existing_file(DAILY_LOG_CANDIDATES)
    if not path:
        return pd.DataFrame(), "Not found"

    try:
        df = pd.read_csv(path)
        if df.empty:
            return pd.DataFrame(), str(path)

        if "predicted_price" in df.columns and "entry_price" not in df.columns:
            df["entry_price"] = df["predicted_price"]

        if "current" not in df.columns and "entry_price" in df.columns:
            df["current"] = df["entry_price"]

        if "decision" in df.columns and "stance" not in df.columns:
            df["stance"] = df["decision"]

        if "mode" in df.columns and "source_mode" not in df.columns:
            df["source_mode"] = df["mode"]

        if "run_date" not in df.columns:
            df["run_date"] = "latest"

        if "symbol" in df.columns:
            df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()

        if "source_mode" in df.columns:
            df["source_mode"] = df["source_mode"].astype(str).str.lower().str.strip()

        sort_cols = [c for c in ["confidence", "score"] if c in df.columns]
        if sort_cols:
            df = df.sort_values(by=sort_cols, ascending=False)

        return df, str(path)
    except Exception as e:
        return pd.DataFrame(), f"{path} (failed to load: {e})"


def load_perf_data():
    path = find_existing_file(PERF_LOG_CANDIDATES)
    if not path:
        return pd.DataFrame(), "Not found"

    try:
        df = pd.read_csv(path)
        if df.empty:
            return pd.DataFrame(), str(path)

        if "run_date" in df.columns:
            df["run_date"] = pd.to_datetime(df["run_date"], errors="coerce")

        return df, str(path)
    except Exception as e:
        return pd.DataFrame(), f"{path} (failed to load: {e})"


# --------------------------------------------------
# Daily filtering helpers
# --------------------------------------------------
def get_available_dates(df: pd.DataFrame):
    if df.empty or "run_date" not in df.columns:
        return []
    vals = df["run_date"].dropna().astype(str).unique().tolist()
    return sorted(vals, reverse=True)


def filter_daily_df(df: pd.DataFrame, selected_date: str, selected_mode: str):
    out = df.copy()

    if not out.empty and selected_date != "all" and "run_date" in out.columns:
        out = out[out["run_date"].astype(str) == selected_date].copy()

    if not out.empty and selected_mode != "all" and "source_mode" in out.columns:
        out = out[out["source_mode"].astype(str).str.lower() == selected_mode].copy()

    return out


def get_selected_row(df: pd.DataFrame, symbol: str | None):
    if df.empty:
        return None

    if symbol:
        match = df[df["symbol"].astype(str).str.upper() == symbol.upper()]
        if not match.empty:
            return match.iloc[0]

    return df.iloc[0]


# --------------------------------------------------
# Performance helpers
# --------------------------------------------------
def filter_perf_by_window(perf_df: pd.DataFrame, history_window: str):
    if perf_df.empty or "run_date" not in perf_df.columns:
        return perf_df.copy()

    out = perf_df.copy()
    out = out.dropna(subset=["run_date"]).copy()
    if out.empty:
        return out

    if history_window == "all":
        return out

    max_dt = out["run_date"].max()
    if pd.isna(max_dt):
        return out

    if history_window == "today":
        start = max_dt.normalize()
    elif history_window == "7d":
        start = max_dt.normalize() - pd.Timedelta(days=6)
    elif history_window == "30d":
        start = max_dt.normalize() - pd.Timedelta(days=29)
    else:
        return out

    return out[out["run_date"] >= start].copy()


def summarize_performance(perf_df: pd.DataFrame):
    if perf_df.empty or "outcome" not in perf_df.columns:
        return {
            "evaluated": "0",
            "wins": "0",
            "losses": "0",
            "not_hit": "0",
            "win_rate": "Pending",
            "wins_int": 0,
            "losses_int": 0,
            "not_hit_int": 0,
        }

    eval_df = perf_df[perf_df["outcome"].isin(["🏆 Target Hit", "🛑 Stop Hit", "⏳ Not Hit"])].copy()

    total = len(eval_df)
    wins = int((eval_df["outcome"] == "🏆 Target Hit").sum()) if total else 0
    losses = int((eval_df["outcome"] == "🛑 Stop Hit").sum()) if total else 0
    not_hit = int((eval_df["outcome"] == "⏳ Not Hit").sum()) if total else 0
    win_rate = f"{(wins / total * 100):.2f}%" if total else "Pending"

    return {
        "evaluated": str(total),
        "wins": str(wins),
        "losses": str(losses),
        "not_hit": str(not_hit),
        "win_rate": win_rate,
        "wins_int": wins,
        "losses_int": losses,
        "not_hit_int": not_hit,
    }


def build_trend_chart_data(perf_df: pd.DataFrame):
    trend_chart_data = {"labels": [], "wins": [], "losses": [], "not_hit": []}

    if not perf_df.empty and "run_date" in perf_df.columns and "outcome" in perf_df.columns:
        df = perf_df.copy()
        df = df[df["outcome"].isin(["🏆 Target Hit", "🛑 Stop Hit", "⏳ Not Hit"])].copy()
        if not df.empty:
            df["day"] = df["run_date"].dt.strftime("%Y-%m-%d")
            grouped = df.groupby("day")["outcome"].value_counts().unstack(fill_value=0)
            trend_chart_data = {
                "labels": grouped.index.tolist(),
                "wins": grouped.get("🏆 Target Hit", pd.Series([0] * len(grouped), index=grouped.index)).tolist(),
                "losses": grouped.get("🛑 Stop Hit", pd.Series([0] * len(grouped), index=grouped.index)).tolist(),
                "not_hit": grouped.get("⏳ Not Hit", pd.Series([0] * len(grouped), index=grouped.index)).tolist(),
            }

    return trend_chart_data


# --------------------------------------------------
# AI panel helpers
# --------------------------------------------------
def build_ai_engine_summary(df: pd.DataFrame, selected_row):
    if df.empty or selected_row is None:
        return {
            "market_regime": "Pending",
            "top_symbol": "Pending",
            "confidence": "Pending",
            "score": "Pending",
            "insight": "No signal data loaded yet.",
            "risk_note": "Waiting for bot output.",
        }

    insight = (
        selected_row.get("key_insight")
        or selected_row.get("llm_insights")
        or selected_row.get("stance_reason")
        or "No AI insight generated for this row yet."
    )

    risk_note = "Signals loaded from your current bot outputs."
    if "risk" in selected_row.index:
        risk_note = f"Risk: {fmt_plain(selected_row.get('risk'))}"

    return {
        "market_regime": fmt_plain(selected_row.get("market_regime")),
        "top_symbol": fmt_plain(selected_row.get("symbol")),
        "confidence": fmt_plain(selected_row.get("confidence")),
        "score": fmt_plain(selected_row.get("score")),
        "insight": fmt_plain(insight, "No AI insight generated for this row yet."),
        "risk_note": fmt_plain(risk_note),
    }


def build_ai_persona_messages(selected_row):
    if selected_row is None:
        return [
            "Waiting for bot output...",
            "No active signal loaded yet.",
            "Stand by for market analysis.",
        ]

    top_symbol = fmt_plain(selected_row.get("symbol"), "Unknown")
    regime = fmt_plain(selected_row.get("market_regime"), "Unavailable")
    confidence = fmt_plain(selected_row.get("confidence"), "Pending")
    score = fmt_plain(selected_row.get("score"), "Pending")
    risk = fmt_plain(selected_row.get("risk"), "Unknown")
    headline = fmt_plain(selected_row.get("main_news_title"), "No linked headline yet.")
    insight = fmt_plain(
        selected_row.get("key_insight") or selected_row.get("llm_insights"),
        "No AI insight generated for this row yet.",
    )

    return [
        f"Scanning current signal for {top_symbol}.",
        f"Market regime status: {regime}.",
        f"Confidence reading: {confidence}. Score reading: {score}.",
        f"Risk monitor status: {risk}.",
        f"Headline focus: {headline}",
        f"Insight: {insight}",
    ]


# --------------------------------------------------
# Live overlay helpers
# --------------------------------------------------
def fetch_live_snapshot(symbol: str):
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="1d", interval="5m", auto_adjust=False)

        if hist is None or hist.empty:
            return {"live_price": None, "as_of": "Unavailable"}

        close_col = hist["Close"]
        if isinstance(close_col, pd.DataFrame):
            close_col = close_col.iloc[:, 0]

        latest_price = float(close_col.dropna().iloc[-1]) if not close_col.dropna().empty else None

        return {
            "live_price": latest_price,
            "as_of": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    except Exception:
        return {"live_price": None, "as_of": "Unavailable"}


def get_candlestick_data(symbol: str):
    try:
        df = yf.download(
            symbol,
            period="5d",
            interval="30m",
            auto_adjust=False,
            progress=False,
            threads=False,
        )

        if df is None or df.empty:
            return []

        if hasattr(df.columns, "levels"):
            cleaned = pd.DataFrame()
            for col in ["Open", "High", "Low", "Close"]:
                if (col, symbol) in df.columns:
                    cleaned[col] = df[(col, symbol)]
                elif col in df.columns:
                    cleaned[col] = df[col]
            df = cleaned

        needed = ["Open", "High", "Low", "Close"]
        if not all(col in df.columns for col in needed):
            return []

        df = df.dropna(subset=needed).copy()
        if df.empty:
            return []

        candles = []
        for idx, row in df.iterrows():
            candles.append({
                "time": pd.Timestamp(idx).strftime("%Y-%m-%d %H:%M:%S"),
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"]),
            })

        return candles

    except Exception as e:
        print("candle error:", e)
        return []

def generate_stock_chart(symbol: str, entry=None, target=None, stop=None):
    """
    Reliable server-side stock chart with stronger visibility and safer label spacing.
    """
    attempts = [
        {"period": "1d", "interval": "5m"},
        {"period": "5d", "interval": "30m"},
        {"period": "1mo", "interval": "1d"},
        {"period": "3mo", "interval": "1d"},
    ]

    for cfg in attempts:
        try:
            print(f"[chart] trying {symbol} period={cfg['period']} interval={cfg['interval']}")

            df = yf.download(
                symbol,
                period=cfg["period"],
                interval=cfg["interval"],
                auto_adjust=False,
                progress=False,
                threads=False,
            )

            if df is None or df.empty:
                continue

            if isinstance(df.columns, pd.MultiIndex):
                if ("Close", symbol) in df.columns:
                    close_series = df[("Close", symbol)]
                elif ("Close", symbol.upper()) in df.columns:
                    close_series = df[("Close", symbol.upper())]
                else:
                    close_cols = [col for col in df.columns if col[0] == "Close"]
                    if not close_cols:
                        continue
                    close_series = df[close_cols[0]]
            else:
                if "Close" not in df.columns:
                    continue
                close_series = df["Close"]

            close_series = pd.to_numeric(close_series, errors="coerce").dropna()
            if close_series.empty:
                continue

            plt.style.use("dark_background")
            fig, ax = plt.subplots(figsize=(9.4, 4.2))
            fig.patch.set_facecolor("#05070d")
            ax.set_facecolor("#070b14")

            x_vals = close_series.index
            y_vals = close_series.values

            # Main line
            ax.plot(
                x_vals,
                y_vals,
                linewidth=2.3,
                color="#8be9f5",
                zorder=3
            )

            # Glow line
            ax.plot(
                x_vals,
                y_vals,
                linewidth=7,
                color="#8be9f5",
                alpha=0.08,
                zorder=2
            )

            # Fill
            ax.fill_between(
                x_vals,
                y_vals,
                min(y_vals),
                color="#52d3ff",
                alpha=0.08,
                zorder=1
            )

            # Add right padding so labels don't clip
            try:
                ax.set_xlim(x_vals[0], x_vals[-1] + (x_vals[-1] - x_vals[0]) * 0.08)
            except Exception:
                pass

            label_x = x_vals[-1]

            same_target_as_entry = (
                    entry is not None and target is not None and abs(entry - target) < 0.01
            )

            if entry is not None:
                ax.axhline(entry, linestyle="--", linewidth=1.6, alpha=0.95, color="#52d3ff")
                ax.annotate(
                    f"Entry {entry:.2f}",
                    xy=(label_x, entry),
                    xytext=(6, 0),
                    textcoords="offset points",
                    color="#c8f6ff",
                    fontsize=8,
                    va="center",
                    ha="left",
                    clip_on=False
                )

            if target is not None and not same_target_as_entry:
                ax.axhline(target, linestyle="-", linewidth=1.6, alpha=0.95, color="#2dd881")
                ax.annotate(
                    f"Target {target:.2f}",
                    xy=(label_x, target),
                    xytext=(6, 0),
                    textcoords="offset points",
                    color="#b8ffd3",
                    fontsize=8,
                    va="center",
                    ha="left",
                    clip_on=False
                )

            if stop is not None:
                ax.axhline(stop, linestyle="-", linewidth=1.6, alpha=0.95, color="#ff6b81")
                ax.annotate(
                    f"Stop {stop:.2f}",
                    xy=(label_x, stop),
                    xytext=(6, 0),
                    textcoords="offset points",
                    color="#ffd0d8",
                    fontsize=8,
                    va="center",
                    ha="left",
                    clip_on=False
                )

            latest_price = float(y_vals[-1])
            ax.scatter(
                x_vals[-1],
                latest_price,
                s=42,
                color="#ecffff",
                edgecolors="#8be9f5",
                linewidths=1.0,
                zorder=4
            )

            ax.set_title(f"{symbol} Recent Price", fontsize=12, pad=10, color="#f3f7ff")
            ax.grid(alpha=0.12, color="#89a4c7")
            ax.tick_params(axis="x", labelsize=7, colors="#9fb0d0")
            ax.tick_params(axis="y", labelsize=7, colors="#9fb0d0")

            for spine in ax.spines.values():
                spine.set_alpha(0.22)
                spine.set_color("#7f93b2")

            plt.tight_layout(pad=1.4)

            buf = io.BytesIO()
            plt.savefig(
                buf,
                format="png",
                bbox_inches="tight",
                dpi=165,
                facecolor=fig.get_facecolor()
            )
            plt.close(fig)
            buf.seek(0)

            encoded = base64.b64encode(buf.getvalue()).decode("utf-8")
            print(f"[chart] success for {symbol}, bytes={len(encoded)}")
            return encoded

        except Exception as e:
            print(f"[chart] error for {symbol}: {e}")
            continue

    print(f"[chart] final fail for {symbol}")
    return None

HTML = """
<!doctype html>
<html>
<head>
    <title>Stock Alert Bot Dashboard</title>
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    {% if refresh_interval != "off" %}
    <meta http-equiv="refresh" content="{{ refresh_seconds }}">
    {% endif %}
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://unpkg.com/lightweight-charts@4.2.0/dist/lightweight-charts.standalone.production.js"></script>

    <style>
        :root {
            --text: #ecf2ff;
            --muted: #9fb0d0;
            --border: rgba(96, 118, 155, 0.22);
            --shadow: 0 16px 36px rgba(0, 0, 0, 0.42);
            --hover-shadow: 0 20px 44px rgba(0, 0, 0, 0.50);
        }

        * { box-sizing: border-box; }

        body {
            margin: 0;
            font-family: Arial, Helvetica, sans-serif;
            color: var(--text);
            background:
                radial-gradient(circle at 12% 20%, rgba(74, 211, 255, 0.14), transparent 20%),
                radial-gradient(circle at 88% 18%, rgba(165, 92, 255, 0.14), transparent 22%),
                linear-gradient(180deg, #05070d, #090d18 42%, #070b14 100%);
        }

        .container {
            max-width: 1520px;
            margin: 0 auto;
            padding: 16px;
        }

        .hero {
            border-radius: 26px;
            padding: 20px 22px;
            border: 1px solid rgba(120, 140, 190, 0.18);
            background:
                radial-gradient(circle at 15% 20%, rgba(82,211,255,0.16), transparent 22%),
                radial-gradient(circle at 85% 25%, rgba(154,107,255,0.14), transparent 24%),
                linear-gradient(135deg, rgba(12, 18, 34, 0.98), rgba(10, 16, 30, 0.96));
            box-shadow:
                0 28px 60px rgba(0,0,0,0.40),
                0 8px 24px rgba(82,211,255,0.07),
                inset 0 1px 0 rgba(255,255,255,0.06),
                inset 0 -18px 40px rgba(0,0,0,0.18);
            margin-bottom: 14px;
            position: relative;
            overflow: hidden;
        }

        .hero:before {
            content: "";
            position: absolute;
            inset: 0;
            background: linear-gradient(90deg, transparent, rgba(255,255,255,0.03), transparent);
            transform: skewX(-18deg) translateX(-120%);
            animation: shine 7s linear infinite;
            pointer-events: none;
        }

        .hero:after {
            content: "";
            position: absolute;
            right: -90px;
            top: -70px;
            width: 300px;
            height: 260px;
            border-radius: 50%;
            background: radial-gradient(circle, rgba(82,211,255,0.10), transparent 68%);
            filter: blur(10px);
            pointer-events: none;
        }

        @keyframes shine {
            0% { transform: skewX(-18deg) translateX(-120%); }
            100% { transform: skewX(-18deg) translateX(160%); }
        }

        .hero-title {
            font-size: 34px;
            font-weight: 900;
            margin-bottom: 10px;
            letter-spacing: 0.2px;
            text-shadow: 0 4px 18px rgba(82,211,255,0.10);
        }

        .hero-sub {
            color: var(--muted);
            font-size: 14px;
        }

        .hero-pills {
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
            margin-top: 16px;
        }

        .pill {
            padding: 9px 13px;
            border-radius: 999px;
            border: 1px solid rgba(120, 160, 220, 0.20);
            background: linear-gradient(180deg, rgba(22,30,52,0.96), rgba(12,18,34,0.96));
            color: var(--text);
            font-size: 13px;
            box-shadow:
                0 8px 18px rgba(0,0,0,0.22),
                inset 0 1px 0 rgba(255,255,255,0.05);
            transition: transform 0.20s ease, box-shadow 0.20s ease, border-color 0.20s ease, background 0.20s ease;
            cursor: default;
        }

        .pill:hover {
            transform: translateY(-3px) scale(1.02);
            border-color: rgba(120, 190, 255, 0.34);
            box-shadow:
                0 14px 28px rgba(82,211,255,0.12),
                0 10px 20px rgba(0,0,0,0.28),
                inset 0 1px 0 rgba(255,255,255,0.07);
        }

        .command-grid {
            display: grid;
            grid-template-columns: 1.18fr 0.92fr;
            gap: 10px;
            align-items: start;
        }

        .left-stack, .right-stack {
            display: grid;
            gap: 10px;
        }

        .top-left-grid {
            display: grid;
            grid-template-columns: 0.95fr 1.05fr;
            gap: 10px;
        }

        .panel {
            background: linear-gradient(180deg, rgba(13,19,36,0.96), rgba(10,15,28,0.96));
            border: 1px solid var(--border);
            border-radius: 20px;
            box-shadow:
                0 16px 34px rgba(0, 0, 0, 0.36),
                inset 0 1px 0 rgba(255,255,255,0.04);
            padding: 12px;
            transition: transform 0.20s ease, box-shadow 0.20s ease, border-color 0.20s ease;
            transform-style: preserve-3d;
        }

        .panel:hover {
            transform: translateY(-4px);
            box-shadow:
                0 24px 48px rgba(0, 0, 0, 0.42),
                0 8px 24px rgba(82,211,255,0.08),
                inset 0 1px 0 rgba(255,255,255,0.05);
            border-color: rgba(120, 180, 255, 0.26);
        }

        .panel h2 {
            margin: 0 0 10px 0;
            font-size: 24px;
        }

        .ai-brain {
            border-radius: 18px;
            padding: 12px;
            border: 1px solid rgba(90,169,255,0.18);
            background:
                radial-gradient(circle at center, rgba(154,107,255,0.18), rgba(82,211,255,0.08) 45%, rgba(10,15,28,0.95) 75%),
                linear-gradient(180deg, rgba(15,21,42,0.95), rgba(10,15,28,0.98));
            min-height: 270px;
        }

        .brain-title {
            font-size: 24px;
            font-weight: 800;
            margin-bottom: 8px;
        }

        .brain-sub {
            color: var(--muted);
            line-height: 1.5;
            font-size: 14px;
        }

        .robot-wrap {
            display: flex;
            justify-content: center;
            align-items: center;
            margin: 10px 0 6px 0;
        }

        .brain-orb {
            width: 120px;
            height: 120px;
            border-radius: 50%;
            position: relative;
            background:
                radial-gradient(circle at 30% 30%, rgba(82,211,255,0.95), rgba(154,107,255,0.75), rgba(9,13,24,0.08) 70%);
            box-shadow:
                0 0 35px rgba(82,211,255,0.28),
                0 0 55px rgba(154,107,255,0.20),
                inset 0 0 28px rgba(255,255,255,0.10);
            animation: floatOrb 4s ease-in-out infinite;
        }

        .brain-orb:after {
            content: "";
            position: absolute;
            inset: 18px;
            border-radius: 50%;
            border: 1px dashed rgba(255,255,255,0.22);
        }

        .robot-core {
            position: absolute;
            inset: 20px;
            border-radius: 50%;
            background:
                radial-gradient(circle at 35% 35%, rgba(120,220,255,0.95), rgba(154,107,255,0.85), rgba(20,26,50,0.3) 75%);
            box-shadow:
                0 0 22px rgba(82,211,255,0.35),
                0 0 44px rgba(154,107,255,0.18);
            animation: pulseCore 2.8s ease-in-out infinite;
        }

        .robot-eye {
            position: absolute;
            top: 46px;
            width: 12px;
            height: 12px;
            border-radius: 50%;
            background: #aef3ff;
            box-shadow: 0 0 10px rgba(82,211,255,0.8);
            animation: blinkEye 5s infinite;
        }

        .robot-eye-left { left: 39px; }
        .robot-eye-right { right: 39px; }

        .robot-mouth {
            position: absolute;
            left: 50%;
            bottom: 34px;
            transform: translateX(-50%);
            width: 38px;
            height: 10px;
            border-radius: 0 0 20px 20px;
            border-bottom: 2px solid rgba(180,220,255,0.85);
            opacity: 0.9;
            animation: talkMouth 1.6s ease-in-out infinite;
        }

        .assistant-status {
            margin-top: 8px;
            text-align: center;
            font-size: 12px;
            letter-spacing: 1.2px;
            color: #9fe7ff;
            text-transform: uppercase;
        }

        .assistant-message-box {
            margin-top: 10px;
            padding: 12px 14px;
            border-radius: 16px;
            border: 1px solid rgba(96,118,155,0.24);
            background: rgba(10,15,28,0.72);
            min-height: 64px;
            display: flex;
            align-items: center;
            justify-content: center;
        }

        .assistant-message {
            text-align: center;
            color: #dfeaff;
            line-height: 1.5;
            font-size: 13px;
        }

        @keyframes floatOrb {
            0%, 100% { transform: translateY(0px); }
            50% { transform: translateY(-5px); }
        }

        @keyframes pulseCore {
            0%, 100% { transform: scale(1); opacity: 0.95; }
            50% { transform: scale(1.05); opacity: 1; }
        }

        @keyframes blinkEye {
            0%, 45%, 48%, 100% { transform: scaleY(1); }
            46%, 47% { transform: scaleY(0.1); }
        }

        @keyframes talkMouth {
            0%, 100% { width: 38px; opacity: 0.8; }
            50% { width: 24px; opacity: 1; }
        }

        .metric-grid {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 8px;
        }

        .mini-card, .stat-box, .metric-box {
            background: linear-gradient(180deg, rgba(17,24,39,0.98), rgba(11,18,31,0.98));
            border: 1px solid var(--border);
            border-radius: 16px;
            padding: 9px 10px;
            transition: transform 0.18s ease, box-shadow 0.18s ease, border-color 0.18s ease;
        }

        .mini-card:hover, .stat-box:hover, .metric-box:hover {
            transform: translateY(-2px);
            box-shadow: 0 16px 34px rgba(82,211,255,0.08);
            border-color: rgba(120, 180, 255, 0.28);
        }

        .mini-label, .stat-label, .metric-label {
            color: var(--muted);
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 0.8px;
            margin-bottom: 6px;
        }

        .mini-value, .metric-value {
            font-size: 18px;
            font-weight: 800;
        }

        .mini-note, .section-body {
            color: var(--muted);
            font-size: 13px;
            line-height: 1.45;
        }

        .toolbar {
            display: flex;
            gap: 8px;
            flex-wrap: wrap;
            margin-bottom: 10px;
            align-items: end;
        }

        .toolbar .field {
            min-width: 120px;
            flex: 1 1 120px;
        }

        .toolbar label {
            display: block;
            margin-bottom: 5px;
            font-size: 12px;
            color: var(--muted);
        }

        .toolbar select {
            width: 100%;
            padding: 8px 10px;
            border-radius: 12px;
            border: 1px solid rgba(120,160,220,0.18);
            background: linear-gradient(180deg, rgba(12, 19, 36, 0.98), rgba(10,15,28,0.98));
            color: var(--text);
            outline: none;
            transition: transform 0.18s ease, box-shadow 0.18s ease, border-color 0.18s ease;
            cursor: pointer;
        }

        .toolbar select:hover,
        .toolbar select:focus {
            transform: translateY(-2px);
            box-shadow:
                0 12px 24px rgba(82,211,255,0.08),
                inset 0 1px 0 rgba(255,255,255,0.04);
            border-color: rgba(120, 180, 255, 0.34);
        }

        .table-wrap {
            overflow-x: auto;
            border-radius: 18px;
            border: 1px solid var(--border);
        }

        table {
            width: 100%;
            border-collapse: collapse;
        }

        th {
            text-align: left;
            padding: 9px 8px;
            font-size: 13px;
            background: linear-gradient(180deg, #1b2740, #182235);
            border-bottom: 1px solid var(--border);
        }

        td {
            padding: 9px 8px;
            border-bottom: 1px solid rgba(96,118,155,0.14);
            font-size: 13px;
            white-space: nowrap;
        }

        tbody tr {
            transition: transform 0.18s ease, background 0.18s ease, box-shadow 0.18s ease;
        }

        tbody tr:hover {
            transform: translateY(-1px);
        }

        tbody tr:hover td {
            background: rgba(90,169,255,0.05);
        }

        .symbol-link {
            color: #67a7ff;
            text-decoration: none;
            font-weight: 700;
        }

        .symbol-link:hover {
            text-decoration: underline;
        }

        .badge {
            display: inline-block;
            padding: 6px 12px;
            border-radius: 999px;
            font-size: 12px;
            font-weight: 700;
            transition: transform 0.18s ease, box-shadow 0.18s ease, filter 0.18s ease;
            cursor: pointer;
        }

        .badge:hover {
            transform: translateY(-2px) scale(1.04);
            box-shadow: 0 10px 22px rgba(82,211,255,0.10);
            filter: brightness(1.06);
        }

        .badge-go {
            color: #9cf0b8;
            background: rgba(45,216,129,0.14);
            border: 1px solid rgba(45,216,129,0.28);
        }

        .badge-watch {
            color: #ffe39a;
            background: rgba(255,209,102,0.14);
            border: 1px solid rgba(255,209,102,0.28);
        }

        .badge-stop {
            color: #ffafbb;
            background: rgba(255,107,129,0.14);
            border: 1px solid rgba(255,107,129,0.28);
        }

        .badge-neutral {
            color: #dbe7ff;
            background: rgba(159,176,208,0.12);
            border: 1px solid rgba(159,176,208,0.20);
        }

        .selected-card-top {
            display: grid;
            grid-template-columns: 1fr auto;
            gap: 10px;
            align-items: start;
            margin-bottom: 10px;
        }

        .detail-title {
            font-size: 28px;
            font-weight: 900;
        }

        .detail-sub {
            color: var(--muted);
            margin-top: 4px;
            font-size: 14px;
        }

        .levels {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 8px;
            margin-bottom: 10px;
        }

        .level {
            border-radius: 16px;
            padding: 10px;
            border: 1px solid var(--border);
            transition: transform 0.16s ease, box-shadow 0.16s ease;
        }

        .level:hover {
            transform: translateY(-2px);
            box-shadow: 0 14px 32px rgba(82,211,255,0.08);
        }

        .entry {
            background: linear-gradient(180deg, rgba(82,211,255,0.10), rgba(13,19,36,0.95));
        }

        .target {
            background: linear-gradient(180deg, rgba(45,216,129,0.12), rgba(13,19,36,0.95));
        }

        .stop {
            background: linear-gradient(180deg, rgba(255,107,129,0.12), rgba(13,19,36,0.95));
        }

        .level-name {
            color: var(--muted);
            font-size: 11px;
            text-transform: uppercase;
            margin-bottom: 6px;
            letter-spacing: 0.8px;
        }

        .level-price {
            font-size: 18px;
            font-weight: 900;
        }

        .chart-box {
            border-radius: 18px;
            border: 1px solid rgba(120,180,255,0.16);
            background:
                radial-gradient(circle at top left, rgba(82,211,255,0.06), transparent 28%),
                radial-gradient(circle at top right, rgba(154,107,255,0.06), transparent 30%),
                linear-gradient(180deg, rgba(6,10,18,0.92), rgba(12,18,32,0.98));
            padding: 10px;
            margin-bottom: 8px;
            min-height: 210px;
            box-shadow:
                0 18px 40px rgba(0,0,0,0.35),
                inset 0 1px 0 rgba(255,255,255,0.04);
        }

        #candlestick-chart {
            width: 100%;
            height: 260px;
            border-radius: 12px;
            overflow: hidden;
        }

        .empty-box {
            text-align: center;
            color: var(--muted);
            padding: 40px 18px;
            border: 1px dashed rgba(96,118,155,0.24);
            border-radius: 16px;
        }

.detail-stats {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 10px;
    margin: 10px 0 10px 0;
}

.stat-box {
    background:
        linear-gradient(180deg, rgba(15,22,38,0.98), rgba(10,15,28,0.98));
    border: 1px solid rgba(110,140,190,0.14);
    border-radius: 14px;
    padding: 10px 12px;
    min-height: 70px;

    display: flex;
    flex-direction: column;
    justify-content: center;

    position: relative;
    overflow: hidden;

    transition:
        transform 0.18s ease,
        box-shadow 0.18s ease,
        border-color 0.18s ease;
}

.stat-box::before {
    content: "";
    position: absolute;
    inset: 0;
    background:
        linear-gradient(120deg,
        transparent,
        rgba(82,211,255,0.05),
        transparent);
    opacity: 0;
    transition: opacity 0.2s ease;
}

.stat-box:hover::before {
    opacity: 1;
}

.stat-box:hover {
    transform: translateY(-2px);
    border-color: rgba(120,180,255,0.26);
    box-shadow: 0 12px 28px rgba(82,211,255,0.06);
}

.stat-label {
    color: #8ea3c7;
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 0.9px;
    margin-bottom: 6px;
}

.stat-value {
    font-size: 15px;
    font-weight: 800;
    color: #f1f5ff;
    line-height: 1.15;
}

.section {
    border-radius: 14px;
    border: 1px solid rgba(110,140,190,0.14);
    background:
        linear-gradient(180deg, rgba(12,18,32,0.94), rgba(9,14,26,0.96));
    padding: 10px 12px;
    margin-top: 8px;
    transition: transform 0.16s ease, box-shadow 0.16s ease, border-color 0.16s ease;
}

.section:hover {
    transform: translateY(-1px);
    border-color: rgba(120,180,255,0.22);
    box-shadow: 0 10px 22px rgba(82,211,255,0.05);
}

.info-stack {
    display: grid;
    gap: 8px;
    margin-top: 4px;
}

.section-title {
    font-size: 15px;
    font-weight: 700;
    margin-bottom: 6px;
    color: #f0f4ff;
}

.section-body {
    color: #9fb0d0;
    font-size: 13px;
    line-height: 1.45;
}

        .donut-area {
            display: grid;
            grid-template-columns: 0.95fr 1.05fr;
            gap: 10px;
            align-items: stretch;
        }

        .donut-card {
            border-radius: 20px;
            border: 1px solid var(--border);
            background: linear-gradient(180deg, rgba(17,24,39,0.98), rgba(11,18,31,0.98));
            padding: 10px;
            min-height: 220px;
            height: 220px;
            position: relative;
            overflow: hidden;
        }

        .donut-card canvas {
            width: 100% !important;
            height: 165px !important;
        }

        .trend-card {
            border-radius: 18px;
            border: 1px solid rgba(120,180,255,0.18);
            background:
                radial-gradient(circle at top left, rgba(82,211,255,0.10), transparent 28%),
                radial-gradient(circle at top right, rgba(154,107,255,0.10), transparent 28%),
                linear-gradient(180deg, rgba(17,24,39,0.98), rgba(11,18,31,0.98));
            padding: 10px;
            margin-top: 8px;
            min-height: 170px;
            height: 170px;
            box-shadow:
                0 14px 34px rgba(0,0,0,0.35),
                inset 0 1px 0 rgba(255,255,255,0.04);
        }

        .trend-card canvas {
            width: 100% !important;
            height: 115px !important;
        }

        .perf-mini-grid {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 8px;
            margin-top: 8px;
            margin-bottom: 8px;
        }

        .compact-table-title {
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 10px;
            margin-bottom: 8px;
        }

        .small-note {
            color: var(--muted);
            font-size: 12px;
        }

        @media (max-width: 1250px) {
            .command-grid,
            .top-left-grid,
            .donut-area {
                grid-template-columns: 1fr;
            }
        }

        @media (max-width: 900px) {
            .levels,
            .detail-stats,
            .metric-grid,
            .perf-mini-grid {
                grid-template-columns: 1fr 1fr;
            }
        }

        @media (max-width: 680px) {
            .levels,
            .detail-stats,
            .metric-grid,
            .perf-mini-grid {
                grid-template-columns: 1fr;
            }
        }
    </style>
</head>
<body>
<div class="container">

    <div class="hero">
        <div class="hero-title">Stock Alert Bot Dashboard</div>
        <div class="hero-sub">AI-assisted visual monitoring dashboard driven by current bot outputs and logs.</div>

        <div class="hero-pills">
            <div class="pill">Mode: {{ selected_mode }}</div>
            <div class="pill">Date: {{ selected_date }}</div>
            <div class="pill">Rows: {{ row_count }}</div>
            <div class="pill">Symbol: {{ selected_symbol or "N/A" }}</div>
            <div class="pill">History: {{ history_window }}</div>
            <div class="pill">Refresh: {{ refresh_interval }}</div>
        </div>
    </div>

    <div class="command-grid">
        <div class="left-stack">

            <div class="top-left-grid">
                <div class="panel">
                    <h2>AI Market Engine</h2>
                    <div class="ai-brain">
                        <div class="brain-title">AI Trading Assistant</div>
                        <div class="brain-sub">Surface your current strategy signals, market context, and insights from the bot’s output layer.</div>

                        <div class="robot-wrap">
                            <div class="brain-orb">
                                <div class="robot-core"></div>
                                <div class="robot-eye robot-eye-left"></div>
                                <div class="robot-eye robot-eye-right"></div>
                                <div class="robot-mouth"></div>
                            </div>
                        </div>

                        <div class="assistant-status">STATUS: ACTIVE</div>

                        <div class="assistant-message-box">
                            <div id="assistant-message" class="assistant-message">
                                {{ persona_messages[0] if persona_messages else ai_summary.insight }}
                            </div>
                        </div>

                        <div class="brain-sub" style="margin-top:8px;">
                            {{ ai_summary.insight }}
                        </div>

                        <div class="section" style="margin-top:10px;">
                            <div class="section-title">Quick Signal Snapshot</div>
                            <div class="metric-grid">
                                <div class="mini-card">
                                    <div class="mini-label">Symbol</div>
                                    <div class="mini-value">{{ detail.symbol }}</div>
                                </div>
                                <div class="mini-card">
                                    <div class="mini-label">Stance</div>
                                    <div class="mini-value">{{ detail.stance }}</div>
                                </div>
                                <div class="mini-card">
                                    <div class="mini-label">Confidence</div>
                                    <div class="mini-value">{{ detail.confidence }}</div>
                                </div>
                                <div class="mini-card">
                                    <div class="mini-label">Score</div>
                                    <div class="mini-value">{{ detail.score }}</div>
                                </div>
                                <div class="mini-card" style="grid-column: 1 / span 2;">
                                    <div class="mini-label">Risk</div>
                                    <div class="mini-note">{{ ai_summary.risk_note }}</div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>

                <div class="panel">
                    <h2>Strategy Performance</h2>

                    <div class="toolbar">
                        <input type="hidden" name="symbol" value="{{ selected_symbol }}">
                        <div class="field">
                            <label for="history_window_top">History Window</label>
                            <select id="history_window_top" onchange="updateFilter('history_window', this.value)">
                                <option value="today" {% if history_window == "today" %}selected{% endif %}>Today</option>
                                <option value="7d" {% if history_window == "7d" %}selected{% endif %}>Last 7 Days</option>
                                <option value="30d" {% if history_window == "30d" %}selected{% endif %}>Last 30 Days</option>
                                <option value="all" {% if history_window == "all" %}selected{% endif %}>All History</option>
                            </select>
                        </div>
                    </div>

                    <div class="perf-mini-grid">
                        <div class="metric-box">
                            <div class="metric-label">Evaluated</div>
                            <div class="metric-value">{{ perf.evaluated }}</div>
                        </div>
                        <div class="metric-box">
                            <div class="metric-label">Wins</div>
                            <div class="metric-value">{{ perf.wins }}</div>
                        </div>
                        <div class="metric-box">
                            <div class="metric-label">Losses</div>
                            <div class="metric-value">{{ perf.losses }}</div>
                        </div>
                        <div class="metric-box">
                            <div class="metric-label">Win Rate</div>
                            <div class="metric-value">{{ perf.win_rate }}</div>
                        </div>
                    </div>

                    <div class="donut-area">
                        <div class="donut-card">
                            <div class="compact-table-title">
                                <h2 style="font-size:18px; margin:0;">Outcome Share</h2>
                                <div class="small-note">Donut view</div>
                            </div>
                            <canvas id="donutChart"></canvas>
                        </div>

                        <div class="panel" style="padding:10px; min-height:220px;">
                            <div class="metric-grid">
                                <div class="mini-card">
                                    <div class="mini-label">Market Regime</div>
                                    <div class="mini-value">{{ ai_summary.market_regime }}</div>
                                </div>
                                <div class="mini-card">
                                    <div class="mini-label">Top Opportunity</div>
                                    <div class="mini-value">{{ ai_summary.top_symbol }}</div>
                                </div>
                                <div class="mini-card">
                                    <div class="mini-label">Confidence</div>
                                    <div class="mini-value">{{ ai_summary.confidence }}</div>
                                </div>
                                <div class="mini-card">
                                    <div class="mini-label">Score</div>
                                    <div class="mini-value">{{ ai_summary.score }}</div>
                                </div>
                                <div class="mini-card" style="grid-column: 1 / span 2;">
                                    <div class="mini-label">Risk Note</div>
                                    <div class="mini-note">{{ ai_summary.risk_note }}</div>
                                </div>
                            </div>
                        </div>
                    </div>

                    <div class="trend-card">
                        <div class="compact-table-title">
                            <h2 style="font-size:18px; margin:0;">Top Picks Comparison</h2>
                            <div class="small-note">Top scored symbols</div>
                        </div>
                        <canvas id="comparisonChart"></canvas>
                    </div>
                </div>
            </div>

            <div class="panel">
                <div class="compact-table-title">
                    <h2>Today's Picks</h2>
                    <div class="small-note">Read-only bot output view</div>
                </div>

                <div class="toolbar">
                    <div class="field">
                        <label for="run_date">Date</label>
                        <select id="run_date" onchange="updateFilter('run_date', this.value)">
                            <option value="all" {% if selected_date == "all" %}selected{% endif %}>All</option>
                            {% for d in available_dates %}
                            <option value="{{ d }}" {% if d == selected_date %}selected{% endif %}>{{ d }}</option>
                            {% endfor %}
                        </select>
                    </div>

                    <div class="field">
                        <label for="mode">Mode</label>
                        <select id="mode" onchange="updateFilter('mode', this.value)">
                            <option value="all" {% if selected_mode == "all" %}selected{% endif %}>All</option>
                            <option value="premarket" {% if selected_mode == "premarket" %}selected{% endif %}>Premarket</option>
                            <option value="midday" {% if selected_mode == "midday" %}selected{% endif %}>Midday</option>
                        </select>
                    </div>

                    <div class="field">
                        <label for="symbol">Select Symbol</label>
                        <select id="symbol" onchange="updateFilter('symbol', this.value)">
                            {% for sym in symbols %}
                            <option value="{{ sym }}" {% if sym == selected_symbol %}selected{% endif %}>{{ sym }}</option>
                            {% endfor %}
                        </select>
                    </div>

                    <div class="field">
                        <label for="refresh">Live Refresh</label>
                        <select id="refresh" onchange="updateFilter('refresh', this.value)">
                            <option value="off" {% if refresh_interval == "off" %}selected{% endif %}>Off</option>
                            <option value="30s" {% if refresh_interval == "30s" %}selected{% endif %}>30 sec</option>
                            <option value="60s" {% if refresh_interval == "60s" %}selected{% endif %}>60 sec</option>
                        </select>
                    </div>
                </div>

                <div class="table-wrap">
                    <table>
                        <thead>
                            <tr>
                                <th>Symbol</th>
                                <th>Current</th>
                                <th>Entry</th>
                                <th>Target</th>
                                <th>Stop</th>
                                <th>Score</th>
                                <th>Confidence</th>
                                <th>R:R</th>
                                <th>Stance</th>
                                <th>Mode</th>
                            </tr>
                        </thead>
                        <tbody>
                            {% for row in rows %}
                            <tr>
                                <td><a class="symbol-link" href="/?symbol={{ row.symbol }}&mode={{ selected_mode }}&run_date={{ selected_date }}&history_window={{ history_window }}&refresh={{ refresh_interval }}">{{ row.symbol }}</a></td>
                                <td>{{ row.current_fmt }}</td>
                                <td>{{ row.entry_fmt }}</td>
                                <td>{{ row.target_fmt }}</td>
                                <td>{{ row.stop_fmt }}</td>
                                <td>{{ row.score }}</td>
                                <td>{{ row.confidence }}</td>
                                <td>{{ row.expected_rr }}</td>
                                <td><span class="badge {{ row.stance_class }}">{{ row.stance }}</span></td>
                                <td>{{ row.source_mode }}</td>
                            </tr>
                            {% endfor %}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>

        <div class="right-stack">
            <div class="panel">
                <div class="selected-card-top">
                    <div>
                        <div class="detail-title">{{ detail.symbol }}</div>
                        <div class="detail-sub">{{ detail.price_category }}{% if detail.market_regime != "Pending" %} • Regime: {{ detail.market_regime }}{% endif %}</div>
                    </div>
                    <div><span class="badge {{ detail.stance_class }}">{{ detail.stance }}</span></div>
                </div>

                <div class="levels">
                    <div class="level entry">
                        <div class="level-name">Entry</div>
                        <div class="level-price">{{ detail.entry_fmt }}</div>
                    </div>
                    <div class="level target">
                        <div class="level-name">Target</div>
                        <div class="level-price">{{ detail.target_fmt }}</div>
                    </div>
                    <div class="level stop">
                        <div class="level-name">Stop</div>
                        <div class="level-price">{{ detail.stop_fmt }}</div>
                    </div>
                </div>

                <div class="chart-box">
                    <div id="candlestick-chart"></div>

                    {% if not candle_data %}
                        <div class="empty-box" style="margin-top:10px;">
                            Candlestick data unavailable right now.
                        </div>
                    {% endif %}
                </div>

                <div class="detail-stats">
                    <div class="stat-box">
                        <div class="stat-label">Logged Price</div>
                        <div class="stat-value">{{ detail.current_fmt }}</div>
                    </div>
                    <div class="stat-box">
                        <div class="stat-label">Live Price</div>
                        <div class="stat-value">{{ detail.live_price_fmt }}</div>
                    </div>
                    <div class="stat-box">
                        <div class="stat-label">Confidence</div>
                        <div class="stat-value">{{ detail.confidence }}</div>
                    </div>
                    <div class="stat-box">
                        <div class="stat-label">Score</div>
                        <div class="stat-value">{{ detail.score }}</div>
                    </div>
                    <div class="stat-box">
                        <div class="stat-label">To Target</div>
                        <div class="stat-value">{{ detail.to_target_pct }}</div>
                    </div>
                    <div class="stat-box">
                        <div class="stat-label">To Stop</div>
                        <div class="stat-value">{{ detail.to_stop_pct }}</div>
                    </div>
                    <div class="stat-box">
                        <div class="stat-label">R:R</div>
                        <div class="stat-value">{{ detail.expected_rr }}</div>
                    </div>
                    <div class="stat-box">
                        <div class="stat-label">Win Prob</div>
                        <div class="stat-value">{{ detail.win_prob }}</div>
                    </div>
                    <div class="stat-box">
                        <div class="stat-label">Pct Change</div>
                        <div class="stat-value">{{ detail.pct_change_fmt }}</div>
                    </div>
                </div>

<div class="info-stack">
    <div class="section">
        <div class="section-title">Live Snapshot</div>
        <div class="section-body">
            Current live overlay is for visualization only.<br>
            Last refresh: {{ detail.live_as_of }}
        </div>
    </div>

    <div class="section">
        <div class="section-title">Latest News</div>
        <div class="section-body">{{ detail.main_news_title }}</div>
    </div>

    <div class="section">
        <div class="section-title">Insight Summary</div>
        <div class="section-body">{{ detail.key_insight }}</div>
    </div>

    <div class="section">
        <div class="section-title">Reasons / Notes</div>
        <div class="section-body">{{ detail.reasons }}</div>
    </div>
</div>

<script>
    const personaMessages = {{ persona_messages|tojson }};
    let personaIndex = 0;

    function rotateAssistantMessage() {
        const el = document.getElementById("assistant-message");
        if (!el || !personaMessages || personaMessages.length === 0) return;
        personaIndex = (personaIndex + 1) % personaMessages.length;
        el.textContent = personaMessages[personaIndex];
    }
    setInterval(rotateAssistantMessage, 3500);

    function updateFilter(key, value) {
        const url = new URL(window.location.href);
        url.searchParams.set(key, value);

        if (key !== "symbol" && !url.searchParams.get("symbol")) {
            const symbolSelect = document.getElementById("symbol");
            if (symbolSelect) url.searchParams.set("symbol", symbolSelect.value);
        }

        if (!url.searchParams.get("mode")) url.searchParams.set("mode", "{{ selected_mode }}");
        if (!url.searchParams.get("run_date")) url.searchParams.set("run_date", "{{ selected_date }}");
        if (!url.searchParams.get("history_window")) url.searchParams.set("history_window", "{{ history_window }}");
        if (!url.searchParams.get("refresh")) url.searchParams.set("refresh", "{{ refresh_interval }}");

        window.location.href = url.toString();
    }

    const perfSummary = {{ perf_chart_data|tojson }};
    const donutValues = [perfSummary.wins, perfSummary.losses, perfSummary.not_hit];
    const donutSafeValues = donutValues.every(v => Number(v) === 0) ? [1, 0, 0] : donutValues;

    new Chart(document.getElementById("donutChart"), {
        type: "doughnut",
        data: {
            labels: ["Wins", "Losses", "Not Hit"],
            datasets: [{
                data: donutSafeValues,
                backgroundColor: [
                    "rgba(45,216,129,0.88)",
                    "rgba(255,107,129,0.88)",
                    "rgba(255,209,102,0.88)"
                ],
                borderColor: [
                    "rgba(45,216,129,1)",
                    "rgba(255,107,129,1)",
                    "rgba(255,209,102,1)"
                ],
                borderWidth: 1.6,
                hoverOffset: 8
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            cutout: "62%",
            animation: false,
            plugins: {
                legend: {
                    position: "bottom",
                    labels: { color: "#dce8ff" }
                }
            }
        }
    });

    const comparisonLabels = {{ comparison_labels|tojson }};
    const comparisonScores = {{ comparison_scores|tojson }};

    new Chart(document.getElementById("comparisonChart"), {
        type: "bar",
        data: {
            labels: comparisonLabels.length ? comparisonLabels : ["No Data"],
            datasets: [{
                label: "Score",
                data: comparisonScores.length ? comparisonScores : [0],
                backgroundColor: [
                    "rgba(82,211,255,0.78)",
                    "rgba(45,216,129,0.78)",
                    "rgba(255,209,102,0.78)",
                    "rgba(154,107,255,0.78)",
                    "rgba(255,107,129,0.78)"
                ],
                borderRadius: 10
            }]
        },
        options: {
            indexAxis: "y",
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false }
            },
            scales: {
                x: {
                    beginAtZero: true,
                    ticks: { color: "#9fb0d0" },
                    grid: { color: "rgba(159,176,208,0.05)" }
                },
                y: {
                    ticks: { color: "#dce8ff" },
                    grid: { display: false }
                }
            }
        }
    });

    const candleData = {{ candle_data|tojson }};

    try {
        if (candleData && candleData.length > 0) {
            const chartContainer = document.getElementById("candlestick-chart");

            const chart = LightweightCharts.createChart(chartContainer, {
                layout: {
                    background: { color: "#070b14" },
                    textColor: "#dce8ff"
                },
                grid: {
                    vertLines: { color: "rgba(159,176,208,0.08)" },
                    horzLines: { color: "rgba(159,176,208,0.08)" }
                },
                width: chartContainer.clientWidth || 600,
                height: 260,
                rightPriceScale: {
                    borderColor: "rgba(159,176,208,0.18)"
                },
                timeScale: {
                    borderColor: "rgba(159,176,208,0.18)",
                    timeVisible: true
                }
            });

            const candleSeries = chart.addCandlestickSeries({
                upColor: "#2dd881",
                downColor: "#ff6b81",
                borderUpColor: "#2dd881",
                borderDownColor: "#ff6b81",
                wickUpColor: "#2dd881",
                wickDownColor: "#ff6b81"
            });

            const formatted = candleData.map(c => ({
                time: Math.floor(new Date(c.time).getTime() / 1000),
                open: c.open,
                high: c.high,
                low: c.low,
                close: c.close
            }));

            candleSeries.setData(formatted);

            const entry = {{ detail.entry_raw if detail.entry_raw is not none else 'null' }};
            const target = {{ detail.target_raw if detail.target_raw is not none else 'null' }};
            const stop = {{ detail.stop_raw if detail.stop_raw is not none else 'null' }};

            if (entry !== null) {
                candleSeries.createPriceLine({
                    price: entry,
                    color: "#52d3ff",
                    lineWidth: 2,
                    lineStyle: 2,
                    title: "Entry"
                });
            }

            if (target !== null && (entry === null || Math.abs(target - entry) > 0.01)) {
                candleSeries.createPriceLine({
                    price: target,
                    color: "#2dd881",
                    lineWidth: 2,
                    title: "Target"
                });
            }

            if (stop !== null) {
                candleSeries.createPriceLine({
                    price: stop,
                    color: "#ff6b81",
                    lineWidth: 2,
                    title: "Stop"
                });
            }

            chart.timeScale().fitContent();

            window.addEventListener("resize", () => {
                chart.applyOptions({
                    width: chartContainer.clientWidth || 600
                });
            });
        }
    } catch (err) {
        console.error("Candlestick chart error:", err);
        const box = document.getElementById("candlestick-chart");
        if (box) {
            box.innerHTML = '<div style="color:#ff9aa8; padding:20px; text-align:center;">Candlestick script failed. Check browser console.</div>';
        }
    }

    document.querySelectorAll(".panel, .mini-card, .stat-box, .metric-box, .level").forEach((card) => {
        card.addEventListener("mousemove", (e) => {
            const rect = card.getBoundingClientRect();
            const x = e.clientX - rect.left;
            const y = e.clientY - rect.top;

            const rotateY = ((x / rect.width) - 0.5) * 6;
            const rotateX = ((y / rect.height) - 0.5) * -6;

            card.style.transform = `perspective(900px) rotateX(${rotateX}deg) rotateY(${rotateY}deg) translateY(-2px)`;
        });

        card.addEventListener("mouseleave", () => {
            card.style.transform = "";
        });
    });
</script>
</body>
</html>
"""






@app.route("/", methods=["GET"])
def dashboard():
    daily_df, daily_source = load_daily_data()
    perf_df_all, perf_source = load_perf_data()

    history_window = request.args.get("history_window", "7d").strip().lower()
    if history_window not in {"today", "7d", "30d", "all"}:
        history_window = "7d"

    refresh_interval = request.args.get("refresh", "off").strip().lower()
    if refresh_interval not in {"off", "30s", "60s"}:
        refresh_interval = "off"

    refresh_seconds = 0
    if refresh_interval == "30s":
        refresh_seconds = 30
    elif refresh_interval == "60s":
        refresh_seconds = 60

    perf_df = filter_perf_by_window(perf_df_all, history_window)
    perf = summarize_performance(perf_df)
    trend_chart_data = build_trend_chart_data(perf_df)

    if daily_df.empty:
        return render_template_string(
            HTML,
            rows=[],
            row_count=0,
            selected_symbol=None,
            selected_mode="all",
            selected_date="all",
            history_window=history_window,
            refresh_interval=refresh_interval,
            refresh_seconds=refresh_seconds,
            available_dates=[],
            symbols=[],
            detail={},
            candle_data=[],
            ai_summary={
                "market_regime": "Pending",
                "top_symbol": "Pending",
                "confidence": "Pending",
                "score": "Pending",
                "insight": "No daily log found.",
                "risk_note": "Waiting for bot output.",
            },
            persona_messages=["Waiting for bot output..."],
            perf=perf,
            perf_chart_data={
                "wins": perf["wins_int"],
                "losses": perf["losses_int"],
                "not_hit": perf["not_hit_int"],
            },
            trend_chart_data=trend_chart_data,
            perf_source=perf_source,
        )

    available_dates = get_available_dates(daily_df)

    selected_date = request.args.get("run_date", "")
    if not selected_date:
        selected_date = available_dates[0] if available_dates else "all"

    selected_mode = request.args.get("mode", "all").strip().lower()
    selected_symbol = request.args.get("symbol", "").strip().upper()

    filtered_df = filter_daily_df(daily_df, selected_date, selected_mode)

    if filtered_df.empty:
        return render_template_string(
            HTML,
            rows=[],
            row_count=0,
            selected_symbol=None,
            selected_mode=selected_mode,
            selected_date=selected_date,
            history_window=history_window,
            refresh_interval=refresh_interval,
            refresh_seconds=refresh_seconds,
            available_dates=available_dates,
            symbols=[],
            detail={},
            candle_data=[],
            ai_summary={
                "market_regime": "Pending",
                "top_symbol": "Pending",
                "confidence": "Pending",
                "score": "Pending",
                "insight": "No rows found for selected filters.",
                "risk_note": "Try a different date or mode.",
            },
            persona_messages=["No rows found for selected filters."],
            perf=perf,
            perf_chart_data={
                "wins": perf["wins_int"],
                "losses": perf["losses_int"],
                "not_hit": perf["not_hit_int"],
            },
            trend_chart_data=trend_chart_data,
            perf_source=perf_source,
        )

    row = get_selected_row(filtered_df, selected_symbol)
    selected_symbol = str(row.get("symbol", "")).upper()
    symbols = filtered_df["symbol"].astype(str).str.upper().tolist()

    live = fetch_live_snapshot(selected_symbol)
    overlay_price = live["live_price"] if live["live_price"] is not None else safe_float(row.get("current"))
    candle_data = get_candlestick_data(selected_symbol)

    stock_chart = generate_stock_chart(
        selected_symbol,
        entry=safe_float(row.get("entry_price")),
        target=safe_float(row.get("target_price")),
        stop=safe_float(row.get("stop_loss")),
    )
    print("stock_chart exists:", stock_chart is not None)

    detail = {
        "symbol": selected_symbol,
        "current_fmt": fmt_money(row.get("current")),
        "entry_fmt": fmt_money(row.get("entry_price")),
        "target_fmt": fmt_money(row.get("target_price")),
        "stop_fmt": fmt_money(row.get("stop_loss")),
        "score": fmt_plain(row.get("score")),
        "entry_raw": safe_float(row.get("entry_price")),
        "target_raw": safe_float(row.get("target_price")),
        "stop_raw": safe_float(row.get("stop_loss")),
        "confidence": fmt_plain(row.get("confidence")),
        "expected_rr": fmt_plain(row.get("expected_rr"), "Not calculated"),
        "win_prob": fmt_plain(row.get("win_prob"), "Pending model"),
        "pct_change_fmt": fmt_pct(row.get("pct_change")),
        "stance": fmt_plain(row.get("stance", row.get("decision", "Pending"))),
        "stance_class": stance_badge_class(fmt_plain(row.get("stance", row.get("decision", "Pending")))),
        "main_news_title": fmt_plain(row.get("main_news_title"), "No linked news title in current log."),
        "key_insight": fmt_plain(
            row.get("key_insight") or row.get("llm_insights"),
            "No AI insight generated for this row yet."
        ),
        "reasons": fmt_plain(row.get("reasons"), "No notes available in current log."),
        "market_regime": fmt_plain(row.get("market_regime"), "Pending"),
        "price_category": fmt_plain(row.get("price_category"), "Stock Detail"),
        "live_price_fmt": fmt_money(live["live_price"]),
        "live_as_of": fmt_plain(live["as_of"], "Unavailable"),
        "to_target_pct": calc_distance_pct(overlay_price, row.get("target_price")),
        "to_stop_pct": calc_distance_pct(overlay_price, row.get("stop_loss")),
    }

    display_rows = []
    for _, r in filtered_df.iterrows():
        stance = fmt_plain(r.get("stance", r.get("decision", "Pending")))
        display_rows.append({
            "symbol": fmt_plain(r.get("symbol")),
            "current_fmt": fmt_money(r.get("current")),
            "entry_fmt": fmt_money(r.get("entry_price")),
            "target_fmt": fmt_money(r.get("target_price")),
            "stop_fmt": fmt_money(r.get("stop_loss")),
            "score": fmt_plain(r.get("score")),
            "confidence": fmt_plain(r.get("confidence")),
            "expected_rr": fmt_plain(r.get("expected_rr"), "Pending"),
            "stance": stance,
            "stance_class": stance_badge_class(stance),
            "source_mode": fmt_plain(r.get("source_mode", r.get("mode", "Pending"))),
        })

    ai_summary = build_ai_engine_summary(filtered_df, row)
    persona_messages = build_ai_persona_messages(row)

    comparison_labels = []
    comparison_scores = []

    top_compare_df = filtered_df.copy()
    if "score" in top_compare_df.columns:
        top_compare_df["score_num"] = pd.to_numeric(top_compare_df["score"], errors="coerce")
        top_compare_df = top_compare_df.dropna(subset=["score_num"]).sort_values("score_num", ascending=False).head(5)

        comparison_labels = top_compare_df["symbol"].astype(str).tolist()
        comparison_scores = top_compare_df["score_num"].astype(float).tolist()

    return render_template_string(
        HTML,
        rows=display_rows,
        row_count=len(display_rows),
        selected_symbol=selected_symbol,
        selected_mode=selected_mode,
        selected_date=selected_date,
        history_window=history_window,
        refresh_interval=refresh_interval,
        refresh_seconds=refresh_seconds,
        available_dates=available_dates,
        symbols=symbols,
        detail=detail,
        candle_data=candle_data,
        stock_chart=stock_chart,
        ai_summary=ai_summary,
        persona_messages=persona_messages,
        perf=perf,
        perf_chart_data={
            "wins": perf["wins_int"],
            "losses": perf["losses_int"],
            "not_hit": perf["not_hit_int"],
        },
        trend_chart_data=trend_chart_data,
        comparison_labels=comparison_labels,
        comparison_scores=comparison_scores,
        perf_source=perf_source,
    )


if __name__ == "__main__":
    app.run(port=5001, debug=True)