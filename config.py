# config.py (UPDATED)
import os
from pathlib import Path
from datetime import time
from dotenv import load_dotenv

# Load .env from project root (same folder as this file)
load_dotenv(dotenv_path=Path(__file__).with_name(".env"))

# -----------------------------
# App env
# -----------------------------
APP_ENV = (os.getenv("APP_ENV") or "prod").strip().lower()
IS_LOCAL = (APP_ENV == "local")

# -----------------------------
# Environment-aware email routing
# -----------------------------
EMAIL_SUBJECT_PREFIX_PROD = os.getenv("EMAIL_SUBJECT_PREFIX_PROD", "[PROD]")
EMAIL_SUBJECT_PREFIX_LOCAL = os.getenv("EMAIL_SUBJECT_PREFIX_LOCAL", "[LOCAL TEST]")

SENDER_EMAIL = (os.getenv("SENDER_EMAIL") or "").strip()
RECEIVER_EMAIL = (os.getenv("RECEIVER_EMAIL") or "").strip()
LOCAL_RECEIVER_EMAIL = (os.getenv("LOCAL_RECEIVER_EMAIL") or "").strip()  # optional

# Email provider controls (used by email_sender.py)
EMAIL_PROVIDER = (os.getenv("EMAIL_PROVIDER") or "resend").strip().lower()
EMAIL_FALLBACK_TO_GMAIL = (os.getenv("EMAIL_FALLBACK_TO_GMAIL") == "1")

# -----------------------------
# API keys
# -----------------------------
NEWS_API_KEY = (os.getenv("NEWS_API_KEY") or "").strip()
FMP_API_KEY = (os.getenv("FMP_API_KEY") or "").strip()
FINNHUB_API_KEY = (os.getenv("FINNHUB_API_KEY") or "").strip()

# -----------------------------
# Universe / Limits
# -----------------------------
TOP_N = int(os.getenv("TOP_N", "20"))
TRADE_MAX_PICKS = int(os.getenv("TRADE_MAX_PICKS", "3"))

# Midday movers
SUDDEN_MOVER_PCT_THRESHOLD = float(os.getenv("SUDDEN_MOVER_PCT_THRESHOLD", "2.0"))

# -----------------------------
# Scoring thresholds
# -----------------------------
SCORE_HIGH = int(os.getenv("SCORE_HIGH", "65"))
SCORE_MEDIUM = int(os.getenv("SCORE_MEDIUM", "45"))

# -----------------------------
# Confidence / gating (tune here, not in main.py)
# -----------------------------
MIN_CONFIDENCE_TO_TRADE = int(os.getenv("MIN_CONFIDENCE_TO_TRADE", "7"))

# Midday alert gate
SUDDEN_MOVER_MIN_CONFIDENCE = int(os.getenv("SUDDEN_MOVER_MIN_CONFIDENCE", "6"))

# Price affordability gates
MAX_PRICE = float(os.getenv("MAX_PRICE", "500"))
ELITE_SCORE_OVERRIDE = int(os.getenv("ELITE_SCORE_OVERRIDE", "92"))
ELITE_CONF_OVERRIDE = int(os.getenv("ELITE_CONF_OVERRIDE", "9"))

# -----------------------------
# Market-wide risk gates (optional usage in main.py)
# -----------------------------
VIX_SKIP_THRESHOLD = float(os.getenv("VIX_SKIP_THRESHOLD", "25.0"))
SPY_GAP_DOWN_SKIP_PCT = float(os.getenv("SPY_GAP_DOWN_SKIP_PCT", "-1.25"))
SPY_GAP_DOWN_TIGHTEN_PCT = float(os.getenv("SPY_GAP_DOWN_TIGHTEN_PCT", "-0.60"))
MAX_ALLOWED_VOLATILITY_P90 = float(os.getenv("MAX_ALLOWED_VOLATILITY_P90", "6.0"))
MARKET_DOWNSHIFT_BLOCK = (os.getenv("MARKET_DOWNSHIFT_BLOCK", "1") == "1")

# -----------------------------
# Postmarket evaluation knobs (NEW)
# -----------------------------
# Intraday interval for hit-checking: "1m","2m","5m","15m","30m","60m"
INTRADAY_INTERVAL = (os.getenv("INTRADAY_INTERVAL") or "5m").strip()

# When a single bar touches BOTH target and stop:
# - "stop_first" => conservative (treat as Stop hit)
# - "target_first" => optimistic (treat as Target hit)
CONSERVATIVE_SAME_BAR_POLICY = (os.getenv("CONSERVATIVE_SAME_BAR_POLICY") or "stop_first").strip().lower()

# Fallback behavior when intraday data is missing:
# - "1" => strict: do NOT count close >= target / close <= stop as hit (report Not Hit)
# - "0" => allow close_fallback as you currently do
EVAL_FALLBACK_STRICT = (os.getenv("EVAL_FALLBACK_STRICT") or "0").strip() == "1"

# Market session times (Chicago)
MARKET_OPEN_CT = time(8, 30)
MARKET_CLOSE_CT = time(15, 0)
POST_MARKET_START_CT = time(15, 10)

# -----------------------------
# Colors (HTML + Excel)
# -----------------------------
RISK_COLORS = {"Low": "#d4edda", "Medium": "#fff3cd", "High": "#f8d7da"}
TRADE_COLORS = {"✅ Preferable": "#d4edda", "⚠️ Moderate": "#fff3cd"}
SCORE_COLORS = {"Green": "#28a745", "Yellow": "#ffc107", "Red": "#dc3545"}

# -----------------------------
# Backward-compat placeholders (older code paths)
# -----------------------------
EXPECTED_UPSIDE_HIGH = 1.10
EXPECTED_UPSIDE_MEDIUM = 1.05
EXPECTED_DOWN = 0.98


def validate_config(require_email: bool = False) -> None:
    """
    Validate only what you need.
    - require_email=False: safe for premarket / data runs
    - require_email=True: enforce sender/receiver presence
    """
    missing = []

    if require_email:
        if not SENDER_EMAIL:
            missing.append("SENDER_EMAIL")
        if not (RECEIVER_EMAIL or LOCAL_RECEIVER_EMAIL):
            missing.append("RECEIVER_EMAIL (or LOCAL_RECEIVER_EMAIL)")

    if not missing:
        return

    msg = "Missing required env vars: " + ", ".join(missing)
    if IS_LOCAL:
        print("⚠️", msg, "(allowed in local)")
    else:
        raise RuntimeError(msg)


def print_config_summary() -> None:
    """
    Quick debug snapshot (no secrets).
    """
    recv = LOCAL_RECEIVER_EMAIL or RECEIVER_EMAIL
    print(f"🧩 CONFIG app_env={APP_ENV} provider={EMAIL_PROVIDER} receiver_set={bool(recv)}")
    print(
        f"🧩 gates: conf_trade>={MIN_CONFIDENCE_TO_TRADE} "
        f"midday_conf>={SUDDEN_MOVER_MIN_CONFIDENCE} mover_thr={SUDDEN_MOVER_PCT_THRESHOLD}% "
        f"max_price={MAX_PRICE}"
    )
    print(
        f"🧩 postmarket_eval: interval={INTRADAY_INTERVAL} same_bar_policy={CONSERVATIVE_SAME_BAR_POLICY} "
        f"fallback_strict={EVAL_FALLBACK_STRICT}"
    )