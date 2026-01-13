# config.py
from dotenv import load_dotenv
import os

load_dotenv()

# -----------------------------
# Secrets / Env
# -----------------------------
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
APP_PASSWORD = os.getenv("APP_PASSWORD")
RECEIVER_EMAIL = os.getenv("RECEIVER_EMAIL")

NEWS_API_KEY = os.getenv("NEWS_API_KEY")
FMP_API_KEY = os.getenv("FMP_API_KEY")
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")

# -----------------------------
# Universe / Limits
# -----------------------------
TOP_N = 20                   # how many movers you analyze & store in Excel
EMAIL_TOP_PER_CATEGORY = 10  # if you later expand email grouping
TRADE_MAX_PICKS = 3          # keep small for trust (2–5 is best)

# -----------------------------
# Colors (used for HTML + Excel)
# -----------------------------
RISK_COLORS = {"Low": "#d4edda", "Medium": "#fff3cd", "High": "#f8d7da"}
TRADE_COLORS = {"✅ Preferable": "#d4edda", "⚠️ Moderate": "#fff3cd"}
SCORE_COLORS = {"Green": "#28a745", "Yellow": "#ffc107", "Red": "#dc3545"}

# -----------------------------
# Scoring thresholds (more selective = more trustworthy)
# -----------------------------
SCORE_HIGH = 65
SCORE_MEDIUM = 45

# -----------------------------
# (Deprecated) Old upside factors
# You switched to ATR-based forecast_engine.py
# Keep these only so old code paths don't crash.
# -----------------------------
EXPECTED_UPSIDE_HIGH = 1.10
EXPECTED_UPSIDE_MEDIUM = 1.05
EXPECTED_DOWN = 0.98

# -----------------------------
# Trust / Quality gates
# -----------------------------
MIN_CONFIDENCE_TO_TRADE = 7      # 6–7 is realistic; 8 may block most days
MIN_DATA_QUALITY_TO_TRADE = 8    # future use (if you compute quality)
ALLOW_FALLBACK_DATA = False      # good: don't send if data was fake

# -----------------------------
# Mid-day sudden movers alert
# -----------------------------
SUDDEN_MOVER_PCT_THRESHOLD = 3.0  # alert if abs(% change) >= this
SUDDEN_MOVER_MIN_CONFIDENCE = 7   # only alert if confidence is meaningful

# -----------------------------
# Post-market timing (CST)
# -----------------------------
POST_MARKET_START_HOUR = 15  # 3 PM CST market close
POST_MARKET_START_MIN = 10   # buffer after close (3:10 PM CST)

# -----------------------------
# File names
# -----------------------------
DAILY_LOG_CSV = "daily_stock_log.csv"