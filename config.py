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
TOP_N = 20                 # how many movers you analyze & store in Excel
EMAIL_TOP_PER_CATEGORY = 10 # email per category (but we’ll still cap total picks)
TRADE_MAX_PICKS = 5         # the real “I might trade these” list size (2–5 is best)

# -----------------------------
# Colors (used for HTML + Excel)
# -----------------------------
RISK_COLORS = {"Low": "#d4edda", "Medium": "#fff3cd", "High": "#f8d7da"}
TRADE_COLORS = {"✅ Preferable": "#d4edda", "⚠️ Moderate": "#fff3cd"}
SCORE_COLORS = {"Green": "#28a745", "Yellow": "#ffc107", "Red": "#dc3545"}

# -----------------------------
# Scoring thresholds
# -----------------------------
SCORE_HIGH = 50
SCORE_MEDIUM = 25

EXPECTED_UPSIDE_HIGH = 1.10
EXPECTED_UPSIDE_MEDIUM = 1.05
EXPECTED_DOWN = 0.98

# -----------------------------
# Trust / Quality gates
# -----------------------------
MIN_CONFIDENCE_TO_TRADE = 8     # only allow “Approved Picks” if >= 8
MIN_DATA_QUALITY_TO_TRADE = 8   # quality score 1–10 (we will compute in main.py)
ALLOW_FALLBACK_DATA = False     # if True, bot can send picks even when it used sample data (not recommended)

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