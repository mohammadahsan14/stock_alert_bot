# config.py
from dotenv import load_dotenv
import os

load_dotenv()

SENDER_EMAIL = os.getenv("SENDER_EMAIL")
APP_PASSWORD = os.getenv("APP_PASSWORD")
RECEIVER_EMAIL = os.getenv("RECEIVER_EMAIL")
NEWS_API_KEY = os.getenv("NEWS_API_KEY")
TOP_N = 50


# Colors
RISK_COLORS = {"Low": "#d4edda", "Medium": "#fff3cd", "High": "#f8d7da"}
TRADE_COLORS = {"✅ Preferable": "#d4edda", "⚠️ Moderate": "#fff3cd"}
SCORE_COLORS = {"Green": "#28a745", "Yellow": "#ffc107", "Red": "#dc3545"}

# Thresholds
SCORE_HIGH = 50
SCORE_MEDIUM = 25
EXPECTED_UPSIDE_HIGH = 1.10
EXPECTED_UPSIDE_MEDIUM = 1.05
EXPECTED_DOWN = 0.98