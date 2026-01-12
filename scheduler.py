import schedule
import time
import logging
from main import main
from datetime import datetime

# -----------------------------
# Logging configuration
# -----------------------------
logging.basicConfig(
    filename="outputs/stock_alert_log.txt",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# -----------------------------
# Wrapper with error handling
# -----------------------------
def run_main():
    try:
        logging.info("Running stock alert bot...")
        main()
        logging.info("Stock alert executed successfully ✅")
    except Exception as e:
        logging.error(f"Stock alert failed ❌: {e}")

# -----------------------------
# Schedule jobs (8:30 AM ET, Mon-Fri)
# -----------------------------
schedule.every().monday.at("08:30").do(run_main)
schedule.every().tuesday.at("08:30").do(run_main)
schedule.every().wednesday.at("08:30").do(run_main)
schedule.every().thursday.at("08:30").do(run_main)
schedule.every().friday.at("08:30").do(run_main)

logging.info("Scheduler started, awaiting next run...")

# -----------------------------
# Keep the script running
# -----------------------------
while True:
    schedule.run_pending()
    time.sleep(30)