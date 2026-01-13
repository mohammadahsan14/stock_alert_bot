# scheduler.py
import schedule
import time
import logging
import subprocess
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import atexit

# -----------------------------
# Lock (prevents multiple schedulers)
# -----------------------------
LOCK_FILE = Path("outputs/scheduler.lock")
LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)

def _pid_is_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)  # check if process exists
        return True
    except OSError:
        return False

if LOCK_FILE.exists():
    try:
        existing_pid = int(LOCK_FILE.read_text().strip())
    except Exception:
        existing_pid = None

    if existing_pid and _pid_is_running(existing_pid):
        print(f"❌ Scheduler already running (PID {existing_pid}). Exiting.")
        sys.exit(1)
    else:
        # stale lock
        try:
            LOCK_FILE.unlink()
        except Exception:
            pass

LOCK_FILE.write_text(str(os.getpid()))

@atexit.register
def _cleanup_lock():
    try:
        LOCK_FILE.unlink(missing_ok=True)
    except Exception:
        pass

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    filename="outputs/stock_alert_log.txt",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a",
)

# -----------------------------
# Runner
# -----------------------------
PY = sys.executable
MAIN = str(Path(__file__).with_name("main.py"))

STARTED_AT = datetime.now()
GRACE_MINUTES = 2          # allow small delay (machine lag)
CATCHUP_WINDOW_MIN = 5     # only block catch-up during first N minutes after start

def _too_late_for_today(target_hhmm: str) -> bool:
    """
    Prevent 'catch-up' runs if you start the scheduler after the target time.
    Only blocks during the first few minutes after scheduler startup.
    """
    now = datetime.now()
    target = datetime.strptime(target_hhmm, "%H:%M").replace(
        year=now.year, month=now.month, day=now.day
    )
    missed = now > (target + timedelta(minutes=GRACE_MINUTES))
    just_started = (now - STARTED_AT) < timedelta(minutes=CATCHUP_WINDOW_MIN)
    return missed and just_started

def run_mode(mode: str):
    # prevent catch-up runs right after starting scheduler late
    if mode == "premarket" and _too_late_for_today("08:30"):
        logging.info("Skipping catch-up premarket (started after 08:30)")
        return
    if mode == "midday" and _too_late_for_today("12:00"):
        logging.info("Skipping catch-up midday (started after 12:00)")
        return
    if mode == "postmarket" and _too_late_for_today("15:15"):
        logging.info("Skipping catch-up postmarket (started after 15:15)")
        return

    try:
        logging.info(f"Running mode={mode} ...")
        logging.info(f"CMD: {PY} {MAIN} --mode {mode}")

        result = subprocess.run(
            [PY, MAIN, "--mode", mode],
            capture_output=True,
            text=True,
            timeout=60 * 20,  # 20 minutes safety timeout
        )

        logging.info(f"mode={mode} exit={result.returncode}")

        if result.stdout:
            logging.info(f"STDOUT:\n{result.stdout}")
        if result.stderr:
            logging.error(f"STDERR:\n{result.stderr}")

    except subprocess.TimeoutExpired:
        logging.error(f"⏰ Timeout: mode={mode} took too long and was killed.")
    except Exception as e:
        logging.error(f"Run failed for mode={mode}: {e}")

# -----------------------------
# Schedule (LOCAL machine time - Chicago)
# -----------------------------
for day in ["monday", "tuesday", "wednesday", "thursday", "friday"]:
    getattr(schedule.every(), day).at("08:30").do(run_mode, "premarket")
    getattr(schedule.every(), day).at("12:00").do(run_mode, "midday")
    getattr(schedule.every(), day).at("15:15").do(run_mode, "postmarket")

logging.info(f"Scheduler started ✅ pid={os.getpid()} | awaiting next run...")

# -----------------------------
# Main loop + heartbeat
# -----------------------------
last_heartbeat = datetime.now()

while True:
    schedule.run_pending()

    # Heartbeat every 10 minutes so you know it's alive
    if datetime.now() - last_heartbeat > timedelta(minutes=10):
        logging.info(f"Heartbeat ❤️ pid={os.getpid()} still running")
        last_heartbeat = datetime.now()

    time.sleep(10)