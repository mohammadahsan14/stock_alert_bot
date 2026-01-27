#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$APP_DIR"

APP_ENV="${APP_ENV:-local}"
MODE="${1:-premarket}"
[[ -z "${MODE}" ]] && MODE="premarket"

case "$MODE" in
  premarket|midday|postmarket) ;;
  *)
    echo "Invalid mode: '$MODE' (use: premarket | midday | postmarket)" >&2
    exit 2
    ;;
esac

# Activate venv (assumes local venv is .venv in repo)
if [[ ! -f "$APP_DIR/.venv/bin/activate" ]]; then
  echo "❌ Virtualenv not found at $APP_DIR/.venv" >&2
  exit 1
fi
source "$APP_DIR/.venv/bin/activate"

LOG_DIR="$APP_DIR/outputs/$APP_ENV"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/cron.log"

# -------------------------------------------------
# Locking (portable: works on macOS + Linux)
# -------------------------------------------------
LOCK_DIR="$LOG_DIR/run.lockdir"

cleanup_lock() {
  rm -rf "$LOCK_DIR" 2>/dev/null || true
}
trap cleanup_lock EXIT INT TERM

TS="$(date '+%Y-%m-%d %H:%M:%S')"

# mkdir is atomic: if it fails, someone else is running
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "[$TS] SKIP: Another run is already in progress (APP_ENV=$APP_ENV, mode=$MODE)" >> "$LOG_FILE"
  exit 0
fi

echo "[$TS] Running mode=$MODE (APP_ENV=$APP_ENV)" >> "$LOG_FILE"
python main.py --mode "$MODE" >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

echo "[$TS] Done mode=$MODE (exit=$EXIT_CODE)" >> "$LOG_FILE"
echo "-----------------------------" >> "$LOG_FILE"
exit $EXIT_CODE
