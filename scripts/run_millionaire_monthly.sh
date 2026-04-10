#!/bin/zsh

cd /Users/mohammadahsan/PycharmProjects/stock_alert_bot || exit 1

echo "[$(date)] run_millionaire_monthly.sh started"

export APP_ENV=local

/Users/mohammadahsan/PycharmProjects/stock_alert_bot/.venv/bin/python -c "from longterm_strategy import update_millionaire_tracker; update_millionaire_tracker(0)"

echo "[$(date)] run_millionaire_monthly.sh finished"