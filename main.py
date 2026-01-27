# main.py
from __future__ import annotations

import argparse
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from config import APP_ENV, IS_LOCAL, print_config_summary
from premarket_runner import run_premarket
from midday_runner import run_midday
from postmarket_runner import run_postmarket
from options.option_mode import run_options_mode

LOCAL_TZ = ZoneInfo("America/Chicago")

# Normal trading = premarket + midday + postmarket
NORMAL_MODES = {"premarket", "midday", "postmarket"}


def _is_locked(flag: bool, env_key: str) -> bool:
    """
    Lock can be turned on via:
      - CLI flag (highest priority), or
      - environment variable set to 1/true/yes/on
    """
    if flag:
        return True
    v = os.getenv(env_key, "").strip().lower()
    return v in {"1", "true", "yes", "y", "on"}


def main() -> None:
    parser = argparse.ArgumentParser(description="Stock Alert Bot Runner")

    parser.add_argument(
        "--mode",
        choices=["premarket", "midday", "postmarket", "options", "all"],
        required=True,
        help="Which runner to execute",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run without sending emails (safe test)",
    )

    # ✅ NEW: Locks (you can lock normal trading and/or options trading)
    parser.add_argument(
        "--lock-normal",
        action="store_true",
        help="Lock normal trading (premarket/midday/postmarket). Overrides env LOCK_NORMAL_TRADING.",
    )
    parser.add_argument(
        "--lock-options",
        action="store_true",
        help="Lock options trading. Overrides env LOCK_OPTIONS_TRADING.",
    )

    args = parser.parse_args()

    # Startup banner
    print(f"🧪 APP_ENV={APP_ENV} | IS_LOCAL={IS_LOCAL}")
    print_config_summary()

    # Make --dry-run actually prevent emails across ALL runners
    if args.dry_run:
        os.environ["EMAIL_DRY_RUN"] = "1"
        print("🧪 DRY RUN ENABLED (emails will not be sent)")

    # Resolve locks (CLI flag OR env var)
    normal_locked = _is_locked(args.lock_normal, "LOCK_NORMAL_TRADING")
    options_locked = _is_locked(args.lock_options, "LOCK_OPTIONS_TRADING")

    print(
        f"🔒 Locks: normal_trading={'ON' if normal_locked else 'OFF'} "
        f"| options_trading={'ON' if options_locked else 'OFF'}"
    )

    now = datetime.now(LOCAL_TZ)

    # ---- ROUTING with LOCKS ----
    def run_mode(mode: str) -> None:
        # Block normal trading modes if locked
        if mode in NORMAL_MODES and normal_locked:
            print(f"⛔ NORMAL TRADING LOCKED → skipping: {mode}")
            return

        # Block options mode if locked
        if mode == "options" and options_locked:
            print("⛔ OPTIONS TRADING LOCKED → skipping: options")
            return

        # Execute
        if mode == "premarket":
            run_premarket(now)
        elif mode == "midday":
            run_midday(now)
        elif mode == "postmarket":
            run_postmarket(now)
        elif mode == "options":
            run_options_mode(now=now, dry_run=args.dry_run)

    if args.mode == "all":
        print("🚀 Running ALL modes in sequence (respecting locks)")

        print("\n🌅 PREMARKET")
        run_mode("premarket")

        print("\n⚡ MIDDAY")
        run_mode("midday")

        print("\n🧾 OPTIONS")
        run_mode("options")

        print("\n📊 POSTMARKET")
        run_mode("postmarket")
    else:
        run_mode(args.mode)

    print("\n✅ Run complete")


if __name__ == "__main__":
    main()