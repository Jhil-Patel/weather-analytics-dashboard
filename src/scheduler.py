"""
scheduler.py
------------
Runs the weather fetcher automatically every 60 minutes using APScheduler.
Logs each run to logs/scheduler.log for auditing.

Run:  python src/scheduler.py
      (keep this terminal open; Ctrl-C to stop)
"""

import os
import sys
import logging
import datetime
from pathlib import Path

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from dotenv import load_dotenv

load_dotenv()

# ── Logging ──────────────────────────────────────────────────────────────────
LOG_DIR  = Path(__file__).parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "scheduler.log"

logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("weather_scheduler")


# ── Import fetch function ─────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent))
from fetch_weather import run_fetch


# ── Job definition ────────────────────────────────────────────────────────────
def job():
    log.info("Scheduled job triggered.")
    try:
        n = run_fetch()
        log.info(f"Job completed. {n} cities updated.")
    except Exception as exc:
        log.error(f"Job failed: {exc}", exc_info=True)


def on_job_event(event):
    if event.exception:
        log.error(f"Job raised an exception: {event.exception}")
    else:
        log.info("Job finished without errors.")


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(job, "interval", minutes=60, id="weather_fetch",
                      next_run_time=datetime.datetime.utcnow())  # run immediately too
    scheduler.add_listener(on_job_event, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

    log.info("=" * 50)
    log.info("  Weather Scheduler Started")
    log.info("  Interval: every 60 minutes")
    log.info(f"  Log file: {LOG_FILE}")
    log.info("  Press Ctrl-C to stop")
    log.info("=" * 50)

    try:
        scheduler.start()
    except KeyboardInterrupt:
        log.info("Scheduler stopped by user.")
