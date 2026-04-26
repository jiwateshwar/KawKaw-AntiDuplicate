import logging
from datetime import datetime, timedelta, timezone
from typing import Callable

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger

logger = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None
_JOB_ID = "daily_scan"


def start_scheduler(scan_schedule: str, scan_job_fn: Callable, tz: str = "UTC") -> None:
    global _scheduler
    _scheduler = BackgroundScheduler(timezone=tz)

    hour, minute = _parse_schedule(scan_schedule)
    _scheduler.add_job(
        scan_job_fn,
        trigger=CronTrigger(hour=hour, minute=minute, timezone=tz),
        id=_JOB_ID,
        replace_existing=True,
        misfire_grace_time=3600,
    )
    _scheduler.start()
    logger.info(f"Scheduler started. Daily scan at {hour:02d}:{minute:02d} ({tz})")


def stop_scheduler() -> None:
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")


def trigger_immediate_scan(scan_job_fn: Callable) -> None:
    global _scheduler
    if not _scheduler:
        return
    run_at = datetime.now(timezone.utc) + timedelta(seconds=1)
    _scheduler.add_job(
        scan_job_fn,
        trigger=DateTrigger(run_date=run_at),
        id="immediate_scan",
        replace_existing=True,
    )
    logger.info("Immediate scan scheduled")


def reschedule_job(scan_schedule: str, scan_job_fn: Callable, tz: str = "UTC") -> None:
    global _scheduler
    if not _scheduler:
        return
    hour, minute = _parse_schedule(scan_schedule)
    _scheduler.reschedule_job(
        _JOB_ID,
        trigger=CronTrigger(hour=hour, minute=minute, timezone=tz),
    )
    logger.info(f"Scan rescheduled to {hour:02d}:{minute:02d} ({tz})")


def _parse_schedule(schedule: str) -> tuple[int, int]:
    """Parse 'HH:MM' into (hour, minute). Defaults to 02:30 on error."""
    try:
        parts = schedule.strip().split(":")
        return int(parts[0]), int(parts[1])
    except Exception:
        logger.warning(f"Invalid scan schedule '{schedule}', defaulting to 02:30")
        return 2, 30
