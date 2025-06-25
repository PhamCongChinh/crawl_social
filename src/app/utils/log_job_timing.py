from datetime import datetime, timezone
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import logging

log = logging.getLogger("apscheduler")
scheduler = AsyncIOScheduler()  # hoặc dùng instance thật nếu bạn đã có sẵn

def log_job_timing(job_id: str):
    job = scheduler.get_job(job_id)
    now = datetime.now(timezone.utc)

    if job and job.next_run_time:
        seconds = (job.next_run_time - now).total_seconds()
        minutes = seconds // 60
        log.info(f"⏳ Job '{job_id}' sẽ chạy lại sau {int(minutes)} phút {int(seconds % 60)} giây")
    else:
        log.warning(f"⚠️ Không tìm thấy next_run_time cho job '{job_id}'")