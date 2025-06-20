from fastapi import HTTPException
from app.modules.scheduler.model import JobModel
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from app.tasks.crawl_tiktok import crawl_tiktok

scheduler = AsyncIOScheduler()

async def add_job(metadata: JobModel):
    job_id = metadata.id
    # ✅ Chuẩn hóa trigger
    try:
        if metadata.trigger_type == "cron":
            if not metadata.cron:
                raise HTTPException(status_code=400, detail="Thiếu cron expression cho trigger_type='cron'")
            trigger = CronTrigger.from_crontab(metadata.cron)
        elif metadata.trigger_type == "interval":
            if not metadata.interval_seconds:
                raise HTTPException(status_code=400, detail="Thiếu interval_seconds cho trigger_type='interval'")
            trigger = IntervalTrigger(seconds=metadata.interval_seconds)
        else:
            raise ValueError("trigger_type phải là 'cron' hoặc 'interval'")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Lỗi định dạng trigger: {e}")

    # ✅ Đăng ký job
    try:
        scheduler.add_job(
            func=crawl_tiktok.delay,  # Gửi task sang Celery
            trigger=trigger,
            id=job_id,
            args=[job_id, metadata.channel_id],
            name=f"{metadata.channel_id}-{metadata.crawl_type}",
            replace_existing=True,
            misfire_grace_time=30 # cho phép job trễ 30s vẫn chạy
        )
        print(f"✅ Job {job_id} đã được lập lịch ({'cron' if metadata.cron else 'interval'})")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi khi thêm job: {e}")