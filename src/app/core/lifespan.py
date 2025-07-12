from fastapi import FastAPI

from contextlib import asynccontextmanager
from app.config import mongo_connection
from app.config import postgres_connection

from app.scheduler.model import JobModel
from app.scheduler.service import scheduler, add_job

import logging
log = logging.getLogger(__name__) 

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Bắt đầu...")
    await mongo_connection.connect()
    await postgres_connection.connect()

    # Start APScheduler
    scheduler.start()
    # Khôi phục các job đã lưu
    jobs_in_db = await JobModel.find_all().to_list()
    for job in jobs_in_db:
        if job.status == "active":
            await add_job(job)
    log.info("Scheduler đã được bật")
    yield
    scheduler.shutdown()
    await mongo_connection.disconnect()
    await postgres_connection.close()
    log.info("Tắt ...")