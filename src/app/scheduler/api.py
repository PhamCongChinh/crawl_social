

from fastapi import APIRouter, HTTPException

from app.scheduler.model import JobModel
from app.scheduler.models.jobs_log import JobLog
from app.scheduler.service import add_job, scheduler


router = APIRouter()

@router.post("")
async def create_job(job: JobModel):
    # Lưu vào Mongo
    await job.create()
    # Add vào scheduler
    # await add_job(job)
    return {"message": "Job created", "job_name": job.job_name}

@router.get("")
async def list_jobs():
    jobs = await JobModel.find_all().to_list()
    return jobs

@router.delete("/{job_id}")
async def delete_job(job_id: str):
    job = await JobModel.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    scheduler.remove_job(job_id)
    await job.delete()
    return {"message": "Job deleted"}

@router.post("/{job_id}/pause")
async def pause_job(job_id: str):
    scheduler.pause_job(job_id)
    await JobModel.find_one(JobModel.id == job_id).update({"$set": {"status": "paused"}})
    return {"message": "Job paused"}

@router.post("/{job_id}/resume")
async def resume_job(job_id: str):
    job = await JobModel.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    await add_job(job)
    await job.set({"status": "active"})
    return {"message": "Job resumed"}


@router.get("/{job_id}/logs")
async def get_logs(job_id: str):
    logs = await JobLog.find(JobLog.job_id == job_id).sort("-timestamp").to_list()
    return logs
