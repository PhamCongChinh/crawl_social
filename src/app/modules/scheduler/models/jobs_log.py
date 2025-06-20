from beanie import Document
from datetime import datetime
from pydantic import Field
from typing import Optional, Literal

class JobLog(Document):
    job_id: str
    status: Literal["success", "error"]
    message: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    class Settings:
        name = "tiktok_job_logs"
