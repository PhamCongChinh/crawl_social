from uuid import uuid4
from beanie import Document
from typing import Literal, Optional
from datetime import datetime
from pydantic import BaseModel, Field

class JobModel(Document):
    id: str = Field(default_factory=lambda: str(uuid4()))
    channel_id: str
    crawl_type: Literal["tiktok" ,"channel", "post", "comment", "profile", "search"]
    trigger_type: Literal["cron", "interval"]
    cron: Optional[str] = None
    interval_seconds: Optional[int] = None
    status: Literal["active", "paused"] = "active"
    created_at: datetime = Field(default_factory=datetime.utcnow)

    class Settings:
        name = "tiktok_jobs"

