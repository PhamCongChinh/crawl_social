from typing import Optional
from beanie import Document
from datetime import datetime
from pydantic import Field
from app.utils.timezone import now_vn

class VideoModel(Document):
    video_id: str
    video_url: str
    contents: Optional[str] = None
    create_time: Optional[int] = None
    
    org_id: Optional[int] = None
    source_type: Optional[int] = None
    source_name: Optional[str] = None
    source_url: Optional[str] = None
    source_channel: Optional[str] = None

    status: str = Field("pending", description="pending|crawling|done|error")
    created_at: datetime = Field(default_factory=now_vn)
    updated_at: datetime = Field(default_factory=now_vn)

    class Settings:
        name = "tiktok_videos"
        # Tạo index unique trên channel_id
        indexes = [
            "video_url",  # tự động non-unique nếu muốn, thêm unique bên dưới
        ]