from datetime import datetime
from typing import Optional
from bson import ObjectId
from pydantic import BaseModel, Field

from app.utils.timezone import now_vn

class VideoResponse(BaseModel):
    # id: str = Field(..., alias="_id")  # Convert ObjectId -> str
    video_id: str
    video_url: str
    # contents: Optional[str] = None
    create_time: Optional[int] = None
    
    org_id: Optional[int] = None
    source_type: Optional[int] = None
    source_name: Optional[str] = None
    source_url: Optional[str] = None
    source_channel: Optional[str] = None

    status: str = Field("pending", description="pending|crawling|done|error")
    # created_at: datetime = Field(default_factory=now_vn)
    # updated_at: datetime = Field(default_factory=now_vn)

    class Config:
        populate_by_name = True
        # json_encoders = {ObjectId: str}