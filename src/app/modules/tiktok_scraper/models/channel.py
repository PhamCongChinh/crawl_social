from typing import Optional
from beanie import Document
from datetime import datetime, timezone

from pydantic import BaseModel, Field, field_validator

from app.utils.timezone import now_vn

class ChannelStats(BaseModel):
    collect_count: Optional[int] = Field(None, alias="collectCount")
    comment_count: Optional[int] = Field(None, alias="commentCount")
    digg_count: Optional[int] = Field(None, alias="diggCount")
    play_count: Optional[int] = Field(None, alias="playCount")
    share_count: Optional[int] = Field(None, alias="shareCount")

class ChannelModel(Document):
    id: Optional[str] = Field(None, alias="_id")  # map với _id MongoDB
    desc: Optional[str] = None
    contents: Optional[str] = None
    createTime: Optional[int] = None
    stats: Optional[ChannelStats] = None

    org_id: Optional[int] = None
    source_type: Optional[int] = None
    source_name: Optional[str] = None
    source_url: Optional[str] = None
    source_channel: Optional[str] = None
    crawled: Optional[int] = 0  # 0: chưa crawl, 1: đã crawl, 2: đã crawl comments
    status: str = "pending" # pending, crawling, done, error
    updated_at: Optional[datetime] = None

    # @field_validator("create_time", mode="before")
    # @classmethod
    # def convert_timestamp(cls, v):
    #     if isinstance(v, int):
    #         return datetime.fromtimestamp(v)
    #     return v

    class Settings:
        name = "tiktok_channels"