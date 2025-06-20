from beanie import Document
from pydantic import BaseModel, Field, field_validator
from typing import List, Optional
from datetime import datetime, date

# class Video(BaseModel):
#     duration: Optional[int] = None
#     ratio: Optional[str] = None
#     cover: Optional[str] = None
#     playAddr: Optional[str] = None
#     downloadAddr: Optional[str] = None
#     bitrate: Optional[int] = None

# class Author(BaseModel):
#     id: Optional[str] = None
#     uniqueId: Optional[str] = None
#     nickname: Optional[str] = None
#     avatarLarger: Optional[str] = None
#     signature: Optional[str] = None
#     verified: Optional[bool] = None

# class Stats(BaseModel):
#     diggCount: Optional[int] = None
#     shareCount: Optional[int] = None
#     commentCount: Optional[int] = None
#     playCount: Optional[int] = None
#     collectCount: Optional[str] = None

# class TextExtra(BaseModel):
#     hashtag_name: Optional[str] = Field(None, alias="hashtagName")

# class ContentBlock(BaseModel):
#     text_extra: Optional[List[TextExtra]] = Field(None, alias="textExtra")

# class TiktokChannel(BaseModel):
#     contents: Optional[List[ContentBlock]] = None

class PostModel(Document):
    # id: Optional[str] = Field(None, alias="_id")  # map với _id MongoDB
    # desc: Optional[str] = None
    # create_time: Optional[datetime] = None
    # updated_at: Optional[datetime] = None
    # video: Optional[Video] = None
    # author: Optional[Author] = None
    # stats: Optional[Stats] = None
    # diversificationLabels: Optional[List[str]] = None
    # suggestedWords: Optional[List[str]] = None

    id: Optional[str] = Field(None, alias="_id")  # map với _id MongoDB
    doc_type: Optional[int] = None                  # 1 POST 2 COMMENT
    crawl_source: Optional[int] = None              # 1 FACEBOOK, 2 TIKTOK, 3 YOUTUBE
    crawl_source_code: Optional[str] = None         # 'tt', 'fb', 'yt', 'web'
    pub_time: Optional[int] = None                  # 12141231
    crawl_time: Optional[int] = None
    subject_id: Optional[str] = ""
    title: Optional[str] = None
    description: Optional[str] = None
    content: Optional[str] = ""
    url: Optional[str] = None                       # https://www.tiktok.com/@officialhanoifc/video/7412644913587801351
    media_urls: Optional[str] = "[]"
    comments: Optional[int] = 0
    shares: Optional[int] = 0
    reactions: Optional[int] = 0
    favors: Optional[int] = 0
    views: Optional[int] = 0
    web_tags: Optional[str] = "[]"
    web_keywords: Optional[str] = "[]"
    auth_id: Optional[str] = None               # author id
    auth_name: Optional[str] = None             # author name
    auth_type: Optional[int] = None             # Chưa rõ
    auth_url: Optional[str] = None              # Author avatarlarge
    source_id: Optional[str] = None
    source_type: Optional[int] = None       # 5 Tiktok
    source_name: Optional[str] = None       # "VFF Official"
    source_url: Optional[str] = None        # "https://www.tiktok.com/@vff.official"
    reply_to: Optional[str] = None
    level: Optional[int] = 0
    org_id: Optional[int] = None
    sentiment: Optional[int] = 0
    isPriority: Optional[bool] = None       # Nguồn ưu tiên
    crawl_bot: Optional[str] = None         # Tên bot

    # --- Validators ---
    # @field_validator("create_time", mode="before")
    # @classmethod
    # def parse_create_time(cls, v):
    #     if isinstance(v, int):
    #         return datetime.fromtimestamp(v)
    #     return v

    # @field_validator("updated_at", mode="before")
    # @classmethod
    # def parse_updated_at(cls, v):
    #     if isinstance(v, str):
    #         try:
    #             return datetime.strptime(v, "%Y-%m")
    #         except:
    #             pass
    #     elif isinstance(v, int):
    #         return datetime.fromtimestamp(v)
    #     return v

    # # --- Helper property ---
    # @property
    # def hashtags(self) -> List[str]:
    #     if not self.contents:
    #         return []
    #     return [
    #         tag.hashtag_name
    #         for content in self.contents
    #         if content.text_extra
    #         for tag in content.text_extra
    #         if tag.hashtag_name
    #     ]


    

    class Settings:
        name = "tiktok_posts"