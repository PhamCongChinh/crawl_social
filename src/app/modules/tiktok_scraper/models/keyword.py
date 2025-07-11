from typing import Optional
from beanie import Document, Indexed
from pydantic import Field
from pymongo import ASCENDING, IndexModel

class KeywordModel(Document):
    keyword: str
    org_id: Optional[int] = None
    source_crawl: Optional[int] = None
    source_name: Optional[str] = None
    source_type: Optional[int] = None
    source_channel: Optional[str] = None
    last_success_post_url: Optional[str] = None

    class Settings:
        name = "tiktok_keywords"