from typing import Optional
from beanie import Document
from pymongo import ASCENDING, IndexModel

class KeywordModel(Document):
    keyword: Optional[str] = None
    org_id: Optional[int] = None
    source_name: Optional[str] = None
    source_type: Optional[int] = None
    source_channel: Optional[str] = None
    last_success_post_url: Optional[str] = None

    class Settings:
        name = "tiktok_keywords"