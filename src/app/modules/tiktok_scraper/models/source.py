from typing import Optional
from beanie import Document
from pymongo import ASCENDING, IndexModel

class SourceModel(Document):
    org_id: Optional[int] = None
    source_name: Optional[str] = None
    source_type: Optional[int] = None
    source_url: Optional[str] = None
    source_channel: Optional[str] = None
    last_success_post_url: Optional[str] = None

    class Settings:
        name = "tiktok_sources"
        indexes = [
            IndexModel([("source_url", ASCENDING)], unique=True)
        ]