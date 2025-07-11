from typing import Optional
from pydantic import BaseModel


class CreateKeywordRequest(BaseModel):
    keyword: str
    org_id: int
    source_crawl: Optional[int]
    source_name: Optional[str]
    source_type: Optional[int]
    source_channel: Optional[str]