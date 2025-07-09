from typing import Optional
from pydantic import BaseModel


class CrawlPostBackdateRequest(BaseModel):
    from_date: Optional[int] = None  # Unix
    to_date: Optional[int] = None