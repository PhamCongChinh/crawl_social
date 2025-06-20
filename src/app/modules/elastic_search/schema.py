from typing import Optional
from pydantic import BaseModel, Field, field_validator, validator

class ElasticSearchRequest(BaseModel):
    # id: Optional[str] = Field(default=None, min_length=1, max_length=500)
    # doc_type: int = Field(default=1)                 # 1 POST 2 COMMENT
    # crawl_source: Optional[int] = None              # 1 FACEBOOK, 2 TIKTOK, 3 YOUTUBE
    # crawl_source_code: Optional[str] = None         # 'tt', 'fb', 'yt', 'web'
    # pub_time: Optional[int] = None                  # 12141231
    # crawl_time: Optional[int] = None
    # subject_id: Optional[str] = ""
    # title: Optional[str] = None
    # description: Optional[str] = None
    # content: Optional[str] = ""
    # url: Optional[str] = None                       # https://www.tiktok.com/@officialhanoifc/video/7412644913587801351
    # media_urls: Optional[str] = Field(default='[]')
    # comments: Optional[int] = 0
    # shares: Optional[int] = 0
    # reactions: Optional[int] = 0
    # favors: Optional[int] = 0
    # views: Optional[int] = 0
    # web_tags: Optional[str] = Field(default='[]')
    # web_keywords: Optional[str] = Field(default='[]')
    # auth_id: str = Field(max_length=3000)               # author id
    # auth_name: str = Field(max_length=3000)             # author name
    # auth_type: int             # Chưa rõ
    # auth_url: Optional[str] = None              # Author avatarlarge
    # source_id: str = Field(max_length=3000)
    # source_type: Optional[int] = None       # 5 Tiktok
    # source_name: str = Field(max_length=3000)      # "VFF Official"
    # source_url: str = Field(max_length=3000)        # "https://www.tiktok.com/@vff.official"
    # reply_to: Optional[str] = None
    # level: Optional[int] = None
    # org_id: int
    # sentiment: int = Field(default=0)
    # isPriority: bool       # Nguồn ưu tiên
    # crawl_bot: str        # Tên bot

    id: Optional[str] = Field(default=None, min_length=1)
    doc_type: int = Field(default=1)  # Giả sử POST = 1
    source_type: int
    crawl_source: int = Field(default=2)  # Giả sử FACEBOOK = 1
    crawl_source_code: str = Field(default='tt')  # enum chỉ có 'fb'
    pub_time: int
    crawl_time: int
    subject_id: Optional[str] = Field(default='', max_length=255)
    title: Optional[str] = None
    description: Optional[str] = None
    content: Optional[str] = Field(default='')
    url: str = Field(max_length=3000)
    media_urls: Optional[str] = Field(default='[]')
    comments: int = Field(default=0, ge=0)
    shares: int = Field(default=0, ge=0)
    reactions: int = Field(default=0, ge=0)
    favors: int = Field(default=0, ge=0)
    views: int = Field(default=0, ge=0)
    web_tags: Optional[str] = Field(default='[]')
    web_keywords: Optional[str] = Field(default='[]')
    auth_id: str = Field(max_length=3000)
    auth_name: str = Field(max_length=3000)
    auth_type: int
    auth_url: str = Field(max_length=3000)
    source_id: str = Field(max_length=3000)
    source_name: str = Field(max_length=3000)
    source_url: str = Field(max_length=3000)
    reply_to: Optional[str] = None
    level: Optional[int] = None
    org_id: int
    sentiment: int = Field(default=0)
    isPriority: bool
    crawl_bot: str

    @field_validator(
        "id", "subject_id", "title", "description", "content", "url", "media_urls",
        "web_tags", "web_keywords", "auth_id", "auth_name", "auth_url",
        "source_id", "source_name", "source_url", "reply_to", "crawl_bot"
    )
    @classmethod
    def trim_strings(cls, v):
        if isinstance(v, str):
            return v.strip()
        return v