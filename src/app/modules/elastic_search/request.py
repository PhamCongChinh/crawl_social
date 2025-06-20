from datetime import datetime
import json
from typing import Any, Dict
from zoneinfo import ZoneInfo
from bson import Int64
from app.modules.elastic_search.schema import ElasticSearchRequest
from app.modules.tiktok_scraper.models.channel import ChannelModel

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

def dataToES(raw: Dict[str, Any], channel: Any) -> dict:
    # return ElasticSearchRequest(
    #     # post_id = getattr(channel, "id", ""),
    #     post_id='https://www.tiktok.com/@officialhanoifc/video/7354034475514285330',
    #     doc_type=1,
    #     crawl_source=2,
    #     crawl_source_code= getattr(channel, "source_channel", "tt"),
    #     pub_time= Int64(int(raw.get("createTime", 0))),
    #     crawl_time= int(datetime.now(VN_TZ).timestamp()),
    #     org_id= getattr(channel, "org_id", 0),
    #     subject_id= raw.get("subject_id", ""),
    #     title= raw.get("subject_id", None),
    #     description= raw.get("desc", ""),
    #     content= raw.get("desc", ""),
    #     url= f"https://www.tiktok.com/@{raw.get('author', {}).get('uniqueId', '')}/video/{raw['id']}",
    #     media_urls= "[]",
    #     comments=raw.get("stats", {}).get("commentCount", 0),
    #     shares=raw.get("stats", {}).get("shareCount", 0),
    #     reactions=raw.get("stats", {}).get("diggCount", 0),
    #     favors= int(raw.get("stats", {}).get("collectCount", 0) or 0),
    #     views=raw.get("stats", {}).get("playCount", 0),
    #     web_tags= json.dumps(raw.get("diversificationLabels", [])),
    #     web_keywords=json.dumps(raw.get("web_keywords", [])),
    #     auth_id= raw.get("author", {}).get("id", ""),
    #     auth_name=raw.get("author", {}).get("nickname", ""),
    #     auth_type=1,
    #     # auth_url=raw.get("author", {}).get("avatarLarger", ""),
    #     auth_url="",

    #     source_id=f"https://www.tiktok.com/@{raw.get('author', {}).get('uniqueId', '')}",
    #     source_type=getattr(channel, "source_type", 0),
    #     source_name=raw.get("author", {}).get("nickname", ""),
    #     source_url=getattr(channel, "source_url", ""), #channel.source_url,

    #     reply_to=raw.get("reply_to", None),
    #     level=raw.get("level", None) ,
    #     sentiment=0,
    #     isPriority= True, # nguồn ưu tiên
    #     crawl_bot="tiktok_post",
    # ).model_dump()

    return ElasticSearchRequest(
        # post_id = getattr(channel, "id", ""),
        id=getattr(channel, "id", ""),
        pub_time= Int64(int(raw.get("createTime", 0))),
        crawl_time= int(datetime.now(VN_TZ).timestamp()),
        org_id=2,
        description= raw.get("desc", ""),
        content= raw.get("desc", ""),
        url= f"https://www.tiktok.com/@{raw.get('author', {}).get('uniqueId', '')}/video/{raw['id']}",
        auth_id= "123",
        auth_name="Ha Noi",
        auth_type=1,
        auth_url="",
        source_id=f"https://www.tiktok.com/@{raw.get('author', {}).get('uniqueId', '')}",
        source_type=getattr(channel, "source_type", 0),
        source_name=raw.get("author", {}).get("nickname", ""),
        source_url=getattr(channel, "source_url", ""),

        isPriority= True, # nguồn ưu tiên 
        crawl_bot="tiktok_post"
    ).model_dump()

    # return {
    #     "doc_type": 1,
    #     "source_type": 5,
    #     "crawl_source": 2,
    #     "crawl_source_code": "tt",
    #     "pub_time": 1742695200,
    #     "crawl_time": 1744169968,
    #     "subject_id": "7484660873085209874",
    #     "title": None,
    #     "description": None,
    #     "content": "🤝 Giao hữu: Hà Nội 3️⃣ - 0️⃣ Hải Phòng ⚽️ Văn Tùng, Đức Hoàng, Đình Hai #HanoiFC #PrideofHanoi",
    #     "url": "TikTok · Hanoi Football Club",
    #     "media_urls": "[]",
    #     "comments": 12,
    #     "shares": 21,
    #     "reactions": 1160,
    #     "favors": 0,
    #     "views": 47300,
    #     "web_tags": "[]",
    #     "web_keywords": "[]",
    #     "auth_id": "7259742952787510278",
    #     "auth_name": "Hanoi Football Club",
    #     "auth_type": 1,
    #     "auth_url": "Hanoi Football Club on TikTok",
    #     "source_id": "7259742952787510278",
    #     "source_name": "Hanoi Football Club",
    #     "source_url": "Hanoi Football Club on TikTok",
    #     "reply_to": None,
    #     "level": None,
    #     "org_id": 2,
    #     "sentiment": 0,
    #     # "mg_sync": True,
    #     # "@timestamp": "2025-04-09T03:39:28.814Z",
    #     "id": "Visit TikTok to discover videos!",
    #     "crawl_bot": "tiktok_post",
    #     # "mg_sync_at": "2025-04-09T03:40:09.700Z",
    #     "org_ids": [
    #         2
    #     ]
    # }
