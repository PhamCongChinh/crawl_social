from datetime import datetime
from zoneinfo import ZoneInfo

from bson import Int64

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

class RequestToES:
    @staticmethod
    def flatten_post_data_classified(raw: dict, data: dict) -> dict:
        return {
            "id": raw.get("id", None),
            "doc_type": 1,  # POST = 1, COMMENT = 2
            "crawl_source": 2,
            "crawl_source_code": data.get("source_channel", None),
            "pub_time": Int64(int(raw.get("createTime", 0))),
            "crawl_time": int(datetime.now(VN_TZ).timestamp()),
            "org_id": data.get("org_id", None),
            "subject_id": raw.get("subject_id", None),
            "title": raw.get("title", None),
            "description": raw.get("description", None),
            "content": raw.get("desc", None),
            "url": f"https://www.tiktok.com/@{raw.get('author', {}).get('uniqueId', '')}/video/{raw['id']}",
            "media_urls": "[]",
            "comments": raw.get("stats", {}).get("commentCount", 0),
            "shares": raw.get("stats", {}).get("shareCount", 0),
            "reactions": raw.get("stats", {}).get("diggCount", 0),
            "favors": int(raw.get("stats", {}).get("collectCount", 0) or 0),
            "views": raw.get("stats", {}).get("playCount", 0),
            "web_tags": "[]",#json.dumps(raw.get("diversificationLabels", [])),
            "web_keywords": "[]",# json.dumps(raw.get("suggestedWords", [])),
            "auth_id": raw.get("author", {}).get("id", ""),
            "auth_name": raw.get("author", {}).get("nickname", ""),
            "auth_type": 1,
            "auth_url": f"https://www.tiktok.com/@{raw.get('author', {}).get('uniqueId', '')}",
            "source_id": raw.get("id", None),
            "source_type": data.get("source_type", None),
            "source_name": data.get("source_name", None),
            "source_url": data.get("source_url", None),
            "reply_to": None,
            "level": None,
            "sentiment": 0,
            "isPriority": True,
            "crawl_bot": "tiktok_post",
        }