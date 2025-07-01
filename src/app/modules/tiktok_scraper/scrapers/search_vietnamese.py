import datetime
import secrets
import time
import json
import logging
from typing import Dict, List
from urllib.parse import quote_plus, urlencode
from langdetect import detect
from scrapfly import ScrapeApiResponse, ScrapeConfig

from app.core.scrapfly import SCRAPFLY, BASE_CONFIG

log = logging.getLogger(__name__)

# ===========================
# 🔍 Hàm lọc tiếng Việt
# ===========================
def is_vietnamese(text: str) -> bool:
    try:
        return detect(text) == "vi"
    except:
        return False


# ===========================
# 🔍 Parse dữ liệu từ TikTok API
# ===========================
def parse_search(response: ScrapeApiResponse) -> List[Dict]:
    data = json.loads(response.scrape_result["content"])
    search_data = data.get("data", [])
    parsed_search = []

    for item in search_data:
        if item.get("type") == 1:
            video_data = item.get("item", {})
            result = {
                "id": video_data.get("id"),
                "desc": video_data.get("desc"),
                "createTime": video_data.get("createTime"),
                "video": video_data.get("video"),
                "author": video_data.get("author"),
                "stats": video_data.get("stats"),
                "authorStats": video_data.get("authorStats"),
                "type": item["type"],
            }

            # Gợi ý thủ công để xác định liên quan Việt Nam
            desc = (result.get("desc") or "").lower()
            keywords = ["việt nam", "vietnam", "sài gòn", "hà nội", "vn", "vietnamese"]
            result["vietnam_related"] = any(k in desc for k in keywords)
            parsed_search.append(result)

    return parsed_search


# ===========================
# 🧠 Tạo session TikTok như người dùng Việt
# ===========================
async def obtain_session(keyword: str) -> str:
    session_id = f"tiktok_session_{int(time.time())}"
    timestamp_ms = int(time.time() * 1000)
    url = f"https://www.tiktok.com/search?q={quote_plus(keyword)}&t={timestamp_ms}"

    await SCRAPFLY.async_scrape(
        ScrapeConfig(
            url,
            **BASE_CONFIG,
            session=session_id,
            render_js=True,
            country="VN"
            # ,
            # headers={
            #     "accept-language": "vi-VN,vi;q=0.9,en;q=0.8",
            #     "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
            # },
        )
    )
    return session_id


# ===========================
# 🚀 Hàm scrape TikTok search kết quả Việt Nam
# ===========================
async def scrape_search_vietnam(keyword: str, max_search: int = 60, search_count: int = 12) -> List[Dict]:
    def generate_search_id():
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        random_hex = secrets.token_hex((32 - len(timestamp)) // 2).upper()
        return timestamp + random_hex

    def form_api_url(cursor: int):
        base_url = "https://www.tiktok.com/api/search/general/full/?"
        params = {
            "keyword": quote_plus(keyword),
            "offset": cursor,
            "search_id": generate_search_id(),
        }
        return base_url + urlencode(params)

    log.info(f"🔍 Bắt đầu scrape TikTok VN cho từ khóa: '{keyword}'")
    session_id = await obtain_session(keyword)
    log.info(f"✅ Session ID đã tạo: {session_id}")

    # --- Trang đầu ---
    first_page = await SCRAPFLY.async_scrape(
        ScrapeConfig(
            form_api_url(0),
            **BASE_CONFIG,
            session=session_id,
            country="VN",
            headers={
                "content-type": "application/json",
                "accept-language": "vi-VN,vi;q=0.9,en;q=0.8",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
                "referer": f"https://www.tiktok.com/search?q={quote_plus(keyword)}",
            },
        )
    )
    search_data = parse_search(first_page)

    # --- Trang tiếp theo ---
    _other_pages = [
        ScrapeConfig(
            form_api_url(cursor),
            **BASE_CONFIG,
            session=session_id,
            country="VN",
            headers={
                "content-type": "application/json",
                "accept-language": "vi-VN,vi;q=0.9,en;q=0.8",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
                "referer": f"https://www.tiktok.com/search?q={quote_plus(keyword)}",
            },
        )
        for cursor in range(search_count, max_search + search_count, search_count)
    ]

    async for response in SCRAPFLY.concurrent_scrape(_other_pages):
        search_data.extend(parse_search(response))

    # --- Lọc kết quả tiếng Việt ---
    vietnam_filtered = [
        item for item in search_data
        if item.get("vietnam_related") or is_vietnamese(item.get("desc", ""))
    ]

    log.info(f"🎯 Đã lọc {len(vietnam_filtered)} kết quả tiếng Việt từ tổng {len(search_data)}")
    return vietnam_filtered
