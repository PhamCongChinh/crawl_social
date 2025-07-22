import os
import datetime
import secrets
import json
import uuid
import jmespath
from typing import Dict, List
from urllib.parse import urlencode, quote, urlparse, parse_qs
from scrapfly import ScrapeConfig, ScrapflyClient, ScrapeApiResponse

import logging

from app.helpers.telegram import Telegram
log = logging.getLogger(__name__)

from app.core.scrapfly import SCRAPFLY

def parse_search(response: ScrapeApiResponse) -> List[Dict]:
    """parse search data from the API response"""
    data = json.loads(response.scrape_result["content"])
    search_data = data["data"]
    parsed_search = []
    for item in search_data:
        if item["type"] == 1:  # get the item if it was item only
            result = jmespath.search(
                """{
                id: id,
                desc: desc,
                createTime: createTime,
                video: video,
                author: author,
                stats: stats,
                authorStats: authorStats
                }""",
                item["item"],
            )
            result["type"] = item["type"]
            parsed_search.append(result)

    # wheter there is more search results: 0 or 1. There is no max searches available
    has_more = data["has_more"]
    return parsed_search


async def obtain_session(url: str) -> str:
    """create a session to save the cookies and authorize the search API"""
    session_id = f"tiktok_search_session_{uuid.uuid4().hex[:6]}"
    res = await SCRAPFLY.async_scrape(ScrapeConfig(
        url,
        asp=True,
        proxy_pool="public_datacenter_pool",  # hoặc residential_pool nếu muốn IP người dùng
        country="AU",
        render_js=True,
        rendering_stage="domcontentloaded",
        session=session_id,
        retry=False,
        timeout=30000
        )
    )
    if res.cost > 6:
        Telegram.send_alert(f"Chi phí: {res.cost} - Từ khóa: {url}")
        log.warning(f"[SCRAPFLY KEYWORD]❌ Chi phí: {res.cost} - Từ khóa: {url}")
    else:
        log.info(f"[SCRAPFLY KEYWORD]✅ Chi phí: Chi phí: {res.cost} - Từ khóa: {url}")

    return session_id


async def scrape_search(keyword: str, max_search: int, search_count: int = 12) -> List[Dict]:
    def generate_search_id():
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        random_hex_length = (32 - len(timestamp)) // 2  # calculate bytes needed
        random_hex = secrets.token_hex(random_hex_length).upper()
        random_id = timestamp + random_hex
        return random_id

    def form_api_url(cursor: int):
        base_url = "https://www.tiktok.com/api/search/general/full/?"
        params = {
            "keyword": keyword, #quote(keyword)
            "offset": cursor,  # the index to start from
            "search_id": generate_search_id(),
        }
        return base_url + urlencode(params)

    log.info("Đang thiết lập session gọi API tìm kiếm TikTok...")
    session_id = await obtain_session(url="https://www.tiktok.com/search?q=" + quote(keyword)) #quote(keyword)

    log.info("scraping the first search batch")
    first_page = await SCRAPFLY.async_scrape(
        ScrapeConfig(
            form_api_url(cursor=0),
            # **BASE_CONFIG,
            asp=False,
            proxy_pool="public_datacenter_pool",  # hoặc residential_pool nếu muốn IP người dùng
            country="AU",
            retry=False,
            timeout=30000,
            rendering_stage="domcontentloaded",
            headers={
                "content-type": "application/json",
            },
            session=session_id,
        )
    )
    if first_page.cost > 1:
        Telegram.send_alert(f"Chi phí: {first_page.cost} - Từ khóa: {keyword}")
        log.warning(f"[SCRAPFLY KEYWORD]❌ Chi phí: {first_page.cost} - Từ khóa: {keyword}")
    else:
        log.info(f"[SCRAPFLY KEYWORD]✅ Chi phí: Chi phí: {first_page.cost} - Từ khóa: {keyword}")
    
    search_data = parse_search(first_page)

    # scrape the remaining comments concurrently
    # log.info(f"scraping search pagination, remaining {max_search // search_count} more pages")
    # _other_pages = [
    #     ScrapeConfig(
    #         form_api_url(cursor=cursor), 
    #         asp=False,
    #         proxy_pool="public_datacenter_pool",  # hoặc residential_pool nếu muốn IP người dùng
    #         country="AU",
    #         retry=False,
    #         timeout=30000,
    #         rendering_stage="domcontentloaded",
    #         headers={"content-type": "application/json"}, 
    #         session=session_id
    #     )
    #     for cursor in range(search_count, max_search + search_count, search_count)
    # ]
    # async for response in SCRAPFLY.concurrent_scrape(_other_pages):
    #     data = parse_search(response)
    #     search_data.extend(data)

    log.info(f"scraped {len(search_data)} from the search API from the keyword {keyword}")
    return search_data