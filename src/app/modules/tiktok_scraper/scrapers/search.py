import datetime
import json
import secrets
from typing import Dict, List
from scrapfly import ScrapeApiResponse, ScrapeConfig
import jmespath

from urllib.parse import urlencode, quote, urlparse, parse_qs

import logging
log = logging.getLogger(__name__)

from app.core.scrapfly import SCRAPFLY, BASE_CONFIG


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
    session_id = "tiktok_search_session"
    await SCRAPFLY.async_scrape(ScrapeConfig(url, **BASE_CONFIG, render_js=True, session=session_id))
    return session_id


async def scrape_search(keyword: str, max_search: int, search_count: int = 12) -> List[Dict]:
    """scrape tiktok search data from the search API"""

    def generate_search_id():
        # get the current datetime and format it as YYYYMMDDHHMMSS
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        # calculate the length of the random hex required for the total length (32)
        random_hex_length = (32 - len(timestamp)) // 2  # calculate bytes needed
        random_hex = secrets.token_hex(random_hex_length).upper()
        random_id = timestamp + random_hex
        return random_id

    def form_api_url(cursor: int):
        """form the reviews API URL and its pagination values"""
        base_url = "https://www.tiktok.com/api/search/general/full/?"
        params = {
            "keyword": quote(keyword),
            "offset": cursor,  # the index to start from
            "search_id": generate_search_id(),
        }
        return base_url + urlencode(params)

    log.info("obtaining a session for the search API")
    session_id = await obtain_session(url="https://www.tiktok.com/search?q=" + quote(keyword))

    log.info("scraping the first search batch")
    first_page = await SCRAPFLY.async_scrape(
        ScrapeConfig(
            form_api_url(cursor=0),
            **BASE_CONFIG,
            headers={
                "content-type": "application/json",
            },
            session=session_id,
        )
    )
    search_data = parse_search(first_page)

    # scrape the remaining comments concurrently
    log.info(f"scraping search pagination, remaining {max_search // search_count} more pages")
    _other_pages = [
        ScrapeConfig(
            form_api_url(cursor=cursor), **BASE_CONFIG, headers={"content-type": "application/json"}, session=session_id
        )
        for cursor in range(search_count, max_search + search_count, search_count)
    ]
    async for response in SCRAPFLY.concurrent_scrape(_other_pages):
        data = parse_search(response)
        search_data.extend(data)

    log.info(f"scraped {len(search_data)} from the search API from the keyword {keyword}")
    return search_data