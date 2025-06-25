import json
from typing import Dict, List
from scrapfly import ScrapeApiResponse, ScrapeConfig
import jmespath
from app.core.scrapfly import SCRAPFLY

import logging

from app.utils.lock import acquire_scrapfly_lock
log = logging.getLogger(__name__)

import hashlib

def hash_url(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()

def parse_channel(response: ScrapeApiResponse):
    _xhr_calls = response.scrape_result["browser_data"]["xhr_call"]
    post_calls = [c for c in _xhr_calls if "/api/post/item_list/" in c["url"]]
    channel_data = []
    for post_call in post_calls:
        try:
            data = json.loads(post_call["response"]["body"])["itemList"]
        except Exception:
            raise Exception("Post data couldn't load")
        channel_data.extend(data)
    # parse all the data using jmespath
    parsed_data = []
    for post in channel_data:
        result = jmespath.search(
            """{
                createTime: createTime,
                desc: desc,
                id: id,
                stats: stats,
                contents: contents[].desc
            }""",
            post,
        )
        
        # content_descs = [desc for desc in result.get("contents", []) if desc]
        # content_str = " ".join(content_descs)
        # result["contents"] = content_str

        parsed_data.append(result)
    
    return parsed_data

async def scrape_channel(url: str) -> List[Dict]:
    """scrape video data from a channel (profile with videos)"""
    session_id = f"tiktok-{hash_url(url)}"
    # js code for scrolling down with maximum 15 scrolls. It stops at the end without using the full iterations
    # Lấy full
    # js = """const scrollToEnd = (i = 0) => (window.innerHeight + window.scrollY >= document.body.scrollHeight || i >= 15) ? (console.log("Reached the bottom or maximum iterations. Stopping further iterations."), setTimeout(() => console.log("Waited 10 seconds after all iterations."), 10000)) : (window.scrollTo(0, document.body.scrollHeight), setTimeout(() => scrollToEnd(i + 1), 5000)); scrollToEnd();"""
    # Lấy 2 lần cuộn
    js = """
        document.documentElement.style.scrollBehavior = 'auto';
        const scrollToEnd = (i = 0) =>
            (window.innerHeight + window.scrollY >= document.body.scrollHeight || i >= 1)
                ? setTimeout(() => console.log("Đã scroll đủ cho 8 video."), 3000)
                : (window.scrollTo(0, document.body.scrollHeight), setTimeout(() => scrollToEnd(i + 1), 2000));
        scrollToEnd();
    """

    log.info(f"Đang quét trang kênh với URL {url} để lấy dữ liệu bài viết")

    async with acquire_scrapfly_lock(session_id):
        response = await SCRAPFLY.async_scrape(
            ScrapeConfig(
                url,
                asp=True,
                wait_for_selector="//div[@data-e2e='user-post-item-list']",
                render_js=True,
                # auto_scroll=True, # Full
                auto_scroll=False, # Không full
                rendering_wait=10000,
                js=js,
                debug=False,
                session=True,
                session_id=session_id
            )
        )

    data = parse_channel(response)
    log.info(f"Đã quét được dữ liệu của {len(data)} bài viết")
    return data