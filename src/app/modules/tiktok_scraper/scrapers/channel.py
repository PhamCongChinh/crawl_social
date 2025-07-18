import asyncio
import json
import re
from typing import Dict, List
from scrapfly import ScrapeApiResponse, ScrapeConfig
import jmespath
from app.core.scrapfly import SCRAPFLY

import logging
log = logging.getLogger(__name__)

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
        contents = result.get("contents", [])
        if isinstance(contents, list):
            result["contents"] = " ".join([x for x in contents if isinstance(x, str) and x.strip()])
        else:
            result["contents"] = str(contents or "")
        parsed_data.append(result)
    return parsed_data

async def scrape_channel(url: str) -> List[Dict]:
    """scrape video data from a channel (profile with videos)"""
    # js code for scrolling down with maximum 15 scrolls. It stops at the end without using the full iterations
    js = """const scrollToEnd = (i = 0) => (window.innerHeight + window.scrollY >= document.body.scrollHeight || i >= 15) ? (console.log("Reached the bottom or maximum iterations. Stopping further iterations."), setTimeout(() => console.log("Waited 10 seconds after all iterations."), 10000)) : (window.scrollTo(0, document.body.scrollHeight), setTimeout(() => scrollToEnd(i + 1), 5000)); scrollToEnd();"""
    log.info(f"[Scraper Video] Đang thu thập dữ liệu từ {url}.")
    max_attempts = 3
    for attempt in range(max_attempts):
        for attempt in range(max_attempts):
            try:
                response = await SCRAPFLY.async_scrape(ScrapeConfig(
                    url,
                    asp=True,
                    proxy_pool="public_datacenter_pool",
                    wait_for_selector="//div[@data-e2e='user-post-item-list']",
                    render_js=True,
                    rendering_wait=3000,
                    cost_budget=10,
                    retry=False,
                    timeout=30000,
                    rendering_stage="domcontentloaded"
                ))
                if response.cost > 6:
                    log.info(f"[Scraper  Cost] ❌ Cost: {response.cost} | Status: {response.status_code} | URL: {url}")
                else:
                    log.info(f"[Scraper  Cost] Cost: {response.cost} | Status: {response.status_code} | URL: {url}")
                data = parse_channel(response)
                log.info(f"[Scraper Video] Đã quét được {len(data)} bài viết từ kênh {url}")
                return data
            except Exception as e:
                if attempt < max_attempts - 1:
                    log.warning(f"[Scraper Video] Thử lại {attempt+1}/{max_attempts} → {url} vì {e}")
                    await asyncio.sleep(2 + attempt)
                else:
                    raise
    