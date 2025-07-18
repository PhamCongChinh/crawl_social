import asyncio
import json
from typing import Dict, List
from scrapfly import ApiHttpServerError, ScrapeApiResponse, ScrapeConfig, ScrapflyScrapeError
import jmespath

import logging

from app.utils.delay import async_delay
log = logging.getLogger(__name__)

from app.core.scrapfly import SCRAPFLY, BASE_CONFIG

# Post
def parse_post(response: ScrapeApiResponse) -> Dict:
    """parse hidden post data from HTML"""
    selector = response.selector
    data = selector.xpath("//script[@id='__UNIVERSAL_DATA_FOR_REHYDRATION__']/text()").get()
    post_data = json.loads(data)["__DEFAULT_SCOPE__"]["webapp.video-detail"]["itemInfo"]["itemStruct"]
    parsed_post_data = jmespath.search(
        """{
            id: id,
            desc: desc,
            createTime: createTime,
            video: video.{duration: duration, ratio: ratio, cover: cover, playAddr: playAddr, downloadAddr: downloadAddr, bitrate: bitrate},
            author: author.{id: id, uniqueId: uniqueId, nickname: nickname, avatarLarger: avatarLarger, signature: signature, verified: verified},
            stats: stats,
            locationCreated: locationCreated,
            diversificationLabels: diversificationLabels,
            suggestedWords: suggestedWords,
            contents: contents[].{textExtra: textExtra[].{hashtagName: hashtagName}}
        }""",
        post_data,
    )
    return parsed_post_data

# async def scrape_posts(urls: List[str]) -> List[Dict]:
#     """scrape tiktok posts data from their URLs"""
#     to_scrape = [ScrapeConfig(
#         url, 
#         **BASE_CONFIG,
#         render_js=True) for url in urls]
#     data = []
#     async for response in SCRAPFLY.concurrent_scrape(to_scrape):
#         post_data = parse_post(response)
#         data.append(post_data)
#     log.info(f"scraped {len(data)} posts from post pages")
#     return data

# async def scrape_posts(urls: List[str]) -> List[Dict]:
#     """scrape tiktok posts data from their URLs"""
#     to_scrape = [ScrapeConfig(
#         url, 
#         **BASE_CONFIG,
#         render_js=True) for url in urls]
#     data = []
#     try:
#         async for response in SCRAPFLY.concurrent_scrape(to_scrape):
#             try:
#                 if not response.content:
#                     log.warning(f"No content from {response.config.url}")
#                     continue
#                 post_data = parse_post(response)
#                 data.append(post_data)
#             except Exception as e:
#                 log.error(f"Error parsing post data: {e}")
#                 continue
#     except ApiHttpServerError as e:
#         log.error(f"Scrapfly fatal error: {str(e)}")
#         # bỏ qua, không crash task
#         pass

#     log.info(f"✅ Scraped {len(data)}/{len(urls)} posts from post pages")
#     return data

async def scrape_posts(urls: List[str]) -> List[Dict]:
    """scrape tiktok posts data from their URLs"""
    to_scrape = [ScrapeConfig(
        url,
        proxy_pool="public_datacenter_pool",
        asp=False,
        # **BASE_CONFIG,
        cost_budget=10,
        rendering_stage="domcontentloaded",
        retry=False,
        timeout=30000
    ) for url in urls]
    data = []
    async for response in SCRAPFLY.concurrent_scrape(to_scrape):
        cost = response.cost
        status = response.status_code
        url = response.scrape_config.url
        if cost > 6:
            log.warning(f"[Scraper Post] ❌ High Cost: {cost} | Status: {status} | URL: {url}")
        else:
            log.info(f"[Scraper Post] ✅ Cost: {cost} | Status: {status} | URL: {url}")
        try:
            post_data = parse_post(response)
            data.append(post_data)
        except Exception as e:
            log.error(f"[Scraper Post] ⚠️ Lỗi parse URL: {url} → {e}")

        await async_delay(1,2)
    log.info(f"[Scraper Post] Đã quét {len(data)} post")
    return data