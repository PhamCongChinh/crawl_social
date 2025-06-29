import json
from typing import Dict, List
from scrapfly import ScrapeApiResponse, ScrapeConfig
import jmespath

import logging
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

async def scrape_posts(urls: List[str]) -> List[Dict]:
    """scrape tiktok posts data from their URLs"""
    to_scrape = [ScrapeConfig(
        url, 
        **BASE_CONFIG,
        render_js=True) for url in urls]
    data = []
    async for response in SCRAPFLY.concurrent_scrape(to_scrape):
        post_data = parse_post(response)
        data.append(post_data)
    log.info(f"scraped {len(data)} posts from post pages")
    return data