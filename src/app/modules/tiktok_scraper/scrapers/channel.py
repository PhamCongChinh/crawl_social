import json
from typing import Dict, List
from scrapfly import ScrapeApiResponse, ScrapeConfig
import jmespath
from app.core.scrapfly import SCRAPFLY

import logging
log = logging.getLogger(__name__)


def parse_channel(response: ScrapeApiResponse):
    """parse channel video data from XHR calls"""
    # extract the xhr calls and extract the ones for videos
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
        # contents: contents[].{desc: desc, textExtra: textExtra[].{hashtagName: hashtagName}}
        # Có Hashtag
        # content_str = " ".join([c for c in result.get("contents", []) if c])
        # result["contents"] = content_str  # Gán lại contents thành chuỗi
        
        # Gộp các đoạn mô tả thành chuỗi (bỏ hashtag)
        content_descs = [desc for desc in result.get("contents", []) if desc]
        content_str = " ".join(content_descs)
        result["contents"] = content_str

        parsed_data.append(result)
    
    return parsed_data

async def scrape_channel(url: str) -> List[Dict]:
    """scrape video data from a channel (profile with videos)"""
    # js code for scrolling down with maximum 15 scrolls. It stops at the end without using the full iterations
    # Lấy full
    js = """const scrollToEnd = (i = 0) => (window.innerHeight + window.scrollY >= document.body.scrollHeight || i >= 15) ? (console.log("Reached the bottom or maximum iterations. Stopping further iterations."), setTimeout(() => console.log("Waited 10 seconds after all iterations."), 10000)) : (window.scrollTo(0, document.body.scrollHeight), setTimeout(() => scrollToEnd(i + 1), 5000)); scrollToEnd();"""
    # Lấy max 20 bài
    # js = """
    #     const scrollUntilEnoughPosts = (targetCount = 4, i = 0, maxScrolls = 3) => {
    #     const posts = document.querySelectorAll("[data-e2e='user-post-item']");
    #     if (posts.length >= targetCount || i >= maxScrolls) {
    #         console.log(`✅ Đã thấy ${posts.length} bài viết hoặc đạt giới hạn ${i} lần cuộn. Dừng lại.`);
    #         setTimeout(() => console.log("⏱️ Chờ thêm 10s cho TikTok load nội dung..."), 3000);
    #         return;
    #     }

    #     console.log(`🔁 Cuộn lần ${i + 1}... (Hiện tại có ${posts.length} bài viết)`);
    #     window.scrollTo(0, document.body.scrollHeight);
    #     setTimeout(() => scrollUntilEnoughPosts(targetCount, i + 1, maxScrolls), 2000);
    #     };

    #     scrollUntilEnoughPosts();
    # """
    log.info(f"Đang quét trang kênh với URL {url} để lấy dữ liệu bài viết")
    response = await SCRAPFLY.async_scrape(
        ScrapeConfig(
            url,
            asp=True,
            wait_for_selector="//div[@data-e2e='user-post-item-list']",
            render_js=True,
            auto_scroll=True,
            rendering_wait=10000,
            js=js,
            debug=False,
            session=False,
        )
    )

    data = parse_channel(response)
    log.info(f"Đã quét được dữ liệu của {len(data)} bài viết")
    return data