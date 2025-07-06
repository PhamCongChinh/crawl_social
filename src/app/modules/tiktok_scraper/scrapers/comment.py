import json
from typing import Dict, List
from urllib.parse import parse_qs, urlencode, urlparse
import uuid
from scrapfly import ScrapeApiResponse, ScrapeConfig
import jmespath
from app.core.scrapfly import BASE_CONFIG, SCRAPFLY

import logging
log = logging.getLogger(__name__)

def parse_comments(response: ScrapeApiResponse) -> List[Dict]:
    """parse comments data from the API response"""
    data = json.loads(response.scrape_result["content"])
    comments_data = data["comments"]
    total_comments = data["total"]
    parsed_comments = []
    # refine the comments with JMESPath
    for comment in comments_data:
        result = jmespath.search(
            """{
            text: text,
            comment_language: comment_language,
            digg_count: digg_count,
            reply_comment_total: reply_comment_total,
            author_pin: author_pin,
            create_time: create_time,
            cid: cid,
            nickname: user.nickname,
            unique_id: user.unique_id,
            aweme_id: aweme_id
            }""",
            comment,
        )
        parsed_comments.append(result)
    return {"comments": parsed_comments, "total_comments": total_comments}


async def retrieve_comment_params(post_url: str, session: str) -> Dict:
    """retrieve query parameters for the comments API"""
    response = await SCRAPFLY.async_scrape(
        ScrapeConfig(
            post_url,
            **BASE_CONFIG,
            render_js=True,
            rendering_wait=5000,
            session=session,
            wait_for_selector="//a[@data-e2e='comment-avatar-1']",
        )
    )

    _xhr_calls = response.scrape_result["browser_data"]["xhr_call"]
    for i in _xhr_calls:
        if "api/comment/list" not in i["url"]:
            continue
        url = urlparse(i["url"])
        qs = parse_qs(url.query)
        # remove the params we'll override
        for key in ["count", "cursor"]:
            _ = qs.pop(key, None)
        api_params = {key: value[0] for key, value in qs.items()}
        return api_params


async def scrape_comments(post_url: str, comments_count: int = 20, max_comments: int = None) -> List[Dict]:
    try:
        """scrape comments from tiktok posts using hidden APIs"""
        post_id = post_url.split("/video/")[1].split("?")[0]
        session_id = uuid.uuid4().hex  # generate a random session ID for the comments API
        api_params = await retrieve_comment_params(post_url, session_id)

        def form_api_url(cursor: int):
            """form the reviews API URL and its pagination values"""
            base_url = "https://www.tiktok.com/api/comment/list/?"
            params = {"count": comments_count, "cursor": cursor, **api_params}  # the index to start from
            return base_url + urlencode(params)
        
        log.info("scraping the first comments batch")
        first_page = await SCRAPFLY.async_scrape(
            ScrapeConfig(
                form_api_url(cursor=0), **BASE_CONFIG,
                headers={"content-type": "application/json"},
                render_js=True, session=session_id
            )
        )
        data = parse_comments(first_page)
        comments_data = data["comments"]
        total_comments = data["total_comments"]

        # get the maximum number of comments to scrape
        if max_comments and max_comments < total_comments:
            total_comments = max_comments

        # scrape the remaining comments concurrently
        _other_pages = [
            ScrapeConfig(
                form_api_url(cursor=cursor), **BASE_CONFIG,
                headers={"content-type": "application/json"},
                session=session_id, render_js=True
            )
            for cursor in range(comments_count, total_comments + comments_count, comments_count)
        ]
        
        for scrape_config in _other_pages:
            response = await SCRAPFLY.async_scrape(scrape_config)
            try:
                data = parse_comments(response)["comments"]
            except Exception as e:
                log.error(f"error scraping comments: {e}")
                continue
            comments_data.extend(data)

        log.info(f"scraped {len(comments_data)} from the comments API from the post with the ID {post_id}")
        return comments_data
    except Exception as e:
        log.error(f"error scraping comments from {post_url}: {e}")
        return []
