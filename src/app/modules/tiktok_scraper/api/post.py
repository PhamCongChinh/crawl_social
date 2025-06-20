import json
from pathlib import Path
from fastapi import APIRouter

from app.modules.elastic_search.request import dataToES
from app.modules.elastic_search.service import postToES
from app.modules.tiktok_scraper.scrapers.post import scrape_posts
from app.modules.tiktok_scraper.services.channel import ChannelService
from app.modules.tiktok_scraper.services.post import PostService

import logging

from app.utils.delay import async_delay
log = logging.getLogger(__name__)

router = APIRouter()

@router.get("/posts")
async def get_posts():
    log.info("Đang lấy dữ liệu post")
    try:
        data = await PostService.get_posts()
        return data
    except Exception as e:
        pass

@router.get("/posts/crawl")
async def crawl_posts():
    try:
        log.info("Đang lấy channels từ cơ sở dữ liệu TikTok")
        channels = await ChannelService.get_channels()
        log.info(f"Đã tìm thấy {len(channels)} channels trong cơ sở dữ liệu")
        
        flatten = []
        i = 1
        for channel in channels:
            data = await scrape_posts(urls=[channel.id])
            log.info(channel.id)
            if data and len(data) > 0:
                post = dataToES(data[0], channel=channel)
                flatten.append(post)
                print(f"✅ Thêm vào flatten: {post['id']}")
            else:
                print(f"❌ Không có data từ channel {channel.id}")
            i = i + 1
            if i == 10:
                break
        await PostService.upsert_posts_bulk(flatten)
        # a = {
        #     "doc_type": 1,
        #     "source_type": 5,
        #     "crawl_source": 2,
        #     "crawl_source_code": 'tt',
        #     "pub_time": 1727753860,
        #     "crawl_time": 1727753874,
        #     "subject_id": None,
        #     "title": None,
        #     "description": None,
        #     "content": '💭 Màu áo mới, cảm xúc mới - Hêndrio cùng những chia sẻ đầu tiên #HanoiFC #PrideofHanoi',
        #     "url": 'https://www.tiktok.com/@officialhanoifc/video/7504098685383494929',
        #     "media_urls": '[]',
        #     "comments": 20,
        #     "shares": 3,
        #     "reactions": 0,
        #     "favors": 0,
        #     "views": 0,
        #     "web_tags": '[]',
        #     "web_keywords": '[]',
        #     "auth_id": '100001029320191',
        #     "auth_name": 'Nguyen Hoang Linh',
        #     "auth_type": 1,
        #     "auth_url": 'https://www.tiktok.com/@officialhanoifc',
        #     "source_id": '100064772583943',
        #     "source_name": 'Hanoi Football Club',
        #     "source_url": 'https://www.tiktok.com/@officialhanoifc',
        #     "reply_to": None,
        #     "level": None,
        #     "sentiment": 0,
        #     "org_id": 2,
        #     "isPriority": True,
        # }
        # flatten.append(a)
        # print(flatten)
        result = await postToES(flatten)
        # result = None
        await async_delay(1, 3)
        return result
    except Exception as e:
        log.error(e)