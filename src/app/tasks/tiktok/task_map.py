from app.tasks.tiktok.channel import crawl_tiktok_channels
from app.tasks.tiktok.post import crawl_tiktok_posts

from app.tasks.tiktok.tiktok import crawl_channel_then_post

TASK_MAP = {
    "tiktok": crawl_channel_then_post,
    # "channel": crawl_tiktok_channels,
    # "post": crawl_tiktok_posts,
    # "comment": crawl_tiktok_comments,
    # "profile": crawl_tiktok_profiles,
    # "search": crawl_tiktok_search,
}