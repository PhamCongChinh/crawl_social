from app.tasks.tiktok.channel import crawl_tiktok_channels
from app.tasks.tiktok.post import crawl_tiktok_posts

TASK_MAP = {
    "channel": crawl_tiktok_channels,
    # "post": crawl_tiktok_posts,
    # "comment": crawl_tiktok_comments,
    # "profile": crawl_tiktok_profiles,
    # "search": crawl_tiktok_search,
}