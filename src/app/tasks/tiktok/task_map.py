from app.tasks.tiktok.channel import crawl_tiktok_channels_hourly
from app.tasks.tiktok.comment import crawl_tiktok_comments_hourly
from app.tasks.tiktok.post import crawl_tiktok_posts_hourly

from app.tasks import count_down_1, count_down_2, count_down_3

TASK_MAP = {
    "channel": crawl_tiktok_channels_hourly,
    "post": crawl_tiktok_posts_hourly,
    "comment": crawl_tiktok_comments_hourly,
    # "profile": crawl_tiktok_profiles,
    # "search": crawl_tiktok_search,
    # "channel": count_down_1,
    # "post": crawl_tiktok_posts_hourly,
    # "comment": count_down_3
}