from app.tasks.tiktok.channel import crawl_tiktok_channels_classified
from app.tasks.tiktok.comment import crawl_tiktok_comments_hourly
from app.tasks.tiktok.post import crawl_tiktok_posts_hourly

from app.tasks.tiktok.video import crawl_video_all_classified, crawl_video_all_keyword, crawl_video_all_unclassified

TASK_MAP = {
    # "channel": crawl_tiktok_channels_classified,
    # "post": crawl_tiktok_posts_hourly,
    # "comment": crawl_tiktok_comments_hourly,
    # "profile": crawl_tiktok_profiles,

    "video_classified": crawl_video_all_classified,
    "video_unclassified": crawl_video_all_unclassified,
    "video_keyword": crawl_video_all_keyword
}