from celery import Celery
from app.config.settings import settings
from celery.schedules import crontab
from kombu import Queue

celery_app = Celery(
    "worker",
    broker=settings.redis_broker_url,
    backend=settings.redis_backend_url
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="Asia/Ho_Chi_Minh",
    enable_utc=False,
)

celery_app.autodiscover_tasks([
    "app.tasks.tiktok"
])

# Queue
celery_app.conf.task_queue = (
    Queue("tiktok_platform"),
    Queue("thread_platform"),
)


# Định nghĩa lịch trình cho các tác vụ định kỳ
# Ví dụ: crawl TikTok channels mỗi giờ
# beat_schedule = {
    # 'crawl-every-hour': {
    #     'task': 'app.tasks.tiktok.channel.crawl_tiktok_channels_hourly',
    #     'schedule': crontab(minute=0, hour='6,8,10,12,14,16'),  # mỗi đầu giờ
    #     'options': {'queue': 'hourly_queue'},
    # },
    # 'crawl-post-every-hour': {
    #     'task': 'app.tasks.tiktok.channel.crawl_tiktok_posts_hourly',
    #     'schedule': crontab(minute=15, hour='6,8,10,12,14,16'),  # mỗi đầu giờ
    #     'options': {'queue': 'hourly_queue'},
    # },
    # 'crawl-comment-every-hour': {
    #     'task': 'app.tasks.tiktok.channel.crawl_tiktok_comments_hourly',
    #     'schedule': crontab(minute=45, hour='6,8,10,12,14,16'),  # mỗi đầu giờ
    #     'options': {'queue': 'hourly_queue'},
    # }
# }
# celery_app.conf.beat_schedule = beat_schedule