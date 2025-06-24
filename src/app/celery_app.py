import time
from celery import Celery
from celery.schedules import crontab
from app.config.settings import settings

celery_app = Celery(
    "worker",
    broker=settings.redis_broker_url,
    backend=settings.redis_backend_url,
    # include=[
    #     "app.tasks.crawl_tiktok"
    # ]
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="Asia/Ho_Chi_Minh",
    enable_utc=False,
)

celery_app.autodiscover_tasks(["app.tasks"])

# celery_app.conf.task_routes = {
#     "tasks.fast_tasks.*": {"queue": "fast_queue"},
#     "tasks.slow_tasks.*": {"queue": "slow_queue"},
# }

# celery_app.conf.task_default_queue = "default"

# CHỖ QUAN TRỌNG: tên này là tên Python package (không phải path)


# DEBUG in ra khi khởi động
print("✅ CELERY CONNECTED:")
print(" - Broker :", settings.redis_broker_url)
print(" - Backend:", settings.redis_backend_url)
print(" - Loaded tasks:", list(celery_app.tasks.keys()))