from app.worker import celery_app
import time

@celery_app.task(
    queue="tiktok_test",
    name="app.tasks.tiktok.video.test"
)
def countdown(seconds: int):
    seconds = 10
    while seconds > 0:
        mins, secs = divmod(seconds, 60)
        print(f"\r⏳ Còn lại: {mins:02d}:{secs:02d}", end="")
        time.sleep(1)
        seconds -= 1
    print("\n🎉 Hết giờ!")
