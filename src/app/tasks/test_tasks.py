import time
from app.worker import celery_app

@celery_app.task(name="app.tasks.test")
def test_task(x: int):
    print(f"🚀 Start task {x}")
    time.sleep(5)
    print(f"✅ Done task {x}")
    return f"Task {x} done"