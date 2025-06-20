@echo off
echo 💥 Đang khởi động Redis...
start "Redis" cmd /k "cd /d C:\Redis && redis-server.exe"

timeout /t 2 >nul

echo 🚀 Khởi động Celery Worker...
start "Celery Worker" cmd /k "poetry run celery -A app.worker worker --loglevel=info --pool=threads"

echo ⏰ Khởi động Celery Beat...
start "Celery Beat" cmd /k "poetry run celery -A app.worker beat --loglevel=info"

echo 🌼 Khởi động Flower...
start "Flower" cmd /k "poetry run celery -A app.worker flower --port=5555"

timeout /t 1 >nul

echo ⚡ Khởi động FastAPI server...
start "FastAPI" cmd /k "poetry run uvicorn app.main:app --reload --host=0.0.0.0 --port=8000"

echo ✅ Mọi thứ đã sẵn sàng! Mở http://localhost:5555 để xem Flower.
