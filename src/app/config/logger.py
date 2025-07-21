from datetime import datetime, timedelta, timezone
from pathlib import Path
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
import logging
import time

log = logging.getLogger("api")

class LoggingAPI(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        response = await call_next(request)
        duration = round((time.time() - start_time) * 1000, 2)

        log.info(f"{request.method} {request.url.path} - {response.status_code} - {duration}ms")
        return response

class VietnameseColorFormatter(logging.Formatter):
    VN_TZ = timezone(timedelta(hours=7))  # GMT+7

    COLORS = {
        'DEBUG': "\033[94m",    # Xanh dương nhạt
        'INFO': "\033[92m",     # Xanh lá
        'WARNING': "\033[93m",  # Vàng
        'ERROR': "\033[91m",    # Đỏ
        'CRITICAL': "\033[95m", # Tím
    }
    RESET = "\033[0m"

    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, self.VN_TZ)
        return dt.strftime(datefmt or "%Y-%m-%d %H:%M:%S")

    def format(self, record):
        levelname = record.levelname
        color = self.COLORS.get(levelname, self.RESET)
        record.levelname = f"{color}{levelname}{self.RESET}"
        record.msg = f"{color}{record.msg}{self.RESET}"
        return super().format(record)

def setup_logging():
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    today_str = datetime.now().strftime("%Y-%m-%d")
    log_file = log_dir / f"app_{today_str}.log"

    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,

        "formatters": {
            "color_vn": {
                "()": VietnameseColorFormatter,
                "format": "[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S"
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "color_vn",
                "level": "INFO",
                "stream": "ext://sys.stdout"
            },
            # "file_info": {
            #     "class": "logging.handlers.TimedRotatingFileHandler",
            #     "formatter": "color_vn",  # Hoặc tạo formatter riêng cho file
            #     "filename": str(log_file),
            #     "when": "midnight",        # reset mỗi đêm
            #     "backupCount": 7,          # giữ 7 file log cũ
            #     "encoding": "utf8",
            #     "utc": False
            # }
            # "file_error": {
            #     "class": "logging.handlers.TimedRotatingFileHandler",
            #     "formatter": "color_vn",  # Hoặc tạo formatter riêng cho file
            #     "filename": str(log_file),
            #     "when": "midnight",        # reset mỗi đêm
            #     "backupCount": 7,          # giữ 7 file log cũ
            #     "encoding": "utf8",
            #     "utc": False
            # }
        },
        "root": {
            "level": "INFO",
            "handlers": ["console"]
        }
    }

    logging.config.dictConfig(logging_config)