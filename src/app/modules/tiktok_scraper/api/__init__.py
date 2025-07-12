from fastapi import APIRouter

from .source import router as source_router
from .video import router as video_router
from .channel import router as channel_router
from .post import router as post_router
from .comment import router as comment_router
from .search import router as search_router

router = APIRouter()

router.include_router(source_router)
router.include_router(video_router)
router.include_router(channel_router)
router.include_router(post_router)
router.include_router(comment_router)
router.include_router(search_router)