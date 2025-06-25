from fastapi import APIRouter

from .source import router as source_router
from .channel import router as channel_router
from .post import router as post_router

router = APIRouter()

router.include_router(source_router)
router.include_router(channel_router)
router.include_router(post_router)
