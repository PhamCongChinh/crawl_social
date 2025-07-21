from fastapi import HTTPException
from pymongo import UpdateOne
from app.modules.tiktok_scraper.models.post import PostModel
from app.utils.timezone import now_vn

import logging
log = logging.getLogger(__name__)

class PostService:
    @staticmethod
    async def get_posts():
        return await PostModel.find_all().to_list()
    
    # @staticmethod
    # async def upsert_posts_bulk(posts: list[dict], channel: ChannelModel): #channel: ChannelModel
    #     try:
    #         log.info(f"Đang upsert {len(posts)} posts vào cơ sở dữ liệu")
    #         if not posts:
    #             log.warning("Không có dữ liệu để upsert (bulk)")
    #             return

    #         now = now_vn()
    #         operations = []

    #         for post in posts:
    #             if not post.get("id"):
    #                 log.warning("Channel không có ID, bỏ qua")
    #                 continue

    #             _id = post["id"] # nếu update thì dùng id làm _id
    #             # _id = channel.id
    #             post["org_id"] = channel.org_id
    #             post["source_type"] = channel.source_type
    #             post["source_name"] = channel.source_name
    #             post["source_url"] = channel.source_url
    #             post["source_channel"] = channel.source_channel
    #             post["updated_at"] = now

    #             update_doc = {
    #                 "$set": {
    #                     **post,
    #                     "updated_at": now,
    #                 },
    #                 "$setOnInsert": {
    #                     "created_at": now
    #                 }
    #             }

    #             operations.append(
    #                 UpdateOne(
    #                     {"_id": _id},
    #                     update_doc,
    #                     upsert=True
    #                 )
    #             )

    #         if not operations:
    #             log.info("Không có operation nào được tạo cho bulk upsert.")
    #             return
    #         log.info(f"Đang thực hiện bulk upsert với {len(operations)} operations")
    #         result = await PostModel.get_motor_collection().bulk_write(operations)
    #         log.info(f"Bulk upsert xong: inserted={result.upserted_count}, modified={result.modified_count}")
    #     except Exception as e:
    #         log.error(f"Bulk upsert thất bại: {e}")
    #         raise HTTPException(status_code=500, detail="Không thể upsert bài viết")