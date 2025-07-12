from datetime import datetime, timedelta
import logging
from zoneinfo import ZoneInfo

from pymongo import ASCENDING, UpdateOne
from beanie.operators import In, And
from app.modules.tiktok_scraper.models.video import VideoModel
from app.utils.timezone import now_vn
log = logging.getLogger(__name__)
class VideoService:

    # @staticmethod
    # async def ensure_indexes():
    #     # Tạo index unique trên channel_id nếu chưa có
    #     coll = ChannelModelTest.get_motor_collection()
    #     await coll.create_index([("channel_id", ASCENDING)], unique=True)

    @staticmethod
    async def get_videos():
        vn_now = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
        hour_ago = vn_now - timedelta(hours=15)

        from_timestamp = int(hour_ago.timestamp())
        to_timestamp = int(vn_now.timestamp())
        log.info(from_timestamp)
        log.info(to_timestamp)
        # return await VideoModel.find_all().to_list()
        return await VideoModel.find(
            And(
                VideoModel.status == "pending",
                VideoModel.create_time >= from_timestamp,
                VideoModel.create_time < to_timestamp,
            )
        ).to_list()

    @staticmethod
    async def upsert_channels_bulk_keyword(scrape_data: list[dict], keyword: dict):
        """
            Bulk upsert list channels theo channel_id:
            - Nếu tồn tại → update tất cả các trường + updated_at
            - Nếu không → insert mới với created_at & status mặc định
        """
        try:
            if not scrape_data:
                log.warning("Không có dữ liệu để upsert (bulk)")
                return
            now = now_vn()
            operations = []

            for data in scrape_data:
                cid = data.get("id")
                if not cid:
                    log.warning("Channel không có ID, bỏ qua")
                    continue
                
                mapped = {
                    "video_id": cid,#
                    "video_url": f"https://www.tiktok.com/@{data.get('author', {}).get('uniqueId', '')}/video/{cid}",
                    "contents": data.get("desc"),#
                    "create_time": data.get("createTime"),#
                    "org_id": keyword["org_id"],
                    "source_type": keyword["source_type"],
                    "source_name": data.get("author", {}).get("nickname", ""),
                    "source_url": f"https://www.tiktok.com/@{data.get('author', {}).get('uniqueId', '')}/video/{cid}",
                    "source_channel": keyword["source_channel"],
                    "updated_at": now,
                }

                update_doc = {
                    "$set": mapped,
                    "$setOnInsert": {
                        "status": "pending",
                        "created_at": now
                    }
                }
                operations.append(
                    UpdateOne(
                        {"video_id": cid},  # ← Lọc theo field channel_id,
                        update_doc,
                        upsert=True
                    )
                )
            if not operations:
                log.info("Không có operation nào được tạo cho bulk upsert.")
                return
            result = await VideoModel.get_motor_collection().bulk_write(operations)
            print(result)
            # log.info(f"Bulk upsert xong: inserted={result.upserted_count}, modified={result.modified_count}")
            return {
                "matched": result.matched_count,
                "modified": result.modified_count,
                "upserted": len(result.upserted_ids)
            }
        except Exception as e:
            log.error(e)

    @staticmethod
    async def upsert_channels_bulk_channel(scrape_data: list[dict], channel: dict):
        """
            Bulk upsert list channels theo channel_id:
            - Nếu tồn tại → update tất cả các trường + updated_at
            - Nếu không → insert mới với created_at & status mặc định
        """
        try:
            if not scrape_data:
                log.warning("Không có dữ liệu để upsert (bulk)")
                return
            now = now_vn()
            operations = []

            for data in scrape_data:
                cid = data.get("id")
                if not cid:
                    log.warning("Channel không có ID, bỏ qua")
                    continue
                
                mapped = {
                    "video_id": cid,
                    "video_url": f'{channel["source_url"]}/video/{cid}',
                    "contents": data.get("desc"),
                    "create_time": data.get("createTime"),
                    "org_id": channel["org_id"],
                    "source_type": channel["source_type"],
                    "source_name": channel["source_name"],
                    "source_url": f'{channel["source_url"]}/video/{cid}',
                    "source_channel": channel["source_channel"],
                    "updated_at": now,
                }

                update_doc = {
                    "$set": mapped,
                    "$setOnInsert": {
                        "status": "pending",
                        "created_at": now
                    }
                }
                operations.append(
                    UpdateOne(
                        {"video_id": cid},  # ← Lọc theo field channel_id,
                        update_doc,
                        upsert=True
                    )
                )
            if not operations:
                log.info("Không có operation nào được tạo cho bulk upsert.")
                return
            result = await VideoModel.get_motor_collection().bulk_write(operations)
            print(result)
            # log.info(f"Bulk upsert xong: inserted={result.upserted_count}, modified={result.modified_count}")
            return {
                "matched": result.matched_count,
                "modified": result.modified_count,
                "upserted": len(result.upserted_ids)
            }
        except Exception as e:
            log.error(e)

