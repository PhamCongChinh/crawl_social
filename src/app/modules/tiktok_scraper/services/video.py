from datetime import datetime, timedelta
import logging
from zoneinfo import ZoneInfo

from pymongo import ASCENDING, UpdateOne
from beanie.operators import In, And, Eq
from app.modules.tiktok_scraper.dto.videos import VideoResponse
from app.modules.tiktok_scraper.models.video import VideoModel
from app.utils.timezone import now_vn
log = logging.getLogger(__name__)
class VideoService:

    @staticmethod
    async def get_videos() -> list[VideoModel]:
        videos = await VideoModel.find_all().limit(20).to_list()
        return videos
    
    # V1
    @staticmethod
    async def upsert_processing_to_pending():
        return await VideoModel.find(
            Eq(VideoModel.status, "processing")
        ).update_many({"$set": {"status": "pending"}})
    

    # v1
    @staticmethod
    async def upsert_videos_bulk_keyword(scrape_data: list[dict], keyword: dict):
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
                    "video_url": f"https://www.tiktok.com/@{data.get('author', {}).get('uniqueId', '')}/video/{cid}",
                    "contents": data.get("desc"),
                    "create_time": data.get("createTime"),
                    "org_id": 0,#keyword["org_id"],
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
            return {
                "inserted": result.upserted_count, # Thêm mới
                "matched": result.matched_count,
                "modified": result.modified_count,
                "upserted": len(result.upserted_ids)
            }
        except Exception as e:
            log.error(e)
            raise

    # v1
    @staticmethod
    async def upsert_videos_bulk_url(scrape_data: list[dict], channel: dict):
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
                    "source_url": channel["source_url"],
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
            return {
                "inserted": result.upserted_count, # Thêm mới
                "matched": result.matched_count,
                "modified": result.modified_count,
                "upserted": len(result.upserted_ids)
            }
        except Exception as e:
            log.error(e)
            raise

    # v1
    @staticmethod
    async def get_videos_daily():
        vn_now = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
        # Lấy thời điểm đầu ngày (0h00)
        # start_of_day = datetime(vn_now.year, vn_now.month, vn_now.day, tzinfo=ZoneInfo("Asia/Ho_Chi_Minh"))
        # from_timestamp = int(start_of_day.timestamp())
        # to_timestamp = int(vn_now.timestamp())
        # log.info(f"Giờ hiện tại: {vn_now}")
        # log.info(f"Bắt đầu ngày: {start_of_day}")

        # 24h trước
        from_hour_ago = vn_now - timedelta(hours=24)
        from_timestamp = int(from_hour_ago.timestamp())
        to_timestamp = int(vn_now.timestamp())

        log.info(f"Bắt đầu từ: {from_timestamp}")
        log.info(f"Giờ hiện tại: {vn_now}")
        
        return await VideoModel.find(
            And(
                VideoModel.status == "pending",
                VideoModel.create_time >= from_timestamp,
                VideoModel.create_time < to_timestamp,
            )
        ).to_list()
    
    # v1
    @staticmethod
    async def get_videos_backdate(from_date: int, to_date: int):
        return await VideoModel.find(
            And(
                VideoModel.status == "pending",
                VideoModel.create_time >= from_date,
                VideoModel.create_time < to_date,
            )
        ).to_list()