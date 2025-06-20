from datetime import datetime
from zoneinfo import ZoneInfo

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

def now_vn():
    dt = datetime.now(VN_TZ)
    return dt.isoformat()