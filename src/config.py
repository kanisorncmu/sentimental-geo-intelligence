from pathlib import Path
import datetime, pytz

class CFG:
    DATA_RAW = Path("data/raw")
    DATA_PROCESSED = Path("data/processed")
    PUBLIC = Path("public")

TZ = pytz.timezone("Asia/Bangkok")
def today_th_str():
    return datetime.datetime.now(TZ).strftime("%Y-%m-%d")
