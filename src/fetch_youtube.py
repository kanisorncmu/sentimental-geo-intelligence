import os, json, time, re
from datetime import datetime, timedelta
from pathlib import Path
from googleapiclient.discovery import build

from config import CFG, today_th_str
from utils import get_logger
logger = get_logger(); ISO = "%Y-%m-%dT%H:%M:%SZ"

KEYWORDS = [
    "ข่าว ไทย","ข่าวการเมือง","ข่าวต่างประเทศ","ข่าวด่วน",
    "ชายแดน ไทย กัมพูชา","ไทย กัมพูชา ปะทะ","ความขัดแย้ง ไทย กัมพูชา",
    "ปราสาทพระวิหาร","ช่องสะงำ","ช่องอานม้า",
    "Thailand Cambodia border","Preah Vihear","Ta Muen Thom"
]

PROVINCES = ["กรุงเทพมหานคร","กระบี่","กาญจนบุรี","กาฬสินธุ์","กำแพงเพชร","ขอนแก่น","จันทบุรี","ฉะเชิงเทรา","ชลบุรี",
"ชัยนาท","ชัยภูมิ","ชุมพร","เชียงราย","เชียงใหม่","ตรัง","ตราด","ตาก","นครนายก","นครปฐม","นครพนม","นครราชสีมา",
"นครศรีธรรมราช","นครสวรรค์","นนทบุรี","นราธิวาส","น่าน","บึงกาฬ","บุรีรัมย์","ปทุมธานี","ประจวบคีรีขันธ์",
"ปราจีนบุรี","ปัตตานี","พระนครศรีอยุธยา","พะเยา","พังงา","พัทลุง","พิจิตร","พิษณุโลก","เพชรบุรี","เพชรบูรณ์",
"แพร่","ภูเก็ต","มหาสารคาม","มุกดาหาร","แม่ฮ่องสอน","ยโสธร","ยะลา","ร้อยเอ็ด","ระนอง","ระยอง","ราชบุรี",
"ลพบุรี","ลำปาง","ลำพูน","เลย","ศรีสะเกษ","สกลนคร","สงขลา","สตูล","สมุทรปราการ","สมุทรสงคราม","สมุทรสาคร",
"สระแก้ว","สระบุรี","สิงห์บุรี","สุโขทัย","สุพรรณบุรี","สุราษฎร์ธานี","สุรินทร์","หนองคาย","หนองบัวลำภู",
"อ่างทอง","อำนาจเจริญ","อุดรธานี","อุตรดิตถ์","อุทัยธานี","อุบลราชธานี"]
ALIASES = {"กทม":"กรุงเทพมหานคร","กรุงเทพ":"กรุงเทพมหานคร","อยุธยา":"พระนครศรีอยุธยา","โคราช":"นครราชสีมา"}
_BOUNDARY = r"(?<![\wก-๙]){name}(?![\wก-๙])"
_PATTERNS = {p: re.compile(_BOUNDARY.format(name=re.escape(p))) for p in PROVINCES}
_ALIAS_PATTERNS = {k: re.compile(_BOUNDARY.format(name=re.escape(k))) for k in ALIASES}

def detect_province(text):
    if not text: return None
    for k, pat in _ALIAS_PATTERNS.items():
        if pat.search(text): return ALIASES[k]
    for p, pat in _PATTERNS.items():
        if pat.search(text): return p
    return None

def yt():
    key = os.getenv("YT_API_KEY")
    assert key, "YT_API_KEY not set"
    return build("youtube","v3",developerKey=key,cache_discovery=False)

def _search(service, date, q, max_videos=60):
    start = f"{date}T00:00:00Z"
    end   = (datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)).strftime(ISO)
    resp = service.search().list(
        part="id", type="video", regionCode="TH", maxResults=50,
        order="viewCount", q=q, publishedAfter=start, publishedBefore=end
    ).execute()
    return [it["id"]["videoId"] for it in resp.get("items",[]) if it.get("id",{}).get("videoId")]

def _search_news(service, date, max_videos=60):
    start = f"{date}T00:00:00Z"
    end   = (datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)).strftime(ISO)
    resp = service.search().list(
        part="id", type="video", regionCode="TH", videoCategoryId="25",
        maxResults=min(max_videos,50), order="viewCount",
        publishedAfter=start, publishedBefore=end
    ).execute()
    return [it["id"]["videoId"] for it in resp.get("items",[]) if it.get("id",{}).get("videoId")]

def _video_meta(service, ids):
    out = {}
    for i in range(0, len(ids), 50):
        chunk = ids[i:i+50]
        resp = service.videos().list(part="snippet", id=",".join(chunk)).execute()
        for it in resp.get("items",[]):
            vid = it["id"]; sn = it.get("snippet",{}) or {}
            out[vid] = {
                "title": sn.get("title"),
                "desc": sn.get("description"),
                "tags": sn.get("tags") or [],
                "published_at": sn.get("publishedAt"),
                "channel_id": sn.get("channelId"),
                "channel_title": sn.get("channelTitle"),
            }
    return out

def _source_area_from_meta(m):
    texts = [m.get("title","") or ""]
    if m.get("tags"): texts.append(" ".join(m["tags"]))
    texts.append(m.get("desc","") or "")
    texts.append(m.get("channel_title","") or "")
    for t in texts:
        p = detect_province(t)
        if p: return p
    return None

def _iter_comments(service, vid, limit):
    token=None; fetched=0
    while True:
        req = service.commentThreads().list(
            part="snippet", videoId=vid, maxResults=100, order="relevance", pageToken=token
        )
        try:
            resp = req.execute()
        except Exception as e:
            if "disabled" in str(e).lower(): return
            time.sleep(1.2); continue
        for it in resp.get("items",[]):
            s = it["snippet"]["topLevelComment"]["snippet"]
            yield {
                "video_id": vid,
                "comment_id": it["snippet"]["topLevelComment"]["id"],
                "text": s.get("textDisplay") or s.get("textOriginal") or "",
                "published_at": s.get("publishedAt"),
                "like_count": s.get("likeCount",0),
                "author": s.get("authorDisplayName"),
            }
            fetched+=1
            if fetched>=limit: return
        token = resp.get("nextPageToken")
        if not token: return

def main(date=None, target_per_day=12000, max_comments_total=200000, videos_per_day=120):
    date = date or today_th_str()
    raw_path = CFG.DATA_RAW / date / "comments.jsonl"
    raw_path.parent.mkdir(parents=True, exist_ok=True)

    svc = yt()
    ids = []
    for kw in KEYWORDS:
        ids += _search(svc, date, kw, max_videos=videos_per_day)
    ids += _search_news(svc, date, max_videos=videos_per_day)

    # dedup
    vids = list(dict.fromkeys(ids))

    meta = _video_meta(svc, vids)
    per_vid = max(1, max_comments_total//max(1,len(vids)))
    n=0
    with open(raw_path,"w",encoding="utf-8") as f:
        for vid in vids:
            sa = _source_area_from_meta(meta.get(vid,{}))
            for row in _iter_comments(svc, vid, limit=per_vid):
                row.update({
                    "video_title": meta.get(vid,{}).get("title"),
                    "video_published_at": meta.get(vid,{}).get("published_at"),
                    "video_channel_id": meta.get(vid,{}).get("channel_id"),
                    "video_channel_title": meta.get(vid,{}).get("channel_title"),
                    "source_area": sa
                })
                f.write(json.dumps(row, ensure_ascii=False)+"\n")
                n+=1
                if n>=target_per_day: break
            if n>=target_per_day: break
    logger.info(f"{date} raw rows: {n} → {raw_path}")
    return str(raw_path)

if __name__=="__main__":
    import argparse
    p=argparse.ArgumentParser()
    p.add_argument("--date", default=None)
    p.add_argument("--target-per-day", type=int, default=12000)
    a=p.parse_args()
    main(a.date, a.target_per_day)
