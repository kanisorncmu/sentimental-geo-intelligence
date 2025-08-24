import json, re, html, unicodedata
from pathlib import Path
from config import CFG, today_th_str
from utils import get_logger

logger = get_logger()
URL_RE = re.compile(r"https?://\S+|www\.\S+")
WS_RE  = re.compile(r"\s+")

def _clean(s):
    if not s: return ""
    s = html.unescape(s)
    s = URL_RE.sub("", s)
    s = s.replace("\n"," ").replace("\r"," ")
    s = WS_RE.sub(" ", s).strip()
    return unicodedata.normalize("NFC", s)

def process_day(date=None):
    date = date or today_th_str()
    src = CFG.DATA_RAW / date / "comments.jsonl"
    out = CFG.DATA_PROCESSED / date / "clean.jsonl"
    out.parent.mkdir(parents=True, exist_ok=True)

    seen=set(); n_in=0; n_out=0
    with open(src,"r",encoding="utf-8") as fin, open(out,"w",encoding="utf-8") as fout:
        for line in fin:
            n_in+=1
            try: o=json.loads(line)
            except: continue
            cid=o.get("comment_id")
            if not cid or cid in seen: continue
            seen.add(cid)
            text=_clean(o.get("text",""))
            if not text: continue
            rec = {
                "date": date,
                "video_id": o.get("video_id"),
                "comment_id": cid,
                "text": text,
                "author": o.get("author"),
                "published_at": o.get("published_at"),
                "like_count": o.get("like_count",0),
                "video_title": o.get("video_title"),
                "video_published_at": o.get("video_published_at"),
                "source_area": o.get("source_area"),
            }
            fout.write(json.dumps(rec, ensure_ascii=False)+"\n"); n_out+=1
    logger.info(f"{date} clean: in={n_in} → out={n_out} → {out}")
    return str(out)

if __name__=="__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--date", default=None, help="YYYY-MM-DD")
    args = p.parse_args()
    print(process_day(args.date))
