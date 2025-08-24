import json
from collections import Counter
from config import CFG, today_th_str
from utils import write_json, get_logger

logger = get_logger()

def aggregate(date=None):
    date = date or today_th_str()
    src = CFG.DATA_PROCESSED / date / "clean.jsonl"
    by = Counter(); total=0
    with open(src,"r",encoding="utf-8") as f:
        for line in f:
            o=json.loads(line)
            prov=o.get("source_area") or "ไม่ทราบจังหวัด"
            by[prov]+=1; total+=1
    provinces=[{"province":k,"n_comments":v} for k,v in by.most_common()]
    obj={"date":date,"total_comments":total,"provinces":provinces}
    CFG.PUBLIC.mkdir(parents=True, exist_ok=True)
    write_json(CFG.PUBLIC/"daily.json", obj)
    (CFG.PUBLIC/"history").mkdir(parents=True, exist_ok=True)
    write_json(CFG.PUBLIC/"history"/f"{date}.json", obj)
    logger.info(f"wrote public/daily.json (total={total})")
    return obj

if __name__=="__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--date", default=None, help="YYYY-MM-DD")
    args = p.parse_args()
    print(aggregate(args.date))
