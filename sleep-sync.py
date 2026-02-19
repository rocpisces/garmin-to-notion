import os
import requests
from datetime import datetime, timedelta, timezone
from garminconnect import Garmin

TZ = timezone(timedelta(hours=8))
DAYS_BACK = int(os.getenv("DAYS_BACK", "30"))
NOTION_VERSION = "2022-06-28"


def notion_headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def notion_query_by_date(token, db_id, date_str):
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    payload = {
        "filter": {"property": "Date", "date": {"equals": date_str}},
        "page_size": 1,
    }
    r = requests.post(url, headers=notion_headers(token), json=payload, timeout=30)
    if r.status_code >= 300:
        raise RuntimeError(f"Notion query failed {r.status_code}: {r.text}")
    return r.json()


def notion_upsert(token, db_id, date_str, props):
    existing = notion_query_by_date(token, db_id, date_str)
    if existing.get("results"):
        page_id = existing["results"][0]["id"]
        url = f"https://api.notion.com/v1/pages/{page_id}"
        r = requests.patch(url, headers=notion_headers(token), json={"properties": props}, timeout=30)
        if r.status_code >= 300:
            raise RuntimeError(f"Notion update failed {r.status_code}: {r.text}")
        return "updated"
    else:
        url = "https://api.notion.com/v1/pages"
        r = requests.post(
            url,
            headers=notion_headers(token),
            json={"parent": {"database_id": db_id}, "properties": props},
            timeout=30,
        )
        if r.status_code >= 300:
            raise RuntimeError(f"Notion create failed {r.status_code}: {r.text}")
        return "created"


def safe_get(d, *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def try_call(obj, name, *args):
    fn = getattr(obj, name, None)
    if not callable(fn):
        return None
    try:
        return fn(*args)
    except Exception:
        return None


def main():
    garmin_email = os.environ["GARMIN_EMAIL"]
    garmin_password = os.environ["GARMIN_PASSWORD"]
    notion_token = os.environ["NOTION_TOKEN"]
    notion_sleep_db_id = os.environ["NOTION_SLEEP_DB_ID"]

    garmin = Garmin(garmin_email, garmin_password, is_cn=True)
    garmin.login()

    # Debug: 看看库里到底有哪些 sleep/hrv 方法（跑通后你可以删）
    cand = sorted([m for m in dir(garmin) if "sleep" in m.lower() or "hrv" in m.lower()])
    print("Garmin methods containing sleep/hrv:")
    print(", ".join(cand))

    today = datetime.now(TZ).date()
    start = today - timedelta(days=DAYS_BACK)

    count = 0
    for i in range(DAYS_BACK + 1):
        d = start + timedelta(days=i)
        date_str = d.isoformat()

        # 取睡眠：不同版本方法名可能不同，这里做几个尝试
        sleep = (
            try_call(garmin, "get_sleep_data", date_str)
            or try_call(garmin, "get_sleep_data", d.strftime("%Y-%m-%d"))
            or try_call(garmin, "get_sleep_data", d.strftime("%Y-%m-%dT00:00:00"))
        )

        if not sleep:
            # 没有睡眠就跳过（允许漏记/没戴表）
            continue

        # 常见结构：dailySleepDTO / sleepSummary / 睡眠阶段列表
        dto = safe_get(sleep, "dailySleepDTO", default={}) or sleep

        # 下面字段在不同账号/版本里可能叫法不同，所以尽量多兜底
        score = safe_get(dto, "sleepScores", "overall", "value", default=None)
        if score is None:
            score = safe_get(dto, "sleepScore", default=None)

        # 总睡眠/各阶段秒数（常见）
        total_sec = safe_get(dto, "sleepTimeSeconds", default=None)
        deep_sec = safe_get(dto, "deepSleepSeconds", default=None)
        light_sec = safe_get(dto, "lightSleepSeconds", default=None)
        rem_sec = safe_get(dto, "remSleepSeconds", default=None)
        awake_sec = safe_get(dto, "awakeSleepSeconds", default=None)

        # 入睡/醒来时间（常见用 epoch ms）
        bedtime_ms = safe_get(dto, "sleepStartTimestampGMT", default=None) or safe_get(dto, "sleepStartTimestampLocal", default=None)
        wake_ms = safe_get(dto, "sleepEndTimestampGMT", default=None) or safe_get(dto, "sleepEndTimestampLocal", default=None)

        # 静息心率（有的在 dto，有的在 summary）
        rhr = safe_get(dto, "restingHeartRate", default=None) or safe_get(sleep, "restingHeartRate", default=None)

        # HRV：不同版本差异很大，这里尝试取一个“当日 HRV 值”
        hrv_val = None
        hrv = try_call(garmin, "get_hrv_data", date_str) or try_call(garmin, "get_hrv", date_str)
        if isinstance(hrv, dict):
            # 常见字段名兜底
            hrv_val = hrv.get("hrvValue") or hrv.get("value") or safe_get(hrv, "data", 0, "value", default=None)

        def sec_to_min(x):
            try:
                return round(float(x) / 60.0, 1)
            except Exception:
                return None

        def ms_to_iso(x):
            try:
                # Garmin 常用毫秒时间戳（本地/UTC混用）；这里按时间戳生成可读时间
                return datetime.fromtimestamp(int(x) / 1000.0, TZ).isoformat()
            except Exception:
                return None

        props = {
            "Date": {"date": {"start": date_str}},
        }

        def maybe_set_number(name, val):
            if val is None:
                return
            props[name] = {"number": float(val)}

        def maybe_set_datetime(name, iso_str):
            if not iso_str:
                return
            props[name] = {"date": {"start": iso_str}}

        maybe_set_number("Sleep Score", score)
        maybe_set_number("Total Sleep (min)", sec_to_min(total_sec))
        maybe_set_number("Deep (min)", sec_to_min(deep_sec))
        maybe_set_number("Light (min)", sec_to_min(light_sec))
        maybe_set_number("REM (min)", sec_to_min(rem_sec))
        maybe_set_number("Awake (min)", sec_to_min(awake_sec))
        maybe_set_datetime("Bedtime", ms_to_iso(bedtime_ms))
        maybe_set_datetime("Wake Time", ms_to_iso(wake_ms))
        maybe_set_number("Resting HR", rhr)
        maybe_set_number("HRV (ms)", hrv_val)

        result = notion_upsert(notion_token, notion_sleep_db_id, date_str, props)
        print(date_str, result)
        count += 1

    print("Done:", count, "sleep days synced.")


if __name__ == "__main__":
    main()
