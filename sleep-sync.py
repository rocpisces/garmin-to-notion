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


def sec_to_min(x):
    try:
        return round(float(x) / 60.0, 1)
    except Exception:
        return None


def ms_to_iso_local(ms):
    try:
        return datetime.fromtimestamp(int(ms) / 1000.0, TZ).isoformat()
    except Exception:
        return None


def maybe_number(props, name, val):
    if val is None:
        return
    props[name] = {"number": float(val)}


def maybe_date(props, name, iso_str):
    if not iso_str:
        return
    props[name] = {"date": {"start": iso_str}}


def maybe_text(props, name, text):
    if text is None:
        return
    props[name] = {"rich_text": [{"text": {"content": str(text)}}]}


def maybe_select(props, name, option):
    if option is None:
        return
    props[name] = {"select": {"name": str(option)}}


def main():
    garmin_email = os.environ["GARMIN_EMAIL"]
    garmin_password = os.environ["GARMIN_PASSWORD"]
    notion_token = os.environ["NOTION_TOKEN"]
    notion_sleep_db_id = os.environ["NOTION_SLEEP_DB_ID"]

    garmin = Garmin(garmin_email, garmin_password, is_cn=True)
    garmin.login()

    today = datetime.now(TZ).date()
    start = today - timedelta(days=DAYS_BACK)

    count = 0

    for i in range(DAYS_BACK + 1):
        d = start + timedelta(days=i)
        date_str = d.isoformat()

        # Garmin CN: 睡眠
        sleep = garmin.get_sleep_data(date_str)
        if not sleep or "dailySleepDTO" not in sleep:
            continue

        dto = sleep["dailySleepDTO"]

        # 必要字段
        sleep_score = None
        try:
            sleep_score = dto["sleepScores"]["overall"]["value"]
        except Exception:
            pass

        total_min = sec_to_min(dto.get("sleepTimeSeconds"))
        deep_min = sec_to_min(dto.get("deepSleepSeconds"))
        light_min = sec_to_min(dto.get("lightSleepSeconds"))
        rem_min = sec_to_min(dto.get("remSleepSeconds"))
        awake_min = sec_to_min(dto.get("awakeSleepSeconds"))

        bedtime = ms_to_iso_local(dto.get("sleepStartTimestampLocal"))
        wake_time = ms_to_iso_local(dto.get("sleepEndTimestampLocal"))

        # Garmin 里更稳定的是 avgHeartRate（不是 restingHeartRate）
        resting_hr = dto.get("avgHeartRate")

        # Garmin CN: HRV（夜间均值）
        hrv_ms = None
        hrv_week = None
        hrv_status = None
        try:
            hrv = garmin.get_hrv_data(date_str)
            hrv_summary = hrv.get("hrvSummary", {})
            hrv_ms = hrv_summary.get("lastNightAvg")
            hrv_week = hrv_summary.get("weeklyAvg")
            hrv_status = hrv_summary.get("status")
        except Exception:
            pass

        # “质量”用 Garmin 给的标签补上（Sleep Quality 在你数据里是 None）
        sleep_grade = None
        sleep_feedback = dto.get("sleepScoreFeedback")
        try:
            sleep_grade = dto["sleepScores"]["overall"]["qualifierKey"]  # EXCELLENT/GOOD/FAIR...
        except Exception:
            pass

        props = {
            "Date": {"date": {"start": date_str}},
        }

        # 你的数据库字段（已确认存在）
        maybe_number(props, "Sleep Score", sleep_score)
        maybe_number(props, "Total Sleep (min)", total_min)
        maybe_number(props, "Deep (min)", deep_min)
        maybe_number(props, "Light (min)", light_min)
        maybe_number(props, "REM (min)", rem_min)
        maybe_number(props, "Awake (min)", awake_min)
        maybe_date(props, "Bedtime", bedtime)
        maybe_date(props, "Wake Time", wake_time)
        maybe_number(props, "Resting HR", resting_hr)
        maybe_number(props, "HRV (ms)", hrv_ms)

        # 可选增强字段（如果你在 Notion 里新增了，就会写入；没新增也不影响）
        maybe_number(props, "HRV Weekly Avg", hrv_week)
        maybe_select(props, "HRV Status", hrv_status)
        maybe_select(props, "Sleep Grade", sleep_grade)
        maybe_text(props, "Sleep Feedback", sleep_feedback)

        result = notion_upsert(notion_token, notion_sleep_db_id, date_str, props)
        print(date_str, result)
        count += 1

    print("Done:", count, "sleep days synced.")


if __name__ == "__main__":
    main()
