import os
import requests
from datetime import datetime, timedelta, timezone
from garminconnect import Garmin

TZ = timezone(timedelta(hours=8))
DAYS_BACK = int(os.getenv("DAYS_BACK", "30"))
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "50"))
NOTION_VERSION = "2022-06-28"


def notion_headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def notion_query_by_activity_id(token, db_id, activity_id: int):
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    payload = {
        "filter": {"property": "Activity ID", "number": {"equals": int(activity_id)}},
        "page_size": 1,
    }
    r = requests.post(url, headers=notion_headers(token), json=payload, timeout=30)
    if r.status_code >= 300:
        raise RuntimeError(f"Notion query failed {r.status_code}: {r.text}")
    return r.json()


def notion_upsert_by_activity_id(token, db_id, activity_id: int, props):
    existing = notion_query_by_activity_id(token, db_id, activity_id)
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
        return round(float(x) / 60.0, 2)
    except Exception:
        return None


def meter_to_km(x):
    try:
        return round(float(x) / 1000.0, 3)
    except Exception:
        return None


def speed_mps_to_pace_min_per_km(speed):
    # pace = (1000m / speed m/s) seconds -> minutes
    try:
        if not speed or float(speed) <= 0:
            return None
        sec_per_km = 1000.0 / float(speed)
        return round(sec_per_km / 60.0, 2)
    except Exception:
        return None


def pick(d, *paths, default=None):
    """
    paths: list of tuples, each tuple is nested keys
    """
    for p in paths:
        cur = d
        ok = True
        for k in p:
            if not isinstance(cur, dict) or k not in cur:
                ok = False
                break
            cur = cur[k]
        if ok:
            return cur
    return default


def maybe_number(props, name, val):
    if val is None:
        return
    props[name] = {"number": float(val)}


def maybe_select(props, name, val):
    if val is None:
        return
    props[name] = {"select": {"name": str(val)}}


def main():
    garmin_email = os.environ["GARMIN_EMAIL"]
    garmin_password = os.environ["GARMIN_PASSWORD"]
    notion_token = os.environ["NOTION_TOKEN"]
    notion_activity_db_id = os.environ["NOTION_ACTIVITY_DB_ID"]

    garmin = Garmin(garmin_email, garmin_password, is_cn=True)
    garmin.login()

    # 分页拉取直到覆盖 DAYS_BACK
    cutoff = (datetime.now(TZ) - timedelta(days=DAYS_BACK)).date()

    all_acts = []
    start = 0
    while True:
        batch = garmin.get_activities(start, PAGE_SIZE)  # (start, limit)
        if not batch:
            break

        all_acts.extend(batch)

        # 看到最老的一条是否已经早于 cutoff？早于则可以停
        last = batch[-1]
        st = last.get("startTimeLocal")
        if st:
            # 常见格式: "2026-01-31 01:12:00"
            try:
                last_date = datetime.strptime(st, "%Y-%m-%d %H:%M:%S").date()
                if last_date < cutoff:
                    break
            except Exception:
                pass

        start += PAGE_SIZE
        if start > 2000:  # 安全阈值，防止异常死循环
            break

    count = 0

    for act in all_acts:
        activity_id = act.get("activityId")
        if not activity_id:
            continue

        start_time = act.get("startTimeLocal")
        if not start_time:
            continue

        try:
            dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=TZ)
        except Exception:
            continue

        if dt.date() < cutoff:
            continue

        activity_name = act.get("activityName") or f"Activity {activity_id}"
        activity_type = pick(act, ("activityType", "typeKey"), default=None)

        # 详情（很多字段在 summaryDTO 里）
        activity = garmin.get_activity(activity_id) or {}
        summary = activity.get("summaryDTO", {}) or {}
        training = activity.get("activityTrainingEffect", {}) or {}


        distance_km = meter_to_km(act.get("distance"))
        duration_min = sec_to_min(act.get("duration"))
        calories = act.get("calories")

        avg_hr = act.get("averageHR")
        max_hr = act.get("maxHR")

        avg_pace = speed_mps_to_pace_min_per_km(act.get("averageSpeed"))
        best_pace = speed_mps_to_pace_min_per_km(act.get("maxSpeed"))

        # 兜底：有些字段在 summaryDTO
        avg_power = summary.get("averagePower")
        max_power = summary.get("maxPower")

        aerobic_te = training.get("aerobicTrainingEffect")
        anaerobic_te = training.get("anaerobicTrainingEffect")

        avg_cadence = summary.get("averageRunCadence")


        # 心率区间：多 key 兜底
        hr_zones = (
            details.get("timeInHRZone")
            or details.get("timeInHrZone")
            or details.get("timeInHeartRateZones")
            or summary.get("timeInHRZone")
            or summary.get("timeInHrZone")
            or summary.get("timeInHeartRateZones")
            or []
        )

        z1 = z2 = z3 = z4 = z5 = None
        if isinstance(hr_zones, list):
            for z in hr_zones:
                zn = z.get("zoneNumber") or z.get("zone") or z.get("zoneNum")
                sec = z.get("seconds") or z.get("secs") or z.get("value")
                m = sec_to_min(sec)
                if zn == 1:
                    z1 = m
                elif zn == 2:
                    z2 = m
                elif zn == 3:
                    z3 = m
                elif zn == 4:
                    z4 = m
                elif zn == 5:
                    z5 = m

        # Title 建议带日期避免视觉重复（但唯一键仍是 Activity ID）
        title = f"{dt.strftime('%Y-%m-%d')} · {activity_name}"

        props = {
            "Activity Name": {"title": [{"text": {"content": title}}]},
            "Date": {"date": {"start": dt.isoformat()}},
            "Activity ID": {"number": float(activity_id)},
        }

        maybe_select(props, "Type", activity_type)
        maybe_number(props, "Distance (km)", distance_km)
        maybe_number(props, "Duration (min)", duration_min)
        maybe_number(props, "Calories", calories)
        maybe_number(props, "Avg HR", avg_hr)
        maybe_number(props, "Max HR", max_hr)
        maybe_number(props, "Avg Pace (min/km)", avg_pace)
        maybe_number(props, "Best Pace (min/km)", best_pace)
        maybe_number(props, "Aerobic TE", aerobic_te)
        maybe_number(props, "Anaerobic TE", anaerobic_te)
        maybe_number(props, "Avg Power", avg_power)
        maybe_number(props, "Max Power", max_power)
        maybe_number(props, "Avg Cadence", avg_cadence)

        maybe_number(props, "Z1 (min)", z1)
        maybe_number(props, "Z2 (min)", z2)
        maybe_number(props, "Z3 (min)", z3)
        maybe_number(props, "Z4 (min)", z4)
        maybe_number(props, "Z5 (min)", z5)

        result = notion_upsert_by_activity_id(notion_token, notion_activity_db_id, activity_id, props)
        print(activity_id, title, result)
        count += 1

    print("Done:", count, "activities synced.")


if __name__ == "__main__":
    main()
