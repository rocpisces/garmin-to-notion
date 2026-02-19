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


def notion_query_by_name(token, db_id, name):
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    payload = {
        "filter": {
            "property": "Activity Name",
            "title": {"equals": name},
        },
        "page_size": 1,
    }
    r = requests.post(url, headers=notion_headers(token), json=payload, timeout=30)
    if r.status_code >= 300:
        raise RuntimeError(f"Notion query failed {r.status_code}: {r.text}")
    return r.json()


def notion_upsert(token, db_id, name, props):
    existing = notion_query_by_name(token, db_id, name)
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
    except:
        return None


def meter_to_km(x):
    try:
        return round(float(x) / 1000.0, 3)
    except:
        return None


def sec_per_km_to_min(x):
    try:
        return round(float(x) / 60.0, 2)
    except:
        return None


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

    activities = garmin.get_activities(0, 50)

    count = 0

    for act in activities:

        activity_id = act["activityId"]
        activity_name = act.get("activityName", f"Activity {activity_id}")
        activity_type = act.get("activityType", {}).get("typeKey")

        start_time = act.get("startTimeLocal")
        if not start_time:
            continue

        date_obj = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        if (datetime.now() - date_obj).days > DAYS_BACK:
            continue

        details = garmin.get_activity_details(activity_id)

        distance_km = meter_to_km(act.get("distance"))
        duration_min = sec_to_min(act.get("duration"))
        calories = act.get("calories")

        avg_hr = act.get("averageHR")
        max_hr = act.get("maxHR")

        avg_speed = act.get("averageSpeed")
        best_speed = act.get("maxSpeed")

        avg_pace = None
        best_pace = None

        if avg_speed:
            avg_pace = sec_per_km_to_min(1000 / avg_speed)

        if best_speed:
            best_pace = sec_per_km_to_min(1000 / best_speed)

        avg_power = details.get("averagePower")
        max_power = details.get("maxPower")

        aerobic_te = details.get("aerobicTrainingEffect")
        anaerobic_te = details.get("anaerobicTrainingEffect")

        avg_cadence = details.get("averageRunCadence")

        # 心率区间
        z1 = z2 = z3 = z4 = z5 = None

        hr_zones = details.get("timeInHRZone", [])

        if hr_zones and isinstance(hr_zones, list):
            for zone in hr_zones:
                zone_num = zone.get("zoneNumber")
                minutes = sec_to_min(zone.get("seconds"))
                if zone_num == 1:
                    z1 = minutes
                elif zone_num == 2:
                    z2 = minutes
                elif zone_num == 3:
                    z3 = minutes
                elif zone_num == 4:
                    z4 = minutes
                elif zone_num == 5:
                    z5 = minutes

        props = {
            "Activity Name": {"title": [{"text": {"content": activity_name}}]},
            "Date": {"date": {"start": date_obj.isoformat()}},
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

        result = notion_upsert(notion_token, notion_activity_db_id, activity_name, props)
        print(activity_name, result)
        count += 1

    print("Done:", count, "activities synced.")


if __name__ == "__main__":
    main()
