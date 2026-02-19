import os
import requests
from datetime import datetime, timedelta, timezone
from garminconnect import Garmin

TZ = timezone(timedelta(hours=8))
DAYS_BACK = int(os.getenv("DAYS_BACK", "30"))
NOTION_VERSION = "2022-06-28"


def to_float(x):
    try:
        return float(x)
    except Exception:
        return None


def to_kg_maybe(x):
    v = to_float(x)
    if v is None:
        return None
    if v > 200:
        return v / 1000.0
    return v


def to_kg_delta_maybe(x):
    v = to_float(x)
    if v is None:
        return None
    if abs(v) > 20:
        return v / 1000.0
    return v


def notion_headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def notion_query(token, db_id, date_str):
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    payload = {
        "filter": {
            "property": "Date",
            "date": {"equals": date_str},
        },
        "page_size": 1,
    }
    r = requests.post(url, headers=notion_headers(token), json=payload)
    if r.status_code >= 300:
        raise RuntimeError(r.text)
    return r.json()


def notion_upsert(token, db_id, date_str, props):
    existing = notion_query(token, db_id, date_str)
    if existing.get("results"):
        page_id = existing["results"][0]["id"]
        url = f"https://api.notion.com/v1/pages/{page_id}"
        r = requests.patch(url, headers=notion_headers(token), json={"properties": props})
        if r.status_code >= 300:
            raise RuntimeError(r.text)
        return "updated"
    else:
        url = "https://api.notion.com/v1/pages"
        r = requests.post(
            url,
            headers=notion_headers(token),
            json={"parent": {"database_id": db_id}, "properties": props},
        )
        if r.status_code >= 300:
            raise RuntimeError(r.text)
        return "created"


def main():
    garmin_email = os.environ["GARMIN_EMAIL"]
    garmin_password = os.environ["GARMIN_PASSWORD"]
    notion_token = os.environ["NOTION_TOKEN"]
    notion_db_id = os.environ["NOTION_WEIGHT_DB_ID"]

    garmin = Garmin(garmin_email, garmin_password, is_cn=True)
    garmin.login()

    today = datetime.now(TZ).date()
    start = today - timedelta(days=DAYS_BACK)

    data = garmin.get_body_composition(start.isoformat(), today.isoformat())

    if not data:
        raise RuntimeError("No Garmin data returned.")

    records = data if isinstance(data, list) else data.get("dateWeightList", [])

    count = 0

    for r in records:
        date_str = r.get("calendarDate") or r.get("date")
        if not date_str:
            continue

        d = datetime.fromisoformat(date_str).date()
        if d < start or d > today:
            continue

        weight = to_kg_maybe(r.get("weight"))
        body_fat_pct = to_float(r.get("bodyFat"))
        body_water_pct = to_float(r.get("bodyWater"))
        body_fat_kg = to_kg_maybe(r.get("fatMass"))
        skeletal_muscle = to_kg_maybe(r.get("muscleMass"))
        bone_mass = to_kg_maybe(r.get("boneMass"))
        bmi = to_float(r.get("bmi"))
        change = to_kg_delta_maybe(r.get("delta"))

        props = {
            "Date": {"date": {"start": date_str}},
            "Weight": {"number": weight},
        }

        if change is not None:
            props["Change"] = {"number": change}
        if body_fat_pct is not None:
            props["Body Fat %"] = {"number": body_fat_pct}
        if body_fat_kg is not None:
            props["Body Fat (kg)"] = {"number": body_fat_kg}
        if skeletal_muscle is not None:
            props["Skeletal Muscle"] = {"number": skeletal_muscle}
        if bone_mass is not None:
            props["Bone Mass"] = {"number": bone_mass}
        if body_water_pct is not None:
            props["Body Water %"] = {"number": body_water_pct}
        if bmi is not None:
            props["BMI"] = {"number": bmi}

        result = notion_upsert(notion_token, notion_db_id, date_str, props)
        print(date_str, result)
        count += 1

    print("Done:", count, "records synced.")


if __name__ == "__main__":
    main()
