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
    # Garmin 可能返回 g（例如 80550）
    if v > 200:
        return v / 1000.0
    return v


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


def notion_query_last_weight_before(token, db_id, date_str, lookback_days=120):
    """
    从 Notion 里找 date_str 之前最近的一条记录的 Weight，用于算 Change。
    支持漏记（比如隔了几天才有下一条）。
    """
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    # 给一个时间窗口，避免全库扫描
    d = datetime.fromisoformat(date_str).date()
    start = (d - timedelta(days=lookback_days)).isoformat()

    payload = {
        "filter": {
            "and": [
                {"property": "Date", "date": {"on_or_after": start}},
                {"property": "Date", "date": {"before": date_str}},
            ]
        },
        "sorts": [{"property": "Date", "direction": "descending"}],
        "page_size": 1,
    }
    r = requests.post(url, headers=notion_headers(token), json=payload, timeout=30)
    if r.status_code >= 300:
        raise RuntimeError(f"Notion query(last) failed {r.status_code}: {r.text}")

    js = r.json()
    if not js.get("results"):
        return None

    page = js["results"][0]
    props = page.get("properties", {})
    w = props.get("Weight", {}).get("number")
    return w


def notion_update_page(token, page_id, props):
    url = f"https://api.notion.com/v1/pages/{page_id}"
    r = requests.patch(url, headers=notion_headers(token), json={"properties": props}, timeout=30)
    if r.status_code >= 300:
        raise RuntimeError(f"Notion update failed {r.status_code}: {r.text}")
    return r.json()


def notion_create_page(token, db_id, props):
    url = "https://api.notion.com/v1/pages"
    r = requests.post(
        url,
        headers=notion_headers(token),
        json={"parent": {"database_id": db_id}, "properties": props},
        timeout=30,
    )
    if r.status_code >= 300:
        raise RuntimeError(f"Notion create failed {r.status_code}: {r.text}")
    return r.json()


def notion_upsert(token, db_id, date_str, props):
    existing = notion_query_by_date(token, db_id, date_str)
    if existing.get("results"):
        page_id = existing["results"][0]["id"]
        notion_update_page(token, page_id, props)
        return "updated"
    else:
        notion_create_page(token, db_id, props)
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
    if not records:
        raise RuntimeError("No records in Garmin response.")

    # Garmin 记录可能乱序，先按日期升序处理，保证 change 计算稳定
    def rec_date(r):
        ds = r.get("calendarDate") or r.get("date")
        return ds or "0000-00-00"

    records = sorted(records, key=rec_date)

    count = 0
    for r in records:
        date_str = r.get("calendarDate") or r.get("date")
        if not date_str:
            continue

        d = datetime.fromisoformat(date_str).date()
        if d < start or d > today:
            continue

        weight = to_kg_maybe(r.get("weight"))

        body_fat_pct = to_float(r.get("bodyFat"))  # 例如 25.3
        body_water_pct = to_float(r.get("bodyWater"))  # 例如 55.1

        body_fat_kg = to_kg_maybe(r.get("fatMass"))
        skeletal_muscle = to_kg_maybe(r.get("muscleMass"))
        bone_mass = to_kg_maybe(r.get("boneMass"))
        bmi = to_float(r.get("bmi"))

        # 1) 补齐 Body Fat (kg)
        if body_fat_kg is None and weight is not None and body_fat_pct is not None:
            body_fat_kg = round(weight * body_fat_pct / 100.0, 3)

        # 2) 计算 Change：用 Notion 里“前一次记录体重”
        change = None
        if weight is not None:
            prev_w = notion_query_last_weight_before(notion_token, notion_db_id, date_str, lookback_days=180)
            if prev_w is not None:
                change = round(weight - float(prev_w), 3)

        props = {
            "Date": {"date": {"start": date_str}},
            "Weight": {"number": weight},
        }

        def maybe_set(name, val):
            if val is None:
                return
            props[name] = {"number": val}

        maybe_set("Change", change)
        maybe_set("Body Fat %", body_fat_pct)
        maybe_set("Body Fat (kg)", body_fat_kg)
        maybe_set("Skeletal Muscle", skeletal_muscle)
        maybe_set("Bone Mass", bone_mass)
        maybe_set("Body Water %", body_water_pct)
        maybe_set("BMI", bmi)

        result = notion_upsert(notion_token, notion_db_id, date_str, props)
        print(date_str, result)
        count += 1

    print("Done:", count, "records synced.")


if __name__ == "__main__":
    main()
