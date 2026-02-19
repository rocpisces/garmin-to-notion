import os
import requests
from datetime import datetime, timedelta, timezone
from garminconnect import Garmin

# --------- Config ---------
TZ = timezone(timedelta(hours=8))  # Asia/Shanghai
DAYS_BACK = int(os.getenv("DAYS_BACK", "14"))  # 拉取最近天数
# --------------------------

def iso_date(dt: datetime) -> str:
    return dt.astimezone(TZ).strftime("%Y-%m-%d")

def to_float(x):
    try:
        return float(x)
    except Exception:
        return None

# ------------------ Notion API Helpers ------------------

NOTION_VERSION = os.getenv("NOTION_VERSION", "2022-06-28")

def notion_headers(token: str):
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }

def notion_query_by_date(notion_token: str, db_id: str, date_str: str):
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    payload = {
        "filter": {
            "property": "Date",
            "date": {"equals": date_str},
        },
        "page_size": 1,
    }
    r = requests.post(url, headers=notion_headers(notion_token), json=payload, timeout=30)
    if r.status_code >= 300:
        raise RuntimeError(f"Notion query failed {r.status_code}: {r.text}")
    return r.json()

def notion_update_page(notion_token: str, page_id: str, props: dict):
    url = f"https://api.notion.com/v1/pages/{page_id}"
    payload = {"properties": props}
    r = requests.patch(url, headers=notion_headers(notion_token), json=payload, timeout=30)
    if r.status_code >= 300:
        raise RuntimeError(f"Notion page update failed {r.status_code}: {r.text}")
    return r.json()

def notion_create_page(notion_token: str, db_id: str, props: dict):
    url = "https://api.notion.com/v1/pages"
    payload = {"parent": {"database_id": db_id}, "properties": props}
    r = requests.post(url, headers=notion_headers(notion_token), json=payload, timeout=30)
    if r.status_code >= 300:
        raise RuntimeError(f"Notion page create failed {r.status_code}: {r.text}")
    return r.json()

def notion_upsert_weight(notion_token: str, db_id: str, date_str: str, props: dict):
    existing = notion_query_by_date(notion_token, db_id, date_str)
    if existing.get("results"):
        page_id = existing["results"][0]["id"]
        notion_update_page(notion_token, page_id, props)
        return "updated"
    else:
        notion_create_page(notion_token, db_id, props)
        return "created"

# ------------------ Main Sync ------------------

def main():
    garmin_email = os.environ["GARMIN_EMAIL"]
    garmin_password = os.environ["GARMIN_PASSWORD"]
    notion_token = os.environ["NOTION_TOKEN"]
    notion_db_id = os.environ["NOTION_WEIGHT_DB_ID"]
    print("NOTION_WEIGHT_DB_ID =", notion_db_id)

    # 诊断：直接 GET 数据库对象，看 Notion 到底认不认这个 ID
    diag_url = f"https://api.notion.com/v1/databases/{notion_db_id}"
    r = requests.get(diag_url, headers=notion_headers(notion_token), timeout=30)
    print("Notion GET /databases status:", r.status_code)
    print("Notion GET /databases body (first 300 chars):", r.text[:300])

    
    garmin = Garmin(garmin_email, garmin_password, is_cn=True)
    garmin.login()

    today = datetime.now(TZ).date()
    start = today - timedelta(days=DAYS_BACK)

    # --- debug: list possible Garmin methods related to weight ---
    candidates = sorted([
        m for m in dir(garmin)
        if any(k in m.lower() for k in ["weight", "body", "composition"])
    ])
    print("Garmin methods containing weight/body/composition:")
    print(", ".join(candidates))

    # Try multiple getters
    def try_call(method_name, *args):
        fn = getattr(garmin, method_name, None)
        if not callable(fn):
            return None
        try:
            return fn(*args)
        except TypeError:
            return None
        except Exception as e:
            print(f"[WARN] {method_name}{args} failed: {type(e).__name__}: {e}")
            return None

    attempts = [
        ("get_body_composition_by_date_range", (start.isoformat(), today.isoformat())),
        ("get_body_composition", (start.isoformat(), today.isoformat())),
        ("get_body_composition", tuple()),
        ("get_stats_and_body", (start.isoformat(), today.isoformat())),
        ("get_body_weight", (start.isoformat(), today.isoformat())),
        ("get_body_weight", tuple()),
        ("get_weight_data", (start.isoformat(), today.isoformat())),
        ("get_weight_data", tuple()),
    ]

    data = None
    for name, args in attempts:
        data = try_call(name, *args)
        if data:
            print(f"[OK] Using {name}{args}")
            break

    if not data:
        raise RuntimeError("No body composition records fetched from Garmin API.")

    # Parse data into list of dicts
    records = []
    if isinstance(data, dict):
        for key in ["dateWeightList", "weightList", "dailyBodyComp", "bodyCompList", "weighIns", "items"]:
            if key in data and isinstance(data[key], list):
                records = data[key]
                break
        if not records and isinstance(data.get("data"), list):
            records = data["data"]
    elif isinstance(data, list):
        records = data

    if not records:
        print("Raw data type:", type(data))
        print("Raw preview:", str(data)[:800])
        raise RuntimeError("Fetched Garmin data but could not parse.")

    count = 0
    for r in records:
        # Determine date
        date_str = None
        if "calendarDate" in r:
            date_str = r["calendarDate"]
        elif "date" in r:
            date_str = r["date"]
        elif "startTimeInSeconds" in r:
            date_str = iso_date(datetime.fromtimestamp(int(r["startTimeInSeconds"]), TZ))
        if not date_str:
            continue

        try:
            d = datetime.fromisoformat(date_str).date()
        except Exception:
            continue

        if d < start or d > today:
            continue

        # Parse fields
        weight = to_float(r.get("weight") or r.get("weightInKg"))
        body_fat_pct = to_float(r.get("bodyFat") or r.get("bodyFatPct"))
        body_water_pct = to_float(r.get("bodyWater") or r.get("bodyWaterPct"))
        body_fat_kg = to_float(r.get("fatMass") or r.get("bodyFatMass"))
        skeletal_muscle = to_float(r.get("muscleMass") or r.get("skeletalMuscleMass"))
        bone_mass = to_float(r.get("boneMass") or r.get("boneMassInKg"))
        bmi = to_float(r.get("bmi"))
        change = to_float(r.get("delta") or r.get("change") or r.get("weightDelta"))

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

        result = notion_upsert_weight(notion_token, notion_db_id, date_str, props)
        count += 1
        print(f"{date_str}: {result}")

    print(f"Done. Upserted {count} weight records (last {DAYS_BACK} days).")

if __name__ == "__main__":
    main()
