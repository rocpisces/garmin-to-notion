import os
from datetime import datetime, timedelta, timezone

from garminconnect import Garmin
import requests

# --------- Config ---------
TZ = timezone(timedelta(hours=8))  # Asia/Shanghai
DAYS_BACK = int(os.getenv("DAYS_BACK", "14"))  # 先拉近两周，验证用
# --------------------------


def iso_date(dt: datetime) -> str:
    return dt.astimezone(TZ).strftime("%Y-%m-%d")


def to_float(x):
    try:
        return float(x)
    except Exception:
        return None


NOTION_VERSION = os.getenv("NOTION_VERSION", "2022-06-28")


def notion_headers(token: str):
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def notion_query_by_date(notion_token: str, db_id: str, date_str: str):
    # POST /v1/databases/{database_id}/query
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



def main():
    garmin_email = os.environ["GARMIN_EMAIL"]
    garmin_password = os.environ["GARMIN_PASSWORD"]
    notion_token = os.environ["NOTION_TOKEN"]
    notion_db_id = os.environ["NOTION_WEIGHT_DB_ID"]

    # Login Garmin CN
    garmin = Garmin(garmin_email, garmin_password, is_cn=True)
    garmin.login()


    # 拉取最近 N 天体重（防漏记；每天 upsert）
    today = datetime.now(TZ).date()
    start = today - timedelta(days=DAYS_BACK)

    # garminconnect 的体重 API 在不同版本里字段略有差异：
    # 尝试两种常见方法：get_body_composition / get_body_composition_by_date_range
    # 若都不可用，会抛错并在 Actions 日志里看到（我们再按你的实际返回改）。
    records = []

    # ---------- DEBUG: 列出所有可能的接口名 ----------
    candidates = sorted([
        m for m in dir(garmin)
        if any(k in m.lower() for k in ["weight", "body", "composition"])
    ])
    print("Garmin methods containing weight/body/composition:")
    print(", ".join(candidates))
    # --------------------------------------------------

    def try_call(method_name, *args, **kwargs):
        fn = getattr(garmin, method_name, None)
        if not callable(fn):
            return None
        try:
            return fn(*args, **kwargs)
        except TypeError:
            return None
        except Exception as e:
            print(f"[WARN] {method_name}{args} failed: {type(e).__name__}: {e}")
            return None

    # ---------- 尝试尽可能多的常见方法组合 ----------
    start_s = start.isoformat()
    end_s = today.isoformat()

    attempts = [
        ("get_body_composition_by_date_range", (start_s, end_s), {}),
        ("get_body_composition_by_date_range", (start_s,), {}),
        ("get_body_composition", (start_s, end_s), {}),
        ("get_body_composition", (start_s,), {}),
        ("get_body_composition", tuple(), {}),
        ("get_body_composition_data", (start_s, end_s), {}),
        ("get_body_composition_data", tuple(), {}),
        ("get_weight_data", (start_s, end_s), {}),
        ("get_weight_data", tuple(), {}),
        ("get_weight_by_date_range", (start_s, end_s), {}),
        ("get_weight_by_date_range", tuple(), {}),
        ("get_body_weight", (start_s, end_s), {}),
        ("get_body_weight", tuple(), {}),
    ]

    data = None
    for name, args, kwargs in attempts:
        data = try_call(name, *args, **kwargs)
        if data:
            print(f"[OK] Using {name}{args}")
            break

    if not data:
        raise RuntimeError(
            "No body composition records fetched. "
            "See printed method list above; we will pick the correct one."
        )

    # ---------- 统一把各种返回结构归一成 records(list[dict]) ----------
    records = []
    if isinstance(data, dict):
        # 常见 key 兜底（不同版本会不一样）
        for key in ["dateWeightList", "weightList", "dailyBodyComp", "bodyCompList", "weighIns", "items"]:
            if key in data and isinstance(data[key], list):
                records = data[key]
                break
        if not records and isinstance(data.get("data"), list):
            records = data["data"]
    elif isinstance(data, list):
        records = data

    if not records:
        # 把 data 打出来（截断）便于我们看结构
        print("Raw data type:", type(data))
        print("Raw data preview:", str(data)[:1500])
        raise RuntimeError("Fetched data but could not parse records list.")


    # 统一解析并 upsert
    count = 0
    for r in records:
        # 常见字段：date / calendarDate / startTimeInSeconds / weight / bodyFat / bodyWater / boneMass / muscleMass / bmi / delta
        # 日期
        if "calendarDate" in r:
            date_str = r["calendarDate"]
        elif "date" in r:
            date_str = r["date"]
        elif "startTimeInSeconds" in r:
            date_str = iso_date(datetime.fromtimestamp(int(r["startTimeInSeconds"]), TZ))
        else:
            # 跳过无法识别日期的记录
            continue

        # 只保留 start~today
        try:
            d = datetime.fromisoformat(date_str).date()
        except Exception:
            # 有些会是 "YYYY-MM-DD"
            d = datetime.strptime(date_str[:10], "%Y-%m-%d").date()
            date_str = d.strftime("%Y-%m-%d")

        if d < start or d > today:
            continue

        weight = to_float(r.get("weight") or r.get("weightInKg"))
        if weight is None:
            continue

        # Garmin 体脂/水分通常是百分数（例如 18.8）
        body_fat_pct = to_float(r.get("bodyFat") or r.get("bodyFatPct") or r.get("bodyFatPercentage"))
        body_water_pct = to_float(r.get("bodyWater") or r.get("bodyWaterPct") or r.get("bodyWaterPercentage"))

        # 体内脂肪(kg) 有的字段叫 fatMass
        body_fat_kg = to_float(r.get("fatMass") or r.get("bodyFatMass"))

        skeletal_muscle = to_float(r.get("muscleMass") or r.get("skeletalMuscleMass") or r.get("muscleMassInKg"))
        bone_mass = to_float(r.get("boneMass") or r.get("boneMassInKg"))
        bmi = to_float(r.get("bmi"))

        # 变化（有的字段叫 delta / change）
        change = to_float(r.get("delta") or r.get("change") or r.get("weightDelta"))

        props = {
            "Date": {"date": {"start": date_str}},
            "Weight": {"number": weight},
        }

        # 下面字段只有在 Notion 里存在同名属性时才会写；不存在就跳过，避免报错
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
