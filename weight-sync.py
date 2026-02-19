import os
from datetime import datetime, timedelta, timezone

from garminconnect import Garmin
from notion_client import Client

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


def notion_query_by_date(notion: Client, db_id: str, date_str: str):
    # Date property must be named "Date"
    return notion.databases.query(
        database_id=db_id,
        filter={
            "property": "Date",
            "date": {"equals": date_str},
        },
        page_size=1,
    )


def notion_upsert_weight(notion: Client, db_id: str, date_str: str, props: dict):
    existing = notion_query_by_date(notion, db_id, date_str)
    if existing.get("results"):
        page_id = existing["results"][0]["id"]
        notion.pages.update(page_id=page_id, properties=props)
        return "updated"
    else:
        notion.pages.create(
            parent={"database_id": db_id},
            properties=props,
        )
        return "created"


def main():
    garmin_email = os.environ["GARMIN_EMAIL"]
    garmin_password = os.environ["GARMIN_PASSWORD"]
    notion_token = os.environ["NOTION_TOKEN"]
    notion_db_id = os.environ["NOTION_WEIGHT_DB_ID"]

    # Login Garmin CN
    garmin = Garmin(garmin_email, garmin_password, is_cn=True)
    garmin.login()

    notion = Client(auth=notion_token)

    # 拉取最近 N 天体重（防漏记；每天 upsert）
    today = datetime.now(TZ).date()
    start = today - timedelta(days=DAYS_BACK)

    # garminconnect 的体重 API 在不同版本里字段略有差异：
    # 尝试两种常见方法：get_body_composition / get_body_composition_by_date_range
    # 若都不可用，会抛错并在 Actions 日志里看到（我们再按你的实际返回改）。
    records = []

    # 方案 A：按日期区间取
    for getter_name in ["get_body_composition_by_date_range", "get_body_composition"]:
        getter = getattr(garmin, getter_name, None)
        if callable(getter):
            try:
                if getter_name == "get_body_composition_by_date_range":
                    data = getter(start.isoformat(), today.isoformat())
                else:
                    # 有的版本返回最近一段时间，需要自己筛
                    data = getter()
                # data 可能是 dict 或 list
                if isinstance(data, dict) and "dateWeightList" in data:
                    records = data["dateWeightList"]
                elif isinstance(data, list):
                    records = data
                elif isinstance(data, dict) and "weightList" in data:
                    records = data["weightList"]
                else:
                    # 兜底：尝试直接当 list 处理
                    records = data
                break
            except Exception:
                continue

    if not records:
        raise RuntimeError("No body composition records fetched from Garmin API.")

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

        result = notion_upsert_weight(notion, notion_db_id, date_str, props)
        count += 1
        print(f"{date_str}: {result}")

    print(f"Done. Upserted {count} weight records (last {DAYS_BACK} days).")


if __name__ == "__main__":
    main()
