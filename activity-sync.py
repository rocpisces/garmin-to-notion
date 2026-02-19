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
    try:
        if not speed or float(speed) <= 0:
            return None
        sec_per_km = 1000.0 / float(speed)
        return round(sec_per_km / 60.0, 2)
    except Exception:
        return None


def maybe_number(props, name, val):
    if val is None:
        return
    props[name] = {"number": float(val)}


def maybe_select(props, name, val):
    if val is None:
        return
    props[name] = {"select": {"name": str(val)}}


def safe_call(obj, method_name, *args, **kwargs):
    fn = getattr(obj, method_name, None)
    if not callable(fn):
        return None
    try:
        return fn(*args, **kwargs)
    except Exception:
        return None


def get_hr_zone_thresholds():
    # 优先环境变量（你可以放在 GitHub Secrets）
    def env_int(name, default):
        try:
            return int(os.getenv(name, default))
        except Exception:
            return int(default)

    z1 = env_int("HR_Z1_MAX", "120")
    z2 = env_int("HR_Z2_MAX", "140")
    z3 = env_int("HR_Z3_MAX", "155")
    z4 = env_int("HR_Z4_MAX", "170")
    z5 = env_int("HR_Z5_MAX", "999")
    return z1, z2, z3, z4, z5


def bucket_hr_zones_from_stream(hr_values):
    """
    hr_values: list[int] 每个点默认代表1秒（若你的 stream 是每秒/每n秒采样，这里是近似）
    输出 Z1..Z5 分钟
    """
    if not hr_values:
        return None, None, None, None, None

    z1_max, z2_max, z3_max, z4_max, z5_max = get_hr_zone_thresholds()

    sec = [0, 0, 0, 0, 0]
    for hr in hr_values:
        if hr is None:
            continue
        try:
            h = int(hr)
        except Exception:
            continue

        if h <= z1_max:
            sec[0] += 1
        elif h <= z2_max:
            sec[1] += 1
        elif h <= z3_max:
            sec[2] += 1
        elif h <= z4_max:
            sec[3] += 1
        elif h <= z5_max:
            sec[4] += 1
        else:
            sec[4] += 1

    return tuple(round(s / 60.0, 2) for s in sec)


def extract_hr_stream(details):
    """
    你之前日志里有 heartRateDTOs。
    我们把里面的 bpm 拉出来。
    """
    hr_dtos = details.get("heartRateDTOs") if isinstance(details, dict) else None
    if not isinstance(hr_dtos, list):
        return []
    vals = []
    for p in hr_dtos:
        # 常见字段：heartRate / value
        v = p.get("heartRate") if isinstance(p, dict) else None
        if v is None and isinstance(p, dict):
            v = p.get("value")
        vals.append(v)
    return vals


def extract_power_stream(details):
    """
    有些账号会在 activityDetailMetrics 里包含 power/cadence 等 stream。
    这里做一个通用抓取：遍历 activityDetailMetrics 找含 'power' 的
    """
    metrics = details.get("activityDetailMetrics") if isinstance(details, dict) else None
    if not isinstance(metrics, list):
        return []

    # metricDescriptors 里会给 key/index 映射，但太复杂；
    # 我们用一个更稳的：直接找每条 metric 的 'metrics' 数组并取数值
    # 如果你的返回结构不同，这段可能取不到，但不会报错。
    power_vals = []

    for m in metrics:
        if not isinstance(m, dict):
            continue
        # 常见：m['metrics'] 是数组，每个元素包含 value
        arr = m.get("metrics")
        if not isinstance(arr, list):
            continue
        for it in arr:
            if not isinstance(it, dict):
                continue
            # Garmin 里 power 有时叫 'power'
            v = it.get("power")
            if v is None:
                v = it.get("value")
            # 过滤掉明显不合理值
            try:
                fv = float(v)
            except Exception:
                continue
            if 0 <= fv <= 2000:
                power_vals.append(fv)

    return power_vals


def mean_max(vals):
    if not vals:
        return None, None
    try:
        m = sum(vals) / len(vals)
        mx = max(vals)
        return round(m, 1), round(mx, 1)
    except Exception:
        return None, None


def main():
    garmin_email = os.environ["GARMIN_EMAIL"]
    garmin_password = os.environ["GARMIN_PASSWORD"]
    notion_token = os.environ["NOTION_TOKEN"]
    notion_activity_db_id = os.environ["NOTION_ACTIVITY_DB_ID"]

    garmin = Garmin(garmin_email, garmin_password, is_cn=True)
    garmin.login()

    cutoff = (datetime.now(TZ) - timedelta(days=DAYS_BACK)).date()

    # 分页取活动列表
    all_acts = []
    start = 0
    while True:
        batch = garmin.get_activities(start, PAGE_SIZE)
        if not batch:
            break
        all_acts.extend(batch)

        last = batch[-1]
        st = last.get("startTimeLocal")
        if st:
            try:
                last_date = datetime.strptime(st, "%Y-%m-%d %H:%M:%S").date()
                if last_date < cutoff:
                    break
            except Exception:
                pass

        start += PAGE_SIZE
        if start > 3000:
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
        activity_type = (act.get("activityType") or {}).get("typeKey")

        # summary（稳定）
        activity = safe_call(garmin, "get_activity", activity_id) or {}
        summary = activity.get("summaryDTO", {}) if isinstance(activity, dict) else {}

        # details（用于 stream 兜底算 zones/power）
        details = safe_call(garmin, "get_activity_details", activity_id) or {}

        # --- 基础字段 ---
        distance_km = meter_to_km(act.get("distance") or summary.get("distance"))
        duration_min = sec_to_min(act.get("duration") or summary.get("duration") or summary.get("movingDuration"))
        calories = act.get("calories") or summary.get("calories")

        avg_hr = act.get("averageHR") or summary.get("averageHR")
        max_hr = act.get("maxHR") or summary.get("maxHR")

        avg_pace = speed_mps_to_pace_min_per_km(act.get("averageSpeed") or summary.get("averageSpeed"))
        best_pace = speed_mps_to_pace_min_per_km(act.get("maxSpeed") or summary.get("maxSpeed"))

        # --- TE：优先专用方法（若库里有） ---
        training = (
            activity.get("activityTrainingEffect") if isinstance(activity, dict) else None
        ) or safe_call(garmin, "get_activity_training_effect", activity_id) or {}
        aerobic_te = None
        anaerobic_te = None
        if isinstance(training, dict):
            aerobic_te = training.get("aerobicTrainingEffect") or training.get("aerobic")
            anaerobic_te = training.get("anaerobicTrainingEffect") or training.get("anaerobic")

        # --- Cadence：summary 或专用方法 ---
        avg_cadence = summary.get("averageRunCadence") if isinstance(summary, dict) else None
        if avg_cadence is None:
            cad = safe_call(garmin, "get_activity_cadence", activity_id)
            if isinstance(cad, dict):
                avg_cadence = cad.get("averageRunCadence") or cad.get("avgCadence")

        # --- Power：summary / 专用 / stream 估算 ---
        avg_power = summary.get("averagePower") if isinstance(summary, dict) else None
        max_power = summary.get("maxPower") if isinstance(summary, dict) else None

        if avg_power is None or max_power is None:
            pow_obj = safe_call(garmin, "get_activity_power", activity_id)
            if isinstance(pow_obj, dict):
                avg_power = avg_power or pow_obj.get("averagePower") or pow_obj.get("avgPower")
                max_power = max_power or pow_obj.get("maxPower") or pow_obj.get("peakPower")

        if avg_power is None or max_power is None:
            # 最后兜底：从 stream 估算（如果抓得到）
            pvals = extract_power_stream(details) if isinstance(details, dict) else []
            m, mx = mean_max(pvals)
            avg_power = avg_power or m
            max_power = max_power or mx

        # --- HR zones：优先专用方法，否则用 HR stream 分桶 ---
        z1 = z2 = z3 = z4 = z5 = None

        zones_obj = safe_call(garmin, "get_activity_hr_in_time_zones", activity_id)
        if isinstance(zones_obj, dict):
            # 尝试解析常见结构
            zones_list = (
                zones_obj.get("timeInHRZone")
                or zones_obj.get("timeInHeartRateZones")
                or zones_obj.get("zones")
                or []
            )
            if isinstance(zones_list, list):
                for z in zones_list:
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
        else:
            # 兜底：用 HR stream 分桶统计
            hr_vals = extract_hr_stream(details) if isinstance(details, dict) else []
            if hr_vals:
                z1, z2, z3, z4, z5 = bucket_hr_zones_from_stream(hr_vals)

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
