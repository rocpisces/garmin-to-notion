import os
import time
import json
import math
import hashlib
import requests
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from garminconnect import Garmin

# -----------------------------
# Config
# -----------------------------
TZ = timezone(timedelta(hours=int(os.getenv("TZ_OFFSET_HOURS", "8"))))
DAYS_BACK = int(os.getenv("DAYS_BACK", "30"))
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "50"))
MAX_ACTIVITIES = int(os.getenv("MAX_ACTIVITIES", "3000"))

NOTION_VERSION = os.getenv("NOTION_VERSION", "2022-06-28")
NOTION_REQ_INTERVAL_SEC = float(os.getenv("NOTION_REQ_INTERVAL_SEC", "0.35"))  # basic throttle
NOTION_MAX_RETRIES = int(os.getenv("NOTION_MAX_RETRIES", "6"))

# If you want to download FIT/TCX/GPX to disk (optional)
DOWNLOAD_FILES = os.getenv("DOWNLOAD_FILES", "0") == "1"
FILES_DIR = os.getenv("FILES_DIR", "./garmin_files")

# If you host files via a base URL (optional)
# Example: https://your-domain.com/garmin_files
FILES_BASE_URL = os.getenv("FILES_BASE_URL", "").rstrip("/")

# Sync splits DB (optional)
SYNC_SPLITS = os.getenv("SYNC_SPLITS", "0") == "1"

# Garmin CN
GARMIN_IS_CN = os.getenv("GARMIN_IS_CN", "1") == "1"

# -----------------------------
# Notion property names (customizable)
# Make these match your Notion DB fields.
# -----------------------------
P = {
    # Activities DB
    "ACT_TITLE": os.getenv("PROP_ACT_TITLE", "Activity Name"),  # Title property
    "ACT_ID": os.getenv("PROP_ACT_ID", "Activity ID"),          # Number property used for upsert
    "DATE": os.getenv("PROP_DATE", "Date"),                     # Date
    "TYPE": os.getenv("PROP_TYPE", "Type"),                     # Select

    "DIST_KM": os.getenv("PROP_DIST_KM", "Distance (km)"),
    "DUR_MIN": os.getenv("PROP_DUR_MIN", "Duration (min)"),
    "MOVE_MIN": os.getenv("PROP_MOVE_MIN", "Moving (min)"),

    "AVG_PACE": os.getenv("PROP_AVG_PACE", "Avg Pace (min/km)"),
    "BEST_PACE": os.getenv("PROP_BEST_PACE", "Best Pace (min/km)"),

    "AVG_HR": os.getenv("PROP_AVG_HR", "Avg HR"),
    "MAX_HR": os.getenv("PROP_MAX_HR", "Max HR"),

    "AVG_PWR": os.getenv("PROP_AVG_PWR", "Avg Power"),
    "MAX_PWR": os.getenv("PROP_MAX_PWR", "Max Power"),

    "ELEV_GAIN": os.getenv("PROP_ELEV_GAIN", "Elev Gain (m)"),
    "CAL": os.getenv("PROP_CAL", "Calories"),

    "TE_AER": os.getenv("PROP_TE_AER", "Aerobic TE"),
    "TE_ANA": os.getenv("PROP_TE_ANA", "Anaerobic TE"),

    # Running dynamics (optional)
    "CAD": os.getenv("PROP_CAD", "Avg Cadence"),
    "GCT": os.getenv("PROP_GCT", "Avg GCT (ms)"),
    "VO": os.getenv("PROP_VO", "Avg VO (cm)"),
    "STRIDE": os.getenv("PROP_STRIDE", "Avg Stride (m)"),

    # HR zones (seconds)
    "HR_Z1": os.getenv("PROP_HR_Z1", "HR Z1 (s)"),
    "HR_Z2": os.getenv("PROP_HR_Z2", "HR Z2 (s)"),
    "HR_Z3": os.getenv("PROP_HR_Z3", "HR Z3 (s)"),
    "HR_Z4": os.getenv("PROP_HR_Z4", "HR Z4 (s)"),
    "HR_Z5": os.getenv("PROP_HR_Z5", "HR Z5 (s)"),

    # Power zones (seconds)
    "PZ1": os.getenv("PROP_PZ1", "PZ1 (s)"),
    "PZ2": os.getenv("PROP_PZ2", "PZ2 (s)"),
    "PZ3": os.getenv("PROP_PZ3", "PZ3 (s)"),
    "PZ4": os.getenv("PROP_PZ4", "PZ4 (s)"),
    "PZ5": os.getenv("PROP_PZ5", "PZ5 (s)"),

    # File links (URL)
    "FIT_URL": os.getenv("PROP_FIT_URL", "FIT Link"),
    "TCX_URL": os.getenv("PROP_TCX_URL", "TCX Link"),
    "GPX_URL": os.getenv("PROP_GPX_URL", "GPX Link"),

    # Splits DB (if enabled)
    "SPLIT_DB_ID": os.getenv("NOTION_SPLITS_DB_ID", ""),
    "SPLIT_TITLE": os.getenv("PROP_SPLIT_TITLE", "Split ID"),          # Title
    "SPLIT_ACT_REL": os.getenv("PROP_SPLIT_ACT_REL", "Activity"),      # Relation to Activities DB
    "SPLIT_KM": os.getenv("PROP_SPLIT_KM", "KM"),
    "SPLIT_TIME": os.getenv("PROP_SPLIT_TIME", "Split Time (s)"),
    "SPLIT_PACE": os.getenv("PROP_SPLIT_PACE", "Split Pace (min/km)"),
    "SPLIT_AVG_HR": os.getenv("PROP_SPLIT_AVG_HR", "Split Avg HR"),
    "SPLIT_AVG_PWR": os.getenv("PROP_SPLIT_AVG_PWR", "Split Avg Power"),
    "SPLIT_CAD": os.getenv("PROP_SPLIT_CAD", "Split Cadence"),
}

# -----------------------------
# Helpers
# -----------------------------
def notion_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }

def _sleep():
    time.sleep(NOTION_REQ_INTERVAL_SEC)

def notion_request(method: str, url: str, token: str, payload: Optional[dict] = None) -> dict:
    """
    Notion API wrapper with basic retry/backoff for 429/5xx.
    """
    headers = notion_headers(token)
    last_err = None
    for i in range(NOTION_MAX_RETRIES):
        try:
            _sleep()
            if method == "POST":
                r = requests.post(url, headers=headers, json=payload, timeout=30)
            elif method == "PATCH":
                r = requests.patch(url, headers=headers, json=payload, timeout=30)
            else:
                r = requests.get(url, headers=headers, timeout=30)

            if r.status_code < 300:
                return r.json()

            # retryable
            if r.status_code in (429, 500, 502, 503, 504):
                wait = min(20, (2 ** i) + 0.3)
                last_err = f"{r.status_code}: {r.text}"
                time.sleep(wait)
                continue

            raise RuntimeError(f"Notion request failed {r.status_code}: {r.text}")

        except Exception as e:
            last_err = str(e)
            wait = min(20, (2 ** i) + 0.3)
            time.sleep(wait)

    raise RuntimeError(f"Notion request failed after retries: {last_err}")

def notion_query_by_activity_id(token: str, db_id: str, activity_id: int) -> dict:
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    payload = {
        "filter": {"property": P["ACT_ID"], "number": {"equals": int(activity_id)}},
        "page_size": 1,
    }
    return notion_request("POST", url, token, payload)

def notion_upsert_activity(token: str, db_id: str, activity_id: int, props: dict) -> Tuple[str, str]:
    existing = notion_query_by_activity_id(token, db_id, activity_id)
    if existing.get("results"):
        page_id = existing["results"][0]["id"]
        url = f"https://api.notion.com/v1/pages/{page_id}"
        notion_request("PATCH", url, token, {"properties": props})
        return "updated", page_id
    else:
        url = "https://api.notion.com/v1/pages"
        resp = notion_request("POST", url, token, {"parent": {"database_id": db_id}, "properties": props})
        return "created", resp["id"]

def notion_query_split_by_unique(token: str, db_id: str, split_title: str) -> Optional[str]:
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    payload = {"filter": {"property": P["SPLIT_TITLE"], "title": {"equals": split_title}}, "page_size": 1}
    resp = notion_request("POST", url, token, payload)
    if resp.get("results"):
        return resp["results"][0]["id"]
    return None

def notion_upsert_split(token: str, db_id: str, split_title: str, props: dict) -> str:
    page_id = notion_query_split_by_unique(token, db_id, split_title)
    if page_id:
        url = f"https://api.notion.com/v1/pages/{page_id}"
        notion_request("PATCH", url, token, {"properties": props})
        return "updated"
    else:
        url = "https://api.notion.com/v1/pages"
        notion_request("POST", url, token, {"parent": {"database_id": db_id}, "properties": props})
        return "created"

def safe_call(obj: Any, method_names: List[str], *args, **kwargs):
    """
    Try multiple method names for library compatibility.
    """
    for name in method_names:
        fn = getattr(obj, name, None)
        if callable(fn):
            try:
                return fn(*args, **kwargs)
            except Exception:
                continue
    return None

def meter_to_km(x) -> Optional[float]:
    try:
        return round(float(x) / 1000.0, 3)
    except Exception:
        return None

def sec_to_min(x) -> Optional[float]:
    try:
        return round(float(x) / 60.0, 2)
    except Exception:
        return None

def speed_mps_to_pace_min_per_km(speed) -> Optional[float]:
    try:
        s = float(speed)
        if s <= 0:
            return None
        sec_per_km = 1000.0 / s
        return round(sec_per_km / 60.0, 2)
    except Exception:
        return None

def maybe_number(props: dict, name: str, val: Any):
    if val is None:
        return
    try:
        props[name] = {"number": float(val)}
    except Exception:
        pass

def maybe_select(props: dict, name: str, val: Any):
    if val is None:
        return
    props[name] = {"select": {"name": str(val)}}

def maybe_url(props: dict, name: str, url: str):
    if not url:
        return
    props[name] = {"url": url}

def make_title_prop(text: str) -> dict:
    return {"title": [{"text": {"content": text}}]}

def sha1_file(path: str) -> str:
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

# -----------------------------
# Zone thresholds (fallback)
# -----------------------------
def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

def get_hr_zone_thresholds() -> Tuple[int, int, int, int, int]:
    # You can set these via GitHub Secrets
    z1 = env_int("HR_Z1_MAX", 131)
    z2 = env_int("HR_Z2_MAX", 151)
    z3 = env_int("HR_Z3_MAX", 165)
    z4 = env_int("HR_Z4_MAX", 171)
    z5 = env_int("HR_Z5_MAX", 999)
    return z1, z2, z3, z4, z5

def get_power_zone_thresholds() -> Tuple[int, int, int, int, int]:
    # Example from your screenshot: 321-356 etc depends on your settings.
    # Configure your own thresholds.
    p1 = env_int("PZ1_MAX", 320)
    p2 = env_int("PZ2_MAX", 356)
    p3 = env_int("PZ3_MAX", 410)
    p4 = env_int("PZ4_MAX", 9999)  # keep as large
    p5 = env_int("PZ5_MAX", 999999)
    # We still return 5 bounds for uniform bucketing
    return p1, p2, p3, p4, p5

def bucket_zones_seconds(values: List[Any], bounds: Tuple[int, int, int, int, int]) -> Tuple[int, int, int, int, int]:
    """
    values: list[int/float], each sample ~ 1 second (approx)
    bounds: max for zone1..zone5
    return seconds in Z1..Z5
    """
    if not values:
        return (0, 0, 0, 0, 0)
    z1_max, z2_max, z3_max, z4_max, z5_max = bounds
    sec = [0, 0, 0, 0, 0]
    for v in values:
        if v is None:
            continue
        try:
            x = float(v)
        except Exception:
            continue
        if x <= z1_max:
            sec[0] += 1
        elif x <= z2_max:
            sec[1] += 1
        elif x <= z3_max:
            sec[2] += 1
        elif x <= z4_max:
            sec[3] += 1
        elif x <= z5_max:
            sec[4] += 1
        else:
            sec[4] += 1
    return tuple(sec)  # seconds

# -----------------------------
# Stream extractors (fallback)
# -----------------------------
def extract_hr_stream(details: dict) -> List[int]:
    # Common key: heartRateDTOs = [{"heartRate": 123}, ...]
    hr_dtos = details.get("heartRateDTOs")
    if not isinstance(hr_dtos, list):
        return []
    out = []
    for p in hr_dtos:
        if not isinstance(p, dict):
            continue
        v = p.get("heartRate", p.get("value"))
        try:
            out.append(int(v))
        except Exception:
            out.append(None)
    return out

def extract_power_stream(details: dict) -> List[float]:
    # Very inconsistent; try a few common places.
    # 1) direct key
    for k in ("powerDTOs", "powerSamples", "power"):
        arr = details.get(k)
        if isinstance(arr, list):
            vals = []
            for p in arr:
                if isinstance(p, dict):
                    v = p.get("power", p.get("value"))
                else:
                    v = p
                try:
                    fv = float(v)
                    if 0 <= fv <= 2000:
                        vals.append(fv)
                except Exception:
                    continue
            if vals:
                return vals

    # 2) activityDetailMetrics (some accounts)
    metrics = details.get("activityDetailMetrics")
    if isinstance(metrics, list):
        vals = []
        for m in metrics:
            if not isinstance(m, dict):
                continue
            arr = m.get("metrics")
            if not isinstance(arr, list):
                continue
            for it in arr:
                if not isinstance(it, dict):
                    continue
                v = it.get("power", it.get("value"))
                try:
                    fv = float(v)
                    if 0 <= fv <= 2000:
                        vals.append(fv)
                except Exception:
                    continue
        if vals:
            return vals
    return []

# -----------------------------
# Garmin Time-in-Zone parsers
# -----------------------------
def parse_time_in_zone_seconds(zones_obj: Any) -> Optional[Tuple[int, int, int, int, int]]:
    """
    Try to parse Garmin 'time in zone' responses into seconds for zones 1..5.
    """
    if not isinstance(zones_obj, dict):
        return None

    zones_list = (
        zones_obj.get("timeInHRZone")
        or zones_obj.get("timeInHeartRateZones")
        or zones_obj.get("timeInPowerZone")
        or zones_obj.get("timeInPowerZones")
        or zones_obj.get("zones")
        or zones_obj.get("zoneTimes")
        or []
    )
    if not isinstance(zones_list, list) or not zones_list:
        return None

    out = [0, 0, 0, 0, 0]
    for z in zones_list:
        if not isinstance(z, dict):
            continue
        zn = z.get("zoneNumber") or z.get("zone") or z.get("zoneNum") or z.get("number")
        sec = z.get("seconds") or z.get("secs") or z.get("value") or z.get("timeSeconds")
        try:
            zn = int(zn)
            sec = int(float(sec))
        except Exception:
            continue
        if 1 <= zn <= 5:
            out[zn - 1] = sec
    return tuple(out)

# -----------------------------
# Files (optional)
# -----------------------------
def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def local_file_path(activity_id: int, ext: str) -> str:
    ensure_dir(FILES_DIR)
    return os.path.join(FILES_DIR, f"{activity_id}.{ext}")

def file_url_from_local(path: str) -> str:
    if not FILES_BASE_URL:
        # Store local path as URL-ish (still useful as reference)
        return path
    # Convert to a URL under FILES_BASE_URL
    # Assumes FILES_DIR is served as the root of FILES_BASE_URL.
    # If not, adjust mapping.
    filename = os.path.basename(path)
    return f"{FILES_BASE_URL}/{filename}"

def maybe_download_activity_file(garmin: Garmin, activity_id: int, kind: str, ext: str, downloader_methods: List[str]) -> str:
    """
    Returns a URL (or local path) for the file, downloading if enabled.
    """
    path = local_file_path(activity_id, ext)
    if DOWNLOAD_FILES:
        if not os.path.exists(path) or os.path.getsize(path) < 100:
            blob = safe_call(garmin, downloader_methods, activity_id)
            if blob:
                # garminconnect returns bytes for downloads in many cases
                try:
                    with open(path, "wb") as f:
                        if isinstance(blob, (bytes, bytearray)):
                            f.write(blob)
                        else:
                            # sometimes returns str content
                            f.write(str(blob).encode("utf-8"))
                except Exception:
                    pass
    return file_url_from_local(path)

# -----------------------------
# Splits parsing
# -----------------------------
def extract_laps(details: dict) -> List[dict]:
    # try common keys
    for k in ("laps", "lapDTOs", "lapSummaries", "splits"):
        arr = details.get(k)
        if isinstance(arr, list) and arr:
            return arr
    return []

def lap_distance_km(lap: dict) -> Optional[float]:
    for k in ("distance", "lapDistance", "totalDistance"):
        if k in lap:
            return meter_to_km(lap.get(k))
    return None

def lap_duration_s(lap: dict) -> Optional[float]:
    for k in ("duration", "lapDuration", "totalDuration"):
        if k in lap:
            try:
                return float(lap.get(k))
            except Exception:
                return None
    return None

def lap_avg_hr(lap: dict) -> Optional[float]:
    for k in ("averageHR", "avgHR", "heartRateAverage"):
        if k in lap:
            try:
                return float(lap.get(k))
            except Exception:
                return None
    return None

def lap_avg_power(lap: dict) -> Optional[float]:
    for k in ("averagePower", "avgPower", "powerAverage"):
        if k in lap:
            try:
                return float(lap.get(k))
            except Exception:
                return None
    return None

def lap_cadence(lap: dict) -> Optional[float]:
    for k in ("averageRunCadence", "avgCadence", "cadenceAverage"):
        if k in lap:
            try:
                return float(lap.get(k))
            except Exception:
                return None
    return None

# -----------------------------
# Main
# -----------------------------
def main():
    garmin_email = os.environ["GARMIN_EMAIL"]
    garmin_password = os.environ["GARMIN_PASSWORD"]
    notion_token = os.environ["NOTION_TOKEN"]
    notion_activity_db_id = os.environ["NOTION_ACTIVITY_DB_ID"]

    garmin = Garmin(garmin_email, garmin_password, is_cn=GARMIN_IS_CN)
    garmin.login()

    cutoff = (datetime.now(TZ) - timedelta(days=DAYS_BACK)).date()

    # 1) Fetch activities with pagination
    all_acts = []
    start = 0
    while True:
        batch = garmin.get_activities(start, PAGE_SIZE)
        if not batch:
            break
        all_acts.extend(batch)

        # stop early if older than cutoff
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
        if start > MAX_ACTIVITIES:
            break

    synced = 0

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

        # Stable summary
        activity = safe_call(garmin, ["get_activity"], activity_id) or {}
        summary = activity.get("summaryDTO", {}) if isinstance(activity, dict) else {}

        # Details for splits & stream fallback
        details = safe_call(garmin, ["get_activity_details"], activity_id) or {}
        if not isinstance(details, dict):
            details = {}

        # -------- Base fields --------
        distance_km = meter_to_km(act.get("distance") or summary.get("distance"))
        duration_s = act.get("duration") or summary.get("duration") or summary.get("elapsedDuration")
        moving_s = summary.get("movingDuration") or summary.get("movingTime") or act.get("movingDuration")

        duration_min = sec_to_min(duration_s)
        moving_min = sec_to_min(moving_s)

        calories = act.get("calories") or summary.get("calories")
        elev_gain = summary.get("elevationGain") or summary.get("elevationGainMeters") or act.get("elevationGain")

        avg_hr = act.get("averageHR") or summary.get("averageHR")
        max_hr = act.get("maxHR") or summary.get("maxHR")

        avg_pace = speed_mps_to_pace_min_per_km(act.get("averageSpeed") or summary.get("averageSpeed"))
        best_pace = speed_mps_to_pace_min_per_km(act.get("maxSpeed") or summary.get("maxSpeed"))

        # TE
        training = (activity.get("activityTrainingEffect") if isinstance(activity, dict) else None) \
                   or safe_call(garmin, ["get_activity_training_effect"], activity_id) \
                   or {}
        aerobic_te = None
        anaerobic_te = None
        if isinstance(training, dict):
            aerobic_te = training.get("aerobicTrainingEffect") or training.get("aerobic")
            anaerobic_te = training.get("anaerobicTrainingEffect") or training.get("anaerobic")

        # Cadence
        avg_cadence = summary.get("averageRunCadence")
        if avg_cadence is None:
            cad_obj = safe_call(garmin, ["get_activity_cadence"], activity_id)
            if isinstance(cad_obj, dict):
                avg_cadence = cad_obj.get("averageRunCadence") or cad_obj.get("avgCadence")

        # Power
        avg_power = summary.get("averagePower")
        max_power = summary.get("maxPower")
        if avg_power is None or max_power is None:
            pow_obj = safe_call(garmin, ["get_activity_power"], activity_id)
            if isinstance(pow_obj, dict):
                avg_power = avg_power or pow_obj.get("averagePower") or pow_obj.get("avgPower")
                max_power = max_power or pow_obj.get("maxPower") or pow_obj.get("peakPower")

        if avg_power is None or max_power is None:
            pvals = extract_power_stream(details)
            if pvals:
                avg_power = avg_power or round(sum(pvals) / len(pvals), 1)
                max_power = max_power or round(max(pvals), 1)

        # Running dynamics (best-effort)
        gct = summary.get("avgGroundContactTime") or summary.get("averageGroundContactTime")
        vo = summary.get("avgVerticalOscillation") or summary.get("averageVerticalOscillation")
        stride = summary.get("avgStrideLength") or summary.get("averageStrideLength")

        # -------- HR Time in Zone (seconds) --------
        hr_zones = None
        zones_obj = safe_call(
            garmin,
            ["get_activity_hr_in_time_zones", "get_activity_hr_in_timezones", "get_activity_hr_in_timezones"],
            activity_id
        )
        hr_zones = parse_time_in_zone_seconds(zones_obj)
        if hr_zones is None:
            # fallback by stream bucketing
            hr_vals = extract_hr_stream(details)
            if hr_vals:
                hr_zones = bucket_zones_seconds(hr_vals, get_hr_zone_thresholds())
            else:
                hr_zones = (0, 0, 0, 0, 0)

        # -------- Power Time in Zone (seconds) --------
        pwr_zones = None
        pz_obj = safe_call(
            garmin,
            ["get_activity_power_in_time_zones", "get_activity_power_in_timezones", "get_activity_power_in_timezones"],
            activity_id
        )
        pwr_zones = parse_time_in_zone_seconds(pz_obj)
        if pwr_zones is None:
            pvals = extract_power_stream(details)
            if pvals:
                pwr_zones = bucket_zones_seconds(pvals, get_power_zone_thresholds())
            else:
                pwr_zones = (0, 0, 0, 0, 0)

        # -------- Files links (optional download) --------
        fit_url = maybe_download_activity_file(
            garmin, activity_id, "fit", "fit",
            downloader_methods=["download_activity", "download_activity_fit", "download_activity_file_fit"]
        )
        tcx_url = maybe_download_activity_file(
            garmin, activity_id, "tcx", "tcx",
            downloader_methods=["download_activity_tcx", "download_activity_file_tcx"]
        )
        gpx_url = maybe_download_activity_file(
            garmin, activity_id, "gpx", "gpx",
            downloader_methods=["download_activity_gpx", "download_activity_file_gpx"]
        )

        # -------- Build Notion properties --------
        title = f"{dt.strftime('%Y-%m-%d')} · {activity_name}"
        props: Dict[str, Any] = {
            P["ACT_TITLE"]: make_title_prop(title),
            P["DATE"]: {"date": {"start": dt.isoformat()}},
            P["ACT_ID"]: {"number": float(activity_id)},
        }

        maybe_select(props, P["TYPE"], activity_type)

        maybe_number(props, P["DIST_KM"], distance_km)
        maybe_number(props, P["DUR_MIN"], duration_min)
        maybe_number(props, P["MOVE_MIN"], moving_min)

        maybe_number(props, P["AVG_PACE"], avg_pace)
        maybe_number(props, P["BEST_PACE"], best_pace)

        maybe_number(props, P["AVG_HR"], avg_hr)
        maybe_number(props, P["MAX_HR"], max_hr)

        maybe_number(props, P["AVG_PWR"], avg_power)
        maybe_number(props, P["MAX_PWR"], max_power)

        maybe_number(props, P["ELEV_GAIN"], elev_gain)
        maybe_number(props, P["CAL"], calories)

        maybe_number(props, P["TE_AER"], aerobic_te)
        maybe_number(props, P["TE_ANA"], anaerobic_te)

        maybe_number(props, P["CAD"], avg_cadence)
        maybe_number(props, P["GCT"], gct)
        maybe_number(props, P["VO"], vo)
        maybe_number(props, P["STRIDE"], stride)

        # zones seconds
        hz1, hz2, hz3, hz4, hz5 = hr_zones
        pz1, pz2, pz3, pz4, pz5 = pwr_zones

        maybe_number(props, P["HR_Z1"], hz1)
        maybe_number(props, P["HR_Z2"], hz2)
        maybe_number(props, P["HR_Z3"], hz3)
        maybe_number(props, P["HR_Z4"], hz4)
        maybe_number(props, P["HR_Z5"], hz5)

        maybe_number(props, P["PZ1"], pz1)
        maybe_number(props, P["PZ2"], pz2)
        maybe_number(props, P["PZ3"], pz3)
        maybe_number(props, P["PZ4"], pz4)
        maybe_number(props, P["PZ5"], pz5)

        maybe_url(props, P["FIT_URL"], fit_url)
        maybe_url(props, P["TCX_URL"], tcx_url)
        maybe_url(props, P["GPX_URL"], gpx_url)

        # -------- Upsert to Notion --------
        status, notion_page_id = notion_upsert_activity(notion_token, notion_activity_db_id, activity_id, props)
        print(activity_id, title, status)
        synced += 1

        # -------- Optional: Sync splits --------
        if SYNC_SPLITS:
            if not P["SPLIT_DB_ID"]:
                raise RuntimeError("SYNC_SPLITS=1 but NOTION_SPLITS_DB_ID is empty")

            laps = extract_laps(details)
            # If Garmin laps are not exactly 1km, we still store them as lap index.
            for idx, lap in enumerate(laps, start=1):
                # Build a unique title for split page
                split_title = f"{activity_id}-L{idx}"

                dist_km = lap_distance_km(lap)
                dur_s = lap_duration_s(lap)
                pace = None
                if dur_s and dist_km and dist_km > 0:
                    pace = round((dur_s / dist_km) / 60.0, 2)

                s_props = {
                    P["SPLIT_TITLE"]: make_title_prop(split_title),
                    # Relation to the activity page
                    P["SPLIT_ACT_REL"]: {"relation": [{"id": notion_page_id}]},
                }
                maybe_number(s_props, P["SPLIT_KM"], dist_km if dist_km is not None else idx)
                maybe_number(s_props, P["SPLIT_TIME"], dur_s)
                maybe_number(s_props, P["SPLIT_PACE"], pace)
                maybe_number(s_props, P["SPLIT_AVG_HR"], lap_avg_hr(lap))
                maybe_number(s_props, P["SPLIT_AVG_PWR"], lap_avg_power(lap))
                maybe_number(s_props, P["SPLIT_CAD"], lap_cadence(lap))

                s_status = notion_upsert_split(notion_token, P["SPLIT_DB_ID"], split_title, s_props)
                print("  split", split_title, s_status)

    print(f"Done: {synced} activities synced.")

if __name__ == "__main__":
    main()
