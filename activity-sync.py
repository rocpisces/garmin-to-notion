import os
import time
import hashlib
import requests
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from garminconnect import Garmin

# -----------------------------
# Basic config
# -----------------------------
TZ = timezone(timedelta(hours=int(os.getenv("TZ_OFFSET_HOURS", "8"))))
DAYS_BACK = int(os.getenv("DAYS_BACK", "30"))
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "50"))
MAX_ACTIVITIES = int(os.getenv("MAX_ACTIVITIES", "3000"))

NOTION_VERSION = os.getenv("NOTION_VERSION", "2022-06-28")
NOTION_REQ_INTERVAL_SEC = float(os.getenv("NOTION_REQ_INTERVAL_SEC", "0.35"))
NOTION_MAX_RETRIES = int(os.getenv("NOTION_MAX_RETRIES", "6"))

# File download (optional)
DOWNLOAD_FILES = os.getenv("DOWNLOAD_FILES", "0") == "1"
FILES_DIR = os.getenv("FILES_DIR", "./garmin_files")
FILES_BASE_URL = os.getenv("FILES_BASE_URL", "").rstrip("/")  # optional, for clickable links

GARMIN_IS_CN = os.getenv("GARMIN_IS_CN", "1") == "1"

# -----------------------------
# Notion: property names (match your DB exports exactly)
# -----------------------------
# Activities DB
A_TITLE = "Activity ID"               # Title column in your Activities DB
A_DATE = "Date"
A_SPORT = "Sport"
A_DIST = "Distance (km)"
A_DUR = "Duration (s)"
A_MOVE = "Moving Time (s)"
A_AVG_PACE = "Avg Pace (min/km)"
A_AVG_HR = "Avg HR"
A_MAX_HR = "Max HR"
A_AVG_PWR = "Avg Power"
A_MAX_PWR = "Max Power"
A_ELEV = "Elev Gain"
A_CAL = "Calories"
A_TE_AER = "TE Aerobic"
A_TE_ANA = "TE Anaerobic"
A_CAD = "Avg Cadence"
A_GCT = "Avg GCT (ms)"
A_VO = "Avg VO (cm)"
A_STRIDE = "Avg Stride Length"
A_HRZ = ["HR Z1 (s)", "HR Z2 (s)", "HR Z3 (s)", "HR Z4 (s)", "HR Z5 (s)"]
A_PZ  = ["PZ1 (s)", "PZ2 (s)", "PZ3 (s)", "PZ4 (s)", "PZ5 (s)"]
A_FIT = "FIT Link"
A_TCX = "TCX Link"
A_GPX = "GPX Link"
A_Z2P = "Z2 %"
A_Z3P = "Z3+ %"

# Splits DB
S_TITLE = "Split ID"       # Title
S_ACT_REL = "Activity"     # Relation to Activities
S_KM = "KM"
S_TIME = "Split Time (s)"
S_PACE = "Split Pace"
S_AVG_HR = "Split Avg HR"
S_AVG_PWR = "Split Avg Power"
S_CAD = "Split Cadence"

# Files DB (Chinese)
F_TITLE = "文件名"          # Title
F_URL = "文件URL"
F_HASH = "文件哈希"
F_TYPE = "文件类型"
F_ACT_REL = "活动"         # Relation to Activities


# -----------------------------
# Notion helpers (retry/backoff)
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
    last_err = None
    for i in range(NOTION_MAX_RETRIES):
        try:
            _sleep()
            if method == "POST":
                r = requests.post(url, headers=notion_headers(token), json=payload, timeout=30)
            elif method == "PATCH":
                r = requests.patch(url, headers=notion_headers(token), json=payload, timeout=30)
            else:
                r = requests.get(url, headers=notion_headers(token), timeout=30)

            if r.status_code < 300:
                return r.json()

            if r.status_code in (429, 500, 502, 503, 504):
                last_err = f"{r.status_code}: {r.text}"
                time.sleep(min(20, (2 ** i) + 0.4))
                continue

            raise RuntimeError(f"Notion request failed {r.status_code}: {r.text}")

        except Exception as e:
            last_err = str(e)
            time.sleep(min(20, (2 ** i) + 0.4))

    raise RuntimeError(f"Notion request failed after retries: {last_err}")

def make_title_prop(text: str) -> dict:
    return {"title": [{"text": {"content": str(text)}}]}

def maybe_number(props: dict, name: str, val: Any):
    if val is None:
        return
    try:
        props[name] = {"number": float(val)}
    except Exception:
        return

def maybe_select(props: dict, name: str, val: Any):
    if val is None:
        return
    props[name] = {"select": {"name": str(val)}}

def maybe_url(props: dict, name: str, url: str):
    if not url:
        return
    props[name] = {"url": url}

def maybe_date(props: dict, name: str, dt_iso: str):
    props[name] = {"date": {"start": dt_iso}}

def notion_query_by_title(token: str, db_id: str, title_prop_name: str, title_value: str) -> dict:
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    payload = {
        "filter": {"property": title_prop_name, "title": {"equals": str(title_value)}},
        "page_size": 1,
    }
    return notion_request("POST", url, token, payload)

def notion_upsert_by_title(token: str, db_id: str, title_prop_name: str, title_value: str, props: dict) -> Tuple[str, str]:
    existing = notion_query_by_title(token, db_id, title_prop_name, title_value)
    if existing.get("results"):
        page_id = existing["results"][0]["id"]
        url = f"https://api.notion.com/v1/pages/{page_id}"
        notion_request("PATCH", url, token, {"properties": props})
        return "updated", page_id
    else:
        url = "https://api.notion.com/v1/pages"
        resp = notion_request("POST", url, token, {"parent": {"database_id": db_id}, "properties": props})
        return "created", resp["id"]


# -----------------------------
# Garmin helpers
# -----------------------------
def safe_call(obj: Any, method_names: List[str], *args, **kwargs):
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

def speed_mps_to_pace_min_per_km(speed) -> Optional[float]:
    try:
        s = float(speed)
        if s <= 0:
            return None
        sec_per_km = 1000.0 / s
        return round(sec_per_km / 60.0, 2)
    except Exception:
        return None

def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

def get_hr_zone_thresholds() -> Tuple[int, int, int, int, int]:
    return (
        env_int("HR_Z1_MAX", 131),
        env_int("HR_Z2_MAX", 151),
        env_int("HR_Z3_MAX", 165),
        env_int("HR_Z4_MAX", 171),
        env_int("HR_Z5_MAX", 999),
    )

def get_power_zone_thresholds() -> Tuple[int, int, int, int, int]:
    return (
        env_int("PZ1_MAX", 320),
        env_int("PZ2_MAX", 356),
        env_int("PZ3_MAX", 410),
        env_int("PZ4_MAX", 9999),
        env_int("PZ5_MAX", 999999),
    )

def bucket_zones_seconds(values: List[Any], bounds: Tuple[int, int, int, int, int]) -> Tuple[int, int, int, int, int]:
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
    return tuple(sec)

def parse_time_in_zone_seconds(zones_obj: Any) -> Optional[Tuple[int, int, int, int, int]]:
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

def extract_hr_stream(details: dict) -> List[int]:
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
    # try common keys first
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

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def local_file_path(activity_id: int, ext: str) -> str:
    ensure_dir(FILES_DIR)
    return os.path.join(FILES_DIR, f"{activity_id}.{ext}")

def file_url_from_local(path: str) -> str:
    if not FILES_BASE_URL:
        return path
    return f"{FILES_BASE_URL}/{os.path.basename(path)}"

def download_file_bytes_to_path(blob: Any, path: str):
    if blob is None:
        return
    try:
        with open(path, "wb") as f:
            if isinstance(blob, (bytes, bytearray)):
                f.write(blob)
            else:
                f.write(str(blob).encode("utf-8"))
    except Exception:
        return

def sha1_file(path: str) -> Optional[str]:
    try:
        h = hashlib.sha1()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None

def maybe_get_file_link(garmin: Garmin, activity_id: int, ext: str, downloader_methods: List[str]) -> Tuple[str, Optional[str]]:
    """
    Returns (url, sha1) if downloaded, else (local_path_or_url, None)
    """
    path = local_file_path(activity_id, ext)
    if DOWNLOAD_FILES:
        if not os.path.exists(path) or os.path.getsize(path) < 100:
            blob = safe_call(garmin, downloader_methods, activity_id)
            download_file_bytes_to_path(blob, path)
        return file_url_from_local(path), sha1_file(path)
    else:
        # No download: still return a stable path (or URL if base is set)
        return file_url_from_local(path), None

def extract_laps(obj: dict) -> List[dict]:
    if not isinstance(obj, dict):
        return []

    # sometimes wrapped
    if isinstance(obj.get("splits"), dict):
        obj = obj["splits"]

    # common keys
    for k in ("lapDTOs", "laps", "lapSummaries", "splitSummaries"):
        arr = obj.get(k)
        if isinstance(arr, list) and arr:
            return arr

    return []



# -----------------------------
# Main sync
# -----------------------------
def main():
    garmin_email = os.environ["GARMIN_EMAIL"]
    garmin_password = os.environ["GARMIN_PASSWORD"]
    notion_token = os.environ["NOTION_TOKEN"]

    notion_activity_db_id = os.environ["NOTION_ACTIVITY_DB_ID"]
    notion_splits_db_id = os.environ["NOTION_SPLITS_DB_ID"]
    notion_files_db_id = os.environ["NOTION_FILES_DB_ID"]

    garmin = Garmin(garmin_email, garmin_password, is_cn=GARMIN_IS_CN)
    garmin.login()

    cutoff = (datetime.now(TZ) - timedelta(days=DAYS_BACK)).date()

    # 1) fetch activities paginated
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

        sport = (act.get("activityType") or {}).get("typeKey") or "unknown"
        activity_name = act.get("activityName") or f"Activity {activity_id}"

        # stable summary
        activity = safe_call(garmin, ["get_activity"], activity_id) or {}
        summary = activity.get("summaryDTO", {}) if isinstance(activity, dict) else {}

        # details (for splits + fallback streams)
        details = safe_call(garmin, ["get_activity_details"], activity_id) or {}
        if not isinstance(details, dict):
            details = {}
        # --- NEW: get splits from dedicated endpoint (Garmin often doesn't include laps in details) ---
        splits_resp = safe_call(
            garmin,
            ["get_activity_splits", "get_activity_split_summaries"],
            activity_id,
        ) or {}
        if not isinstance(splits_resp, dict):
            splits_resp = {}


        distance_km = meter_to_km(act.get("distance") or summary.get("distance"))
        duration_s = act.get("duration") or summary.get("duration") or summary.get("elapsedDuration")
        moving_s = summary.get("movingDuration") or summary.get("movingTime") or act.get("movingDuration")

        avg_pace = speed_mps_to_pace_min_per_km(act.get("averageSpeed") or summary.get("averageSpeed"))

        avg_hr = act.get("averageHR") or summary.get("averageHR")
        max_hr = act.get("maxHR") or summary.get("maxHR")

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

        elev_gain = summary.get("elevationGain") or summary.get("elevationGainMeters") or act.get("elevationGain")
        calories = act.get("calories") or summary.get("calories")

        # TE
        training = (activity.get("activityTrainingEffect") if isinstance(activity, dict) else None) \
                   or safe_call(garmin, ["get_activity_training_effect"], activity_id) \
                   or {}
        te_aer = training.get("aerobicTrainingEffect") if isinstance(training, dict) else None
        te_ana = training.get("anaerobicTrainingEffect") if isinstance(training, dict) else None

        # Running dynamics (best-effort)
        avg_cad = summary.get("averageRunCadence")
        gct = summary.get("avgGroundContactTime") or summary.get("averageGroundContactTime")
        vo = summary.get("avgVerticalOscillation") or summary.get("averageVerticalOscillation")
        stride = summary.get("avgStrideLength") or summary.get("averageStrideLength")

        # HR time in zone (seconds)
        hr_zones = None
        zones_obj = safe_call(
            garmin,
            ["get_activity_hr_in_time_zones", "get_activity_hr_in_timezones", "get_activity_hr_in_timezones"],
            activity_id,
        )
        hr_zones = parse_time_in_zone_seconds(zones_obj)
        if hr_zones is None:
            hr_vals = extract_hr_stream(details)
            hr_zones = bucket_zones_seconds(hr_vals, get_hr_zone_thresholds()) if hr_vals else (0, 0, 0, 0, 0)

        # Power time in zone (seconds)
        pwr_zones = None
        pz_obj = safe_call(
            garmin,
            ["get_activity_power_in_time_zones", "get_activity_power_in_timezones", "get_activity_power_in_timezones"],
            activity_id,
        )
        pwr_zones = parse_time_in_zone_seconds(pz_obj)
        if pwr_zones is None:
            pvals = extract_power_stream(details)
            pwr_zones = bucket_zones_seconds(pvals, get_power_zone_thresholds()) if pvals else (0, 0, 0, 0, 0)

        # Z2% and Z3+% (based on HR zones)
        hz1, hz2, hz3, hz4, hz5 = hr_zones
        hr_total = hz1 + hz2 + hz3 + hz4 + hz5
        z2_pct = round((hz2 / hr_total) * 100, 1) if hr_total > 0 else None
        z3p_pct = round(((hz3 + hz4 + hz5) / hr_total) * 100, 1) if hr_total > 0 else None

        # Files: FIT/TCX/GPX links (+ optional download & hash)
        fit_url, fit_sha1 = maybe_get_file_link(
            garmin, activity_id, "fit",
            downloader_methods=["download_activity", "download_activity_fit", "download_activity_file_fit"],
        )
        tcx_url, tcx_sha1 = maybe_get_file_link(
            garmin, activity_id, "tcx",
            downloader_methods=["download_activity_tcx", "download_activity_file_tcx"],
        )
        gpx_url, gpx_sha1 = maybe_get_file_link(
            garmin, activity_id, "gpx",
            downloader_methods=["download_activity_gpx", "download_activity_file_gpx"],
        )

        # -----------------------------
        # 2) Upsert Activities (Title=Activity ID)
        # -----------------------------
        props_a: Dict[str, Any] = {
            A_TITLE: make_title_prop(str(activity_id)),
        }
        maybe_date(props_a, A_DATE, dt.isoformat())
        maybe_select(props_a, A_SPORT, sport)

        maybe_number(props_a, A_DIST, distance_km)
        maybe_number(props_a, A_DUR, duration_s)
        maybe_number(props_a, A_MOVE, moving_s)
        maybe_number(props_a, A_AVG_PACE, avg_pace)

        maybe_number(props_a, A_AVG_HR, avg_hr)
        maybe_number(props_a, A_MAX_HR, max_hr)
        maybe_number(props_a, A_AVG_PWR, avg_power)
        maybe_number(props_a, A_MAX_PWR, max_power)

        maybe_number(props_a, A_ELEV, elev_gain)
        maybe_number(props_a, A_CAL, calories)
        maybe_number(props_a, A_TE_AER, te_aer)
        maybe_number(props_a, A_TE_ANA, te_ana)

        maybe_number(props_a, A_CAD, avg_cad)
        maybe_number(props_a, A_GCT, gct)
        maybe_number(props_a, A_VO, vo)
        maybe_number(props_a, A_STRIDE, stride)

        # zones seconds
        for i, name in enumerate(A_HRZ):
            maybe_number(props_a, name, hr_zones[i])
        for i, name in enumerate(A_PZ):
            maybe_number(props_a, name, pwr_zones[i])

        maybe_number(props_a, A_Z2P, z2_pct)
        maybe_number(props_a, A_Z3P, z3p_pct)

        maybe_url(props_a, A_FIT, fit_url)
        maybe_url(props_a, A_TCX, tcx_url)
        maybe_url(props_a, A_GPX, gpx_url)

        status_a, activity_page_id = notion_upsert_by_title(
            notion_token, notion_activity_db_id, A_TITLE, str(activity_id), props_a
        )
        print(f"[Activities] {activity_id} {activity_name} -> {status_a}")
        synced += 1

        # -----------------------------
        # 3) Upsert Splits
        # -----------------------------
        laps = extract_laps(splits_resp)
        print(f"  [Splits] laps found: {len(laps)}")  # 方便你看是不是拿到了

        for idx, lap in enumerate(laps, start=1):
            split_id = f"{activity_id}-L{idx}"

            # try common lap fields
            dist_m = lap.get("distance") or lap.get("lapDistance") or lap.get("totalDistance")
            dur_s = lap.get("duration") or lap.get("lapDuration") or lap.get("totalDuration")
            avg_hr_l = lap.get("averageHR") or lap.get("avgHR") or lap.get("heartRateAverage")
            avg_pwr_l = lap.get("averagePower") or lap.get("avgPower") or lap.get("powerAverage")
            cad_l = lap.get("averageRunCadence") or lap.get("avgCadence") or lap.get("cadenceAverage")

            km = meter_to_km(dist_m) if dist_m is not None else None
            pace = None
            try:
                if km and dur_s and float(km) > 0:
                    pace = round((float(dur_s) / float(km)) / 60.0, 2)
            except Exception:
                pace = None

            props_s: Dict[str, Any] = {
                S_TITLE: make_title_prop(split_id),
                S_ACT_REL: {"relation": [{"id": activity_page_id}]},
            }
            maybe_number(props_s, S_KM, km if km is not None else idx)
            maybe_number(props_s, S_TIME, dur_s)
            maybe_number(props_s, S_PACE, pace)
            maybe_number(props_s, S_AVG_HR, avg_hr_l)
            maybe_number(props_s, S_AVG_PWR, avg_pwr_l)
            maybe_number(props_s, S_CAD, cad_l)

            # upsert by title
            status_s, _ = notion_upsert_by_title(
                notion_token, notion_splits_db_id, S_TITLE, split_id, props_s
            )
            print(f"  [Splits] {split_id} -> {status_s}")

        # -----------------------------
        # 4) Upsert Files (FIT/TCX/GPX)
        # -----------------------------
        files = [
            (f"{activity_id}.fit", fit_url, fit_sha1, "FIT"),
            (f"{activity_id}.tcx", tcx_url, tcx_sha1, "TCX"),
            (f"{activity_id}.gpx", gpx_url, gpx_sha1, "GPX"),
        ]
        for fname, furl, fhash, ftype in files:
            props_f: Dict[str, Any] = {
                F_TITLE: make_title_prop(fname),
                F_ACT_REL: {"relation": [{"id": activity_page_id}]},
            }
            maybe_url(props_f, F_URL, furl)
            if fhash:
                props_f[F_HASH] = {"rich_text": [{"text": {"content": fhash}}]}
            props_f[F_TYPE] = {"select": {"name": ftype}}

            status_f, _ = notion_upsert_by_title(
                notion_token, notion_files_db_id, F_TITLE, fname, props_f
            )
            print(f"  [Files] {fname} -> {status_f}")

    print(f"Done: {synced} activities synced.")


if __name__ == "__main__":
    main()
