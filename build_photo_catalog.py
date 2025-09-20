# =============================================
# Script 1: build_photo_catalog.py
# ---------------------------------------------
# Create a CSV catalog of images with key EXIF fields,
# lat/lon (decimal), reverse-geocoded place fields, and a
# proposed folder name (City[_Neighborhood]_YYYY-MM-DD).
# Uses proximity caching to minimize reverse-geocoding calls
# and supports an on-disk cache across runs.
#
# Usage:
#   python3 build_photo_catalog.py \
#     --src /path/to/photos \
#     --out /path/to/catalog.csv \
#     --user-agent "photo-catalog/1.0 (contact: you@example.com)" \
#     --radius-mi 10 \
#     --cache-file /path/to/geo_cache.json \
#     --sleep 1.0
#
# Open the CSV, review/adjust the suggested `proposed_folder`
# (or add an optional `final_folder` column with your edits).
# Then run Script 2 to move/copy files.
# =============================================

# Note on Windows powershell and VScode, after creating a venv with Create Environment:
# Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
# .\venv\Scripts\Activate.ps1

import argparse, csv, json, os, subprocess, math, time
from datetime import datetime, timezone, timedelta
from urllib.parse import urlencode
import urllib.request

EXIF_TOOL_PATH = r"C:\Program Files\exiftool-13.36_64\exiftool.exe"

EARTH_RADIUS_MI = 3958.7613

EXIFTOOL_FIELDS = [
    # Dates
    "DateTimeOriginal", "CreateDate", "OffsetTimeOriginal", "OffsetTime", "OffsetTimeDigitized",
    # GPS
    "GPSLatitude", "GPSLongitude", "GPSAltitude",
    # Camera basics
    "Make", "Model", "LensModel",
    "FNumber", "ExposureTime", "ISO", "FocalLength",
    # Useful image info
    "Orientation", "ImageWidth", "ImageHeight"
]

# ---------- helpers ----------

def run_exiftool(path):
    cmd = [
        EXIF_TOOL_PATH, "-m", "-api", "largefilesupport=1", "-j", "-n",
        *[f"-{t}" for t in EXIFTOOL_FIELDS],
        path,
    ]
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.DEVNULL)
        arr = json.loads(out.decode("utf-8", "ignore"))
        return arr[0] if arr else {}
    except Exception:
        return {}


def run_exiftool_batch(paths, exiftool_exe, fields, batch_size=100, test50=False):
    """
    Run exiftool on paths in batches. Returns {path: metadata_dict}.
    """
    out = {}
    for i in range(0, len(paths), batch_size):
        batch = paths[i:i+batch_size]
        cmd = [exiftool_exe, "-m", "-api", "largefilesupport=1", "-j", "-n", *[f"-{f}" for f in fields], *batch]
        try:
            print(f"  EXIF batch {i+1}-{i+len(batch)} ... ", end="", flush=True)
            raw = subprocess.check_output(cmd, stderr=subprocess.DEVNULL)
            arr = json.loads(raw.decode("utf-8", "ignore"))
            for obj in arr:
                # exiftool's JSON objects include "SourceFile"
                src = obj.get("SourceFile")
                if src:
                    # Normalize path casing and separators so keys match os.path.join output on Windows
                    key = os.path.normcase(os.path.normpath(src))
                    out[key] = obj
        except Exception:
            # fallback: try per-file to avoid losing data on a batch error
            for p in batch:
                try:
                    raw = subprocess.check_output([exiftool_exe, "-m", "-api", "largefilesupport=1", "-j", "-n", *[f"-{f}" for f in fields], p], stderr=subprocess.DEVNULL)
                    arr = json.loads(raw.decode("utf-8", "ignore"))
                    if arr:
                        # Normalize the per-file key the same way
                        key = os.path.normcase(os.path.normpath(p))
                        out[key] = arr[0]
                except Exception:
                    key = os.path.normcase(os.path.normpath(p))
                    out[key] = {}
        if test50 and batch_size >= 50:
            break
    return out


def parse_dt(meta, path):
    raw = meta.get("DateTimeOriginal") or meta.get("CreateDate")
    # EXIF may include timezone offset fields like OffsetTimeOriginal
    offset_raw = meta.get("OffsetTimeOriginal") or meta.get("OffsetTime") or meta.get("OffsetTimeDigitized")
    if raw:
        for fmt in ("%Y:%m:%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                dt = datetime.strptime(raw, fmt)
                # Attach tzinfo if an EXIF offset is present
                if offset_raw:
                    try:
                        # offset_raw expected like '+01:00' or '-05:30'
                        sign = 1 if offset_raw[0] != '-' else -1
                        parts = offset_raw[1:].split(":")
                        oh = int(parts[0]) if parts[0] else 0
                        om = int(parts[1]) if len(parts) > 1 else 0
                        tz = timezone(timedelta(hours=sign * oh, minutes=sign * om))
                        return dt.replace(tzinfo=tz)
                    except Exception:
                        pass
                return dt
            except Exception:
                pass
    ts = os.path.getmtime(path)
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def get_latlon(meta):
    lat = meta.get("GPSLatitude")
    lon = meta.get("GPSLongitude")
    if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
        return lat, lon
    return None


def haversine_miles(lat1, lon1, lat2, lon2):
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat/2)**2 +
        math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
        math.sin(dlon/2)**2
    )
    c = 2 * math.asin(min(1, math.sqrt(a)))
    return EARTH_RADIUS_MI * c


def pick_city(addr):
    for k in ["city", "town", "village", "hamlet", "municipality", "county"]:
        if k in addr:
            return addr[k]
    return addr.get("state") or addr.get("country")


def pick_neighborhood(addr):
    for k in ["neighbourhood", "neighborhood", "suburb", "quarter", "locality", "borough", "district"]:
        if k in addr:
            return addr[k]
    return None


def reverse_geocode(lat, lon, user_agent_email, sleep_between=1.0):
    params = {
        "format": "jsonv2",
        "lat": f"{lat}",
        "lon": f"{lon}",
        "zoom": 18,
        "addressdetails": 1,
    }
    url = "https://nominatim.openstreetmap.org/reverse?" + urlencode(params)
    user_agent = f"custom-photo-organizer/1.0 (contact: {user_agent_email})"
    req = urllib.request.Request(url, headers={"User-Agent": user_agent})
    print(f"Reverse geocoding: {lat},{lon} ... ", end="", flush=True)
    with urllib.request.urlopen(req, timeout=20) as resp:
        data = json.loads(resp.read().decode("utf-8", "ignore"))
    time.sleep(max(0.0, sleep_between))
    addr = data.get("address", {}) if isinstance(data, dict) else {}
    city = pick_city(addr) or "UnknownLocation"
    hood = pick_neighborhood(addr)
    county = addr.get("county")
    state = addr.get("state")
    country_code = (addr.get("country_code") or "").upper()
    # Build a friendly label City[, State]|City[, CC]
    if city and state:
        city_label = f"{city}, {state}"
    elif city and country_code:
        city_label = f"{city}, {country_code}"
    else:
        city_label = city
    full_label = f"{city_label}_{hood}" if hood else city_label
    return {
        "city_label": city_label,
        "neighborhood": hood or "",
        "county": county or "",
        "state": state or "",
        "country_code": country_code or "",
        "folder_label": full_label,
    }


def slugify(s):
    keep = []
    for ch in s:
        if ch.isalnum() or ch in " _-,":
            keep.append(ch)
        else:
            keep.append("_")
    s = "".join(keep).strip(" _-,")
    s = "_".join([p for p in s.split() if p])
    return s or "UnknownLocation"


def parse_utc_offset(tz_str):
    """Parse a timezone string like 'UTC+05:30' and return a timedelta or None."""
    if not tz_str or not tz_str.startswith("UTC"):
        return None
    try:
        sign_char = tz_str[3]
        if sign_char not in "+-":
            return None
        sign = 1 if sign_char == "+" else -1
        hh = int(tz_str[4:6])
        mm = int(tz_str[7:9]) if len(tz_str) >= 9 else 0
        return timedelta(hours=sign * hh, minutes=sign * mm)
    except Exception:
        return None

# ---------- main ----------

def build_catalog(src, out_csv, user_agent, sleep, radius_mi, cache_file, test50=False):
    # discover files
    files = []
    for root, _, names in os.walk(src):
        for n in names:
            if n.lower().endswith((".jpg", ".jpeg", ".mp4")):
                files.append(os.path.join(root, n))
    if not files:
        raise SystemExit("No JPG/JPEG files found.")

    files.sort(key=lambda p: os.path.getmtime(p))

    # Pre-read EXIF metadata in batches to avoid spawning exiftool per-file
    try:
        print(f"Reading EXIF metadata in batches ({len(files)} files)...")
        meta_map = run_exiftool_batch(files, EXIF_TOOL_PATH, EXIFTOOL_FIELDS, batch_size=100, test50=test50)
    except Exception:
        meta_map = {}

    # load cache
    anchors = []
    if cache_file and os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                anchors = json.load(f)
        except Exception:
            anchors = []

    # No GPS-based timezone lookup. Timezone comes only from EXIF offset when available.
    tf = None

    rows = []
    created = 0

    prev_lat = prev_lon = prev_dt = None

    for idx, p in enumerate(files, 1):
        # print(f"Processing {idx}/{len(files)}: {p}")
        # Use batch metadata when available; fall back to single-file call if necessary.
        lookup_key = os.path.normcase(os.path.normpath(p))
        meta = meta_map.get(lookup_key)
        if meta is None:
            print(f"  (EXIF missing from batch, running single on {p}) ... ", end="", flush=True)
            # (debug) show whether our normalized keys include this file
            print(f" Keys(sample)={list(meta_map.keys())[:5]}")
            meta = run_exiftool(p)
        # print(f"Done processing EXIF.")
        dt = parse_dt(meta, p)
        date_str = dt.date().isoformat()
        time_str = dt.time().isoformat(timespec="seconds")
        gps = get_latlon(meta)

        # Prefer EXIF timezone offset if present on the parsed datetime.
        timezone = ""
        if dt.tzinfo is not None:
            try:
                off = dt.tzinfo.utcoffset(dt)
                if off is not None:
                    total_minutes = int(off.total_seconds() / 60)
                    sign = "+" if total_minutes >= 0 else "-"
                    hh = abs(total_minutes) // 60
                    mm = abs(total_minutes) % 60
                    timezone = f"UTC{sign}{hh:02d}:{mm:02d}"
            except Exception:
                timezone = ""

        # Defaults
        label = "UnknownLocation"
        city_label = "UnknownLocation"
        hood = county = state = country_code = ""

        if gps:
            lat, lon = gps
            # try reuse anchor in radius
            match = None
            for a in anchors:
                if haversine_miles(lat, lon, a["lat"], a["lon"]) <= radius_mi:
                    match = a
                    break
            if prev_lat is not None and prev_lon is not None:
                dist_from_prev_pic = haversine_miles(lat, lon, prev_lat, prev_lon)
                # Compute elapsed hours robustly.
                try:
                    if dt.tzinfo is not None and prev_dt.tzinfo is not None:
                        # Both datetimes are timezone-aware: use UTC-normalized difference (no further offset correction needed).
                        delta_sec = (dt.astimezone(timezone.utc) - prev_dt.astimezone(timezone.utc)).total_seconds()
                        hrs = abs(delta_sec) / 3600.0
                    else:
                        # Naive subtraction (wall-clock) — compensate using EXIF-derived timezone strings if available.
                        try:
                            delta_sec = (dt - prev_dt).total_seconds()
                            hrs = abs(delta_sec) / 3600.0
                        except Exception:
                            delta_sec = 0
                        if prev_tz and timezone and prev_tz != timezone:
                            try:
                                prev_off = parse_utc_offset(prev_tz) or (prev_dt.tzinfo.utcoffset(prev_dt) if prev_dt.tzinfo else timedelta(0))
                                curr_off = parse_utc_offset(timezone) or (dt.tzinfo.utcoffset(dt) if dt.tzinfo else timedelta(0))
                                off_diff = (curr_off - prev_off).total_seconds() / 3600.0
                                hrs += off_diff
                                hrs = max(hrs, 0.0)
                            except Exception:
                                pass
                except Exception:
                    hrs = abs((dt - prev_dt).total_seconds()) / 3600.0

                speed_mph = dist_from_prev_pic / hrs if hrs > 0 else None
            else:
                dist_from_prev_pic = None
                speed_mph = None
                hrs = None

            # save for next iteration
            prev_lat, prev_lon = lat, lon
            prev_dt = dt
            prev_tz = timezone

            if match:
                city_label = match["city_label"]
                hood = match.get("neighborhood", "")
                county = match.get("county", "")
                state = match.get("state", "")
                country_code = match.get("country_code", "")
                label = match["folder_label"]
                # Do not use cached anchor timezone; prefer EXIF offset only
            else:
                try:
                    rg = reverse_geocode(lat, lon, user_agent, sleep_between=sleep)
                    city_label = rg["city_label"]
                    hood = rg["neighborhood"]
                    county = rg["county"]
                    state = rg["state"]
                    country_code = rg["country_code"]
                    label = rg["folder_label"]
                except Exception:
                    pass
                anchors.append({
                    "lat": lat, "lon": lon,
                    "city_label": city_label,
                    "neighborhood": hood,
                    "county": county,
                    "state": state,
                    "country_code": country_code,
                    "folder_label": label,
                })
                created += 1

        else:
            dist_from_prev_pic = None
            speed_mph = None
            hrs = None

        # Build proposed folder
        proposed = f"{slugify(label)}_{date_str}"

        rows.append({
            "source_path": p,
            "file_name": os.path.basename(p),
            "date": date_str,
            "time": time_str,
            "timezone": timezone,
            "lat": gps[0] if gps else "",
            "lon": gps[1] if gps else "",
            "geo_source": "EXIF" if gps else "Unknown",
            "miles_from_prev_pic": f"{dist_from_prev_pic:.2f}" if dist_from_prev_pic is not None else "",
            "hrs_from_prev_pic": f"{hrs:.2f}" if hrs is not None else "",
            "mph_from_prev_pic": f"{speed_mph:.2f}" if speed_mph is not None else "",

            # place levels
            "city_label": city_label,
            "neighborhood": hood,
            "county": county,
            "state": state,
            "country_code": country_code,
            # camera/exif lite
            "make": meta.get("Make", ""),
            "model": meta.get("Model", ""),
            "lens": meta.get("LensModel", ""),
            "fnumber": meta.get("FNumber", ""),
            "exposure": meta.get("ExposureTime", ""),
            "iso": meta.get("ISO", ""),
            "focal_length": meta.get("FocalLength", ""),
            "orientation": meta.get("Orientation", ""),
            "width": meta.get("ImageWidth", ""),
            "height": meta.get("ImageHeight", ""),
            # folder suggestion
            "proposed_folder": proposed,
            # Users may add a "final_folder" column manually later.
        })

        if idx % 20 == 0:
            print(f"Cataloged {idx}/{len(files)}... (new anchors this run: {created})")
        
        # temporary: limit to first 50 for testing
        if test50 and idx > 50:
            break

    # Infer missing GPS by temporal proximity (<= 60 minutes)
    # For any row where geo_source is 'Unknown' and lat/lon are empty,
    # pick the lat/lon from the photo closest in date/time (within 60 minutes)
    known = []
    for r in rows:
        if r.get("lat") != "" and r.get("lon") != "":
            try:
                known_dt = datetime.fromisoformat(r["date"] + "T" + r["time"])
                known.append((known_dt, float(r["lat"]), float(r["lon"])))
            except Exception:
                # skip rows with unparsable dates
                continue

    MAX_MINUTES = 60
    for r in rows:
        if r.get("geo_source", "") == "Unknown" and (r.get("lat") == "" or r.get("lon") == ""):
            try:
                r_dt = datetime.fromisoformat(r["date"] + "T" + r["time"])
            except Exception:
                continue
            best = None
            best_delta = None
            for kdt, klat, klon in known:
                delta_min = abs((kdt - r_dt).total_seconds()) / 60.0
                if delta_min <= MAX_MINUTES and (best is None or delta_min < best_delta):
                    best = (klat, klon)
                    best_delta = delta_min
            if best:
                r["lat"], r["lon"] = best
                r["geo_source"] = "inferred"
                # Do not populate timezone from inferred GPS; timezone remains EXIF-derived (if any)

    # write CSV
    fieldnames = list(rows[0].keys()) + ["final_folder"]  # reserve column for edits
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    # save cache
    if cache_file:
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(anchors, f, ensure_ascii=False, indent=2)

    print(f"\nDone. CSV written to: {out_csv}")
    if cache_file:
        print(f"Anchor cache saved to: {cache_file} (total anchors: {len(anchors)})")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Build a CSV catalog of photos with EXIF + reverse-geocoded place info.")
    ap.add_argument("--src", help="Source directory of photos", required=True)
    ap.add_argument("--out", help="Output CSV file", required=True)
    ap.add_argument("--user-agent", required=True, help="Include contact info per Nominatim policy")
    ap.add_argument("--sleep", help="Sleep time between requests (seconds)", type=float, default=1.0)
    ap.add_argument("--radius-mi", help="Radius for reverse geocoding (miles)", type=float, default=10.0)
    ap.add_argument("--cache-file", help="Path to cache file", default="anchors.json")
    ap.add_argument("--test50", action="store_true", help="(dev) Limit to first 50 photos")
    args = ap.parse_args()
    build_catalog(args.src, args.out, args.user_agent, args.sleep, args.radius_mi, args.cache_file, test50=args.test50)


# python.exe .\build_photo_catalog.py --src D:\Pictures_Home\by-camera-after-2010\brian-pixel7-pro\0-staging\Camera\ --out tmp.csv --user-agent bandrewfox@gmail.com