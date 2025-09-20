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
from datetime import datetime, timezone
from urllib.parse import urlencode
import urllib.request

EXIF_TOOL_PATH = "C:/Program Files/exiftool-13.36_64"

EARTH_RADIUS_MI = 3958.7613

EXIFTOOL_FIELDS = [
    # Dates
    "DateTimeOriginal", "CreateDate",
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
        f"{EXIF_TOOL_PATH}/exiftool", "-m", "-api", "largefilesupport=1", "-j", "-n",
        *[f"-{t}" for t in EXIFTOOL_FIELDS],
        path,
    ]
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.DEVNULL)
        arr = json.loads(out.decode("utf-8", "ignore"))
        return arr[0] if arr else {}
    except Exception:
        return {}


def parse_dt(meta, path):
    raw = meta.get("DateTimeOriginal") or meta.get("CreateDate")
    if raw:
        for fmt in ("%Y:%m:%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                dt = datetime.strptime(raw, fmt)
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


def reverse_geocode(lat, lon, user_agent, sleep_between=1.0):
    params = {
        "format": "jsonv2",
        "lat": f"{lat}",
        "lon": f"{lon}",
        "zoom": 18,
        "addressdetails": 1,
    }
    url = "https://nominatim.openstreetmap.org/reverse?" + urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": user_agent})
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


# ---------- main ----------

def build_catalog(src, out_csv, user_agent, sleep, radius_mi, cache_file):
    # discover files
    files = []
    for root, _, names in os.walk(src):
        for n in names:
            if n.lower().endswith((".jpg", ".jpeg")):
                files.append(os.path.join(root, n))
    if not files:
        raise SystemExit("No JPG/JPEG files found.")

    files.sort(key=lambda p: os.path.getmtime(p))

    # load cache
    anchors = []
    if cache_file and os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                anchors = json.load(f)
        except Exception:
            anchors = []

    rows = []
    created = 0

    for idx, p in enumerate(files, 1):
        meta = run_exiftool(p)
        dt = parse_dt(meta, p)
        date_str = dt.date().isoformat()
        time_str = dt.time().isoformat(timespec="seconds")
        gps = get_latlon(meta)

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
            if match:
                city_label = match["city_label"]
                hood = match.get("neighborhood", "")
                county = match.get("county", "")
                state = match.get("state", "")
                country_code = match.get("country_code", "")
                label = match["folder_label"]
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
        # Build proposed folder
        proposed = f"{slugify(label)}_{date_str}"

        rows.append({
            "source_path": p,
            "file_name": os.path.basename(p),
            "date": date_str,
            "time": time_str,
            "lat": gps[0] if gps else "",
            "lon": gps[1] if gps else "",
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
        if idx > 50:
            break

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
    ap.add_argument("--cache-file", help="Path to cache file", default="")
    args = ap.parse_args()
    build_catalog(args.src, args.out, args.user_agent, args.sleep, args.radius_mi, args.cache_file)

