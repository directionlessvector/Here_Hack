import os
import csv
import json
import math
import time
import re
import requests
from rapidfuzz import fuzz

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
ONEMAP_URL = "https://www.onemap.gov.sg/api/common/elastic/search"
ACRA_CSV = "acra_final.csv"
CACHE_DIR = "cache"
DATA_DIR = "data"

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

BRANDS = {
    "shell": {"parent": "Shell Eastern Petroleum Pte Ltd", "keywords": ["shell"]},
    "esso": {"parent": "ExxonMobil Asia Pacific Pte Ltd", "keywords": ["esso", "exxon", "mobil"]},
    "spc": {"parent": "Singapore Petroleum Company Ltd", "keywords": ["spc", "singapore petroleum"]},
    "caltex": {"parent": "Chevron Singapore Pte Ltd", "keywords": ["caltex", "chevron"]},
    "sinopec": {"parent": "Sinopec Fuel Oil (Singapore) Pte Ltd", "keywords": ["sinopec"]},
}

CACHE_MAX_AGE = 7 * 24 * 60 * 60


def _is_fresh(filepath):
    if not os.path.exists(filepath):
        return False
    try:
        mtime = os.path.getmtime(filepath)
        return (time.time() - mtime) < CACHE_MAX_AGE
    except Exception:
        return False


def _save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def detect_brand(name, brand_tag, operator_tag):
    candidates = []
    for v in (brand_tag, operator_tag, name):
        if not v:
            continue
        candidates.append(str(v).lower())

    for cand in candidates:
        for brand_key, brand_meta in BRANDS.items():
            for kw in brand_meta.get("keywords", []):
                if kw in cand:
                    return brand_key
    return None


def fetch_osm_stations():
    path = os.path.join(CACHE_DIR, "osm_raw.json")
    if _is_fresh(path):
        try:
            return _load_json(path)
        except Exception:
            pass

    query = """
[out:json][timeout:60];
area(3600536780)->.sg;
(node[\"amenity\"=\"fuel\"](area.sg);way[\"amenity\"=\"fuel\"](area.sg););
out center tags;
"""
    resp = requests.post(OVERPASS_URL, data=query.strip(), timeout=120)
    resp.raise_for_status()
    data = resp.json()

    stations = []
    for el in data.get("elements", []):
        lat = None
        lon = None
        if el.get("type") == "node":
            lat = el.get("lat")
            lon = el.get("lon")
        elif el.get("type") == "way":
            center = el.get("center")
            if center:
                lat = center.get("lat")
                lon = center.get("lon")

        if lat is None or lon is None:
            continue

        tags = el.get("tags") or {}
        name = tags.get("name") or ""
        brand_tag = tags.get("brand") or ""
        operator_tag = tags.get("operator") or ""

        osm_id = f"{el.get('type')}/{el.get('id')}"

        stations.append({
            "osm_id": osm_id,
            "name": name,
            "brand_tag": brand_tag,
            "operator_tag": operator_tag,
            "lat": lat,
            "lon": lon,
        })

    _save_json(path, stations)
    return stations


def write_osm_csv(stations):
    outpath = os.path.join(DATA_DIR, "osm_stations.csv")
    with open(outpath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["osm_id", "name", "brand", "lat", "lon"])
        writer.writeheader()

        count = 0
        for st in stations:
            brand = detect_brand(st.get("name"), st.get("brand_tag"), st.get("operator_tag"))
            writer.writerow({
                "osm_id": st.get("osm_id"),
                "name": st.get("name", ""),
                "brand": brand or "",
                "lat": st.get("lat"),
                "lon": st.get("lon"),
            })
            count += 1

    print(f"Wrote {count} stations to {outpath}")


def process_acra():
    acra_path = ACRA_CSV
    if not os.path.exists(acra_path):
        raise FileNotFoundError(f"Input ACRA file not found: {acra_path}")

    keywords = ["service station", "petrol station", "fuel station", "filling station"]
    data = {}

    with open(acra_path, "r", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        for row in reader:
            building = (row.get("building_name") or "").strip()
            low_building = building.lower()
            if not any(k in low_building for k in keywords):
                continue

            postal_code = (row.get("postal_code") or row.get("postal") or "").strip()
            street_name = (row.get("street_name") or row.get("street") or "").strip()
            status_raw = (row.get("status") or "").strip().lower()
            if status_raw in ("active", "operating", "open", "in operation"): 
                status = "active"
            elif status_raw in ("closed", "inactive", "shut", "ceased"): 
                status = "closed"
            else:
                status = "unknown"

            brand = None
            low = low_building
            for bkey, bmeta in BRANDS.items():
                for kw in bmeta.get("keywords", []):
                    if kw in low:
                        brand = bkey
                        break
                if brand:
                    break

            if not postal_code:
                continue

            bucket = data.setdefault(postal_code, {
                "brand": brand or "",
                "street": street_name,
                "building": building,
                "active_count": 0,
                "closed_count": 0,
                "total_count": 0,
            })

            # keep brand from first matching; if existing empty and new brand present, assign
            if not bucket.get("brand") and brand:
                bucket["brand"] = brand

            bucket["active_count"] += 1 if status == "active" else 0
            bucket["closed_count"] += 1 if status == "closed" else 0
            bucket["total_count"] += 1

    return data


def geocode_postcodes(postcodes):
    cache_path = os.path.join(CACHE_DIR, "geocodes.json")
    cache = {}
    if os.path.exists(cache_path):
        try:
            cache = _load_json(cache_path)
        except Exception:
            cache = {}

    result = {}
    for postal in postcodes:
        if not postal:
            continue

        if postal in cache:
            result[postal] = cache[postal]
            continue

        params = {
            "searchVal": postal,
            "returnGeom": "Y",
            "getAddrDetails": "Y",
        }
        try:
            resp = requests.get(ONEMAP_URL, params=params, timeout=30)
            resp.raise_for_status()
            payload = resp.json()
            results = payload.get("results") or []
            if not results:
                continue
            first = results[0]
            lat = first.get("LATITUDE") or first.get("latitude") or ""
            lon = first.get("LONGITUDE") or first.get("longitude") or ""
            if not lat or not lon:
                continue
            lat_f = float(lat)
            lon_f = float(lon)
            cache[postal] = (lat_f, lon_f)
            result[postal] = (lat_f, lon_f)
            time.sleep(0.3)
        except Exception:
            continue

    _save_json(cache_path, cache)
    return result


def write_acra_csv(acra_stations, geocoded):
    outpath = os.path.join(DATA_DIR, "acra_addresses.csv")
    with open(outpath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["postal_code", "brand", "street", "building", "lat", "lon", "active_count", "closed_count", "total_count"])
        writer.writeheader()

        count = 0
        for postal, info in acra_stations.items():
            geo = geocoded.get(postal)
            if not geo:
                continue
            lat, lon = geo
            writer.writerow({
                "postal_code": postal,
                "brand": info.get("brand", ""),
                "street": info.get("street", ""),
                "building": info.get("building", ""),
                "lat": lat,
                "lon": lon,
                "active_count": info.get("active_count", 0),
                "closed_count": info.get("closed_count", 0),
                "total_count": info.get("total_count", 0),
            })
            count += 1

    print(f"Wrote {count} ACRA service station addresses to {outpath}")


def fetch_roads():
    path = os.path.join(CACHE_DIR, "roads_raw.json")
    if _is_fresh(path):
        try:
            return _load_json(path)
        except Exception:
            pass

    query = """
[out:json][timeout:120];
area(3600536780)->.sg;
way[\"highway\"~\"^(motorway|trunk|primary|secondary|tertiary)$\"](area.sg);
out geom;
"""
    resp = requests.post(OVERPASS_URL, data=query.strip(), timeout=150)
    resp.raise_for_status()
    data = resp.json()

    points = []
    for el in data.get("elements", []):
        tags = el.get("tags") or {}
        highway_type = tags.get("highway")
        if not highway_type:
            continue

        geom = el.get("geometry") or []
        if not isinstance(geom, list):
            continue

        for i, p in enumerate(geom):
            if i % 10 != 0:
                continue
            lat = p.get("lat")
            lon = p.get("lon")
            if lat is None or lon is None:
                continue
            points.append({"road_type": highway_type, "lat": lat, "lon": lon})

    _save_json(path, points)
    return points


def write_roads_csv(roads):
    outpath = os.path.join(DATA_DIR, "roads_major.csv")
    with open(outpath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["road_type", "lat", "lon"])
        writer.writeheader()

        count = 0
        for pt in roads:
            writer.writerow({
                "road_type": pt.get("road_type", ""),
                "lat": pt.get("lat"),
                "lon": pt.get("lon"),
            })
            count += 1

    print(f"Wrote {count} road points to {outpath}")


def build():
    print("=== BUILDING DATASET ===")

    print("[1/3] Fetching OSM fuel stations...")
    osm = fetch_osm_stations()
    write_osm_csv(osm)

    print("[2/3] Processing ACRA addresses...")
    acra = process_acra()
    geocoded = geocode_postcodes(list(acra.keys()))
    write_acra_csv(acra, geocoded)

    print("[3/3] Fetching road network...")
    roads = fetch_roads()
    write_roads_csv(roads)

    print("=== DONE ===")


if __name__ == "__main__":
    build()
