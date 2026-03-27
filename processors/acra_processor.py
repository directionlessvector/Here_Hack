import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import csv
import json
import math
import time
import requests
from dotenv import load_dotenv
from supabase import create_client

from utils.fuzzy import normalize, fuzzy_match_score

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

ONEMAP_URL = "https://www.onemap.gov.sg/api/common/elastic/search"
ACRA_CSV_PATH = os.getenv("ACRA_CSV_PATH", "acra_final.csv")
GEOCODE_CACHE_PATH = "cache/postal_geocodes.json"

BRAND_PARENT_STATUS = {
    "shell": {"name": "Shell Eastern Petroleum Pte Ltd", "status": "active"},
    "esso": {"name": "ExxonMobil Asia Pacific Pte Ltd", "status": "active"},
    "spc": {"name": "Singapore Petroleum Company Ltd", "status": "active"},
    "caltex": {"name": "Chevron Singapore Pte Ltd", "status": "active"},
    "sinopec": {"name": "Sinopec Fuel Oil (Singapore) Pte Ltd", "status": "active"},
}


def load_acra_csv() -> list:
    if not os.path.exists(ACRA_CSV_PATH):
        print(f"[acra] WARNING: ACRA CSV not found at {ACRA_CSV_PATH}")
        return []

    rows = []
    try:
        with open(ACRA_CSV_PATH, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
    except Exception as exc:
        print(f"[acra] WARNING: Failed to read ACRA CSV: {exc}")
        return []

    print(f"[acra] Loaded {len(rows)} ACRA records")
    return rows


def find_service_station_records(acra_rows: list) -> dict:
    result = {}
    match_terms = [
        "shell service station", "spc service station", "caltex service station",
        "esso service station", "mobil service station", "sinopec service station",
        "petrol station", "fuel station", "filling station",
    ]

    for row in acra_rows:
        building_name = (row.get("building_name") or "").strip()
        if not building_name:
            continue

        b_lower = building_name.lower()
        matches_station = any(term in b_lower for term in match_terms)
        if not matches_station:
            continue

        brand = "unknown"
        if "shell" in b_lower:
            brand = "shell"
        elif "spc" in b_lower:
            brand = "spc"
        elif "caltex" in b_lower:
            brand = "caltex"
        elif "esso" in b_lower or "mobil" in b_lower or "exxon" in b_lower:
            brand = "esso"
        elif "sinopec" in b_lower:
            brand = "sinopec"

        postal_code = str(row.get("postal_code") or "").strip()
        if not postal_code:
            continue

        if postal_code not in result:
            result[postal_code] = {
                "brand": brand,
                "street": row.get("street_name") or "",
                "building": building_name,
                "records": [],
                "active_count": 0,
                "closed_count": 0,
                "unknown_count": 0,
            }

        entry = result[postal_code]
        entry["records"].append(row)
        status = (row.get("status") or "unknown").lower()
        if status == "active":
            entry["active_count"] += 1
        elif status == "closed":
            entry["closed_count"] += 1
        else:
            entry["unknown_count"] += 1

    print(f"[acra] Found {len(result)} unique fuel station postal codes")
    return result


def geocode_postal_code(postal_code: str):
    cache = {}
    if os.path.exists(GEOCODE_CACHE_PATH):
        try:
            with open(GEOCODE_CACHE_PATH, "r", encoding="utf-8") as f:
                cache = json.load(f)
        except Exception:
            cache = {}

    if postal_code in cache:
        return tuple(cache[postal_code])

    params = {
        "searchVal": postal_code,
        "returnGeom": "Y",
        "getAddrDetails": "Y",
    }
    try:
        res = requests.get(ONEMAP_URL, params=params, timeout=10)
        res.raise_for_status()
        data = res.json()
        results = data.get("results") or []
        if not results:
            return None

        lat = float(results[0].get("LATITUDE"))
        lon = float(results[0].get("LONGITUDE"))

        cache[postal_code] = [lat, lon]
        os.makedirs(os.path.dirname(GEOCODE_CACHE_PATH) or "cache", exist_ok=True)
        with open(GEOCODE_CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2)

        return (lat, lon)
    except Exception:
        return None


def geocode_all_station_postcodes(station_postcodes: dict) -> dict:
    geocoded = {}
    for postal_code in station_postcodes:
        coords = geocode_postal_code(postal_code)
        if coords:
            geocoded[postal_code] = coords
        time.sleep(0.3)

    print(f"[acra] Geocoded {len(geocoded)}/{len(station_postcodes)} postal codes")
    return geocoded


def haversine(lat1, lon1, lat2, lon2) -> float:
    r = 6371000
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)

    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def match_osm_to_acra_station(osm_lat, osm_lon, osm_name, osm_brand, station_postcodes, geocoded_postcodes):
    best_match = None
    best_score = 0.0

    for postal_code, info in station_postcodes.items():
        if postal_code not in geocoded_postcodes:
            continue
        acra_lat, acra_lon = geocoded_postcodes[postal_code]
        dist = haversine(osm_lat, osm_lon, acra_lat, acra_lon)
        if dist > 500:
            continue

        if dist < 100:
            dist_score = 1.0
        elif dist < 200:
            dist_score = 0.8
        elif dist < 300:
            dist_score = 0.5
        else:
            dist_score = 0.3

        brand_match_value = 1.0 if (osm_brand and osm_brand == info.get("brand")) else 0.5

        name_sim = 0.0
        if osm_name:
            name_sim = fuzzy_match_score(osm_name, info.get("building", ""), normalize_first=True)

        score = (dist_score * 0.5) + (brand_match_value * 0.3) + (name_sim * 0.2)

        if score > best_score:
            best_score = score
            best_match = {
                "postal_code": postal_code,
                "distance_m": round(dist, 2),
                "brand": info.get("brand"),
                "street": info.get("street"),
                "building": info.get("building"),
                "active_count": info.get("active_count", 0),
                "closed_count": info.get("closed_count", 0),
                "unknown_count": info.get("unknown_count", 0),
                "name_similarity": round(name_sim, 4),
                "confidence": round(score, 4),
                "records": info.get("records", []),
            }

    return best_match


def determine_acra_status(match, osm_brand):
    result = {
        "acra_match_found": False,
        "match_level": "none",
        "acra_uen": None,
        "acra_name": None,
        "acra_status": "unknown",
        "acra_postal_code": None,
        "acra_street": None,
        "acra_building": None,
        "match_distance_m": 9999.0,
        "name_similarity": 0.0,
        "confidence": 0.0,
        "brand_parent_name": None,
        "brand_parent_status": None,
        "active_businesses_at_location": 0,
        "closed_businesses_at_location": 0,
    }

    if match and match.get("confidence", 0) >= 0.4:
        result["acra_match_found"] = True
        result["match_level"] = "station_address" if match["distance_m"] < 200 else "station_proximity"
        result["acra_postal_code"] = match["postal_code"]
        result["acra_street"] = match["street"]
        result["acra_building"] = match["building"]
        result["match_distance_m"] = match["distance_m"]
        result["name_similarity"] = match["name_similarity"]
        result["confidence"] = match["confidence"]
        result["active_businesses_at_location"] = match["active_count"]
        result["closed_businesses_at_location"] = match["closed_count"]

        if match.get("records"):
            best_record = sorted(
                match["records"],
                key=lambda r: (
                    (r.get("status") or "").lower() == "active",
                    r.get("registration_incorporation_date", ""),
                ),
                reverse=True,
            )[0]
            result["acra_uen"] = best_record.get("uen")
            result["acra_name"] = best_record.get("clean_name")

        if result["active_businesses_at_location"] > 0:
            result["acra_status"] = "active"
        elif result["closed_businesses_at_location"] > 0:
            result["acra_status"] = "closed"
        else:
            result["acra_status"] = "unknown"

        return result

    if osm_brand and osm_brand in BRAND_PARENT_STATUS:
        parent = BRAND_PARENT_STATUS[osm_brand]
        result["acra_match_found"] = True
        result["match_level"] = "brand_parent"
        result["brand_parent_name"] = parent["name"]
        result["brand_parent_status"] = parent["status"]
        result["acra_status"] = parent["status"]
        result["confidence"] = 0.5
        return result

    return result


def get_or_create_batch(supabase, feature_id):
    existing = (
        supabase.table("signal_batch")
        .select("batch_id")
        .eq("feature_id", feature_id)
        .in_("batch_status", ["pending", "processing"])
        .limit(1)
        .execute()
    )
    if existing.data:
        return existing.data[0]["batch_id"]

    res = (
        supabase.table("signal_batch")
        .insert({
            "feature_id": feature_id,
            "triggered_by": "scheduler",
            "sources_queried": [],
            "batch_status": "processing",
        })
        .execute()
    )
    return res.data[0]["batch_id"] if res.data else None


def process_all_fuel_stations():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    acra_rows = load_acra_csv()
    if not acra_rows:
        print("[acra] No ACRA data loaded. Exiting.")
        return

    station_postcodes = find_service_station_records(acra_rows)
    geocoded = geocode_all_station_postcodes(station_postcodes)

    res = (
        supabase.table("poi_feature")
        .select("feature_id, osm_id, name, tags, lat, lon")
        .eq("category", "fuel")
        .execute()
    )
    stations = res.data or []
    print(f"[acra] {len(stations)} OSM fuel stations to process")

    processed = 0
    matched_station = 0
    matched_brand = 0
    unmatched = 0
    errors = 0

    for station in stations:
        feature_id = station["feature_id"]
        tags = station.get("tags") or {}
        name = station.get("name")
        lat = station.get("lat")
        lon = station.get("lon")

        if lat is None or lon is None:
            errors += 1
            continue

        try:
            batch_id = get_or_create_batch(supabase, feature_id)

            osm_brand = None
            for tag_key in ("brand", "operator", "name"):
                val = tags.get(tag_key)
                if val:
                    val_lower = str(val).lower()
                    for brand in ("shell", "esso", "spc", "caltex", "sinopec"):
                        if brand in val_lower:
                            osm_brand = brand
                            break
                    if "mobil" in val_lower or "exxon" in val_lower:
                        osm_brand = "esso"
                if osm_brand:
                    break

            station_match = match_osm_to_acra_station(
                lat, lon, name, osm_brand, station_postcodes, geocoded
            )
            result = determine_acra_status(station_match, osm_brand)

            supabase.table("acra_signal").insert({"batch_id": batch_id, **result}).execute()

            existing = (
                supabase.table("signal_batch")
                .select("sources_queried")
                .eq("batch_id", batch_id)
                .single()
                .execute()
            )
            sources = existing.data.get("sources_queried") or []
            if "acra" not in sources:
                sources.append("acra")
                supabase.table("signal_batch").update({"sources_queried": sources}).eq("batch_id", batch_id).execute()

            if result["match_level"] in ("station_address", "station_proximity"):
                matched_station += 1
            elif result["match_level"] == "brand_parent":
                matched_brand += 1
            else:
                unmatched += 1
            processed += 1

        except Exception as exc:
            errors += 1
            print(f"[acra] ERROR feature_id={feature_id}: {exc}")

        if processed % 50 == 0 and processed > 0:
            print(f"[acra] Progress: {processed}/{len(stations)}")

    print(f"\n[acra] ===== SUMMARY =====")
    print(f"  Total processed: {processed}")
    print(f"  Station-level matches: {matched_station}")
    print(f"  Brand-level fallback: {matched_brand}")
    print(f"  Unmatched: {unmatched}")
    print(f"  Errors: {errors}")


if __name__ == "__main__":
    process_all_fuel_stations()
