"""
Geospatial Place Validation Engine
===================================
Validates fuel stations AND restaurants/cafes across multiple structured
data sources (OSM, ACRA, Overture Maps, spatial road proximity) and
assigns a composite confidence score (0-100).
"""

import os
import csv
import json
import math
import time
import re
import requests
import psycopg2
from rapidfuzz import fuzz, process as rfprocess

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
OVERPASS_URL = "https://overpass-api.de/api/interpreter"
ONEMAP_URL = "https://www.onemap.gov.sg/api/common/elastic/search"
ACRA_CSV = "acra_final.csv"
CACHE_DIR = "cache"
DATA_DIR = "data"

OVERTURE_DB_URL = (
    "postgresql://postgres.lixqjbwyksnxfmrirdta:"
    "LekhSanatanNayak@aws-1-ap-northeast-2.pooler.supabase.com:5432/postgres"
)

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

# ── Brand dictionaries ──────────────────────────────────────────────────
FUEL_BRANDS = {
    "shell":   {"keywords": ["shell"]},
    "esso":    {"keywords": ["esso", "exxon", "mobil"]},
    "spc":     {"keywords": ["spc", "singapore petroleum"]},
    "caltex":  {"keywords": ["caltex", "chevron"]},
    "sinopec": {"keywords": ["sinopec"]},
}

RESTAURANT_BRANDS = {
    "starbucks":    {"keywords": ["starbucks"]},
    "mcdonalds":    {"keywords": ["mcdonald", "mcdonalds"]},
    "kfc":          {"keywords": ["kfc", "kentucky fried"]},
    "subway":       {"keywords": ["subway"]},
    "pizza_hut":    {"keywords": ["pizza hut"]},
    "burger_king":  {"keywords": ["burger king"]},
    "jollibee":     {"keywords": ["jollibee"]},
    "toast_box":    {"keywords": ["toast box"]},
    "ya_kun":       {"keywords": ["ya kun"]},
    "kopitiam":     {"keywords": ["kopitiam"]},
    "old_chang_kee":{"keywords": ["old chang kee"]},
    "bk":           {"keywords": ["breadtalk"]},
    "pastamania":   {"keywords": ["pastamania"]},
    "sushi_tei":    {"keywords": ["sushi tei"]},
    "din_tai_fung": {"keywords": ["din tai fung"]},
    "thai_express": {"keywords": ["thai express"]},
    "nandos":       {"keywords": ["nandos", "nando's"]},
    "wingstop":     {"keywords": ["wingstop"]},
    "swensen":      {"keywords": ["swensen"]},
}

# ── ACRA filter keys ────────────────────────────────────────────────────
FUEL_KEYWORDS = ("service station", "petrol station", "fuel station", "filling station")
RESTAURANT_CATEGORIES = {"restaurant", "cafe", "fast_food"}

# ── Scoring weights ─────────────────────────────────────────────────────
FUEL_WEIGHTS = {"osm": 0.30, "acra": 0.25, "overture": 0.20, "spatial": 0.15, "brand": 0.10}
# Restaurants skip spatial → redistribute weight
RESTAURANT_WEIGHTS = {"osm": 0.35, "acra": 0.30, "overture": 0.25, "brand": 0.10}

CACHE_MAX_AGE = 7 * 24 * 60 * 60

# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _is_fresh(filepath):
    if not os.path.exists(filepath):
        return False
    try:
        return (time.time() - os.path.getmtime(filepath)) < CACHE_MAX_AGE
    except Exception:
        return False


def _save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def haversine(lat1, lon1, lat2, lon2):
    """Distance in metres between two WGS-84 points."""
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def detect_brand(text, brands_dict, extra1=None, extra2=None):
    """Detect brand from text using the given brand dictionary."""
    candidates = []
    for v in (extra1, extra2, text):
        if v:
            candidates.append(str(v).lower())
    for cand in candidates:
        for bkey, meta in brands_dict.items():
            for kw in meta.get("keywords", []):
                if kw in cand:
                    return bkey
    return None


# Generic place-type words that inflate fuzzy scores — removed before matching
_STOP_WORDS = {
    # Fuel
    "station", "petrol", "service", "fuel", "filling", "gas", "gasoline",
    "kiosk", "pump", "mart",
    # Restaurant / Cafe
    "restaurant", "cafe", "coffee", "diner", "bistro", "eatery", "kitchen",
    "bar", "pub", "grill", "lounge", "tavern", "food", "court", "bakery",
    "catering", "canteen", "hawker", "kopitiam", "eating", "house",
    "takeaway", "takeout", "delivery", "express",
    # Generic business words
    "pte", "ltd", "sdn", "bhd", "inc", "co", "the", "at", "and", "&",
    "singapore", "sg", "s/s", "ss",
}

# Pre-compiled pattern: match any stop word as whole word
_STOP_RE = re.compile(
    r"\b(" + "|".join(re.escape(w) for w in sorted(_STOP_WORDS, key=len, reverse=True)) + r")\b",
    re.IGNORECASE,
)


def normalize_name(name):
    """Lowercase, strip generic place-type words so fuzzy matching
    focuses on the distinctive part of the name (brand / unique id)."""
    if not name:
        return ""
    n = name.lower().strip()
    n = _STOP_RE.sub("", n)
    # Collapse whitespace and strip punctuation noise
    n = re.sub(r"[''\".,;:\-/\\()]+", " ", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n


# ---------------------------------------------------------------------------
# Data loaders – OSM
# ---------------------------------------------------------------------------

def _fetch_osm(cache_name, query):
    path = os.path.join(CACHE_DIR, f"{cache_name}.json")
    if _is_fresh(path):
        try:
            return _load_json(path)
        except Exception:
            pass

    resp = requests.post(OVERPASS_URL, data=query.strip(), timeout=120)
    resp.raise_for_status()
    data = resp.json()

    items = []
    for el in data.get("elements", []):
        lat = lon = None
        if el.get("type") == "node":
            lat, lon = el.get("lat"), el.get("lon")
        elif el.get("type") == "way":
            c = el.get("center")
            if c:
                lat, lon = c.get("lat"), c.get("lon")
        if lat is None or lon is None:
            continue
        tags = el.get("tags") or {}
        items.append({
            "osm_id": f"{el.get('type')}/{el.get('id')}",
            "name": tags.get("name", ""),
            "brand_tag": tags.get("brand", ""),
            "operator_tag": tags.get("operator", ""),
            "cuisine": tags.get("cuisine", ""),
            "lat": lat,
            "lon": lon,
        })
    _save_json(path, items)
    return items


def load_osm_fuel():
    return _fetch_osm("osm_fuel", """
[out:json][timeout:60];
area(3600536780)->.sg;
(node["amenity"="fuel"](area.sg);way["amenity"="fuel"](area.sg););
out center tags;
""")


def load_osm_restaurants():
    return _fetch_osm("osm_restaurants", """
[out:json][timeout:90];
area(3600536780)->.sg;
(
  node["amenity"~"^(restaurant|cafe|fast_food|food_court|bar|pub)$"](area.sg);
  way["amenity"~"^(restaurant|cafe|fast_food|food_court|bar|pub)$"](area.sg);
);
out center tags;
""")


# ---------------------------------------------------------------------------
# Data loaders – ACRA
# ---------------------------------------------------------------------------

def load_acra_fuel():
    """Parse ACRA CSV for fuel-related businesses (grouped by postal code)."""
    if not os.path.exists(ACRA_CSV):
        return {}
    result = {}
    with open(ACRA_CSV, "r", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        for row in reader:
            building = (row.get("building_name") or "").strip()
            low = building.lower()
            if not any(k in low for k in FUEL_KEYWORDS):
                continue
            postal = (row.get("postal_code") or "").strip()
            street = (row.get("street_name") or "").strip()
            status_raw = (row.get("status") or "").strip().lower()
            if status_raw in ("active", "operating", "open", "in operation"):
                status = "active"
            elif status_raw in ("closed", "inactive", "shut", "ceased"):
                status = "closed"
            else:
                status = "unknown"
            brand = detect_brand(low, FUEL_BRANDS)
            if not postal:
                continue
            bucket = result.setdefault(postal, {
                "brand": brand or "", "street": street, "building": building,
                "status": status, "active_count": 0, "closed_count": 0, "total_count": 0,
            })
            if not bucket.get("brand") and brand:
                bucket["brand"] = brand
            bucket["active_count"] += 1 if status == "active" else 0
            bucket["closed_count"] += 1 if status == "closed" else 0
            bucket["total_count"] += 1
    return result


def load_acra_restaurants():
    """Parse ACRA CSV for restaurant / cafe businesses (list of entries)."""
    if not os.path.exists(ACRA_CSV):
        return []
    result = []
    with open(ACRA_CSV, "r", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        for row in reader:
            category = (row.get("category") or "").strip().lower()
            if category not in RESTAURANT_CATEGORIES:
                continue
            clean_name = (row.get("clean_name") or "").strip()
            if not clean_name or len(clean_name) < 2:
                continue
            postal = (row.get("postal_code") or "").strip()
            street = (row.get("street_name") or "").strip()
            building = (row.get("building_name") or "").strip()
            status_raw = (row.get("status") or "").strip().lower()
            if status_raw in ("active", "operating", "open", "in operation"):
                status = "active"
            elif status_raw in ("closed", "inactive", "shut", "ceased"):
                status = "closed"
            else:
                status = "unknown"
            brand = detect_brand(clean_name, RESTAURANT_BRANDS)
            result.append({
                "name": clean_name,
                "brand": brand or "",
                "building": building,
                "street": street,
                "postal_code": postal,
                "category": category,
                "status": status,
            })
    return result


# ---------------------------------------------------------------------------
# Data loaders – Roads (fuel only)
# ---------------------------------------------------------------------------

def load_road_points():
    path = os.path.join(CACHE_DIR, "roads_raw.json")
    if _is_fresh(path):
        try:
            return _load_json(path)
        except Exception:
            pass
    query = """
[out:json][timeout:120];
area(3600536780)->.sg;
way["highway"~"^(motorway|trunk|primary|secondary|tertiary)$"](area.sg);
out geom;
"""
    resp = requests.post(OVERPASS_URL, data=query.strip(), timeout=150)
    resp.raise_for_status()
    data = resp.json()
    points = []
    for el in data.get("elements", []):
        tags = el.get("tags") or {}
        htype = tags.get("highway")
        if not htype:
            continue
        geom = el.get("geometry") or []
        if not isinstance(geom, list):
            continue
        for i, p in enumerate(geom):
            if i % 10 != 0:
                continue
            lat, lon = p.get("lat"), p.get("lon")
            if lat is not None and lon is not None:
                points.append({"road_type": htype, "lat": lat, "lon": lon})
    _save_json(path, points)
    return points


# ---------------------------------------------------------------------------
# STEP 1: OSM Validation (shared for both types)
# ---------------------------------------------------------------------------

def validate_osm(query_name, query_lat, query_lon, osm_data, brands_dict):
    best_score = 0.0
    best_station = None
    query_norm = normalize_name(query_name)

    for st in osm_data:
        st_norm = normalize_name(st.get("name", ""))
        name_sim = fuzz.token_sort_ratio(query_norm, st_norm) / 100.0 if query_norm and st_norm else 0.0

        dist = None
        if query_lat is not None and query_lon is not None:
            dist = haversine(query_lat, query_lon, st["lat"], st["lon"])

        # Both distance AND name similarity required for high scores
        score = 0.0
        if dist is not None and dist <= 50 and name_sim >= 0.7:
            score = 1.0                       # exact: close + strong name
        elif dist is not None and dist <= 100 and name_sim >= 0.5:
            score = 0.7                       # good: nearby + decent name
        elif name_sim >= 0.7:
            score = 0.7                       # strong name match anywhere
        elif dist is not None and dist <= 50 and name_sim >= 0.3:
            score = 0.5                       # very close + weak name
        elif name_sim >= 0.5:
            score = 0.3                       # moderate name match
        elif dist is not None and dist <= 100 and name_sim >= 0.3:
            score = 0.3                       # nearby + weak name

        if score > best_score:
            best_score = score
            best_station = {
                "osm_id": st.get("osm_id"),
                "name": st.get("name"),
                "brand": detect_brand(st.get("name"), brands_dict, st.get("brand_tag"), st.get("operator_tag")),
                "lat": st["lat"], "lon": st["lon"],
                "distance_m": round(dist, 1) if dist is not None else None,
                "name_similarity": round(name_sim, 3),
            }
    return best_score > 0, round(best_score, 2), best_station


# ---------------------------------------------------------------------------
# STEP 2a: ACRA Validation – Fuel (by building_name / postal code)
# ---------------------------------------------------------------------------

def validate_acra_fuel(query_name, query_lat, query_lon, acra_data):
    if not acra_data:
        return False, "unknown", 0.0, None
    query_norm = normalize_name(query_name)
    best_score = 0.0
    best_status = "unknown"
    best_info = None

    for postal, info in acra_data.items():
        building = info.get("building", "")
        street = info.get("street", "")
        bld_norm = normalize_name(building)
        sim = fuzz.token_sort_ratio(query_norm, bld_norm) / 100.0 if query_norm and bld_norm else 0.0
        street_sim = fuzz.token_sort_ratio(query_norm, normalize_name(street)) / 100.0 if query_norm and street else 0.0
        sim = max(sim, street_sim)
        if sim < 0.5:
            continue

        status = info.get("status", "unknown")
        if info.get("active_count", 0) > 0:
            status = "active"
        elif info.get("closed_count", 0) > 0:
            status = "closed"

        score = _acra_score(sim, status)
        if score > best_score:
            best_score = score
            best_status = status
            best_info = {
                "postal_code": postal, "building": building, "street": street,
                "brand": info.get("brand", ""), "status": status,
                "name_similarity": round(sim, 3),
            }
    return best_score > 0, best_status, round(best_score, 2), best_info


# ---------------------------------------------------------------------------
# STEP 2b: ACRA Validation – Restaurants (by clean_name / category)
# ---------------------------------------------------------------------------

def validate_acra_restaurant(query_name, query_lat, query_lon, acra_data):
    if not acra_data:
        return False, "unknown", 0.0, None

    query_norm = normalize_name(query_name)
    best_score = 0.0
    best_status = "unknown"
    best_info = None

    # Use rapidfuzz process for faster matching on large lists
    names = [normalize_name(e.get("name", "")) for e in acra_data]
    if not query_norm or not names:
        return False, "unknown", 0.0, None

    matches = rfprocess.extract(query_norm, names, scorer=fuzz.token_sort_ratio, limit=10)

    for match_name, match_score, match_idx in matches:
        sim = match_score / 100.0
        if sim < 0.5:
            continue
        entry = acra_data[match_idx]
        status = entry.get("status", "unknown")
        score = _acra_score(sim, status)

        if score > best_score:
            best_score = score
            best_status = status
            best_info = {
                "name": entry.get("name", ""),
                "building": entry.get("building", ""),
                "street": entry.get("street", ""),
                "postal_code": entry.get("postal_code", ""),
                "brand": entry.get("brand", ""),
                "category": entry.get("category", ""),
                "status": status,
                "name_similarity": round(sim, 3),
            }
    return best_score > 0, best_status, round(best_score, 2), best_info


def _acra_score(sim, status):
    if status == "active" and sim >= 0.7:
        return 1.0
    elif status == "active" and sim >= 0.5:
        return 0.8
    elif status == "active":
        return 0.6
    elif status == "unknown" and sim >= 0.7:
        return 0.6
    elif status == "unknown" and sim >= 0.5:
        return 0.4
    elif status == "closed":
        return 0.2
    return 0.0


# ---------------------------------------------------------------------------
# STEP 3: Spatial / Road Proximity (fuel only)
# ---------------------------------------------------------------------------

def validate_spatial(query_lat, query_lon, road_points):
    if query_lat is None or query_lon is None:
        return 0.0, {"error": "no coordinates provided"}
    min_dist = float("inf")
    nearest_road_type = None
    for pt in road_points:
        d = haversine(query_lat, query_lon, pt["lat"], pt["lon"])
        if d < min_dist:
            min_dist = d
            nearest_road_type = pt.get("road_type")
    if min_dist < 50:
        score = 1.0
    elif min_dist < 100:
        score = 0.7
    elif min_dist < 300:
        score = 0.3
    else:
        score = 0.0
    return round(score, 2), {
        "nearest_road_distance_m": round(min_dist, 1),
        "nearest_road_type": nearest_road_type,
    }


# ---------------------------------------------------------------------------
# STEP 4: Overture Maps Validation (parameterised by table)
# ---------------------------------------------------------------------------

def validate_overture(query_name, query_lat, query_lon, table_name, brands_dict):
    try:
        conn = psycopg2.connect(OVERTURE_DB_URL)
        cur = conn.cursor()

        if query_lat is not None and query_lon is not None:
            sql = f"""
                SELECT name, ST_AsText(geom) AS geom_text, brand, addresses,
                       names, operating_status,
                       ST_Distance(geom::geography,
                                   ST_SetSRID(ST_MakePoint(%s,%s),4326)::geography) AS dist_m
                FROM {table_name}
                WHERE ST_DWithin(geom::geography,
                                 ST_SetSRID(ST_MakePoint(%s,%s),4326)::geography, 500)
                ORDER BY dist_m LIMIT 10
            """
            cur.execute(sql, (query_lon, query_lat, query_lon, query_lat))
        else:
            sql = f"""
                SELECT name, ST_AsText(geom) AS geom_text, brand, addresses,
                       names, operating_status, NULL AS dist_m
                FROM {table_name}
                WHERE LOWER(name) LIKE %s
                LIMIT 10
            """
            cur.execute(sql, (f"%{query_name.lower()}%",))

        rows = cur.fetchall()
        conn.close()
        if not rows:
            return False, 0.0, None

        query_norm = normalize_name(query_name)
        best_score = 0.0
        best_match = None

        for r_name, r_geom, r_brand, r_addresses, r_names, r_status, r_dist in rows:
            r_name = r_name or ""
            r_norm = normalize_name(r_name)
            name_sim = fuzz.token_sort_ratio(query_norm, r_norm) / 100.0 if query_norm and r_norm else 0.0

            if r_names and isinstance(r_names, dict):
                primary = r_names.get("primary", "")
                if primary:
                    alt = fuzz.token_sort_ratio(query_norm, normalize_name(primary)) / 100.0
                    name_sim = max(name_sim, alt)

            dist = float(r_dist) if r_dist is not None else None

            # Both distance AND name similarity required for high scores
            score = 0.0
            if dist is not None and dist <= 100 and name_sim >= 0.6:
                score = 1.0                   # exact: close + strong name
            elif dist is not None and dist <= 100 and name_sim >= 0.4:
                score = 0.7                   # good: nearby + decent name
            elif name_sim >= 0.6:
                score = 0.7                   # strong name match anywhere
            elif dist is not None and dist <= 300 and name_sim >= 0.4:
                score = 0.3                   # moderate: further + some name
            elif name_sim >= 0.4:
                score = 0.3                   # moderate name match only

            if score > best_score:
                best_score = score
                brand_name = None
                if r_brand and isinstance(r_brand, dict):
                    b_names = r_brand.get("names") or {}
                    brand_name = b_names.get("primary")

                r_lat = r_lon = None
                if r_geom:
                    m = re.search(r"([\d.]+)\s+([\d.]+)", r_geom)
                    if m:
                        r_lon, r_lat = float(m.group(1)), float(m.group(2))

                best_match = {
                    "name": r_name,
                    "brand": detect_brand(r_name, brands_dict, brand_name) or brand_name or "",
                    "lat": r_lat, "lon": r_lon,
                    "distance_m": round(dist, 1) if dist is not None else None,
                    "name_similarity": round(name_sim, 3),
                    "operating_status": r_status,
                }
        return best_score > 0, round(best_score, 2), best_match
    except Exception as e:
        return False, 0.0, {"error": str(e)}


# ---------------------------------------------------------------------------
# STEP 5: Brand Consistency
# ---------------------------------------------------------------------------

def check_brand_consistency(osm_details, acra_details, overture_details):
    brands_found = {}
    if osm_details and osm_details.get("brand"):
        brands_found["osm"] = osm_details["brand"]
    if acra_details and acra_details.get("brand"):
        brands_found["acra"] = acra_details["brand"]
    if overture_details and overture_details.get("brand"):
        brands_found["overture"] = overture_details["brand"]

    if not brands_found:
        return 0.0, {"brands": brands_found, "note": "no brand detected"}

    unique = set(str(b).lower() for b in brands_found.values() if b)
    if not unique:
        return 0.0, {"brands": brands_found, "note": "no brand detected"}
    if len(unique) == 1 and len(brands_found) >= 2:
        return 1.0, {"brands": brands_found, "note": "consistent across sources"}
    elif len(unique) == 1:
        return 0.5, {"brands": brands_found, "note": "detected in one source only"}
    else:
        return 0.5, {"brands": brands_found, "note": "inconsistent across sources"}


# ---------------------------------------------------------------------------
# STEP 6 & 7: Final Score + Decision
# ---------------------------------------------------------------------------

def compute_final(scores, weights):
    raw = sum(weights.get(k, 0) * scores.get(k, 0) for k in weights)
    final = round(raw * 100, 2)
    if final >= 80:
        decision = "CONFIRMED EXISTING"
    elif final >= 50:
        decision = "LIKELY EXISTS"
    elif final >= 20:
        decision = "UNCERTAIN"
    else:
        decision = "DOES NOT EXIST"
    return final, decision


# ---------------------------------------------------------------------------
# Data caches
# ---------------------------------------------------------------------------

_cache = {
    "fuel_station": {"osm": None, "acra": None, "roads": None},
    "restaurant":   {"osm": None, "acra": None},
}


def _ensure(place_type):
    c = _cache[place_type]

    if c["osm"] is None:
        label = "fuel" if place_type == "fuel_station" else "restaurant"
        print(f"[engine] Loading OSM {label} data ...")
        try:
            c["osm"] = load_osm_fuel() if place_type == "fuel_station" else load_osm_restaurants()
        except Exception as e:
            print(f"[engine]   ! OSM load failed: {e}")
            c["osm"] = []
        print(f"[engine]   -> {len(c['osm'])} entries loaded")

    if c["acra"] is None:
        print(f"[engine] Loading ACRA ({place_type}) ...")
        try:
            c["acra"] = load_acra_fuel() if place_type == "fuel_station" else load_acra_restaurants()
        except Exception as e:
            print(f"[engine]   ! ACRA load failed: {e}")
            c["acra"] = {} if place_type == "fuel_station" else []
        size = len(c["acra"])
        print(f"[engine]   -> {size} entries loaded")

    if place_type == "fuel_station" and c.get("roads") is None:
        print("[engine] Loading road network ...")
        try:
            c["roads"] = load_road_points()
        except Exception as e:
            print(f"[engine]   ! Roads load failed: {e}")
            c["roads"] = []
        print(f"[engine]   -> {len(c['roads'])} points loaded")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def validate(name: str, lat=None, lon=None, place_type="fuel_station") -> dict:
    """
    Validate a place by name + optional coords.
    place_type: 'fuel_station' or 'restaurant'
    """
    _ensure(place_type)
    c = _cache[place_type]

    brands = FUEL_BRANDS if place_type == "fuel_station" else RESTAURANT_BRANDS
    overture_table = "fuel_stations" if place_type == "fuel_station" else "restaurants"
    weights = FUEL_WEIGHTS if place_type == "fuel_station" else RESTAURANT_WEIGHTS

    coord_penalty = 1.0 if (lat is not None and lon is not None) else 0.6

    # Step 1 – OSM
    osm_match, osm_score, osm_details = validate_osm(name, lat, lon, c["osm"], brands)

    # Step 2 – ACRA
    if place_type == "fuel_station":
        acra_match, acra_status, acra_score, acra_details = validate_acra_fuel(name, lat, lon, c["acra"])
    else:
        acra_match, acra_status, acra_score, acra_details = validate_acra_restaurant(name, lat, lon, c["acra"])

    # Step 3 – Spatial (fuel only)
    spatial_score, spatial_details = (0.0, None)
    if place_type == "fuel_station":
        spatial_score, spatial_details = validate_spatial(lat, lon, c["roads"])

    # Step 4 – Overture
    overture_match, overture_score, overture_details = validate_overture(name, lat, lon, overture_table, brands)

    # Step 5 – Brand consistency
    brand_score, brand_details = check_brand_consistency(osm_details, acra_details, overture_details)

    # Coord penalty
    if coord_penalty < 1.0:
        osm_score *= coord_penalty
        overture_score *= coord_penalty
        spatial_score *= coord_penalty

    # Step 6 & 7
    score_map = {"osm": osm_score, "acra": acra_score, "overture": overture_score,
                 "spatial": spatial_score, "brand": brand_score}
    final_score, decision = compute_final(score_map, weights)

    result = {
        "input": {"name": name, "lat": lat, "lon": lon, "place_type": place_type},
        "osm":   {"match": osm_match, "score": osm_score, "details": osm_details},
        "acra":  {"match": acra_match, "status": acra_status, "score": acra_score, "details": acra_details},
        "overture": {"match": overture_match, "score": overture_score, "details": overture_details},
        "brand": {"consistency_score": brand_score, "details": brand_details},
        "final": {"score": final_score, "decision": decision},
    }

    if place_type == "fuel_station":
        result["spatial"] = {"road_proximity_score": spatial_score, "details": spatial_details}

    return result


def suggest(query: str, place_type="fuel_station", limit=8) -> list:
    """Return name suggestions from Overture for autocomplete."""
    if not query or len(query) < 2:
        return []
    table = "fuel_stations" if place_type == "fuel_station" else "restaurants"
    try:
        conn = psycopg2.connect(OVERTURE_DB_URL)
        cur = conn.cursor()
        cur.execute(f"""
            SELECT DISTINCT name,
                   ST_Y(ST_Centroid(geom)) AS lat,
                   ST_X(ST_Centroid(geom)) AS lon
            FROM {table}
            WHERE name IS NOT NULL AND LOWER(name) LIKE %s
            ORDER BY name
            LIMIT %s
        """, (f"%{query.lower()}%", limit))
        rows = cur.fetchall()
        conn.close()
        return [{"name": r[0], "lat": round(r[1], 6), "lon": round(r[2], 6)} for r in rows if r[0]]
    except Exception:
        return []


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python validate_station.py <name> [lat] [lon] [fuel_station|restaurant]")
        sys.exit(1)
    q_name = sys.argv[1]
    q_lat = float(sys.argv[2]) if len(sys.argv) > 2 else None
    q_lon = float(sys.argv[3]) if len(sys.argv) > 3 else None
    pt = sys.argv[4] if len(sys.argv) > 4 else "fuel_station"
    result = validate(q_name, q_lat, q_lon, pt)
    print(json.dumps(result, indent=2, ensure_ascii=False))
