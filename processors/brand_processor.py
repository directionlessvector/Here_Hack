"""
processors/brand_processor.py — Brand verification processor
-------------------------------------------------------------
Verifies fuel stations against known Singapore fuel brand databases
using fuzzy matching. No Google / Microsoft / paid APIs.

Results are written to: brand_signal, signal_batch
"""
 
import os
import re
import json
import math
import time
import requests
from typing import Optional
from dotenv import load_dotenv
from supabase import create_client, Client

from utils.fuzzy import (
    fuzzy_match_score,
    brand_match,
    best_match,
    name_match_score,
    normalize,
    BrandRegistry,
    identify_brand,
)

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

CACHE_DIR  = os.path.join(os.path.dirname(__file__), "..", "cache")
CACHE_TTL  = 24 * 3600  # 24 hours in seconds

USER_AGENT = "Mozilla/5.0 (compatible; POIVerifier/1.0)"
R_EARTH    = 6_371_000

# ---------------------------------------------------------------------------
# Brand definitions
# ---------------------------------------------------------------------------

KNOWN_BRANDS = ["shell", "esso", "spc", "caltex", "sinopec"]

BRAND_ALIASES = {
    "shell":   ["shell", "shell v-power", "shell select"],
    "esso":    ["esso", "exxonmobil", "exxon", "mobil", "esso express"],
    "spc":     ["spc", "singapore petroleum"],
    "caltex":  ["caltex", "chevron", "caltex with techron"],
    "sinopec": ["sinopec", "china petroleum"],
}

BRAND_SOURCES = {
    "shell": {
        "url":    "https://www.shell.com.sg/motorist/shell-station-locator.html",
        "method": "api",
    },
    "esso": {
        "url":    "https://www.esso.com.sg/en-sg/find-station",
        "method": "scrape",
    },
    "spc": {
        "url":    "https://www.spc.com.sg/find-us/",
        "method": "scrape",
    },
    "caltex": {
        "url":    "https://www.caltex.com/sg/find-a-station.html",
        "method": "scrape",
    },
    "sinopec": {
        "url":    "https://www.sinopec.com.sg/stations",
        "method": "scrape",
    },
}


# ---------------------------------------------------------------------------
# Haversine helper
# ---------------------------------------------------------------------------

def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return distance in metres between two WGS-84 coordinates."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi       = math.radians(lat2 - lat1)
    dlambda    = math.radians(lon2 - lon1)
    a = (math.sin(dphi / 2) ** 2
         + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2)
    return R_EARTH * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# ---------------------------------------------------------------------------
# Brand extraction from OSM tags
# ---------------------------------------------------------------------------

def extract_brand_from_tags(tags: dict) -> tuple:
    """
    Extract and identify the fuel brand from OSM tags using fuzzy matching.

    Checks 'brand', 'operator', 'name' tags against KNOWN_BRANDS and
    BRAND_ALIASES using fuzzy scoring.

    Args:
        tags: OSM tags dict for a fuel station.

    Returns:
        (brand_key, confidence_score, source_tag)
        e.g. ("esso", 0.87, "name") or (None, 0.0, "")
    """
    best_brand  = None
    best_score  = 0.0
    best_source = ""

    # First pass: canonical brand list
    matched_brand, score, source = brand_match(tags, KNOWN_BRANDS)
    if score > best_score:
        best_brand, best_score, best_source = matched_brand, score, source

    # Second pass: aliases (catches "ExxonMobil" → "esso", etc.)
    for tag_key in ("brand", "operator", "name"):
        tag_val = tags.get(tag_key)
        if not tag_val:
            continue
        norm_val = normalize(tag_val)
        for brand_key, aliases in BRAND_ALIASES.items():
            for alias in aliases:
                s = fuzzy_match_score(norm_val, alias, normalize_first=False)
                if s > best_score:
                    best_score  = s
                    best_brand  = brand_key
                    best_source = tag_key

    return (best_brand, best_score, best_source)


# ---------------------------------------------------------------------------
# Brand station list fetching (with caching)
# ---------------------------------------------------------------------------

def _cache_path(brand_key: str) -> str:
    return os.path.join(CACHE_DIR, f"brand_{brand_key}_stations.json")


def _cache_is_fresh(path: str) -> bool:
    if not os.path.exists(path):
        return False
    return (time.time() - os.path.getmtime(path)) < CACHE_TTL


def _parse_stations_from_html(html: str, brand_key: str) -> list:
    """
    Best-effort HTML parsing for station name/lat/lon using regex.
    No BeautifulSoup — pure string/regex parsing.

    Returns list of {name, lat, lon, address} dicts (may be empty if
    the page structure can't be parsed).
    """
    stations = []

    # Try JSON-LD / embedded JSON
    json_blocks = re.findall(r'\{[^{}]{50,}\}', html)
    for block in json_blocks:
        try:
            data = json.loads(block)
            lat  = data.get("latitude") or data.get("lat")
            lon  = data.get("longitude") or data.get("lng") or data.get("lon")
            name = data.get("name") or data.get("title") or data.get("address")
            if lat and lon:
                stations.append({
                    "name":    str(name or brand_key),
                    "lat":     float(lat),
                    "lon":     float(lon),
                    "address": str(data.get("address", "")),
                })
        except (json.JSONDecodeError, ValueError, TypeError):
            continue

    # Fallback: look for lat/lng in JS variables
    if not stations:
        coords = re.findall(
            r'"lat(?:itude)?"\s*:\s*([\d.\-]+).*?"l(?:ng|on)(?:gitude)?"\s*:\s*([\d.\-]+)',
            html, re.DOTALL
        )
        for lat_s, lon_s in coords[:50]:
            try:
                stations.append({
                    "name":    brand_key,
                    "lat":     float(lat_s),
                    "lon":     float(lon_s),
                    "address": "",
                })
            except ValueError:
                continue

    return stations


def fetch_brand_stations(brand_key: str) -> list:
    """
    Fetch the station list for a known Singapore fuel brand.

    Results are cached per-brand in cache/brand_{key}_stations.json
    and refreshed if older than 24 hours.

    Args:
        brand_key: One of the KNOWN_BRANDS strings.

    Returns:
        List of {name, lat, lon, address} dicts. Empty list on failure.
    """
    os.makedirs(CACHE_DIR, exist_ok=True)
    cache_file = _cache_path(brand_key)

    if _cache_is_fresh(cache_file):
        with open(cache_file) as f:
            cached = json.load(f)
        print(f"[brand] Using cached {brand_key} stations ({len(cached)} entries)")
        return cached

    source = BRAND_SOURCES.get(brand_key)
    if not source:
        return []

    url = source["url"]
    print(f"[brand] Fetching {brand_key} stations from {url}")

    try:
        resp = requests.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=15,
        )
        resp.raise_for_status()
        stations = _parse_stations_from_html(resp.text, brand_key)
    except Exception as exc:
        print(f"[brand] WARNING: could not fetch {brand_key} stations: {exc}")
        stations = []

    with open(cache_file, "w") as f:
        json.dump(stations, f)

    print(f"[brand] {brand_key}: {len(stations)} stations found/cached")
    return stations


# ---------------------------------------------------------------------------
# Station matching
# ---------------------------------------------------------------------------

def match_station(
    station_lat: float,
    station_lon: float,
    station_name: Optional[str],
    brand_stations: list,
    brand_key: str,
) -> dict:
    """
    Find the best-matching brand station by distance + fuzzy name similarity.

    Confidence formula:
        brand_confidence = (dist_score * 0.6) + (name_score * 0.4)
        Bonus +0.1 if dist_score >= 0.8 AND name_score >= 0.7

    Args:
        station_lat:    POI latitude.
        station_lon:    POI longitude.
        station_name:   POI name from OSM (may be None).
        brand_stations: List of brand station dicts {name, lat, lon}.
        brand_key:      Canonical brand key for fallback name.

    Returns:
        Dict with: company_match, brand_confidence, matched_name,
                   distance_m, name_similarity, match_method
    """
    if not brand_stations:
        return {
            "company_match":    False,
            "brand_confidence": 0.0,
            "matched_name":     None,
            "distance_m":       9999.0,
            "name_similarity":  0.0,
            "match_method":     "all_brands_scan",
        }

    best        = None
    best_conf   = -1.0
    best_dist   = 9999.0
    best_name_s = 0.0

    for bs in brand_stations:
        dist = _haversine(station_lat, station_lon, bs["lat"], bs["lon"])

        if dist < 50:       dist_score = 1.0
        elif dist < 100:    dist_score = 0.8
        elif dist < 200:    dist_score = 0.5
        elif dist < 500:    dist_score = 0.2
        else:               dist_score = 0.0

        name_s = name_match_score(station_name or "", bs.get("name", brand_key))
        conf   = (dist_score * 0.6) + (name_s * 0.4)
        if dist_score >= 0.8 and name_s >= 0.7:
            conf = min(1.0, conf + 0.1)

        if conf > best_conf:
            best_conf   = conf
            best        = bs
            best_dist   = dist
            best_name_s = name_s

    return {
        "company_match":    best_conf >= 0.5,
        "brand_confidence": round(best_conf, 4),
        "matched_name":     best.get("name") if best else None,
        "distance_m":       round(best_dist, 2),
        "name_similarity":  round(best_name_s, 4),
        "match_method":     "tag_fuzzy",
    }


def match_against_all_brands(
    station_lat: float,
    station_lon: float,
    station_name: Optional[str],
    all_brand_stations: dict,
) -> dict:
    """
    Try matching a station against ALL brand station lists.

    Used when OSM tags provide no brand hint.

    Args:
        station_lat:        POI latitude.
        station_lon:        POI longitude.
        station_name:       POI name from OSM.
        all_brand_stations: Dict of brand_key → list of station dicts.

    Returns:
        Best match result dict from match_station(), with match_method
        set to 'all_brands_scan'.
    """
    best_result   = None
    best_conf     = -1.0

    for brand_key, stations in all_brand_stations.items():
        result = match_station(station_lat, station_lon, station_name, stations, brand_key)
        if result["brand_confidence"] > best_conf:
            best_conf   = result["brand_confidence"]
            best_result = result

    if best_result:
        best_result["match_method"] = "all_brands_scan"
        return best_result

    return {
        "company_match":    False,
        "brand_confidence": 0.0,
        "matched_name":     None,
        "distance_m":       9999.0,
        "name_similarity":  0.0,
        "match_method":     "all_brands_scan",
    }


# ---------------------------------------------------------------------------
# Batch / signal helpers
# ---------------------------------------------------------------------------

def get_or_create_batch(supabase: Client, feature_id: int) -> str:
    """Find an existing batch or create a new one. Returns batch_id."""
    res = (
        supabase.table("signal_batch")
        .select("batch_id")
        .eq("feature_id", feature_id)
        .in_("batch_status", ["pending", "processing"])
        .limit(1)
        .execute()
    )
    if res.data:
        return res.data[0]["batch_id"]

    new_batch = (
        supabase.table("signal_batch")
        .insert({
            "feature_id":      feature_id,
            "triggered_by":    "scheduler",
            "sources_queried": [],
            "batch_status":    "processing",
        })
        .execute()
    )
    return new_batch.data[0]["batch_id"]


# ---------------------------------------------------------------------------
# Main processor
# ---------------------------------------------------------------------------

def process_all_fuel_stations() -> None:
    """
    Verify all Singapore fuel stations against known brand databases.

    For each station:
      1. Extract brand from OSM tags (fuzzy).
      2. Match against that brand's station list (or all brands if unknown).
      3. Insert result into brand_signal.
      4. Update signal_batch sources_queried.

    Brand station lists are fetched once and cached for 24 hours.
    Progress is logged every 50 stations.
    """
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    print("[brand] Fetching fuel stations from Supabase...")
    res = (
        supabase.table("poi_feature")
        .select("feature_id, osm_id, name, tags, lat, lon")
        .eq("category", "fuel")
        .execute()
    )
    stations = res.data or []
    total = len(stations)
    print(f"[brand] {total} fuel stations to process")

    # Pre-fetch all brand station lists
    print("[brand] Pre-fetching brand station lists...")
    all_brand_stations = {}
    for brand_key in KNOWN_BRANDS:
        all_brand_stations[brand_key] = fetch_brand_stations(brand_key)
        time.sleep(2)  # polite delay between different brand fetches

    # Build registry once — shared across all stations this run
    registry = BrandRegistry()

    processed = 0
    matched   = 0
    errors    = 0
    conf_sum  = 0.0

    for station in stations:
        feature_id = station["feature_id"]
        tags       = station.get("tags") or {}
        name       = station.get("name")
        lat        = station["lat"]
        lon        = station["lon"]

        try:
            batch_id = get_or_create_batch(supabase, feature_id)

            # ── Brand identification cascade (Step 1-6) ──────────────────
            brand_key, tag_score, source_tag = identify_brand(tags, registry)
            osm_brand_raw = tags.get("brand") or tags.get("operator") or name

            stations_for_brand = all_brand_stations.get(brand_key or "", [])
            has_specific_brand_list = len(stations_for_brand) > 0

            if brand_key and tag_score > 0.5 and has_specific_brand_list:
                # Primary path: match against identified brand's station list
                result = match_station(lat, lon, name, stations_for_brand, brand_key)
                result["match_method"] = source_tag

            elif brand_key and tag_score > 0.5 and not has_specific_brand_list:
                # Brand identified but that brand's station list is empty/unavailable
                # Try all other brands (catches rebranding: tagged SPC but now Caltex)
                other_brands = {k: v for k, v in all_brand_stations.items()
                                if k != brand_key and len(v) > 0}
                if other_brands:
                    result = match_against_all_brands(lat, lon, name, other_brands)
                    if result["brand_confidence"] >= 0.5:
                        result["match_method"] = "rebrand_detected"
                    else:
                        # No match in any brand — record as unverified
                        result = {
                            "company_match": False,
                            "brand_confidence": round(tag_score * 0.5, 4),
                            "matched_name": brand_key,
                            "distance_m": 9999.0,
                            "name_similarity": 0.0,
                            "match_method": f"{source_tag}_no_station_list",
                        }
                else:
                    # All scrapers failed — no station lists at all
                    result = {
                        "company_match": False,
                        "brand_confidence": round(tag_score * 0.5, 4),
                        "matched_name": brand_key,
                        "distance_m": 9999.0,
                        "name_similarity": 0.0,
                        "match_method": f"{source_tag}_all_scrapers_failed",
                    }

            elif not brand_key or tag_score <= 0.5:
                # No brand identified — try matching against all brands
                any_lists = {k: v for k, v in all_brand_stations.items() if len(v) > 0}
                if any_lists:
                    result = match_against_all_brands(lat, lon, name, any_lists)
                else:
                    result = {
                        "company_match": False,
                        "brand_confidence": 0.0,
                        "matched_name": None,
                        "distance_m": 9999.0,
                        "name_similarity": 0.0,
                        "match_method": "unidentified",
                    }

            supabase.table("brand_signal").insert({
                "batch_id":                     batch_id,
                "osm_brand_name":               osm_brand_raw,
                "matched_brand_name":           result["matched_name"],
                "company_match":                result["company_match"],
                "brand_confidence":             result["brand_confidence"],
                "brand_source":                 brand_key,
                "dealer_locator_url":           BRAND_SOURCES.get(brand_key, {}).get("url"),
                "distance_to_listed_station_m": result["distance_m"],
                "name_match_score":             result["name_similarity"],
                "tag_match_score":              round(tag_score, 4),
                "match_method":                 result["match_method"],
            }).execute()

            # Update sources_queried
            existing = (
                supabase.table("signal_batch")
                .select("sources_queried")
                .eq("batch_id", batch_id)
                .single()
                .execute()
            )
            sources = existing.data.get("sources_queried") or []
            if "brand" not in sources:
                sources.append("brand")
                supabase.table("signal_batch").update({
                    "sources_queried": sources
                }).eq("batch_id", batch_id).execute()

            if result["company_match"]:
                matched += 1
            conf_sum  += result["brand_confidence"]
            processed += 1

        except Exception as exc:
            errors += 1
            print(f"[brand] ERROR feature_id={feature_id}: {exc}")

        if processed % 50 == 0 and processed > 0:
            avg_conf = conf_sum / processed if processed else 0
            print(f"[brand] Progress: {processed}/{total} | matched: {matched} | avg_conf: {avg_conf:.2f}")

    avg_conf = conf_sum / processed if processed else 0
    print(f"\n[brand] Done — total: {total} | matched: {matched} | unmatched: {processed - matched} | avg_conf: {avg_conf:.2f} | errors: {errors}")


if __name__ == "__main__":
    process_all_fuel_stations()
