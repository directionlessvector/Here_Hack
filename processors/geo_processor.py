"""
processors/geo_processor.py — Geo-spatial signal processor
-----------------------------------------------------------
Computes spatial validation signals for fuel stations using
Overpass API for road data and haversine math (no numpy needed).

Results are written to: geo_signal, signal_batch
"""

import os
import json
import math
import time
import requests
from datetime import datetime, timezone
from typing import Optional
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

OVERPASS_URL   = "https://overpass-api.de/api/interpreter"
CACHE_DIR      = os.path.join(os.path.dirname(__file__), "..", "cache")
ROADS_CACHE    = os.path.join(CACHE_DIR, "sg_roads.json")
CACHE_MAX_DAYS = 7

ROADS_QUERY = """
[out:json][timeout:120];
area(3600536780)->.sg;
way["highway"~"^(motorway|trunk|primary|secondary|tertiary|residential|service)$"](area.sg);
out geom;
"""

R_EARTH = 6_371_000  # metres


# ---------------------------------------------------------------------------
# Haversine helpers
# ---------------------------------------------------------------------------

def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return distance in metres between two WGS-84 coordinates."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi       = math.radians(lat2 - lat1)
    dlambda    = math.radians(lon2 - lon1)
    a = (math.sin(dphi / 2) ** 2
         + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2)
    return R_EARTH * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# ---------------------------------------------------------------------------
# Road fetching with 7-day cache
# ---------------------------------------------------------------------------

def fetch_singapore_roads() -> list:
    """
    Fetch Singapore road network from Overpass API.

    Results are cached in cache/sg_roads.json and refreshed only if
    the cache is older than CACHE_MAX_DAYS days.

    Returns:
        List of dicts: {osm_id, highway_type, geometry: [{lat, lon}, ...]}
    """
    os.makedirs(CACHE_DIR, exist_ok=True)

    # Return cache if fresh
    if os.path.exists(ROADS_CACHE):
        age_days = (time.time() - os.path.getmtime(ROADS_CACHE)) / 86400
        if age_days < CACHE_MAX_DAYS:
            print(f"[geo] Using cached roads ({age_days:.1f} days old)")
            with open(ROADS_CACHE) as f:
                return json.load(f)

    print("[geo] Fetching Singapore roads from Overpass API...")
    try:
        resp = requests.post(
            OVERPASS_URL,
            data={"data": ROADS_QUERY},
            timeout=180,
        )
        resp.raise_for_status()
        elements = resp.json().get("elements", [])
    except Exception as exc:
        print(f"[geo] Road fetch failed: {exc}")
        return []

    roads = []
    for el in elements:
        geom = el.get("geometry", [])
        if not geom:
            continue
        roads.append({
            "osm_id":       f"way/{el['id']}",
            "highway_type": el.get("tags", {}).get("highway", "unknown"),
            "geometry":     [{"lat": p["lat"], "lon": p["lon"]} for p in geom],
        })

    with open(ROADS_CACHE, "w") as f:
        json.dump(roads, f)

    print(f"[geo] Fetched {len(roads)} road segments, cached to {ROADS_CACHE}")
    return roads


# ---------------------------------------------------------------------------
# Distance computations
# ---------------------------------------------------------------------------

def compute_distance_to_road(
    station_lat: float,
    station_lon: float,
    roads: list,
) -> tuple:
    """
    Find the nearest road segment to a fuel station.

    For each road, compute the minimum haversine distance from the station
    to any node on the road polyline.

    Args:
        station_lat: Station latitude.
        station_lon: Station longitude.
        roads:       List of road dicts from fetch_singapore_roads().

    Returns:
        (nearest_distance_m, nearest_road_type, nearest_road_osm_id)
    """
    best_dist  = float("inf")
    best_type  = "unknown"
    best_osmid = None

    for road in roads:
        for pt in road["geometry"]:
            d = haversine(station_lat, station_lon, pt["lat"], pt["lon"])
            if d < best_dist:
                best_dist  = d
                best_type  = road["highway_type"]
                best_osmid = road["osm_id"]

    return (best_dist, best_type, best_osmid)


def compute_nearest_station(
    feature_id: int,
    station_lat: float,
    station_lon: float,
    all_stations: list,
) -> tuple:
    """
    Find the closest OTHER fuel station (excludes self).

    Args:
        feature_id:   ID of the current station (to exclude from results).
        station_lat:  Current station latitude.
        station_lon:  Current station longitude.
        all_stations: List of all fuel station dicts from Supabase.

    Returns:
        (distance_m, nearest_osm_id) — (9999, None) if no other stations.
    """
    best_dist  = float("inf")
    best_osmid = None

    for other in all_stations:
        if other["feature_id"] == feature_id:
            continue
        d = haversine(station_lat, station_lon, other["lat"], other["lon"])
        if d < best_dist:
            best_dist  = d
            best_osmid = other["osm_id"]

    if best_dist == float("inf"):
        return (9999.0, None)
    return (best_dist, best_osmid)


# ---------------------------------------------------------------------------
# Geo validity score
# ---------------------------------------------------------------------------

def compute_geo_validity_score(
    distance_to_road: float,
    road_type: str,
    nearest_station_dist: float,
) -> float:
    """
    Compute a 0-1 geo validity score for a fuel station.

    Scoring rules:
    - Close to major road  → high score
    - Far from any road    → low score
    - Very close duplicate → penalty (likely duplicate OSM entry)

    Args:
        distance_to_road:     Metres to nearest road node.
        road_type:            OSM highway tag of that road.
        nearest_station_dist: Metres to the closest other fuel station.

    Returns:
        float in [0.0, 1.0]
    """
    major_roads = {"motorway", "trunk", "primary", "secondary"}
    minor_roads = {"tertiary"}

    if distance_to_road < 30 and road_type in major_roads:
        score = 1.0
    elif distance_to_road < 50 and road_type in major_roads:
        score = 0.9
    elif distance_to_road < 50 and road_type in minor_roads:
        score = 0.8
    elif distance_to_road < 100:
        score = 0.5
    elif distance_to_road < 200:
        score = 0.3
    else:
        score = 0.1

    # Duplicate penalty
    if nearest_station_dist < 100:
        score *= 0.5

    return round(min(max(score, 0.0), 1.0), 4)


# ---------------------------------------------------------------------------
# Batch / signal helpers
# ---------------------------------------------------------------------------

def get_or_create_batch(supabase: Client, feature_id: int) -> str:
    """
    Find an existing pending batch or create a new one for this feature.

    Returns the batch_id UUID string.
    """
    res = (
        supabase.table("signal_batch")
        .select("batch_id")
        .eq("feature_id", feature_id)
        .eq("batch_status", "pending")
        .limit(1)
        .execute()
    )
    if res.data:
        return res.data[0]["batch_id"]

    new_batch = (
        supabase.table("signal_batch")
        .insert({
            "feature_id":    feature_id,
            "triggered_by":  "scheduler",
            "sources_queried": [],
            "batch_status":  "processing",
        })
        .execute()
    )
    return new_batch.data[0]["batch_id"]


def mark_batch_complete(supabase: Client, batch_id: str) -> None:
    """Mark a signal_batch row as completed."""
    supabase.table("signal_batch").update({
        "batch_status": "completed",
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }).eq("batch_id", batch_id).execute()


# ---------------------------------------------------------------------------
# Main processor
# ---------------------------------------------------------------------------

def process_all_fuel_stations() -> None:
    """
    Compute geo signals for every fuel station in poi_feature.

    For each station:
      1. Create / find a signal_batch record.
      2. Compute distance to nearest road and nearest station.
      3. Compute geo_validity_score.
      4. Insert result into geo_signal.
      5. Mark batch completed.

    Progress is logged every 50 stations.
    Errors per station are caught and logged without crashing the run.
    """
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    print("[geo] Fetching fuel stations from Supabase...")
    res = (
        supabase.table("poi_feature")
        .select("feature_id, osm_id, lat, lon")
        .eq("category", "fuel")
        .execute()
    )
    stations = res.data or []
    total = len(stations)
    print(f"[geo] {total} fuel stations to process")

    roads = fetch_singapore_roads()
    if not roads:
        print("[geo] No road data available — geo signals will be inaccurate")

    processed = 0
    errors    = 0

    for station in stations:
        feature_id = station["feature_id"]
        lat        = station["lat"]
        lon        = station["lon"]

        try:
            batch_id = get_or_create_batch(supabase, feature_id)

            dist_road, road_type, road_osmid = compute_distance_to_road(lat, lon, roads)
            dist_station, near_osmid         = compute_nearest_station(
                feature_id, lat, lon, stations
            )

            # Cap inf values — not JSON serializable
            if dist_road == float("inf") or dist_road != dist_road:
                dist_road = 9999.0
            if dist_station == float("inf") or dist_station != dist_station:
                dist_station = 9999.0

            geo_score = compute_geo_validity_score(dist_road, road_type, dist_station)

            supabase.table("geo_signal").insert({
                "batch_id":                   batch_id,
                "distance_to_road_m":         round(dist_road, 2),
                "nearest_road_type":          road_type,
                "nearest_road_osm_id":        road_osmid,
                "nearest_station_distance_m": round(dist_station, 2),
                "nearest_station_osm_id":     near_osmid,
                "geo_validity_score":         geo_score,
            }).execute()

            # Append 'geo' to sources_queried
            existing = (
                supabase.table("signal_batch")
                .select("sources_queried")
                .eq("batch_id", batch_id)
                .single()
                .execute()
            )
            sources = existing.data.get("sources_queried") or []
            if "geo" not in sources:
                sources.append("geo")
                supabase.table("signal_batch").update({
                    "sources_queried": sources
                }).eq("batch_id", batch_id).execute()

            mark_batch_complete(supabase, batch_id)
            processed += 1

        except Exception as exc:
            errors += 1
            print(f"[geo] ERROR feature_id={feature_id}: {exc}")

        if processed % 50 == 0 and processed > 0:
            print(f"[geo] Progress: {processed}/{total}")

    print(f"\n[geo] Done — processed: {processed}/{total}, errors: {errors}")


if __name__ == "__main__":
    process_all_fuel_stations()