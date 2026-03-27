"""
OSM Singapore POI Ingestion Script
------------------------------------
Fetches POIs from Overpass API and populates Supabase `poi_feature` table.

Usage:
    python3 populate_osm.py

Requirements:
    pip install requests supabase python-dotenv
"""

import os
import time
from typing import Optional
import requests
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

BATCH_SIZE = 500  # rows per upsert call

# ---------------------------------------------------------------------------
# Overpass Query — targeted POI categories for Singapore
# ---------------------------------------------------------------------------

OVERPASS_QUERY = """
[out:json][timeout:180];
area(3600536780)->.sg;
(
  node["amenity"~"^(restaurant|cafe|fast_food|pharmacy|fuel)$"](area.sg);
  way["amenity"~"^(restaurant|cafe|fast_food|pharmacy|fuel)$"](area.sg);
  node["tourism"~"^(hotel|attraction)$"](area.sg);
  way["tourism"~"^(hotel|attraction)$"](area.sg);
  node["shop"~"^(supermarket|convenience)$"](area.sg);
  way["shop"~"^(supermarket|convenience)$"](area.sg);
  node["leisure"="amusement_arcade"](area.sg);
  way["leisure"="amusement_arcade"](area.sg);
);
out center tags;
"""

# ---------------------------------------------------------------------------
# Category mapping
# ---------------------------------------------------------------------------

CATEGORY_MAP = {
    ("amenity", "restaurant"):      "restaurant",
    ("amenity", "cafe"):            "cafe",
    ("amenity", "fast_food"):       "fast_food",
    ("amenity", "pharmacy"):        "pharmacy",
    ("amenity", "fuel"):            "fuel",
    ("tourism", "hotel"):           "hotel",
    ("tourism", "attraction"):      "tourism",
    ("shop",    "supermarket"):     "grocery",
    ("shop",    "convenience"):     "grocery",
    ("leisure", "amusement_arcade"):"amusement",
}


def map_category(tags: dict) -> Optional[str]:
    for key in ("amenity", "tourism", "shop", "leisure"):
        val = tags.get(key)
        if val and (key, val) in CATEGORY_MAP:
            return CATEGORY_MAP[(key, val)]
    return None


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

def fetch_osm(retries: int = 3) -> dict:
    for attempt in range(1, retries + 1):
        print(f"[fetch] Attempt {attempt}/{retries} — querying Overpass API...")
        try:
            resp = requests.post(
                OVERPASS_URL,
                data={"data": OVERPASS_QUERY},
                timeout=240,
            )
            resp.raise_for_status()
            data = resp.json()
            count = len(data.get("elements", []))
            print(f"[fetch] Success — {count} raw elements received")
            return data
        except Exception as exc:
            print(f"[fetch] Error: {exc}")
            if attempt < retries:
                wait = 15 * attempt
                print(f"[fetch] Retrying in {wait}s...")
                time.sleep(wait)
    print("[fetch] All retries exhausted. Returning empty.")
    return {"elements": []}


# ---------------------------------------------------------------------------
# Parse
# ---------------------------------------------------------------------------

def parse_elements(data: dict) -> list[dict]:
    pois = []
    skipped_coords = 0
    skipped_category = 0

    for el in data.get("elements", []):
        el_type = el.get("type")
        el_id   = el.get("id")
        tags    = el.get("tags") or {}

        # Extract coordinates
        if el_type == "node":
            lat = el.get("lat")
            lon = el.get("lon")
        else:
            center = el.get("center") or {}
            lat = center.get("lat")
            lon = center.get("lon")

        if lat is None or lon is None:
            skipped_coords += 1
            continue

        category = map_category(tags)
        if not category:
            skipped_category += 1
            continue

        pois.append({
            "osm_id":   f"{el_type}/{el_id}",
            "name":     tags.get("name"),       # None is fine — nullable column
            "category": category,
            "tags":     tags,                   # full OSM tags as JSONB
            "lat":      lat,
            "lon":      lon,
        })

    print(f"[parse] {len(pois)} valid POIs extracted")
    print(f"[parse] Skipped — no coords: {skipped_coords}, no category match: {skipped_category}")
    return pois


# ---------------------------------------------------------------------------
# Insert
# ---------------------------------------------------------------------------

def populate_db(supabase: Client, pois: list[dict]) -> None:
    total    = len(pois)
    inserted = 0
    failed   = 0

    for batch_num, i in enumerate(range(0, total, BATCH_SIZE), start=1):
        batch = pois[i : i + BATCH_SIZE]
        try:
            # upsert: on duplicate osm_id, update all fields
            # geom is intentionally excluded — trigger builds it from lat/lon
            supabase.table("poi_feature").upsert(
                batch,
                on_conflict="osm_id",
            ).execute()
            inserted += len(batch)
            print(f"[db] Batch {batch_num}: {inserted}/{total} upserted")
        except Exception as exc:
            failed += len(batch)
            print(f"[db] Batch {batch_num} FAILED: {exc}")

    print(f"\n[db] Done — total: {total} | upserted: {inserted} | failed: {failed}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise EnvironmentError(
            "Missing env vars. Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in .env"
        )

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    print("=" * 50)
    print("  OSM Singapore POI Ingestion")
    print("=" * 50)

    data = fetch_osm()
    pois = parse_elements(data)

    if not pois:
        print("[main] No POIs to insert. Exiting.")
        return

    populate_db(supabase, pois)


if __name__ == "__main__":
    main()
