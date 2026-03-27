"""
processors/web_processor.py — Web & directory signal processor
--------------------------------------------------------------
Checks fuel station websites and public directories for activity signals.
No Google / Microsoft / paid APIs — uses Nominatim and optional Foursquare.

Results are written to: web_signal, signal_batch
"""

import os
import re
import time
import requests
from typing import Optional
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

SUPABASE_URL        = os.getenv("SUPABASE_URL")
SUPABASE_KEY        = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
FOURSQUARE_API_KEY  = os.getenv("FOURSQUARE_API_KEY")  # optional

USER_AGENT      = "Mozilla/5.0 (compatible; POIVerifier/1.0)"
NOMINATIM_AGENT = "POIVerifier/1.0 (fuel-station-verification)"

CLOSURE_KEYWORDS = [
    "permanently closed",
    "closed down",
    "no longer operating",
    "ceased operations",
    "demolished",
    "under renovation",
    "coming soon",
    "opening soon",
    "temporarily closed",
    "moved to",
    "relocated",
]


# ---------------------------------------------------------------------------
# Website check
# ---------------------------------------------------------------------------

def check_website(url: str) -> dict:
    """
    Check if a station's website is reachable and scan for closure keywords.

    Args:
        url: Website URL from OSM tags.

    Returns:
        Dict with: status_code, reachable, closure_keywords_found, found_keywords
    """
    result = {
        "status_code":            None,
        "reachable":              False,
        "closure_keywords_found": False,
        "found_keywords":         [],
    }
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=10,
            allow_redirects=True,
        )
        result["status_code"] = resp.status_code
        result["reachable"]   = resp.status_code < 400

        body = resp.text.lower()
        found = [kw for kw in CLOSURE_KEYWORDS if kw in body]
        result["closure_keywords_found"] = len(found) > 0
        result["found_keywords"]         = found

    except requests.Timeout:
        result["reachable"] = False
    except requests.ConnectionError:
        result["reachable"] = False
    except Exception:
        result["reachable"] = False

    return result


# ---------------------------------------------------------------------------
# Foursquare listing check (optional)
# ---------------------------------------------------------------------------

def check_foursquare_listing(name: str, lat: float, lon: float) -> dict:
    """
    Check if a fuel station appears in Foursquare's place search.

    Requires FOURSQUARE_API_KEY env var (free tier from developer.foursquare.com).
    If key is not set, returns an empty result without raising an error.

    Args:
        name: Station name.
        lat:  Station latitude.
        lon:  Station longitude.

    Returns:
        Dict with: listed, source, review_count, name
    """
    empty = {"listed": False, "source": None, "review_count": 0, "name": None}

    if not FOURSQUARE_API_KEY:
        return empty

    try:
        resp = requests.get(
            "https://api.foursquare.com/v3/places/search",
            headers={
                "Authorization": FOURSQUARE_API_KEY,
                "Accept":        "application/json",
            },
            params={
                "query":      name or "fuel station",
                "ll":         f"{lat},{lon}",
                "radius":     200,
                "categories": "19007",  # Gas Stations
                "limit":      5,
            },
            timeout=10,
        )
        resp.raise_for_status()
        results = resp.json().get("results", [])
        if results:
            top = results[0]
            return {
                "listed":       True,
                "source":       "foursquare",
                "review_count": top.get("stats", {}).get("total_ratings", 0),
                "name":         top.get("name"),
            }
    except Exception as exc:
        print(f"[web] Foursquare error: {exc}")

    return empty


# ---------------------------------------------------------------------------
# Nominatim reverse geocode
# ---------------------------------------------------------------------------

def check_openstreetmap_nominatim(name: str, lat: float, lon: float) -> dict:
    """
    Use Nominatim reverse geocode to check if OSM reflects this station.

    Rate limit: 1 request per second per Nominatim usage policy.

    Args:
        name: Station name to compare against.
        lat:  Station latitude.
        lon:  Station longitude.

    Returns:
        Dict with: address_match, returned_name, returned_address
    """
    result = {
        "address_match":    False,
        "returned_name":    None,
        "returned_address": None,
    }
    try:
        resp = requests.get(
            "https://nominatim.openstreetmap.org/reverse",
            headers={"User-Agent": NOMINATIM_AGENT},
            params={"lat": lat, "lon": lon, "format": "json", "zoom": 18},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()

        returned_name    = data.get("name", "")
        returned_address = data.get("display_name", "")
        result["returned_name"]    = returned_name
        result["returned_address"] = returned_address

        # Simple substring check for name match
        if name and returned_name:
            result["address_match"] = (
                name.lower() in returned_address.lower()
                or returned_name.lower() in name.lower()
            )

    except Exception as exc:
        print(f"[web] Nominatim error: {exc}")

    return result


# ---------------------------------------------------------------------------
# Web activity score
# ---------------------------------------------------------------------------

def compute_web_activity_score(
    website_reachable: bool,
    closure_found: bool,
    directory_listed: bool,
    nominatim_match: bool,
) -> float:
    """
    Compute a 0-1 web activity score from available signals.

    Scoring:
    - Base:                    0.5
    - Reachable website:      +0.2
    - Unreachable / closure:  -0.3
    - Directory listed:       +0.2
    - Nominatim confirms:     +0.1
    Clamped to [0.0, 1.0].

    Returns:
        float in [0.0, 1.0]
    """
    score = 0.5

    if website_reachable and not closure_found:
        score += 0.2
    elif not website_reachable or closure_found:
        score -= 0.3

    if directory_listed:
        score += 0.2
    if nominatim_match:
        score += 0.1

    return round(min(max(score, 0.0), 1.0), 4)


# ---------------------------------------------------------------------------
# Batch helper
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
    Check website and directory activity for every fuel station.

    For each station:
      1. Get or create signal_batch.
      2. Extract website URL from OSM tags.
      3. Check website reachability and closure keywords.
      4. Check Foursquare listing (optional, needs API key).
      5. Reverse geocode with Nominatim (1.2s delay between calls).
      6. Compute web_activity_score.
      7. Insert into web_signal.
      8. Update signal_batch sources_queried.

    Progress is logged every 20 stations (slower due to HTTP calls).
    """
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    print("[web] Fetching fuel stations from Supabase...")
    res = (
        supabase.table("poi_feature")
        .select("feature_id, name, tags, lat, lon")
        .eq("category", "fuel")
        .execute()
    )
    stations = res.data or []
    total    = len(stations)
    print(f"[web] {total} fuel stations to process")

    processed = 0
    errors    = 0

    for station in stations:
        feature_id = station["feature_id"]
        name       = station.get("name")
        tags       = station.get("tags") or {}
        lat        = station["lat"]
        lon        = station["lon"]

        try:
            batch_id = get_or_create_batch(supabase, feature_id)

            # Extract website URL
            url = (
                tags.get("website")
                or tags.get("contact:website")
                or tags.get("url")
            )

            # Website check
            web_result   = check_website(url) if url else {
                "status_code": None, "reachable": False,
                "closure_keywords_found": False, "found_keywords": [],
            }

            # Foursquare (optional)
            fs_result    = check_foursquare_listing(name or "", lat, lon)

            # Nominatim — rate-limited at 1 req/s
            nom_result   = check_openstreetmap_nominatim(name or "", lat, lon)
            time.sleep(1.2)  # Nominatim usage policy

            score = compute_web_activity_score(
                website_reachable=web_result["reachable"],
                closure_found=web_result["closure_keywords_found"],
                directory_listed=fs_result["listed"],
                nominatim_match=nom_result["address_match"],
            )

            supabase.table("web_signal").insert({
                "batch_id":               batch_id,
                "website_url":            url,
                "website_status_code":    web_result["status_code"],
                "website_reachable":      web_result["reachable"],
                "closure_keywords_found": web_result["closure_keywords_found"],
                "closure_keywords_list":  web_result["found_keywords"],
                "directory_listed":       fs_result["listed"],
                "directory_source":       fs_result["source"],
                "review_count":           fs_result["review_count"],
                "listing_active":         fs_result["listed"],
                "web_activity_score":     score,
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
            if "web" not in sources:
                sources.append("web")
                supabase.table("signal_batch").update({
                    "sources_queried": sources
                }).eq("batch_id", batch_id).execute()

            processed += 1

        except Exception as exc:
            errors += 1
            print(f"[web] ERROR feature_id={feature_id}: {exc}")

        if processed % 20 == 0 and processed > 0:
            print(f"[web] Progress: {processed}/{total}")

    print(f"\n[web] Done — processed: {processed}/{total}, errors: {errors}")


if __name__ == "__main__":
    process_all_fuel_stations()
