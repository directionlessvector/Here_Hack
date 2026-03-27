"""
processors/osm_meta_processor.py — OSM metadata signal processor
-----------------------------------------------------------------
Extracts signals purely from OSM data: freshness, tag completeness,
deletion status. No external API calls.

Results are written to: osm_meta_signal, signal_batch
"""

import os
import json
from datetime import datetime, timezone, timedelta
from typing import Optional
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# Tags expected on a well-mapped fuel station
EXPECTED_FUEL_TAGS = [
    "name",
    "brand",
    "operator",
    "fuel:diesel",
    "fuel:octane_95",
    "fuel:octane_98",
    "opening_hours",
    "phone",
    "website",
    "addr:street",
    "addr:housenumber",
    "addr:postcode",
]


# ---------------------------------------------------------------------------
# Signal computations
# ---------------------------------------------------------------------------

def compute_osm_freshness(last_seen_at: str) -> tuple:
    """
    Compute how many days ago the POI was last seen in OSM and a decay score.

    Freshness score decays linearly from 1.0 (just seen) to 0.0 (3 years ago).
    Formula: max(0.0, 1.0 - days / 1095)

    Args:
        last_seen_at: ISO-8601 timestamp string from poi_feature.last_seen_at

    Returns:
        (days_since_last_seen: int, freshness_score: float)
    """
    try:
        last_seen = datetime.fromisoformat(last_seen_at.replace("Z", "+00:00"))
        now       = datetime.now(timezone.utc)
        days      = (now - last_seen).days
    except (ValueError, AttributeError):
        days = 365  # default to 1 year if unparseable

    score = max(0.0, 1.0 - days / 1095)
    return (days, round(score, 4))


def compute_tag_completeness(tags: dict) -> tuple:
    """
    Score how completely a fuel station is tagged in OSM.

    Checks EXPECTED_FUEL_TAGS against the station's tags dict.
    Score = present_count / total_expected (0.0 – 1.0).

    Args:
        tags: OSM tags dict.

    Returns:
        (score: float, present: list[str], missing: list[str])
    """
    present = [t for t in EXPECTED_FUEL_TAGS if t in tags]
    missing = [t for t in EXPECTED_FUEL_TAGS if t not in tags]
    score   = len(present) / len(EXPECTED_FUEL_TAGS)
    return (round(score, 4), present, missing)


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
    Compute OSM metadata signals for every fuel station in poi_feature.

    Purely database-driven — no external HTTP calls.

    For each station:
      1. Get or create a signal_batch record.
      2. Compute freshness from last_seen_at.
      3. Compute tag completeness from tags JSONB.
      4. Insert into osm_meta_signal.
      5. Update signal_batch sources_queried to include 'osm_meta'.

    Progress is logged every 50 stations.
    """
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    print("[osm_meta] Fetching fuel stations from Supabase...")
    res = (
        supabase.table("poi_feature")
        .select("feature_id, osm_id, tags, last_seen_at, is_deleted_in_latest")
        .eq("category", "fuel")
        .execute()
    )
    stations = res.data or []
    total    = len(stations)
    print(f"[osm_meta] {total} fuel stations to process")

    processed = 0
    errors    = 0

    for station in stations:
        feature_id        = station["feature_id"]
        tags              = station.get("tags") or {}
        last_seen_at      = station.get("last_seen_at", "")
        is_deleted        = station.get("is_deleted_in_latest", False)

        try:
            batch_id = get_or_create_batch(supabase, feature_id)

            days, freshness_score           = compute_osm_freshness(last_seen_at)
            tag_score, present, missing     = compute_tag_completeness(tags)

            has_brand_tag    = "brand" in tags
            has_operator_tag = "operator" in tags

            supabase.table("osm_meta_signal").insert({
                "batch_id":               batch_id,
                "osm_last_edit_days":     days,
                "osm_freshness_score":    freshness_score,
                "tag_completeness_score": tag_score,
                "expected_tags_present":  present,
                "expected_tags_missing":  missing,
                "is_deleted_in_latest":   bool(is_deleted),
                "has_brand_tag":          has_brand_tag,
                "has_operator_tag":       has_operator_tag,
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
            if "osm_meta" not in sources:
                sources.append("osm_meta")
                supabase.table("signal_batch").update({
                    "sources_queried": sources
                }).eq("batch_id", batch_id).execute()

            processed += 1

        except Exception as exc:
            errors += 1
            print(f"[osm_meta] ERROR feature_id={feature_id}: {exc}")

        if processed % 50 == 0 and processed > 0:
            print(f"[osm_meta] Progress: {processed}/{total}")

    print(f"\n[osm_meta] Done — processed: {processed}/{total}, errors: {errors}")


if __name__ == "__main__":
    process_all_fuel_stations()
