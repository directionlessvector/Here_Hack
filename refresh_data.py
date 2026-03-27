"""
Data Refresh Automation
=======================
CLI tool to check data freshness and force-refresh any stale cache files.

Usage:
  python refresh_data.py               # show status of all data sources
  python refresh_data.py --refresh     # refresh all stale sources
  python refresh_data.py --force       # force-refresh everything regardless of age
  python refresh_data.py --force --source fuel        # force-refresh only OSM fuel
  python refresh_data.py --force --source restaurants # force-refresh only OSM restaurants
  python refresh_data.py --force --source roads       # force-refresh only road network
  python refresh_data.py --json        # output status as JSON (for API use)
"""

import os
import sys
import time
import json
import argparse
import logging
from datetime import datetime, timezone

# ── Logging ─────────────────────────────────────────────────────────────────
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(LOG_DIR, "refresh.log"), encoding="utf-8"),
    ],
)
log = logging.getLogger("refresh_data")

# ── Constants ────────────────────────────────────────────────────────────────
CACHE_DIR = "cache"
CACHE_MAX_AGE = 7 * 24 * 60 * 60   # 7 days in seconds

CACHE_FILES = {
    "fuel":        os.path.join(CACHE_DIR, "osm_fuel.json"),
    "restaurants": os.path.join(CACHE_DIR, "osm_restaurants.json"),
    "roads":       os.path.join(CACHE_DIR, "roads_raw.json"),
    "osm_raw":     os.path.join(CACHE_DIR, "osm_raw.json"),
}

ACRA_CSV = "acra_final.csv"


# ── Helpers ──────────────────────────────────────────────────────────────────

def _file_age_seconds(path):
    """Return age in seconds, or None if file does not exist."""
    if not os.path.exists(path):
        return None
    return time.time() - os.path.getmtime(path)


def _file_size_kb(path):
    if not os.path.exists(path):
        return 0
    return round(os.path.getsize(path) / 1024, 1)


def _human_age(seconds):
    if seconds is None:
        return "missing"
    if seconds < 60:
        return f"{int(seconds)}s ago"
    if seconds < 3600:
        return f"{int(seconds/60)}m ago"
    if seconds < 86400:
        return f"{int(seconds/3600)}h ago"
    return f"{int(seconds/86400)}d ago"


def _is_fresh(path):
    age = _file_age_seconds(path)
    return age is not None and age < CACHE_MAX_AGE


def _record_count(path):
    """Return number of top-level items in a JSON array file."""
    if not os.path.exists(path):
        return 0
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return len(data) if isinstance(data, list) else 1
    except Exception:
        return -1


# ── Status ───────────────────────────────────────────────────────────────────

def get_status():
    """Return a dict describing the freshness of every data source."""
    status = {}

    for key, path in CACHE_FILES.items():
        age = _file_age_seconds(path)
        status[key] = {
            "path": path,
            "exists": os.path.exists(path),
            "fresh": _is_fresh(path),
            "age_seconds": round(age, 1) if age is not None else None,
            "age_human": _human_age(age),
            "size_kb": _file_size_kb(path),
            "record_count": _record_count(path),
            "last_updated": (
                datetime.fromtimestamp(os.path.getmtime(path), tz=timezone.utc).isoformat()
                if os.path.exists(path) else None
            ),
            "expires_in_human": (
                _human_age(max(0, CACHE_MAX_AGE - age)) + " (fresh)"
                if age is not None and age < CACHE_MAX_AGE
                else "expired"
            ),
        }

    # ACRA CSV — static file, just report its age
    acra_age = _file_age_seconds(ACRA_CSV)
    status["acra_csv"] = {
        "path": ACRA_CSV,
        "exists": os.path.exists(ACRA_CSV),
        "fresh": True,   # ACRA is a static dataset — no auto-refresh
        "age_seconds": round(acra_age, 1) if acra_age is not None else None,
        "age_human": _human_age(acra_age),
        "size_kb": _file_size_kb(ACRA_CSV),
        "note": "Static dataset — replace manually when a new extract is available",
    }

    return status


def print_status(status):
    print("\n╔══════════════════════════════════════════════════════╗")
    print("║         HereHack — Data Source Status               ║")
    print("╚══════════════════════════════════════════════════════╝\n")

    for key, info in status.items():
        icon = "✓" if info["fresh"] else ("✗" if info["exists"] else "!")
        size = f"{info['size_kb']} KB" if info.get("size_kb") else ""
        count = f"  {info.get('record_count')} records" if info.get("record_count") not in (None, 0, -1) else ""
        expires = f"  expires: {info.get('expires_in_human', '')}" if info.get("expires_in_human") else ""
        print(f"  [{icon}] {key:<15} last: {info['age_human']:<12} {size:<10}{count}{expires}")

    print()
    stale = [k for k, v in status.items() if not v["fresh"] and k != "acra_csv"]
    if stale:
        print(f"  Stale sources: {', '.join(stale)}")
        print("  Run:  python refresh_data.py --refresh")
    else:
        print("  All dynamic sources are fresh.")
    print()


# ── Refresh ──────────────────────────────────────────────────────────────────

def _bust_cache(keys):
    """Delete cache files for the given keys so the next fetch is forced."""
    for key in keys:
        path = CACHE_FILES.get(key)
        if path and os.path.exists(path):
            os.remove(path)
            log.info("Deleted stale cache: %s", path)


def refresh_fuel(force=False):
    path = CACHE_FILES["fuel"]
    if not force and _is_fresh(path):
        log.info("OSM fuel cache is fresh — skipping")
        return {"source": "fuel", "action": "skipped", "reason": "fresh"}

    _bust_cache(["fuel"])
    log.info("Fetching OSM fuel stations…")
    try:
        from validate_station import load_osm_fuel
        data = load_osm_fuel()
        log.info("OSM fuel: %d stations fetched", len(data))
        return {"source": "fuel", "action": "refreshed", "count": len(data)}
    except Exception as e:
        log.error("OSM fuel fetch failed: %s", e)
        return {"source": "fuel", "action": "error", "error": str(e)}


def refresh_restaurants(force=False):
    path = CACHE_FILES["restaurants"]
    if not force and _is_fresh(path):
        log.info("OSM restaurants cache is fresh — skipping")
        return {"source": "restaurants", "action": "skipped", "reason": "fresh"}

    _bust_cache(["restaurants"])
    log.info("Fetching OSM restaurants…")
    try:
        from validate_station import load_osm_restaurants
        data = load_osm_restaurants()
        log.info("OSM restaurants: %d entries fetched", len(data))
        return {"source": "restaurants", "action": "refreshed", "count": len(data)}
    except Exception as e:
        log.error("OSM restaurants fetch failed: %s", e)
        return {"source": "restaurants", "action": "error", "error": str(e)}


def refresh_roads(force=False):
    path = CACHE_FILES["roads"]
    if not force and _is_fresh(path):
        log.info("Roads cache is fresh — skipping")
        return {"source": "roads", "action": "skipped", "reason": "fresh"}

    _bust_cache(["roads"])
    log.info("Fetching road network…")
    try:
        from validate_station import load_road_points
        data = load_road_points()
        log.info("Roads: %d points fetched", len(data))
        return {"source": "roads", "action": "refreshed", "count": len(data)}
    except Exception as e:
        log.error("Roads fetch failed: %s", e)
        return {"source": "roads", "action": "error", "error": str(e)}


def refresh_all(force=False):
    log.info("=== Data Refresh Started (force=%s) ===", force)
    results = []
    results.append(refresh_fuel(force=force))
    results.append(refresh_restaurants(force=force))
    results.append(refresh_roads(force=force))
    log.info("=== Data Refresh Complete ===")

    errors = [r for r in results if r.get("action") == "error"]
    if errors:
        log.warning("%d source(s) failed to refresh", len(errors))

    return results


# ── CLI entry ─────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Check and refresh HereHack geospatial data caches"
    )
    parser.add_argument("--refresh", action="store_true",
                        help="Refresh stale sources")
    parser.add_argument("--force", action="store_true",
                        help="Force-refresh regardless of age")
    parser.add_argument("--source", choices=["fuel", "restaurants", "roads", "all"],
                        default="all", help="Which source to target (default: all)")
    parser.add_argument("--json", action="store_true",
                        help="Output status as JSON and exit")

    args = parser.parse_args()

    if args.json:
        status = get_status()
        print(json.dumps(status, indent=2))
        return

    # Always show status first
    status = get_status()
    print_status(status)

    if not args.refresh and not args.force:
        return

    # Determine which sources to act on
    target = args.source

    if args.force or args.refresh:
        if target == "all":
            results = refresh_all(force=args.force)
        elif target == "fuel":
            results = [refresh_fuel(force=args.force)]
        elif target == "restaurants":
            results = [refresh_restaurants(force=args.force)]
        elif target == "roads":
            results = [refresh_roads(force=args.force)]

        print("\nRefresh Results:")
        for r in results:
            action = r.get("action", "?")
            src = r.get("source", "?")
            detail = f"  {r.get('count', '')} records" if "count" in r else f"  {r.get('reason', r.get('error', ''))}"
            print(f"  {src:<15} {action:<12}{detail}")
        print()

    # Show updated status
    print_status(get_status())


if __name__ == "__main__":
    main()
