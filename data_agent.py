"""
Agentic Data Ingestion & Refresh System
========================================
Claude acts as the orchestrator. It decides WHAT to refresh, WHEN, and WHY
by inspecting data freshness, comparing record counts, and reasoning about anomalies.

Architecture:
  Tools → real Python functions Claude can call
  Agent → Claude (claude-opus-4-6) drives the loop

Usage:
  python data_agent.py               # routine check + smart refresh
  python data_agent.py --force       # agent told to force-refresh everything
  python data_agent.py --investigate # deep anomaly investigation mode
"""

import os
import sys
import json
import time
import argparse
import logging
from datetime import datetime, timezone

import anthropic
from anthropic import beta_tool

# ── Logging ─────────────────────────────────────────────────────────────────
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(LOG_DIR, "data_agent.log"), encoding="utf-8"),
    ],
)
log = logging.getLogger("data_agent")

# ── Constants ────────────────────────────────────────────────────────────────
CACHE_DIR = "cache"
CACHE_MAX_AGE = 7 * 24 * 60 * 60   # 7 days

CACHE_FILES = {
    "fuel":        os.path.join(CACHE_DIR, "osm_fuel.json"),
    "restaurants": os.path.join(CACHE_DIR, "osm_restaurants.json"),
    "roads":       os.path.join(CACHE_DIR, "roads_raw.json"),
}

# Snapshot file stores previous record counts for anomaly detection
SNAPSHOT_FILE = os.path.join(LOG_DIR, "data_snapshot.json")


# ── Helper utilities ──────────────────────────────────────────────────────────

def _load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def _save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

def _record_count(path):
    if not os.path.exists(path):
        return 0
    try:
        data = _load_json(path)
        return len(data) if isinstance(data, list) else 1
    except Exception:
        return -1

def _age_seconds(path):
    if not os.path.exists(path):
        return None
    return time.time() - os.path.getmtime(path)

def _load_snapshot():
    if os.path.exists(SNAPSHOT_FILE):
        try:
            return _load_json(SNAPSHOT_FILE)
        except Exception:
            pass
    return {}

def _save_snapshot(data):
    _save_json(SNAPSHOT_FILE, data)


# ── Tool definitions (Claude can call these) ──────────────────────────────────

@beta_tool
def check_data_status() -> dict:
    """Check the freshness and record counts of all cached data sources.
    Returns a dict with age, freshness, record count, and anomaly flags for each source.
    Use this first to understand what needs attention."""
    snapshot = _load_snapshot()
    result = {}
    for key, path in CACHE_FILES.items():
        age = _age_seconds(path)
        count = _record_count(path)
        prev_count = snapshot.get(key, {}).get("count")
        fresh = (age is not None and age < CACHE_MAX_AGE)

        anomaly = None
        if prev_count and count > 0:
            change_pct = abs(count - prev_count) / prev_count * 100
            if change_pct > 30:
                anomaly = f"Record count changed {change_pct:.1f}% vs last snapshot ({prev_count} → {count})"
            elif count < 10:
                anomaly = f"Suspiciously low record count: {count}"

        result[key] = {
            "exists": os.path.exists(path),
            "fresh": fresh,
            "age_hours": round(age / 3600, 1) if age else None,
            "age_days": round(age / 86400, 1) if age else None,
            "expires_in_hours": round((CACHE_MAX_AGE - age) / 3600, 1) if (age and fresh) else 0,
            "record_count": count,
            "previous_count": prev_count,
            "anomaly": anomaly,
        }
    return result


@beta_tool
def refresh_source(source: str, force: bool = False) -> dict:
    """Fetch fresh data for a specific source from its upstream API.
    source: one of 'fuel', 'restaurants', 'roads'
    force: if True, bypass the cache TTL check and always re-fetch
    Returns counts, timing, and success/error status."""
    if source not in CACHE_FILES:
        return {"error": f"Unknown source '{source}'. Choose from: {list(CACHE_FILES.keys())}"}

    path = CACHE_FILES[source]
    age = _age_seconds(path)

    if not force and age is not None and age < CACHE_MAX_AGE:
        return {
            "source": source,
            "action": "skipped",
            "reason": f"Cache is still fresh ({round(age/3600,1)}h old, TTL={CACHE_MAX_AGE//3600}h)",
        }

    # Bust cache
    if os.path.exists(path):
        os.remove(path)
        log.info("Deleted stale cache: %s", path)

    start = time.time()
    try:
        if source == "fuel":
            from validate_station import load_osm_fuel
            data = load_osm_fuel()
        elif source == "restaurants":
            from validate_station import load_osm_restaurants
            data = load_osm_restaurants()
        elif source == "roads":
            from validate_station import load_road_points
            data = load_road_points()

        elapsed = round(time.time() - start, 1)
        count = len(data) if isinstance(data, list) else 1
        log.info("Refreshed %s: %d records in %.1fs", source, count, elapsed)
        return {
            "source": source,
            "action": "refreshed",
            "record_count": count,
            "elapsed_seconds": elapsed,
        }
    except Exception as e:
        elapsed = round(time.time() - start, 1)
        log.error("Refresh failed for %s: %s", source, e)
        return {
            "source": source,
            "action": "error",
            "error": str(e),
            "elapsed_seconds": elapsed,
        }


@beta_tool
def compare_with_snapshot(source: str) -> dict:
    """Compare the current record count against the saved historical snapshot.
    Use this to detect data loss or unexpected growth after a refresh.
    source: one of 'fuel', 'restaurants', 'roads'"""
    if source not in CACHE_FILES:
        return {"error": f"Unknown source '{source}'"}

    path = CACHE_FILES[source]
    snapshot = _load_snapshot()
    current = _record_count(path)
    previous = snapshot.get(source, {}).get("count")

    if previous is None:
        return {"source": source, "current": current, "previous": None, "verdict": "no_snapshot"}

    diff = current - previous
    pct = (diff / previous * 100) if previous else 0

    if current <= 0:
        verdict = "critical_empty"
    elif pct < -30:
        verdict = "severe_drop"
    elif pct < -10:
        verdict = "moderate_drop"
    elif pct > 50:
        verdict = "unexpected_spike"
    else:
        verdict = "normal"

    return {
        "source": source,
        "current_count": current,
        "previous_count": previous,
        "change": diff,
        "change_pct": round(pct, 1),
        "verdict": verdict,
    }


@beta_tool
def save_snapshot() -> dict:
    """Persist current record counts as the baseline snapshot for future comparisons.
    Call this after a successful refresh cycle to update the reference point."""
    snapshot = {}
    for key, path in CACHE_FILES.items():
        count = _record_count(path)
        snapshot[key] = {
            "count": count,
            "saved_at": datetime.now(tz=timezone.utc).isoformat(),
        }
    _save_snapshot(snapshot)
    log.info("Snapshot saved: %s", snapshot)
    return {"saved": snapshot}


@beta_tool
def get_recent_logs(lines: int = 30) -> str:
    """Read the last N lines from the data agent log file.
    Useful for understanding what happened in previous runs."""
    log_path = os.path.join(LOG_DIR, "data_agent.log")
    if not os.path.exists(log_path):
        return "No log file found."
    try:
        with open(log_path, "r", encoding="utf-8") as f:
            all_lines = f.readlines()
        return "".join(all_lines[-lines:])
    except Exception as e:
        return f"Error reading logs: {e}"


# ── Agent runner ──────────────────────────────────────────────────────────────

TOOLS = [check_data_status, refresh_source, compare_with_snapshot, save_snapshot, get_recent_logs]

SYSTEM_PROMPT = """You are a data infrastructure agent for HereHack, a geospatial validation system.

Your job is to manage three cached data sources:
- fuel        → OpenStreetMap fuel stations in Singapore (~170–220 records expected)
- restaurants → OpenStreetMap restaurants/cafes in Singapore (~3000–10000 records expected)
- roads       → Singapore road network points (~30000–40000 records expected)

Each source has a 7-day cache TTL. Data comes from the Overpass API (OpenStreetMap).

Your responsibilities:
1. Check the status of all sources first.
2. Decide which sources need refreshing (stale, missing, or anomalous data).
3. Refresh the appropriate sources, handling errors gracefully.
4. Compare post-refresh counts against the snapshot to detect anomalies.
   - A >30% drop in records is suspicious (API issue, bad fetch, or genuine data loss).
   - A >50% spike is also suspicious.
5. Save an updated snapshot after a successful cycle.
6. Report a clear, concise summary of what you did and any issues found.

Be decisive. Don't ask for confirmation — just act and report.
If a refresh fails, try once more. If it fails again, note it and move on.
If you detect a severe anomaly (verdict=severe_drop or critical_empty), flag it prominently."""


def run_agent(mode: str = "routine"):
    """Run the agentic data ingestion loop."""
    client = anthropic.Anthropic()

    if mode == "force":
        user_prompt = (
            "Force-refresh ALL data sources regardless of their current TTL. "
            "After refreshing, compare counts against the snapshot and save a new snapshot. "
            "Report the full summary."
        )
    elif mode == "investigate":
        user_prompt = (
            "Investigate the current state of all data sources. Check their status and "
            "compare with historical snapshots. Look for anomalies. "
            "Only refresh sources that are stale or show anomalies — do not refresh healthy fresh data. "
            "Provide a detailed analysis."
        )
    else:
        user_prompt = (
            "Perform the routine data maintenance cycle: "
            "1) Check all source statuses. "
            "2) Refresh any sources that are stale (past their 7-day TTL). "
            "3) Verify record counts are within expected ranges. "
            "4) Save an updated snapshot if anything was refreshed. "
            "5) Summarise the outcome."
        )

    log.info("=== Data Agent Started [mode=%s] ===", mode)

    messages = [{"role": "user", "content": user_prompt}]

    # Agentic loop using tool runner
    runner = client.beta.messages.tool_runner(
        model="claude-opus-4-6",
        max_tokens=4096,
        thinking={"type": "adaptive"},
        system=SYSTEM_PROMPT,
        tools=TOOLS,
        messages=messages,
    )

    print(f"\n{'='*60}")
    print(f"  HereHack Data Agent  [mode: {mode}]")
    print(f"{'='*60}\n")

    final_text = ""
    for message in runner:
        # Print tool calls as they happen
        if hasattr(message, "content"):
            for block in message.content:
                if hasattr(block, "type"):
                    if block.type == "tool_use":
                        args_preview = json.dumps(block.input)[:80]
                        print(f"  → [{block.name}] {args_preview}")
                    elif block.type == "text" and block.text.strip():
                        final_text = block.text

    print(f"\n{'─'*60}")
    print(final_text)
    print(f"{'─'*60}\n")
    log.info("=== Data Agent Complete ===")
    return final_text


# ── CLI entry ─────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Agentic data ingestion and refresh for HereHack"
    )
    parser.add_argument("--force", action="store_true",
                        help="Force-refresh all sources regardless of TTL")
    parser.add_argument("--investigate", action="store_true",
                        help="Deep anomaly investigation — only refresh if needed")
    args = parser.parse_args()

    if args.force:
        mode = "force"
    elif args.investigate:
        mode = "investigate"
    else:
        mode = "routine"

    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("Error: ANTHROPIC_API_KEY environment variable not set.")
        sys.exit(1)

    run_agent(mode=mode)


if __name__ == "__main__":
    main()
