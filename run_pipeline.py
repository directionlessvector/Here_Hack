"""
run_pipeline.py — Full pipeline orchestrator
---------------------------------------------
Runs the fuel station verification pipeline in sequence:
  1. Geo-spatial signals
  2. Brand verification
  3. OSM metadata signals
  4. Web & directory signals
  5. Classification engine

Usage:
    python3 run_pipeline.py
"""

import os
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")


def run_pipeline() -> None:
    start_total = time.time()

    print("=" * 60)
    print(f"  FUEL STATION VERIFICATION PIPELINE")
    print(f"  Started: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 60)

    # Step 1 — Geo signals
    try:
        from processors.geo_processor import process_all_fuel_stations as run_geo
        print("\n[PIPELINE] Step 1/5: Geo-spatial signals...")
        t = time.time()
        run_geo()
        print(f"[PIPELINE] Geo done in {time.time() - t:.1f}s")
    except Exception as exc:
        print(f"[PIPELINE] Step 1 FAILED: {exc}")

    # Step 2 — Brand verification (COMMENTED OUT)
    #try:
    #    from processors.brand_processor import process_all_fuel_stations as run_brand
    #    print("\n[PIPELINE] Step 2/5: Brand verification...")
    #    t = time.time()
    #    run_brand()
    #    print(f"[PIPELINE] Brand done in {time.time() - t:.1f}s")
    #except Exception as exc:
    #    print(f"[PIPELINE] Step 2 FAILED: {exc}")

    # Step 2 (NEW) — ACRA verification
    try:
        from processors.acra_processor import process_all_fuel_stations as run_acra
        print("\n[PIPELINE] Step 2/5: ACRA verification...")
        t = time.time()
        run_acra()
        print(f"[PIPELINE] ACRA done in {time.time() - t:.1f}s")
    except Exception as exc:
        print(f"[PIPELINE] Step 2 FAILED: {exc}")

    # Step 3 — OSM meta signals
    try:
        from processors.osm_meta_processor import process_all_fuel_stations as run_osm_meta
        print("\n[PIPELINE] Step 3/5: OSM metadata signals...")
        t = time.time()
        run_osm_meta()
        print(f"[PIPELINE] OSM meta done in {time.time() - t:.1f}s")
    except Exception as exc:
        print(f"[PIPELINE] Step 3 FAILED: {exc}")

    # Step 4 — Web signals
    try:
        from processors.web_processor import process_all_fuel_stations as run_web
        print("\n[PIPELINE] Step 4/5: Web & directory signals...")
        t = time.time()
        run_web()
        print(f"[PIPELINE] Web done in {time.time() - t:.1f}s")
    except Exception as exc:
        print(f"[PIPELINE] Step 4 FAILED: {exc}")

    # Step 5 — Classification
    try:
        from engine.classifier import FuelStationClassifier
        print("\n[PIPELINE] Step 5/5: Classification engine...")
        t = time.time()
        supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)
        clf = FuelStationClassifier(supabase_client)
        clf.classify_all_fuel_stations()
        print(f"[PIPELINE] Classification done in {time.time() - t:.1f}s")
    except Exception as exc:
        print(f"[PIPELINE] Step 5 FAILED: {exc}")

    elapsed = time.time() - start_total
    print(f"\n{'=' * 60}")
    print(f"  PIPELINE COMPLETE — Total time: {elapsed:.1f}s")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    run_pipeline()
