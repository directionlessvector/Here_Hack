"""
engine/classifier.py — Fuel station elimination engine
-------------------------------------------------------
Rule-based classification: ACTIVE / CLOSED / NEW / NON_EXISTENT.
No ML model — pure signal thresholds and process-of-elimination logic.

Results are written to: classification
"""

import os
import json
from typing import Optional
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

ALL_STATES = {"ACTIVE", "CLOSED", "NEW", "NON_EXISTENT"}

MAJOR_ROADS = {"motorway", "trunk", "primary", "secondary"}


# ---------------------------------------------------------------------------
# Default signal bundle (used when a signal table has no data)
# ---------------------------------------------------------------------------

DEFAULT_SIGNALS = {
    "distance_to_road_m":         9999.0,
    "road_type":                  "unknown",
    "nearest_station_distance_m": 9999.0,
    "geo_validity_score":         0.0,
    "company_match":              False,
    "brand_confidence":           0.0,
    "website_reachable":          False,
    "closure_keywords_found":     False,
    "directory_listed":           False,
    "web_activity_score":         0.5,
    "osm_freshness_score":        0.0,
    "tag_completeness_score":     0.0,
    "is_deleted_in_latest":       False,
    "has_brand_tag":              False,
    "cross_source_agreement":     0,
    "presence_in_osm":            True,
    "acra_status":                "unknown",
    "acra_match_found":           False,
    "acra_confidence":            0.0,
    "acra_match_level":           "none",
    "active_businesses_at_location": 0,
}


class FuelStationClassifier:
    """
    Classify each fuel station as ACTIVE, CLOSED, NEW, or NON_EXISTENT
    using a process-of-elimination approach over multiple signal sources.
    """

    def __init__(self, supabase_client: Client):
        self.supabase = supabase_client

    # -----------------------------------------------------------------------
    # Signal gathering
    # -----------------------------------------------------------------------

    def gather_signals(self, feature_id: int) -> dict:
        """
        Collect all signals for a feature from the latest signal_batch.

        Queries signal_batch, geo_signal, brand_signal, web_signal,
        and osm_meta_signal. Returns a flat dict with sensible defaults
        for any missing signals.

        Args:
            feature_id: poi_feature primary key.

        Returns:
            Flat dict of signal values.
        """
        signals = dict(DEFAULT_SIGNALS)

        # Latest batch
        batch_res = (
            self.supabase.table("signal_batch")
            .select("batch_id, cross_source_agreement, batch_status")
            .eq("feature_id", feature_id)
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )
        if not batch_res.data:
            return signals

        batch    = batch_res.data[0]
        batch_id = batch["batch_id"]
        signals["cross_source_agreement"] = batch.get("cross_source_agreement") or 0

        # Geo signal
        geo_res = (
            self.supabase.table("geo_signal")
            .select("*")
            .eq("batch_id", batch_id)
            .limit(1)
            .execute()
        )
        if geo_res.data:
            g = geo_res.data[0]
            signals["distance_to_road_m"]         = g.get("distance_to_road_m")         or 9999.0
            signals["road_type"]                  = g.get("nearest_road_type")           or "unknown"
            signals["nearest_station_distance_m"] = g.get("nearest_station_distance_m") or 9999.0
            signals["geo_validity_score"]         = g.get("geo_validity_score")          or 0.0

        # Brand signal
        brand_res = (
            self.supabase.table("brand_signal")
            .select("*")
            .eq("batch_id", batch_id)
            .limit(1)
            .execute()
        )
        if brand_res.data:
            b = brand_res.data[0]
            signals["company_match"]    = bool(b.get("company_match"))
            signals["brand_confidence"] = b.get("brand_confidence") or 0.0

        # Web signal
        web_res = (
            self.supabase.table("web_signal")
            .select("*")
            .eq("batch_id", batch_id)
            .limit(1)
            .execute()
        )
        if web_res.data:
            w = web_res.data[0]
            signals["website_reachable"]      = bool(w.get("website_reachable"))
            signals["closure_keywords_found"] = bool(w.get("closure_keywords_found"))
            signals["directory_listed"]       = bool(w.get("directory_listed"))
            signals["web_activity_score"]     = w.get("web_activity_score") or 0.5

        # OSM meta signal
        meta_res = (
            self.supabase.table("osm_meta_signal")
            .select("*")
            .eq("batch_id", batch_id)
            .limit(1)
            .execute()
        )
        if meta_res.data:
            m = meta_res.data[0]
            signals["osm_freshness_score"]    = m.get("osm_freshness_score")    or 0.0
            signals["tag_completeness_score"] = m.get("tag_completeness_score") or 0.0
            signals["is_deleted_in_latest"]   = bool(m.get("is_deleted_in_latest"))
            signals["has_brand_tag"]          = bool(m.get("has_brand_tag"))

        # ACRA signal
        acra_res = (
            self.supabase.table("acra_signal")
            .select("*")
            .eq("batch_id", batch_id)
            .limit(1)
            .execute()
        )
        if acra_res.data:
            a = acra_res.data[0]
            signals["acra_status"] = a.get("acra_status") or "unknown"
            signals["acra_match_found"] = bool(a.get("acra_match_found"))
            signals["acra_confidence"] = a.get("confidence") or 0.0
            signals["acra_match_level"] = a.get("match_level") or "none"
            signals["active_businesses_at_location"] = a.get("active_businesses_at_location") or 0

        return signals

    # -----------------------------------------------------------------------
    # Cross-source agreement
    # -----------------------------------------------------------------------

    def compute_cross_source_agreement(self, signals: dict) -> int:
        count = 0
        # ACRA signal (replaces company_match as primary)
        if signals.get("acra_match_found"):
            count += 1
        if signals.get("acra_status") == "active":
            count += 1
        # Geo signal
        if (signals.get("geo_validity_score") or 0) > 0.6:
            count += 1
        # OSM freshness
        if (signals.get("osm_freshness_score") or 0) > 0.5:
            count += 1
        # Tag quality
        if (signals.get("tag_completeness_score") or 0) > 0.3:
            count += 1
        return count

    # -----------------------------------------------------------------------
    # Elimination logic
    # -----------------------------------------------------------------------

    def eliminate_states(self, signals: dict) -> tuple:
        """
        Apply process-of-elimination rules to narrow down possible states.

        Starts with all 4 states and eliminates based on signal thresholds.

        Args:
            signals: Flat signal dict from gather_signals().

        Returns:
            (remaining_states: set, eliminated: list of {state, reason})
        """
        candidates = set(ALL_STATES)
        eliminated = []

        def elim(state: str, reason: str):
            if state in candidates:
                candidates.discard(state)
                eliminated.append({"state": state, "reason": reason})

        agreement    = signals.get("cross_source_agreement", 0)
        in_osm       = signals.get("presence_in_osm", True)
        road_type    = signals.get("road_type", "unknown")
        dist_road    = signals.get("distance_to_road_m", 9999)
        dist_station = signals.get("nearest_station_distance_m", 9999)

        if in_osm:
            # Already in OSM → can't be NEW
            elim("NEW", "Already present in OSM")

            # Don't eliminate ACTIVE if ACRA says active
            acra_says_active = signals.get("acra_status") == "active"
            
            if signals.get("is_deleted_in_latest"):
                elim("ACTIVE", "Marked as deleted in latest OSM snapshot")
            elif not acra_says_active and agreement < 2 and (signals.get("osm_freshness_score") or 0) < 0.3:
                elim("ACTIVE", "No ACRA confirmation, low agreement, stale OSM data")

            # Eliminate CLOSED if ACRA confirms active
            if acra_says_active and (signals.get("geo_validity_score") or 0) > 0.3:
                elim("CLOSED", "ACRA confirms active business + valid geo location")
            elif acra_says_active and signals.get("acra_match_level") in ("station_address", "station_proximity"):
                elim("CLOSED", "ACRA confirms active business at specific station address")

            # Eliminate ACTIVE if ACRA says closed
            if signals.get("acra_status") == "closed" and signals.get("acra_match_level") == "station_address":
                elim("ACTIVE", "ACRA registry shows closed business at this location")

            # Eliminate CLOSED if ACRA says active
            if (
                signals.get("acra_status") == "active"
                and (
                    signals.get("acra_match_level") in ("station_address", "station_proximity")
                    or (signals.get("acra_match_level") == "brand_parent" and (signals.get("geo_validity_score") or 0) > 0.6)
                )
            ):
                elim("CLOSED", "ACRA registry confirms active business")

            # Eliminate NON_EXISTENT if multiple sources confirm it
            if agreement >= 2 and (signals.get("geo_validity_score") or 0) > 0.5:
                elim("NON_EXISTENT", "Cross-source agreement >= 2 and valid geo position")

        else:
            # Not in OSM
            elim("ACTIVE", "Not present in OSM")
            elim("CLOSED", "Not present in OSM — never mapped")

            # Eliminate NEW if location is unsuitable
            if (
                dist_road > 200
                or road_type in ("residential", "service", "track")
                or dist_station < 100
                or (not signals.get("acra_match_found") and agreement < 2)
            ):
                elim("NEW", "Poor road location or duplicate proximity or insufficient signals")

            # Eliminate NON_EXISTENT if external signals strongly confirm it
            if (
                agreement >= 2
                and signals.get("acra_match_found")
                and dist_road < 100
            ):
                elim("NON_EXISTENT", "Company + geo confirm existence — should be NEW")

        return (candidates, eliminated)

    # -----------------------------------------------------------------------
    # Confidence scoring
    # -----------------------------------------------------------------------

    def compute_confidence(self, signals: dict, remaining: set) -> float:
        """
        Compute a weighted confidence score for the classification result.

        Weights:
          company_match        0.30
          geo_validity_score   0.25
          osm_freshness_score  0.20
          cross_source (0-5)   0.15  (normalised)
          web_activity_score   0.10

        Adjustments:
          +0.10 if exactly 1 state remains (certain)
          -0.15 if 3+ states remain (very uncertain)

        Returns:
            float in [0.0, 1.0]
        """
        score = (
            ( {"active": 1.0, "closed": 0.0, "unknown": 0.3}.get(signals.get("acra_status"), 0.3) ) * 0.35
            + (signals.get("geo_validity_score") or 0.0)         * 0.30
            + (signals.get("osm_freshness_score") or 0.0)        * 0.20
            + (signals.get("tag_completeness_score") or 0.0)     * 0.15
        )

        if len(remaining) == 1:
            score += 0.10
        elif len(remaining) >= 3:
            score -= 0.15

        return round(min(max(score, 0.0), 1.0), 4)

    # -----------------------------------------------------------------------
    # Single-station classification
    # -----------------------------------------------------------------------

    def classify(self, feature_id: int) -> dict:
        """
        Classify one fuel station.

        Steps:
        1. Gather all signals.
        2. Compute cross-source agreement.
        3. Eliminate impossible states.
        4. Pick final status.
        5. Compute confidence and risk flags.

        Returns:
            Full result dict for insertion into classification table.
        """
        signals  = self.gather_signals(feature_id)
        agreement = self.compute_cross_source_agreement(signals)
        signals["cross_source_agreement"] = agreement

        remaining, eliminated = self.eliminate_states(signals)

        requires_human = False

        if len(remaining) == 1:
            final_status = list(remaining)[0]
        elif len(remaining) == 0:
            # Contradiction — all eliminated
            final_status   = "NON_EXISTENT"
            requires_human = True
            eliminated.append({
                "state":  "ALL",
                "reason": "Contradiction — all states eliminated; defaulting to NON_EXISTENT",
            })
        elif len(remaining) == 2:
            # Pick based on stronger signal
            if "ACTIVE" in remaining:
                final_status = "ACTIVE" if agreement >= 2 else list(remaining - {"ACTIVE"})[0]
            else:
                final_status = sorted(remaining)[0]
        else:
            # 3+ states remain — very uncertain
            final_status   = "NON_EXISTENT"
            requires_human = True

        confidence = self.compute_confidence(signals, remaining)

        # Risk flags
        risk_flags = []
        if confidence < 0.65:
            risk_flags.append("low_confidence")
        if signals.get("company_match") and signals.get("closure_keywords_found"):
            risk_flags.append("conflicting_signals")
        if (signals.get("osm_freshness_score") or 0) < 0.2:
            risk_flags.append("stale_osm_data")
        if (signals.get("geo_validity_score") or 0) < 0.3:
            risk_flags.append("geo_mismatch")

        if confidence < 0.65 or len(risk_flags) > 1:
            requires_human = True

        # Get latest batch_id for FK
        batch_res = (
            self.supabase.table("signal_batch")
            .select("batch_id")
            .eq("feature_id", feature_id)
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )
        batch_id = batch_res.data[0]["batch_id"] if batch_res.data else None

        return {
            "feature_id":           feature_id,
            "batch_id":             batch_id,
            "final_status":         final_status,
            "confidence":           confidence,
            "eliminated_states":    eliminated,
            "surviving_states":     list(remaining),
            "key_signals": {
                "company_match":    signals.get("company_match"),
                "geo_score":        signals.get("geo_validity_score"),
                "freshness":        signals.get("osm_freshness_score"),
                "web_score":        signals.get("web_activity_score"),
                "agreement":        agreement,
            },
            "signal_summary":       signals,
            "risk_flags":           risk_flags,
            "requires_human_review": requires_human,
        }

    # -----------------------------------------------------------------------
    # Bulk classification
    # -----------------------------------------------------------------------

    def classify_all_fuel_stations(self) -> None:
        """
        Classify every fuel station in poi_feature and write results to DB.

        Prints a summary breakdown by final_status and confidence distribution.
        Progress is logged every 25 stations.
        """
        print("[classifier] Fetching fuel stations...")
        res = (
            self.supabase.table("poi_feature")
            .select("feature_id")
            .eq("category", "fuel")
            .execute()
        )
        stations = res.data or []
        total    = len(stations)
        print(f"[classifier] {total} stations to classify")

        counts     = {"ACTIVE": 0, "CLOSED": 0, "NEW": 0, "NON_EXISTENT": 0}
        human_flag = 0
        conf_bands = {"0.0-0.3": 0, "0.3-0.6": 0, "0.6-0.8": 0, "0.8-1.0": 0}
        errors     = 0

        for i, station in enumerate(stations, 1):
            feature_id = station["feature_id"]
            try:
                result = self.classify(feature_id)
                self.supabase.table("classification").insert(result).execute()

                counts[result["final_status"]] += 1
                if result["requires_human_review"]:
                    human_flag += 1

                c = result["confidence"]
                if c < 0.3:      conf_bands["0.0-0.3"] += 1
                elif c < 0.6:    conf_bands["0.3-0.6"] += 1
                elif c < 0.8:    conf_bands["0.6-0.8"] += 1
                else:            conf_bands["0.8-1.0"] += 1

            except Exception as exc:
                errors += 1
                print(f"[classifier] ERROR feature_id={feature_id}: {exc}")

            if i % 25 == 0:
                print(f"[classifier] Progress: {i}/{total}")

        print("\n[classifier] ===== RESULTS =====")
        for status, count in counts.items():
            print(f"  {status:<15}: {count}")
        print(f"  Needs human review : {human_flag}")
        print(f"  Errors             : {errors}")
        print("\n[classifier] Confidence distribution:")
        for band, count in conf_bands.items():
            print(f"  [{band}]: {count}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    clf      = FuelStationClassifier(supabase)
    clf.classify_all_fuel_stations()
