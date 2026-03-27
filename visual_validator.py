"""
Visual POI Validator – Mapillary + YOLOv8 + OCR
================================================
Final visual confidence layer for the HERE GeoVerify pipeline.

Pipeline position:  ACRA → OSM → Overture → **Visual Validation (this)**

Role:
  - confidence booster
  - conflict resolver
  - last-mile validator

Uses Mapillary street-level imagery, YOLOv8 object detection,
and EasyOCR text extraction to determine physical existence.

IMPORTANT: This module must NEVER override strong upstream consensus blindly.
"""

import os
# Fix PyTorch 2.6+ weights_only default change that breaks YOLO model loading
os.environ["TORCH_FORCE_WEIGHTS_ONLY_LOAD"] = "0"

import re
import io
import math
import traceback
import requests
from PIL import Image
from dotenv import load_dotenv

load_dotenv()

MAPILLARY_TOKEN = os.getenv("MAPILLARY_ACCESS_TOKEN", "")

# ---------------------------------------------------------------------------
# Lazy-loaded heavy models (avoid import time cost)
# ---------------------------------------------------------------------------
_yolo_model = None
_ocr_reader = None


def _get_yolo():
    """Lazy-load YOLOv8 model on first call."""
    global _yolo_model
    if _yolo_model is None:
        # PyTorch 2.6+ defaults weights_only=True which breaks YOLO loading.
        # Patch torch.load to restore old behaviour before ultralytics calls it.
        import torch
        _orig_load = torch.load
        def _safe_load(*args, **kwargs):
            kwargs.setdefault("weights_only", False)
            return _orig_load(*args, **kwargs)
        torch.load = _safe_load

        from ultralytics import YOLO
        _yolo_model = YOLO("yolov8n.pt")  # nano model – fast & lightweight
        print("[visual] YOLOv8n model loaded")
    return _yolo_model


def _get_ocr():
    """Lazy-load EasyOCR reader on first call."""
    global _ocr_reader
    if _ocr_reader is None:
        import easyocr
        _ocr_reader = easyocr.Reader(["en"], gpu=False, verbose=False)
        print("[visual] EasyOCR reader loaded")
    return _ocr_reader


# ---------------------------------------------------------------------------
#  STEP 1: Fetch Mapillary Images
# ---------------------------------------------------------------------------

def fetch_mapillary_images(lat: float, lon: float, radius: int = 50, limit: int = 3) -> list:
    """
    Fetch 1-3 street-level images from Mapillary within `radius` metres.
    Returns list of dicts: {id, thumb_url, captured_at, compass_angle}.

    If no images available → caller should return "uncertain" with confidence ≤ 30.
    """
    if not MAPILLARY_TOKEN:
        print("[visual] No MAPILLARY_ACCESS_TOKEN set – skipping image fetch")
        return []

    # Mapillary v4 uses bbox search.  Convert radius → small bbox.
    # ~0.00045 degrees ≈ 50 m at equator
    delta = radius / 111_320.0
    bbox = f"{lon - delta},{lat - delta},{lon + delta},{lat + delta}"

    url = "https://graph.mapillary.com/images"
    params = {
        "access_token": MAPILLARY_TOKEN,
        "fields": "id,thumb_2048_url,thumb_1024_url,thumb_256_url,captured_at,compass_angle,geometry",
        "bbox": bbox,
        "limit": limit,
    }

    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json().get("data", [])
        results = []
        for img in data[:limit]:
            thumb = (
                img.get("thumb_2048_url")
                or img.get("thumb_1024_url")
                or img.get("thumb_256_url")
            )
            if not thumb:
                continue
            results.append({
                "id": img.get("id"),
                "thumb_url": thumb,
                "captured_at": img.get("captured_at"),
                "compass_angle": img.get("compass_angle"),
            })
        print(f"[visual] Mapillary: {len(results)} image(s) found within {radius}m")
        return results
    except Exception as e:
        print(f"[visual] Mapillary fetch error: {e}")
        return []


def _download_image(url: str) -> "Image.Image | None":
    """Download image from URL and return PIL Image."""
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        return Image.open(io.BytesIO(resp.content)).convert("RGB")
    except Exception:
        return None


# ---------------------------------------------------------------------------
#  STEP 2: Object Detection (YOLOv8)
# ---------------------------------------------------------------------------

# COCO class → semantic category mapping
ACTIVITY_CLASSES = {"car", "truck", "person", "motorcycle", "bus", "bicycle"}
CLOSURE_CLASSES = {"stop sign"}  # COCO has limited barrier classes
VEHICLE_CLASSES = {"car", "truck", "bus", "motorcycle"}
CONSTRUCTION_CLASSES = {"truck"}  # heavy vehicles may indicate construction

# Category-specific objects of interest
FUEL_OBJECTS = {"truck", "car", "motorcycle", "bus"}
RESTAURANT_OBJECTS = {"dining table", "chair", "cup", "bottle", "bowl", "fork",
                      "knife", "spoon", "wine glass", "bench", "person"}


def run_yolo(images: list, category: str) -> dict:
    """
    Run YOLOv8 on a list of PIL images.
    Returns aggregated detection results with semantic flags.

    Detects:
      - Common activity indicators: car, truck, person, motorcycle
      - Closure indicators: barrier, cone, fence, gate (limited in COCO)
      - Category-specific: fuel pumps/canopy vs tables/chairs/signboards
    """
    model = _get_yolo()
    all_labels = []
    all_confidences = []
    category_hits = 0

    target_objects = FUEL_OBJECTS if category == "fuel_station" else RESTAURANT_OBJECTS

    for pil_img in images:
        try:
            results = model(pil_img, verbose=False, conf=0.3)
            for r in results:
                for box in r.boxes:
                    cls_id = int(box.cls[0])
                    label = model.names.get(cls_id, "unknown")
                    conf = float(box.conf[0])
                    all_labels.append(label)
                    all_confidences.append(conf)
                    if label in target_objects:
                        category_hits += 1
        except Exception as e:
            print(f"[visual] YOLO error on image: {e}")

    # Compute semantic flags
    activity = any(l in ACTIVITY_CLASSES for l in all_labels)
    has_vehicles = any(l in VEHICLE_CLASSES for l in all_labels)
    has_people = "person" in all_labels
    closure_detected = any(l in CLOSURE_CLASSES for l in all_labels)

    return {
        "detected_objects": list(set(all_labels)),
        "total_detections": len(all_labels),
        "category_hits": category_hits,
        "activity": activity,
        "has_vehicles": has_vehicles,
        "has_people": has_people,
        "closure_detected": closure_detected,
        "avg_confidence": round(sum(all_confidences) / len(all_confidences), 3) if all_confidences else 0,
    }


# ---------------------------------------------------------------------------
#  STEP 3: OCR
# ---------------------------------------------------------------------------

# Keyword sets (all lowercased for matching)
CLOSED_KEYWORDS = {"closed", "temporarily closed", "not in operation", "permanently closed",
                   "shut down", "ceased", "no longer open"}
OPEN_KEYWORDS = {"open", "24 hours", "24hrs", "24hr", "now open", "welcome",
                 "open daily", "operating hours"}
CONSTRUCTION_KEYWORDS = {"under construction", "maintenance", "renovation",
                         "coming soon", "upgrading", "refurbishment"}
RELOCATION_KEYWORDS = {"relocated", "moved to", "new location", "we have moved",
                       "now at"}
RESTAURANT_BOOST_KEYWORDS = {"dine in", "dine-in", "takeaway", "take away", "menu",
                             "order here", "delivery", "reservations", "buffet"}
FUEL_BOOST_KEYWORDS = {"unleaded", "diesel", "premium", "92", "95", "98",
                       "pump", "self service", "full service"}


def run_ocr(images: list) -> dict:
    """
    Run EasyOCR on images.  Extract text and classify into keyword flags.
    Normalizes all text to lowercase before matching.
    """
    reader = _get_ocr()
    all_texts = []

    for pil_img in images:
        try:
            import numpy as np
            img_array = np.array(pil_img)
            results = reader.readtext(img_array, detail=0)
            all_texts.extend(results)
        except Exception as e:
            print(f"[visual] OCR error on image: {e}")

    full_text = " ".join(all_texts).lower().strip()

    def _has_any(keywords):
        return any(kw in full_text for kw in keywords)

    return {
        "raw_text": full_text[:500],  # Cap at 500 chars
        "closed_sign": _has_any(CLOSED_KEYWORDS),
        "open_sign": _has_any(OPEN_KEYWORDS),
        "construction": _has_any(CONSTRUCTION_KEYWORDS),
        "relocation": _has_any(RELOCATION_KEYWORDS),
        "restaurant_boost": _has_any(RESTAURANT_BOOST_KEYWORDS),
        "fuel_boost": _has_any(FUEL_BOOST_KEYWORDS),
    }


# ---------------------------------------------------------------------------
#  STEP 4 & 5: Signal Extraction + Visual Status Decision
# ---------------------------------------------------------------------------

def determine_status(yolo_results: dict, ocr_results: dict) -> str:
    """
    Determine visual status using strict priority order:
      relocated > under_construction > closed > open > uncertain

    This ensures the most critical signals take precedence.
    """
    if ocr_results["relocation"]:
        return "relocated"
    if ocr_results["construction"] or yolo_results.get("construction_detected", False):
        return "under_construction"
    if ocr_results["closed_sign"] and not ocr_results["open_sign"]:
        return "closed"
    if ocr_results["open_sign"] or yolo_results["activity"]:
        return "open"
    return "uncertain"


# ---------------------------------------------------------------------------
#  STEP 6: Visual Evidence Score
# ---------------------------------------------------------------------------

def compute_visual_evidence_score(
    yolo_results: dict,
    ocr_results: dict,
    status: str,
    num_images: int,
    category: str,
) -> int:
    """
    Compute visual evidence score 0-100.

    Positive signals:
      open_sign → +35
      activity  → +25
      no closure signals → +15
      strong object match (fuel pump / tables) → +15
      OCR keyword match → +10

    Penalties:
      single image → -10
      conflicting signals → -20
    """
    score = 0

    # Open sign → +35
    if ocr_results["open_sign"]:
        score += 35

    # Activity detected (vehicles/people) → +25
    if yolo_results["activity"]:
        score += 25

    # No closure signals → +15
    if not ocr_results["closed_sign"] and not yolo_results["closure_detected"]:
        score += 15

    # Category-specific strong object match → +15
    if yolo_results["category_hits"] >= 2:
        score += 15
    elif yolo_results["category_hits"] >= 1:
        score += 8

    # OCR keyword match (category-specific boost) → +10
    if category == "restaurant" and ocr_results["restaurant_boost"]:
        score += 10
    elif category == "fuel_station" and ocr_results["fuel_boost"]:
        score += 10

    # ── Penalties ──
    if num_images == 1:
        score -= 10  # Single image penalty

    # Conflicting signals penalty
    if ocr_results["open_sign"] and ocr_results["closed_sign"]:
        score -= 20

    # Closure/relocation hard cap
    if status in ("closed", "relocated"):
        score = min(score, 20)
    elif status == "under_construction":
        score = min(score, 35)

    return max(0, min(100, score))


# ---------------------------------------------------------------------------
#  STEP 7: Upstream Integration
# ---------------------------------------------------------------------------

def compute_confidence(
    visual_score: int,
    status: str,
    upstream: dict,
) -> int:
    """
    Compute final confidence (0-100) integrating visual + upstream signals.

    Rules:
      - IF all upstream signals TRUE → visual should NOT downgrade heavily
        unless strong negative evidence
      - IF upstream signals FALSE → visual can only upgrade with strong
        evidence (high activity + open signs)
    """
    upstream_count = sum([
        upstream.get("acra_exists", False),
        upstream.get("osm_exists", False),
        upstream.get("overture_exists", False),
        upstream.get("brand_match", False),
    ])

    # Base confidence from visual score
    confidence = visual_score

    # ── Upstream agreement adjustments ──
    if upstream_count >= 3:
        # Strong upstream consensus — visual should NOT heavily downgrade
        if status == "open":
            confidence = max(confidence, 65)
        elif status == "uncertain":
            confidence = max(confidence, 50)
        elif status == "closed":
            # Only downgrade if visual evidence is strong
            confidence = min(confidence + 20, 45)
    elif upstream_count >= 2:
        if status == "open":
            confidence = max(confidence, 50)
        elif status == "uncertain":
            confidence = max(confidence, 35)
    elif upstream_count <= 1:
        # Weak upstream — visual can only upgrade with strong evidence
        if status == "open" and visual_score >= 60:
            confidence = max(confidence, 55)
        elif status != "open":
            confidence = min(confidence, 40)

    return max(0, min(100, confidence))


# ---------------------------------------------------------------------------
#  STEP 8: Final Decision Hint
# ---------------------------------------------------------------------------

def decision_hint(confidence: int, status: str, upstream: dict) -> str:
    """
    Map visual + upstream context to a decision hint string.

    strong_positive → strong visual + upstream agreement
    weak_positive   → visual supports but weak signals
    neutral         → unclear visual evidence
    weak_negative   → weak closure signals
    strong_negative → strong closure / relocation signals
    """
    upstream_count = sum([
        upstream.get("acra_exists", False),
        upstream.get("osm_exists", False),
        upstream.get("overture_exists", False),
        upstream.get("brand_match", False),
    ])

    if status == "open" and confidence >= 65 and upstream_count >= 2:
        return "strong_positive"
    elif status == "open" and confidence >= 45:
        return "weak_positive"
    elif status in ("closed", "relocated") and confidence <= 25:
        return "strong_negative"
    elif status in ("closed", "relocated", "under_construction"):
        return "weak_negative"
    else:
        return "neutral"


# ---------------------------------------------------------------------------
#  STEP 9: Main Entry Point
# ---------------------------------------------------------------------------

def validate_poi_visual(input_payload: dict) -> dict:
    """
    Main visual validation function.

    Input:
        {
            "latitude": float,
            "longitude": float,
            "category": "fuel_station" | "restaurant",
            "poi_name": string,
            "upstream_signals": {
                "acra_exists": true/false,
                "osm_exists": true/false,
                "overture_exists": true/false,
                "brand_match": true/false
            }
        }

    Output:
        {
            "status": "open | closed | under_construction | relocated | uncertain",
            "confidence": 0-100,
            "visual_evidence_score": 0-100,
            "signals": {
                "open_sign": true/false,
                "closed_sign": true/false,
                "construction": true/false,
                "relocation": true/false,
                "activity": true/false,
                "detected_objects": [],
                "ocr_text": ""
            },
            "final_decision_hint": "strong_positive | weak_positive | neutral | weak_negative | strong_negative"
        }
    """
    lat = input_payload.get("latitude")
    lon = input_payload.get("longitude")
    category = input_payload.get("category", "fuel_station")
    poi_name = input_payload.get("poi_name", "")
    upstream = input_payload.get("upstream_signals", {})

    # Fallback: no coordinates → uncertain
    if lat is None or lon is None:
        return _uncertain_result(upstream, reason="no coordinates provided")

    try:
        # ── Step 1: Fetch Mapillary images ──
        print(f"[visual] Step 1: Fetching Mapillary images for ({lat}, {lon})...")
        image_meta = fetch_mapillary_images(lat, lon, radius=50, limit=3)

        if not image_meta:
            return _uncertain_result(upstream, reason="no Mapillary imagery available")

        # Download actual images
        pil_images = []
        for meta in image_meta:
            img = _download_image(meta["thumb_url"])
            if img:
                pil_images.append(img)

        if not pil_images:
            return _uncertain_result(upstream, reason="failed to download images")

        num_images = len(pil_images)
        print(f"[visual] Step 1 complete: {num_images} image(s) downloaded")

        # ── Step 2: YOLO object detection ──
        print(f"[visual] Step 2: Running YOLOv8 detection ({category})...")
        yolo_results = run_yolo(pil_images, category)
        print(f"[visual] Step 2 complete: {yolo_results['total_detections']} objects detected")

        # ── Step 3: OCR text extraction ──
        print(f"[visual] Step 3: Running OCR...")
        try:
            ocr_results = run_ocr(pil_images)
        except Exception as ocr_err:
            # Step 9 error handling: detection failure → fallback to OCR
            # BUT if OCR itself fails, use empty OCR results
            print(f"[visual] OCR failed, using empty results: {ocr_err}")
            ocr_results = {
                "raw_text": "",
                "closed_sign": False, "open_sign": False,
                "construction": False, "relocation": False,
                "restaurant_boost": False, "fuel_boost": False,
            }
        print(f"[visual] Step 3 complete: {len(ocr_results['raw_text'])} chars extracted")

        # ── Step 4 & 5: Signal extraction + status determination ──
        print("[visual] Steps 4-5: Determining status...")
        status = determine_status(yolo_results, ocr_results)
        print(f"[visual] Steps 4-5 complete: status = {status}")

        # ── Step 6: Visual evidence score ──
        visual_score = compute_visual_evidence_score(
            yolo_results, ocr_results, status, num_images, category
        )
        print(f"[visual] Step 6: visual_evidence_score = {visual_score}")

        # ── Step 7: Confidence with upstream integration ──
        confidence = compute_confidence(visual_score, status, upstream)
        print(f"[visual] Step 7: confidence = {confidence}")

        # ── Step 8: Decision hint ──
        hint = decision_hint(confidence, status, upstream)
        print(f"[visual] Step 8: final_decision_hint = {hint}")

        return {
            "status": status,
            "confidence": confidence,
            "visual_evidence_score": visual_score,
            "images_analysed": num_images,
            "signals": {
                "open_sign": ocr_results["open_sign"],
                "closed_sign": ocr_results["closed_sign"],
                "construction": ocr_results["construction"],
                "relocation": ocr_results["relocation"],
                "activity": yolo_results["activity"],
                "detected_objects": yolo_results["detected_objects"],
                "ocr_text": ocr_results["raw_text"],
            },
            "final_decision_hint": hint,
        }

    except Exception as e:
        # Step 9: API / detection failure → return uncertain, no crash
        print(f"[visual] Pipeline error: {traceback.format_exc()}")
        return _uncertain_result(upstream, reason=f"pipeline error: {str(e)}")


def _uncertain_result(upstream: dict, reason: str = "") -> dict:
    """Return a safe uncertain result when validation can't proceed.
    Confidence ≤ 30 as per spec, but gives some credit if upstream is strong."""
    upstream_count = sum([
        upstream.get("acra_exists", False),
        upstream.get("osm_exists", False),
        upstream.get("overture_exists", False),
        upstream.get("brand_match", False),
    ])
    # Even uncertain, give some confidence if upstream is strong (max 30)
    confidence = min(30, upstream_count * 10)

    return {
        "status": "uncertain",
        "confidence": confidence,
        "visual_evidence_score": 0,
        "images_analysed": 0,
        "signals": {
            "open_sign": False,
            "closed_sign": False,
            "construction": False,
            "relocation": False,
            "activity": False,
            "detected_objects": [],
            "ocr_text": "",
        },
        "final_decision_hint": "neutral",
        "reason": reason,
    }


# ---------------------------------------------------------------------------
#  CLI test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import json
    test = validate_poi_visual({
        "latitude": 1.33942,
        "longitude": 103.77661,
        "category": "fuel_station",
        "poi_name": "Shell Bukit Timah",
        "upstream_signals": {
            "acra_exists": True,
            "osm_exists": True,
            "overture_exists": True,
            "brand_match": True,
        }
    })
    print(json.dumps(test, indent=2, ensure_ascii=False))
