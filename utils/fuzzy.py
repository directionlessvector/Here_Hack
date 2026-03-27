import os
import json
import re
import time
from datetime import datetime, timezone
from typing import Union

from rapidfuzz import fuzz


def normalize(text: str) -> str:
    """
    Normalize text for fuzzy matching.
    """
    if not text:
        return ""

    # Lowercase
    result = text.lower()

    # Remove chars: . , - ( ) # & '
    result = re.sub(r"[.,\-()#&']", "", result)

    # Collapse multiple spaces to single space
    result = re.sub(r"\s+", " ", result).strip()

    # Strip suffixes (only at end, using endswith)
    suffixes = [
        "pte ltd", "private limited", "pvt ltd",
        "petrol station", "gas station", "fuel station", "service station",
        "station", "petrol kiosk"
    ]
    # Sort by length descending
    suffixes.sort(key=len, reverse=True)

    for suffix in suffixes:
        if result.endswith(suffix):
            remaining = result[:-len(suffix)].strip()
            if remaining:  # Only strip if remaining has length > 0
                result = remaining
                break

    # If result would be empty after suffix strip, keep the pre-stripped version
    if not result:
        result = text.lower()
        result = re.sub(r"[.,\-()#&']", "", result)
        result = re.sub(r"\s+", " ", result).strip()

    return result


def fuzzy_match_score(text1: str, text2: str, normalize_first: bool = True) -> float:
    """
    Compute fuzzy match score using rapidfuzz.
    """
    if normalize_first:
        s1 = normalize(text1)
        s2 = normalize(text2)
    else:
        s1 = text1
        s2 = text2

    if not s1 or not s2:
        return 0.0

    # Short string protection
    if len(s1) <= 4 or len(s2) <= 4:
        score = fuzz.ratio(s1, s2) / 100.0
    else:
        ratio = fuzz.ratio(s1, s2) / 100.0
        token_sort = fuzz.token_sort_ratio(s1, s2) / 100.0
        token_set = fuzz.token_set_ratio(s1, s2) / 100.0
        partial = fuzz.partial_ratio(s1, s2) / 100.0 * 0.85
        score = max(ratio, token_sort, token_set, partial)

    return round(max(0.0, min(1.0, score)), 4)


def name_match_score(name1: str, name2: str) -> float:
    """
    Convenience wrapper for name matching.
    """
    return fuzzy_match_score(name1, name2, normalize_first=True)


def best_match(query: str, candidates: list[str], threshold: float = 0.65) -> tuple[Union[str, None], float]:
    """
    Find best matching candidate.
    """
    if not query or not candidates:
        return (None, 0.0)

    query_norm = normalize(query)
    best_candidate = None
    best_score = 0.0

    for candidate in candidates:
        score = fuzzy_match_score(query_norm, normalize(candidate), normalize_first=False)
        if score > best_score:
            best_score = score
            best_candidate = candidate

    return (best_candidate, round(best_score, 4))
    return (None, 0.0)


SEED_BRANDS = {
    "shell": {
        "canonical": "Shell",
        "aliases": ["shell", "shell v-power", "shell select", "shell station"],
        "operator_aliases": ["shell", "shell eastern petroleum", "shell singapore"],
    },
    "esso": {
        "canonical": "Esso",
        "aliases": ["esso", "exxonmobil", "exxon", "mobil", "mobil1", "esso express"],
        "operator_aliases": ["esso", "esso singapore", "exxonmobil", "mobil1"],
    },
    "spc": {
        "canonical": "SPC",
        "aliases": ["spc", "singapore petroleum", "singapore petroleum company"],
        "operator_aliases": ["spc", "singapore petroleum company", "singapore petroleum"],
    },
    "caltex": {
        "canonical": "Caltex",
        "aliases": ["caltex", "chevron", "caltex with techron"],
        "operator_aliases": ["caltex", "chevron singapore", "caltex singapore"],
    },
    "sinopec": {
        "canonical": "Sinopec",
        "aliases": ["sinopec", "china petroleum", "sinopec singapore"],
        "operator_aliases": ["sinopec", "sinopec fuel oil"],
    },
}


class BrandRegistry:
    def __init__(self, persistence_path="cache/brand_registry.json"):
        self.persistence_path = persistence_path
        self.brands = dict(SEED_BRANDS)  # start with seeds
        self._load_discovered()          # merge persisted discoveries
        self._alias_index = self._build_alias_index()

    def _build_alias_index(self) -> dict:
        """Flat dict: every alias string → brand_key. For O(1) lookups."""
        index = {}
        for brand_key, info in self.brands.items():
            for alias in info.get("aliases", []):
                index[normalize(alias)] = brand_key
            for alias in info.get("operator_aliases", []):
                index[normalize(alias)] = brand_key
        return index

    def _load_discovered(self):
        """Load auto-discovered brands from JSON file if it exists."""
        try:
            if os.path.exists(self.persistence_path):
                with open(self.persistence_path) as f:
                    discovered = json.load(f)
                for key, info in discovered.items():
                    if key not in self.brands:
                        self.brands[key] = info
        except Exception:
            pass  # corrupt file — ignore, will be rebuilt

    def save(self):
        """Persist only DISCOVERED brands (not seeds) to JSON."""
        os.makedirs(os.path.dirname(self.persistence_path) or "cache", exist_ok=True)
        discovered = {k: v for k, v in self.brands.items() if v.get("discovered")}
        with open(self.persistence_path, "w") as f:
            json.dump(discovered, f, indent=2, default=str)

    @property
    def known_brands(self) -> list:
        return list(self.brands.keys())

    def get_all_aliases_flat(self) -> dict:
        return dict(self._alias_index)

    def resolve(self, text: str) -> tuple:
        """Resolve text to a brand. Returns (brand_key, confidence)."""
        norm = normalize(text)
        if not norm:
            return (None, 0.0)

        # Exact alias lookup first
        if norm in self._alias_index:
            return (self._alias_index[norm], 1.0)

        # Fuzzy fallback — but NOT for short strings (edge case #7)
        if len(norm) <= 4:
            return (None, 0.0)

        best_brand, best_score = None, 0.0
        for alias, brand_key in self._alias_index.items():
            score = fuzzy_match_score(norm, alias, normalize_first=False)
            if score > best_score:
                best_score = score
                best_brand = brand_key

        if best_score >= 0.7:
            return (best_brand, round(best_score, 4))
        return (None, 0.0)

    def discover_brand(self, name: str, source: str = "auto") -> str:
        """Register a new brand. Returns the brand key."""
        norm = normalize(name)
        if not norm:
            return ""

        # Edge case #10: check if already exists as an alias
        if norm in self._alias_index:
            return self._alias_index[norm]

        # Edge case #11: check fuzzy match against existing — might be a known brand variant
        if len(norm) > 4:
            for alias, brand_key in self._alias_index.items():
                if fuzzy_match_score(norm, alias, normalize_first=False) > 0.8:
                    # It's a variant of an existing brand — add as alias, don't create new
                    self.brands[brand_key]["aliases"].append(norm)
                    self._alias_index[norm] = brand_key
                    self.save()
                    print(f"[brand_registry] Added '{norm}' as alias for '{brand_key}'")
                    return brand_key

        # Genuinely new brand
        brand_key = norm.split()[0] if norm.split() else norm  # first word as key
        if brand_key in self.brands:
            brand_key = norm.replace(" ", "_")  # avoid collision

        self.brands[brand_key] = {
            "canonical": name,
            "aliases": [norm],
            "operator_aliases": [],
            "discovered": True,
            "discovered_at": datetime.now(timezone.utc).isoformat(),
            "discovery_source": source,
        }
        self._alias_index[norm] = brand_key
        self.save()
        print(f"[brand_registry] New brand discovered: '{name}' → key='{brand_key}'")
        return brand_key


def identify_brand(tags: dict, registry: BrandRegistry) -> tuple:
    """
    Identify brand from OSM tags using 6-step cascade.
    Returns: (brand_key, confidence, source_field)
    """
    alias_lookup = registry.get_all_aliases_flat()
    tags = tags or {}

    # STEP 1: Exact brand tag lookup (handles 147/199 stations)
    brand_raw = tags.get("brand")
    if brand_raw:
        norm = normalize(brand_raw)
        if norm in alias_lookup:
            return (alias_lookup[norm], 1.0, "brand")

    # STEP 2: Operator alias lookup (handles ~9 stations)
    operator_raw = tags.get("operator")
    if operator_raw:
        norm_op = normalize(operator_raw)
        # 2a: exact
        if norm_op in alias_lookup:
            return (alias_lookup[norm_op], 0.95, "operator")
        # 2b: fuzzy (skip for short strings)
        if len(norm_op) > 4:
            best_key, best_score = None, 0.0
            for alias, bkey in alias_lookup.items():
                score = fuzzy_match_score(norm_op, alias, normalize_first=False)
                if score > best_score:
                    best_score = score
                    best_key = bkey
            if best_score >= 0.7:
                return (best_key, round(best_score * 0.90, 4), "operator")

    # STEP 3: Token extraction from name (handles ~35 stations)
    name_raw = tags.get("name") or ""
    name_norm = normalize(name_raw)
    if name_norm:
        name_tokens = set(name_norm.split())
        best_match_key = None
        best_alias_len = 0
        for alias, bkey in alias_lookup.items():
            alias_tokens = set(alias.split())
            # ALL alias tokens must appear in name (edge case #1)
            if alias_tokens and alias_tokens.issubset(name_tokens):
                # Prefer longest matching alias
                if len(alias) > best_alias_len:
                    best_alias_len = len(alias)
                    best_match_key = bkey
        if best_match_key:
            return (best_match_key, 0.90, "name")

    # STEP 4: Fuzzy name match (catches typos, ~1-2 stations)
    if name_norm and len(name_norm) > 4:
        best_key, best_score = None, 0.0
        for alias, bkey in alias_lookup.items():
            score = fuzzy_match_score(name_norm, alias, normalize_first=False)
            if score > best_score:
                best_score = score
                best_key = bkey
        if best_score >= 0.8:
            return (best_key, round(best_score * 0.85, 4), "name")

    # STEP 5: Auto-discover unknown brand
    if name_norm and len(name_norm) >= 2:
        new_key = registry.discover_brand(name_raw, source="auto_from_osm")
        if new_key:
            return (new_key, 0.30, "name")

    # STEP 6: Unidentifiable
    return (None, 0.0, "")


def brand_match(tags: dict, known_brands: list) -> tuple:
    """
    Convenience wrapper around identify_brand.
    brand_processor.py calls this with (tags, KNOWN_BRANDS).
    Returns: (brand_key, confidence, source_tag)
    """
    registry = BrandRegistry()
    return identify_brand(tags, registry)


def score_active_from_osm(brand_result, signals):
    """
    STUB — this function should NOT be in fuzzy.py.
    Classification logic belongs in engine/classifier.py.
    This stub exists only to prevent brand_processor.py from crashing.
    Will be removed in Change 2.
    """
    company_match = signals.get("company_match", False)
    conf = 0.5
    if company_match:
        conf = 0.7
    return {
        "status": "UNCERTAIN",
        "confidence": round(conf, 4),
        "reason": "stub_pending_migration_to_classifier",
    }


if __name__ == "__main__":
    # Test normalize
    test_cases = [
        ("Shell", "shell"),
        ("Shell Gas Station", "shell"),
        ("S.P.C.", "spc"),
        ("Caltex Chong Pang", "caltex chong pang"),
        ("Singapore Petroleum Company", "singapore petroleum company"),
        ("Mobil1", "mobil1"),
        ("Esso Choa Chu Kang Way", "esso choa chu kang way"),
        (None, ""),
        ("Station", "station"),
    ]

    print("Testing normalize():")
    for input_val, expected in test_cases:
        result = normalize(input_val)
        status = "PASS" if result == expected else "FAIL"
        print(f"  {input_val!r} → {result!r} ({status})")

    # Test identify_brand
    registry = BrandRegistry()
    test_tags = [
        ({"brand": "Shell", "name": "Shell"}, ("shell", 1.0, "brand")),
        ({"brand": "Esso", "operator": "Esso", "name": "Esso"}, ("esso", 1.0, "brand")),
        ({"brand": "Caltex", "name": "Caltex"}, ("caltex", 1.0, "brand")),
        ({"brand": "SPC", "operator": "SPC", "name": "SPC Sumang"}, ("spc", 1.0, "brand")),
        ({"operator": "Mobil1", "name": "Esso"}, ("esso", 0.95, "operator")),
        ({"operator": "Singapore Petroleum Company", "name": "SPC"}, ("spc", 0.95, "operator")),
        ({"name": "SPC"}, ("spc", 0.90, "name")),
        ({"name": "Caltex Chong Pang"}, ("caltex", 0.90, "name")),
        ({"name": "Shell Bukit Timah"}, ("shell", 0.90, "name")),
        ({"name": "Shell Gas Station"}, ("shell", 0.90, "name")),
        ({"name": "Cnergy"}, ("cnergy", 0.30, "name")),
        ({"name": "Smart Energy"}, ("smart", 0.30, "name")),
        ({}, (None, 0.0, "")),
        ({"name": None}, (None, 0.0, "")),
    ]

    print("\nTesting identify_brand():")
    match_methods = {}
    for tags, expected in test_tags:
        result = identify_brand(tags, registry)
        status = "PASS" if result == expected else "FAIL"
        print(f"  {tags} → {result} ({status})")
        if result[2]:
            match_methods[result[2]] = match_methods.get(result[2], 0) + 1

    print("\nMatch method counts:")
    for method, count in match_methods.items():
        print(f"  {method}: {count}")

    # Test fuzzy_match_score short string protection
    print("\nTesting fuzzy_match_score short string protection:")
    tests = [
        ("spc", "spr", "<=0.65"),
        ("spc", "spc", "1.0"),
        ("shell", "shel", "~0.8"),
    ]
    for s1, s2, expected_desc in tests:
        score = fuzzy_match_score(s1, s2, normalize_first=False)
        print(f"  '{s1}' vs '{s2}': {score} ({expected_desc})")