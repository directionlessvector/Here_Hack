-- ============================================================
-- Migration 001: Signal & Classification Tables
-- Run this in Supabase SQL Editor before running any processors
-- ============================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================
-- 1. signal_batch
-- Tracks each verification run for a POI feature.
-- One batch = one full signal collection cycle for one station.
-- ============================================================
CREATE TABLE IF NOT EXISTS signal_batch (
    batch_id               UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    feature_id             INT8 NOT NULL REFERENCES poi_feature(feature_id) ON DELETE CASCADE,
    triggered_by           TEXT NOT NULL DEFAULT 'scheduler'
                               CHECK (triggered_by IN ('scheduler', 'manual', 'recollect')),
    sources_queried        TEXT[],
    cross_source_agreement INT DEFAULT 0,
    neighbourhood_density  FLOAT8,
    batch_status           TEXT DEFAULT 'pending'
                               CHECK (batch_status IN ('pending', 'processing', 'completed', 'failed')),
    created_at             TIMESTAMPTZ DEFAULT NOW(),
    completed_at           TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_signal_batch_feature_id   ON signal_batch (feature_id);
CREATE INDEX IF NOT EXISTS idx_signal_batch_batch_status ON signal_batch (batch_status);

-- ============================================================
-- 2. geo_signal
-- Spatial validation results: road proximity, duplicate check,
-- building footprint, geocode comparison.
-- ============================================================
CREATE TABLE IF NOT EXISTS geo_signal (
    geo_id                      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    batch_id                    UUID NOT NULL REFERENCES signal_batch(batch_id) ON DELETE CASCADE,
    distance_to_road_m          FLOAT8,
    nearest_road_type           TEXT,
    nearest_road_osm_id         TEXT,
    nearest_station_distance_m  FLOAT8,
    nearest_station_osm_id      TEXT,
    building_footprint_area_m2  FLOAT8,
    land_use_type               TEXT,
    geocode_distance_m          FLOAT8,
    address_match_score         FLOAT8,
    geo_validity_score          FLOAT8,
    created_at                  TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- 3. brand_signal
-- Company/brand verification: fuzzy matching against known
-- fuel brand dealer locators and company databases.
-- ============================================================
CREATE TABLE IF NOT EXISTS brand_signal (
    brand_id                        UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    batch_id                        UUID NOT NULL REFERENCES signal_batch(batch_id) ON DELETE CASCADE,
    osm_brand_name                  TEXT,
    matched_brand_name              TEXT,
    company_match                   BOOLEAN DEFAULT FALSE,
    brand_confidence                FLOAT8,
    brand_source                    TEXT,
    dealer_locator_url              TEXT,
    distance_to_listed_station_m    FLOAT8,
    name_match_score                FLOAT8,
    tag_match_score                 FLOAT8,
    match_method                    TEXT
                                        CHECK (match_method IN (
                                            'tag_exact', 'tag_fuzzy',
                                            'name_fuzzy', 'all_brands_scan'
                                        )),
    created_at                      TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- 4. web_signal
-- Website reachability and directory listing results.
-- Checks for closure keywords and review activity.
-- ============================================================
CREATE TABLE IF NOT EXISTS web_signal (
    web_id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    batch_id                UUID NOT NULL REFERENCES signal_batch(batch_id) ON DELETE CASCADE,
    website_url             TEXT,
    website_status_code     INT,
    website_reachable       BOOLEAN,
    closure_keywords_found  BOOLEAN DEFAULT FALSE,
    closure_keywords_list   TEXT[],
    directory_listed        BOOLEAN DEFAULT FALSE,
    directory_source        TEXT,
    days_since_last_review  INT,
    review_count            INT DEFAULT 0,
    listing_active          BOOLEAN,
    web_activity_score      FLOAT8,
    created_at              TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- 5. osm_meta_signal
-- Signals derived from OSM data itself: freshness, tag
-- completeness, deletion status.
-- ============================================================
CREATE TABLE IF NOT EXISTS osm_meta_signal (
    meta_id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    batch_id                 UUID NOT NULL REFERENCES signal_batch(batch_id) ON DELETE CASCADE,
    osm_last_edit_days       INT,
    osm_freshness_score      FLOAT8,
    tag_completeness_score   FLOAT8,
    expected_tags_present    TEXT[],
    expected_tags_missing    TEXT[],
    is_deleted_in_latest     BOOLEAN,
    has_brand_tag            BOOLEAN,
    has_operator_tag         BOOLEAN,
    created_at               TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- 6. classification
-- Final POI classification result from the elimination engine.
-- ACTIVE / CLOSED / NEW / NON_EXISTENT
-- ============================================================
CREATE TABLE IF NOT EXISTS classification (
    classification_id     UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    feature_id            INT8 NOT NULL REFERENCES poi_feature(feature_id),
    batch_id              UUID NOT NULL REFERENCES signal_batch(batch_id),
    final_status          TEXT NOT NULL
                              CHECK (final_status IN ('ACTIVE', 'CLOSED', 'NEW', 'NON_EXISTENT')),
    confidence            FLOAT8 NOT NULL CHECK (confidence BETWEEN 0 AND 1),
    eliminated_states     JSONB,
    surviving_states      TEXT[],
    key_signals           JSONB,
    signal_summary        JSONB,
    risk_flags            TEXT[],
    requires_human_review BOOLEAN DEFAULT FALSE,
    classified_at         TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_classification_feature_id   ON classification (feature_id);
CREATE INDEX IF NOT EXISTS idx_classification_final_status ON classification (final_status);
CREATE INDEX IF NOT EXISTS idx_classification_classified_at ON classification (classified_at);
