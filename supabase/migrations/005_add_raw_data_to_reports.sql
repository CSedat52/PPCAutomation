-- 005_add_raw_data_to_reports.sql
-- Migration 001 raw_data JSONB kolonunu tanimliyor ama production'a uygulanmamis.
-- targeting_reports / search_term_reports / campaign_reports tablolarina ekler.

ALTER TABLE targeting_reports   ADD COLUMN IF NOT EXISTS raw_data JSONB;
ALTER TABLE search_term_reports ADD COLUMN IF NOT EXISTS raw_data JSONB;
ALTER TABLE campaign_reports    ADD COLUMN IF NOT EXISTS raw_data JSONB;
