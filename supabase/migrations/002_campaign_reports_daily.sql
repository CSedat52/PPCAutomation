-- ============================================
-- 002_campaign_reports_daily.sql
-- campaign_reports tablosuna report_date kolonu ekle
-- kpi_daily'yi campaign_reports'tan besle
-- ============================================

-- 1. campaign_reports tablosuna report_date kolonu ekle
-- (timeUnit=DAILY ile gelen "date" alani buraya yazilacak)
ALTER TABLE campaign_reports 
    ADD COLUMN IF NOT EXISTS report_date DATE;

-- Mevcut kayitlar icin: start_date'i report_date olarak ata
UPDATE campaign_reports 
    SET report_date = start_date::date 
    WHERE report_date IS NULL AND start_date IS NOT NULL;

-- Index: kpi_daily aggregation icin
CREATE INDEX IF NOT EXISTS idx_campaign_reports_daily_agg
    ON campaign_reports (hesap_key, marketplace, report_date)
    WHERE report_date IS NOT NULL;

-- 2. Dogrulama
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'campaign_reports' AND column_name = 'report_date'
    ) THEN
        RAISE NOTICE 'OK: campaign_reports.report_date kolonu mevcut';
    ELSE
        RAISE EXCEPTION 'HATA: report_date kolonu eklenemedi';
    END IF;
END $$;
