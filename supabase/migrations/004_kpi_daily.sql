-- Migration: kpi_daily tablosu
-- Genel bakis sayfasi icin gunluk KPI ozet tablosu
-- targeting_reports'tan aggregate edilir, cok daha hizli okuma saglar

CREATE TABLE IF NOT EXISTS kpi_daily (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  hesap_key TEXT NOT NULL,
  marketplace TEXT NOT NULL,
  report_date DATE NOT NULL,
  spend NUMERIC(12,4) DEFAULT 0,
  sales NUMERIC(12,4) DEFAULT 0,
  clicks INTEGER DEFAULT 0,
  impressions INTEGER DEFAULT 0,
  orders INTEGER DEFAULT 0,
  acos NUMERIC(8,2) DEFAULT 0,
  roas NUMERIC(8,2) DEFAULT 0,
  ctr NUMERIC(8,4) DEFAULT 0,
  cvr NUMERIC(8,4) DEFAULT 0,
  campaign_count INTEGER DEFAULT 0,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(hesap_key, marketplace, report_date),
  FOREIGN KEY (hesap_key, marketplace) REFERENCES marketplaces(hesap_key, marketplace)
);

-- Hizli sorgulama icin indeksler
CREATE INDEX IF NOT EXISTS idx_kpi_daily_date ON kpi_daily(report_date);
CREATE INDEX IF NOT EXISTS idx_kpi_daily_hesap ON kpi_daily(hesap_key, marketplace);

-- RLS politikasi (anon okuma icin)
ALTER TABLE kpi_daily ENABLE ROW LEVEL SECURITY;

CREATE POLICY "kpi_daily_select_all" ON kpi_daily
  FOR SELECT USING (true);

CREATE POLICY "kpi_daily_insert_all" ON kpi_daily
  FOR INSERT WITH CHECK (true);

CREATE POLICY "kpi_daily_update_all" ON kpi_daily
  FOR UPDATE USING (true);
