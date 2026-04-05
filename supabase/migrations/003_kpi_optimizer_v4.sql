-- ============================================
-- Agent 4 KPI Optimizer v4 — Schema Updates
-- ============================================

-- 1. decision_history: hedef_acos ve targeting_type kolonlari
ALTER TABLE decision_history ADD COLUMN IF NOT EXISTS hedef_acos NUMERIC(8,2);
ALTER TABLE decision_history ADD COLUMN IF NOT EXISTS targeting_type TEXT;

-- 2. bid_recommendations: asin kolonu (yoksa ekle)
ALTER TABLE bid_recommendations ADD COLUMN IF NOT EXISTS asin TEXT;

-- 3. Regresyon sonuclari tablosu (ASIN x targeting_type bazli)
CREATE TABLE IF NOT EXISTS bid_param_regression (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hesap_key TEXT NOT NULL,
    marketplace TEXT NOT NULL,
    asin TEXT NOT NULL,
    targeting_type TEXT NOT NULL,             -- 'KEYWORD' veya 'PRODUCT_TARGET'
    hedef_acos NUMERIC(8,2),
    alpha_fit NUMERIC(10,4),
    beta_fit_pp NUMERIC(10,2),
    r_squared NUMERIC(8,4),
    alpha_std_err NUMERIC(10,4),
    beta_std_err NUMERIC(10,2),
    fit_basarili BOOLEAN DEFAULT false,
    veri_noktasi INTEGER DEFAULT 0,
    hassasiyet_mevcut NUMERIC(8,4),
    max_degisim_mevcut NUMERIC(8,4),
    parametre_kaynagi TEXT,
    ort_gap_closure NUMERIC(8,4),
    ort_acos_once NUMERIC(8,2),
    ort_acos_sonra NUMERIC(8,2),
    analysis_date DATE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(hesap_key, marketplace, asin, targeting_type, analysis_date),
    FOREIGN KEY (hesap_key, marketplace) REFERENCES marketplaces(hesap_key, marketplace)
);

-- 4. Regresyon veri noktalari (detay tablosu)
CREATE TABLE IF NOT EXISTS bid_param_regression_data (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    regression_id UUID REFERENCES bid_param_regression(id) ON DELETE CASCADE,
    hesap_key TEXT NOT NULL,
    marketplace TEXT NOT NULL,
    asin TEXT NOT NULL,
    targeting_type TEXT NOT NULL,
    targeting_id TEXT,
    decision_date DATE,
    bid_degisim NUMERIC(10,4),
    acos_once NUMERIC(8,2),
    acos_sonra NUMERIC(8,2),
    acos_degisim NUMERIC(8,2),
    gap_closure NUMERIC(8,4),
    spend_before NUMERIC(12,4),
    spend_after NUMERIC(12,4),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 5. Indeksler
CREATE INDEX IF NOT EXISTS idx_regression_lookup
    ON bid_param_regression(hesap_key, marketplace, asin, targeting_type);
CREATE INDEX IF NOT EXISTS idx_regression_date
    ON bid_param_regression(analysis_date DESC);
CREATE INDEX IF NOT EXISTS idx_regression_data_parent
    ON bid_param_regression_data(regression_id);
CREATE INDEX IF NOT EXISTS idx_decision_history_targeting_type
    ON decision_history(targeting_type);

-- 6. RLS
ALTER TABLE bid_param_regression ENABLE ROW LEVEL SECURITY;
ALTER TABLE bid_param_regression_data ENABLE ROW LEVEL SECURITY;
CREATE POLICY "full_access" ON bid_param_regression FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "full_access" ON bid_param_regression_data FOR ALL USING (true) WITH CHECK (true);
