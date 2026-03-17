-- ============================================
-- Amazon PPC Automation — Supabase Schema v1
-- ============================================
-- Kurallar:
--   Entity tablolari: UPSERT (son durum)
--   Rapor tablolari: INSERT (tarihli, birikmeli, silinmez)
--   Retention 90 gun: SADECE error_logs ve execution_items
--   Config tablolari: DB'den yonetilecek (dashboard)
-- ============================================

-- ==========================================
-- 0. EXTENSIONS
-- ==========================================
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ==========================================
-- 1. TEMEL TABLOLAR
-- ==========================================

-- Hesaplar (credential HARIC)
CREATE TABLE accounts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hesap_key TEXT NOT NULL UNIQUE,          -- vigowood_na, vigowood_eu, qmmp_na
    hesap_adi TEXT NOT NULL,
    seller_name TEXT,
    account_id TEXT,                          -- Amazon account ID
    api_endpoint TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Hesap-Marketplace eslesmeleri
CREATE TABLE marketplaces (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hesap_key TEXT NOT NULL REFERENCES accounts(hesap_key) ON DELETE CASCADE,
    marketplace TEXT NOT NULL,               -- US, CA, UK, DE, ...
    profile_id TEXT,
    marketplace_id TEXT,
    currency TEXT DEFAULT '$',
    timezone TEXT,
    aktif BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(hesap_key, marketplace)
);

-- Pipeline calisma gecmisi
CREATE TABLE pipeline_runs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    session_id TEXT NOT NULL,
    hesap_key TEXT NOT NULL,
    marketplace TEXT NOT NULL,
    pipeline_date DATE NOT NULL,
    status TEXT DEFAULT 'RUNNING',           -- RUNNING, COMPLETED, FAILED, PARTIAL
    started_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    agent1_status TEXT,
    agent2_status TEXT,
    agent3_status TEXT,
    agent4_status TEXT,
    summary JSONB,
    FOREIGN KEY (hesap_key, marketplace) REFERENCES marketplaces(hesap_key, marketplace)
);

-- ==========================================
-- 2. CONFIG TABLOLARI (Dashboard'dan yonetilecek)
-- ==========================================

-- Settings (her marketplace icin)
CREATE TABLE settings (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hesap_key TEXT NOT NULL,
    marketplace TEXT NOT NULL,
    genel_ayarlar JSONB NOT NULL DEFAULT '{}',
    esik_degerleri JSONB NOT NULL DEFAULT '{}',
    asin_hedefleri JSONB NOT NULL DEFAULT '{}',
    segmentasyon_kurallari JSONB NOT NULL DEFAULT '{}',
    agent3_ayarlari JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(hesap_key, marketplace),
    FOREIGN KEY (hesap_key, marketplace) REFERENCES marketplaces(hesap_key, marketplace)
);

-- Bid fonksiyon parametreleri (her marketplace icin)
CREATE TABLE bid_functions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hesap_key TEXT NOT NULL,
    marketplace TEXT NOT NULL,
    tanh_formulu JSONB NOT NULL DEFAULT '{}',
    segment_parametreleri JSONB NOT NULL DEFAULT '{}',
    genel_limitler JSONB NOT NULL DEFAULT '{}',
    asin_parametreleri JSONB NOT NULL DEFAULT '{}',
    ogrenme_gecmisi JSONB NOT NULL DEFAULT '{"degisiklikler": []}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(hesap_key, marketplace),
    FOREIGN KEY (hesap_key, marketplace) REFERENCES marketplaces(hesap_key, marketplace)
);

-- ==========================================
-- 3. AGENT 1 — ENTITY TABLOLARI (UPSERT, son durum)
-- ==========================================

-- Portfolyolar
CREATE TABLE portfolios (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hesap_key TEXT NOT NULL,
    marketplace TEXT NOT NULL,
    portfolio_id TEXT NOT NULL,
    name TEXT,
    state TEXT,
    in_budget BOOLEAN,
    budget JSONB,
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(hesap_key, marketplace, portfolio_id),
    FOREIGN KEY (hesap_key, marketplace) REFERENCES marketplaces(hesap_key, marketplace)
);

-- Kampanyalar (SP + SB + SD tek tablo)
CREATE TABLE campaigns (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hesap_key TEXT NOT NULL,
    marketplace TEXT NOT NULL,
    ad_type TEXT NOT NULL,                   -- SP, SB, SD
    campaign_id TEXT NOT NULL,
    name TEXT,
    state TEXT,
    portfolio_id TEXT,
    start_date TEXT,
    targeting_type TEXT,                     -- MANUAL, AUTO (SP)
    budget NUMERIC(12,2),
    budget_type TEXT,
    cost_type TEXT,
    dynamic_bidding JSONB,                   -- SP bidding strategy
    bid_optimization BOOLEAN,               -- SB
    bid_optimization_strategy TEXT,          -- SB
    goal TEXT,                               -- SB
    tactic TEXT,                             -- SD
    delivery_profile TEXT,                   -- SD
    raw_data JSONB,                          -- Tam API response (ek alanlar icin)
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(hesap_key, marketplace, ad_type, campaign_id),
    FOREIGN KEY (hesap_key, marketplace) REFERENCES marketplaces(hesap_key, marketplace)
);

-- Ad Groups (SP + SB)
CREATE TABLE ad_groups (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hesap_key TEXT NOT NULL,
    marketplace TEXT NOT NULL,
    ad_type TEXT NOT NULL,                   -- SP, SB
    ad_group_id TEXT NOT NULL,
    campaign_id TEXT NOT NULL,
    name TEXT,
    state TEXT,
    default_bid NUMERIC(10,4),
    raw_data JSONB,
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(hesap_key, marketplace, ad_type, ad_group_id),
    FOREIGN KEY (hesap_key, marketplace) REFERENCES marketplaces(hesap_key, marketplace)
);

-- Keywords (SP + SB)
CREATE TABLE keywords (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hesap_key TEXT NOT NULL,
    marketplace TEXT NOT NULL,
    ad_type TEXT NOT NULL,                   -- SP, SB
    keyword_id TEXT NOT NULL,
    campaign_id TEXT NOT NULL,
    ad_group_id TEXT NOT NULL,
    keyword_text TEXT,
    match_type TEXT,                         -- EXACT, PHRASE, BROAD
    state TEXT,
    bid NUMERIC(10,4),
    raw_data JSONB,
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(hesap_key, marketplace, ad_type, keyword_id),
    FOREIGN KEY (hesap_key, marketplace) REFERENCES marketplaces(hesap_key, marketplace)
);

-- Targets (SP + SB + SD)
CREATE TABLE targets (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hesap_key TEXT NOT NULL,
    marketplace TEXT NOT NULL,
    ad_type TEXT NOT NULL,                   -- SP, SB, SD
    target_id TEXT NOT NULL,
    campaign_id TEXT NOT NULL,
    ad_group_id TEXT NOT NULL,
    bid NUMERIC(10,4),
    state TEXT,
    expression_type TEXT,                    -- AUTO, MANUAL
    expression JSONB,
    resolved_expression JSONB,
    raw_data JSONB,
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(hesap_key, marketplace, ad_type, target_id),
    FOREIGN KEY (hesap_key, marketplace) REFERENCES marketplaces(hesap_key, marketplace)
);

-- Product Ads (SP only)
CREATE TABLE product_ads (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hesap_key TEXT NOT NULL,
    marketplace TEXT NOT NULL,
    ad_id TEXT NOT NULL,
    campaign_id TEXT NOT NULL,
    ad_group_id TEXT NOT NULL,
    asin TEXT,
    sku TEXT,
    state TEXT,
    raw_data JSONB,
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(hesap_key, marketplace, ad_id),
    FOREIGN KEY (hesap_key, marketplace) REFERENCES marketplaces(hesap_key, marketplace)
);

-- Negative Keywords (SP + SB)
CREATE TABLE negative_keywords (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hesap_key TEXT NOT NULL,
    marketplace TEXT NOT NULL,
    ad_type TEXT NOT NULL,                   -- SP, SB
    keyword_id TEXT NOT NULL,
    campaign_id TEXT NOT NULL,
    ad_group_id TEXT,                        -- NULL for campaign-level negatives
    keyword_text TEXT,
    match_type TEXT,                         -- NEGATIVE_EXACT, NEGATIVE_PHRASE
    state TEXT,
    scope TEXT DEFAULT 'AD_GROUP',           -- AD_GROUP, CAMPAIGN
    raw_data JSONB,
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(hesap_key, marketplace, ad_type, keyword_id, scope),
    FOREIGN KEY (hesap_key, marketplace) REFERENCES marketplaces(hesap_key, marketplace)
);

-- Negative Targets (SP)
CREATE TABLE negative_targets (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hesap_key TEXT NOT NULL,
    marketplace TEXT NOT NULL,
    target_id TEXT NOT NULL,
    campaign_id TEXT NOT NULL,
    ad_group_id TEXT,
    expression JSONB,
    state TEXT,
    raw_data JSONB,
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(hesap_key, marketplace, target_id),
    FOREIGN KEY (hesap_key, marketplace) REFERENCES marketplaces(hesap_key, marketplace)
);

-- ==========================================
-- 4. AGENT 1 — RAPOR TABLOLARI (INSERT, birikmeli)
-- ==========================================

-- Targeting raporlari (SP+SB+SD, 14d/30d)
CREATE TABLE targeting_reports (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hesap_key TEXT NOT NULL,
    marketplace TEXT NOT NULL,
    ad_type TEXT NOT NULL,                   -- SP, SB, SD
    report_period TEXT NOT NULL,             -- 14d, 30d
    collection_date DATE NOT NULL,           -- Veri toplama tarihi
    start_date DATE,                         -- Rapor baslangic
    end_date DATE,                           -- Rapor bitis
    campaign_id TEXT,
    campaign_name TEXT,
    ad_group_id TEXT,
    ad_group_name TEXT,
    keyword_id TEXT,
    target_id TEXT,
    keyword_text TEXT,
    targeting TEXT,                           -- targeting expression
    match_type TEXT,
    keyword_bid NUMERIC(10,4),
    ad_keyword_status TEXT,
    impressions INTEGER DEFAULT 0,
    clicks INTEGER DEFAULT 0,
    cost NUMERIC(12,4) DEFAULT 0,
    cost_per_click NUMERIC(10,4),
    sales NUMERIC(12,4) DEFAULT 0,
    purchases INTEGER DEFAULT 0,
    units_sold INTEGER DEFAULT 0,
    acos NUMERIC(12,4),
    roas NUMERIC(12,4),
    ctr NUMERIC(14,6),
    cvr NUMERIC(10,6),
    -- SB/SD ek alanlar
    new_to_brand_purchases INTEGER,
    new_to_brand_sales NUMERIC(12,4),
    add_to_cart INTEGER,
    add_to_cart_clicks INTEGER,
    raw_data JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    FOREIGN KEY (hesap_key, marketplace) REFERENCES marketplaces(hesap_key, marketplace)
);

-- Ayni tarih+ad_type+period+entity icin duplicate onleme
CREATE UNIQUE INDEX idx_targeting_reports_unique
    ON targeting_reports(hesap_key, marketplace, ad_type, report_period, collection_date,
                         campaign_id, COALESCE(ad_group_id,''), COALESCE(keyword_id,''), COALESCE(target_id,''));

-- Search term raporlari (SP+SB, 30d)
CREATE TABLE search_term_reports (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hesap_key TEXT NOT NULL,
    marketplace TEXT NOT NULL,
    ad_type TEXT NOT NULL,                   -- SP, SB
    collection_date DATE NOT NULL,
    campaign_id TEXT,
    campaign_name TEXT,
    ad_group_id TEXT,
    ad_group_name TEXT,
    keyword_id TEXT,
    keyword_text TEXT,
    search_term TEXT,
    targeting TEXT,
    match_type TEXT,
    impressions INTEGER DEFAULT 0,
    clicks INTEGER DEFAULT 0,
    cost NUMERIC(12,4) DEFAULT 0,
    cost_per_click NUMERIC(10,4),
    sales NUMERIC(12,4) DEFAULT 0,
    purchases INTEGER DEFAULT 0,
    units_sold INTEGER DEFAULT 0,
    acos NUMERIC(12,4),
    roas NUMERIC(12,4),
    ctr NUMERIC(14,6),
    raw_data JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    FOREIGN KEY (hesap_key, marketplace) REFERENCES marketplaces(hesap_key, marketplace)
);

CREATE UNIQUE INDEX idx_search_term_reports_unique
    ON search_term_reports(hesap_key, marketplace, ad_type, collection_date,
                           campaign_id, COALESCE(ad_group_id,''), COALESCE(keyword_id,''), COALESCE(search_term,''));

-- Campaign raporlari (SP+SB+SD)
CREATE TABLE campaign_reports (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hesap_key TEXT NOT NULL,
    marketplace TEXT NOT NULL,
    ad_type TEXT NOT NULL,
    report_period TEXT NOT NULL,
    collection_date DATE NOT NULL,
    start_date DATE,
    end_date DATE,
    campaign_id TEXT NOT NULL,
    campaign_name TEXT,
    campaign_status TEXT,
    impressions INTEGER DEFAULT 0,
    clicks INTEGER DEFAULT 0,
    cost NUMERIC(12,4) DEFAULT 0,
    sales NUMERIC(12,4) DEFAULT 0,
    purchases INTEGER DEFAULT 0,
    units_sold INTEGER DEFAULT 0,
    acos NUMERIC(12,4),
    roas NUMERIC(12,4),
    raw_data JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(hesap_key, marketplace, ad_type, report_period, collection_date, campaign_id),
    FOREIGN KEY (hesap_key, marketplace) REFERENCES marketplaces(hesap_key, marketplace)
);

-- ==========================================
-- 5. AGENT 2 — ANALIZ CIKTILARI
-- ==========================================

-- Bid tavsiyeleri
CREATE TABLE bid_recommendations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hesap_key TEXT NOT NULL,
    marketplace TEXT NOT NULL,
    analysis_date DATE NOT NULL,
    ad_type TEXT NOT NULL,                   -- SP, SB, SD
    campaign_id TEXT,
    campaign_name TEXT,
    ad_group_id TEXT,
    ad_group_name TEXT,
    keyword_id TEXT,
    target_id TEXT,
    keyword_text TEXT,
    targeting TEXT,
    match_type TEXT,
    segment TEXT NOT NULL,                   -- GORUNMEZ, YETERSIZ_VERI, KAN_KAYBEDEN, etc.
    current_bid NUMERIC(10,4),
    recommended_bid NUMERIC(10,4),
    bid_change_pct NUMERIC(8,4),
    -- Metrikler
    impressions INTEGER DEFAULT 0,
    clicks INTEGER DEFAULT 0,
    cost NUMERIC(12,4) DEFAULT 0,
    sales NUMERIC(12,4) DEFAULT 0,
    orders INTEGER DEFAULT 0,
    acos NUMERIC(12,4),
    cvr NUMERIC(10,6),
    cpc NUMERIC(10,4),
    -- Karar (kullanici onay/red)
    decision TEXT DEFAULT 'PENDING',         -- PENDING, APPROVED, REJECTED, MODIFIED
    decision_bid NUMERIC(10,4),              -- Kullanicinin degistirdigi bid (varsa)
    decided_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    FOREIGN KEY (hesap_key, marketplace) REFERENCES marketplaces(hesap_key, marketplace)
);

CREATE UNIQUE INDEX idx_bid_recommendations_unique
    ON bid_recommendations(hesap_key, marketplace, analysis_date, ad_type,
                           campaign_id, COALESCE(keyword_id,''), COALESCE(target_id,''));

-- Negatif keyword/target adaylari
CREATE TABLE negative_candidates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hesap_key TEXT NOT NULL,
    marketplace TEXT NOT NULL,
    analysis_date DATE NOT NULL,
    ad_type TEXT NOT NULL,
    campaign_id TEXT,
    campaign_name TEXT,
    ad_group_id TEXT,
    keyword_text TEXT,
    targeting TEXT,
    candidate_type TEXT,                     -- KEYWORD, ASIN
    reason TEXT,                             -- Neden negatif onerildi
    impressions INTEGER DEFAULT 0,
    clicks INTEGER DEFAULT 0,
    cost NUMERIC(12,4) DEFAULT 0,
    sales NUMERIC(12,4) DEFAULT 0,
    acos NUMERIC(12,4),
    decision TEXT DEFAULT 'PENDING',
    decided_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    FOREIGN KEY (hesap_key, marketplace) REFERENCES marketplaces(hesap_key, marketplace)
);

-- Harvesting adaylari
CREATE TABLE harvesting_candidates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hesap_key TEXT NOT NULL,
    marketplace TEXT NOT NULL,
    analysis_date DATE NOT NULL,
    ad_type TEXT NOT NULL,
    source_campaign_id TEXT,
    source_campaign_name TEXT,
    source_ad_group_id TEXT,
    search_term TEXT,
    targeting TEXT,
    harvest_type TEXT,                       -- KEYWORD, ASIN
    suggested_match_type TEXT,               -- EXACT, PHRASE
    suggested_bid NUMERIC(10,4),
    impressions INTEGER DEFAULT 0,
    clicks INTEGER DEFAULT 0,
    cost NUMERIC(12,4) DEFAULT 0,
    sales NUMERIC(12,4) DEFAULT 0,
    orders INTEGER DEFAULT 0,
    acos NUMERIC(12,4),
    decision TEXT DEFAULT 'PENDING',
    decided_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    FOREIGN KEY (hesap_key, marketplace) REFERENCES marketplaces(hesap_key, marketplace)
);

-- ==========================================
-- 6. AGENT 3 — EXECUTION
-- ==========================================

-- Execution plan ozeti
CREATE TABLE execution_plans (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hesap_key TEXT NOT NULL,
    marketplace TEXT NOT NULL,
    plan_date DATE NOT NULL,
    session_id TEXT,
    mode TEXT NOT NULL,                      -- DRY_RUN, EXECUTE
    status TEXT DEFAULT 'PENDING',           -- PENDING, EXECUTING, COMPLETED, FAILED
    bid_total INTEGER DEFAULT 0,
    bid_success INTEGER DEFAULT 0,
    bid_error INTEGER DEFAULT 0,
    negative_total INTEGER DEFAULT 0,
    negative_success INTEGER DEFAULT 0,
    negative_error INTEGER DEFAULT 0,
    harvesting_total INTEGER DEFAULT 0,
    harvesting_success INTEGER DEFAULT 0,
    harvesting_error INTEGER DEFAULT 0,
    warnings JSONB DEFAULT '[]',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    FOREIGN KEY (hesap_key, marketplace) REFERENCES marketplaces(hesap_key, marketplace)
);

-- Execution islem detaylari (90 gun retention)
CREATE TABLE execution_items (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    plan_id UUID REFERENCES execution_plans(id) ON DELETE CASCADE,
    hesap_key TEXT NOT NULL,
    marketplace TEXT NOT NULL,
    item_type TEXT NOT NULL,                 -- BID_CHANGE, NEGATIVE_ADD, HARVESTING
    campaign_id TEXT,
    campaign_name TEXT,
    ad_group_id TEXT,
    keyword_id TEXT,
    target_id TEXT,
    targeting TEXT,
    -- Bid degisiklikleri
    old_bid NUMERIC(10,4),
    new_bid NUMERIC(10,4),
    bid_change_pct NUMERIC(8,4),
    -- Negatif eklemeler
    negative_type TEXT,                      -- NEGATIVE_KEYWORD, NEGATIVE_ASIN
    match_type TEXT,
    -- Harvesting
    harvest_type TEXT,
    source_campaign TEXT,
    -- API sonucu
    api_endpoint TEXT,
    api_payload JSONB,
    api_response JSONB,
    status TEXT DEFAULT 'PENDING',           -- PENDING, SUCCESS, FAILED, SKIPPED
    error_message TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Verification sonuclari
CREATE TABLE verification_results (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    plan_id UUID REFERENCES execution_plans(id),
    hesap_key TEXT NOT NULL,
    marketplace TEXT NOT NULL,
    verify_date DATE NOT NULL,
    total_checked INTEGER DEFAULT 0,
    matched INTEGER DEFAULT 0,
    mismatched INTEGER DEFAULT 0,
    not_found INTEGER DEFAULT 0,
    details JSONB DEFAULT '[]',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    FOREIGN KEY (hesap_key, marketplace) REFERENCES marketplaces(hesap_key, marketplace)
);

-- ==========================================
-- 7. AGENT 4 — OPTIMIZER & LEARNING
-- ==========================================

-- Karar gecmisi (SILINMEZ)
CREATE TABLE decision_history (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hesap_key TEXT NOT NULL,
    marketplace TEXT NOT NULL,
    decision_date DATE NOT NULL,
    targeting_id TEXT NOT NULL,               -- SP_campaignId_keywordId_EXACT
    ad_type TEXT,
    targeting TEXT,
    campaign_name TEXT,
    portfolio_id TEXT,
    asin TEXT,
    segment TEXT,
    previous_bid NUMERIC(10,4),
    new_bid NUMERIC(10,4),
    change_pct NUMERIC(8,4),
    metrics JSONB,                            -- impressions, clicks, spend, sales, orders, acos, cvr, cpc
    decision_status TEXT DEFAULT 'PENDING',   -- PENDING, APPLIED, VERIFIED, FAILED
    kpi_after JSONB,                          -- Karar sonrasi performans
    kpi_collected_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    FOREIGN KEY (hesap_key, marketplace) REFERENCES marketplaces(hesap_key, marketplace)
);

-- ASIN davranis profilleri
CREATE TABLE asin_profiles (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hesap_key TEXT NOT NULL,
    marketplace TEXT NOT NULL,
    asin TEXT NOT NULL,
    product_name TEXT,
    profile_data JSONB NOT NULL DEFAULT '{}',
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(hesap_key, marketplace, asin),
    FOREIGN KEY (hesap_key, marketplace) REFERENCES marketplaces(hesap_key, marketplace)
);

-- Segment istatistikleri
CREATE TABLE segment_stats (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hesap_key TEXT NOT NULL,
    marketplace TEXT NOT NULL,
    segment TEXT NOT NULL,
    stats_data JSONB NOT NULL DEFAULT '{}',
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(hesap_key, marketplace, segment),
    FOREIGN KEY (hesap_key, marketplace) REFERENCES marketplaces(hesap_key, marketplace)
);

-- Kalip kutuphanesi
CREATE TABLE patterns (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hesap_key TEXT NOT NULL,
    marketplace TEXT NOT NULL,
    pattern_type TEXT,
    description TEXT,
    pattern_data JSONB NOT NULL DEFAULT '{}',
    detected_at TIMESTAMPTZ DEFAULT NOW(),
    status TEXT DEFAULT 'ACTIVE',            -- ACTIVE, RESOLVED, IGNORED
    FOREIGN KEY (hesap_key, marketplace) REFERENCES marketplaces(hesap_key, marketplace)
);

-- Anomaliler
CREATE TABLE anomalies (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hesap_key TEXT NOT NULL,
    marketplace TEXT NOT NULL,
    anomaly_type TEXT,
    severity TEXT,                            -- LOW, MEDIUM, HIGH, CRITICAL
    description TEXT,
    anomaly_data JSONB NOT NULL DEFAULT '{}',
    detected_at TIMESTAMPTZ DEFAULT NOW(),
    resolved_at TIMESTAMPTZ,
    status TEXT DEFAULT 'ACTIVE',            -- ACTIVE, RESOLVED, IGNORED
    FOREIGN KEY (hesap_key, marketplace) REFERENCES marketplaces(hesap_key, marketplace)
);

-- Optimizer onerileri
CREATE TABLE proposals (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hesap_key TEXT NOT NULL,
    marketplace TEXT NOT NULL,
    proposal_id TEXT NOT NULL,               -- ONR-XXXXXXXX
    proposal_type TEXT,                      -- BID_PARAM, SEGMENT_PARAM, THRESHOLD
    title TEXT,
    description TEXT,
    current_value JSONB,
    proposed_value JSONB,
    rationale TEXT,
    impact_estimate JSONB,
    status TEXT DEFAULT 'PENDING',           -- PENDING, APPROVED, REJECTED
    decided_at TIMESTAMPTZ,
    rejection_reason TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(hesap_key, marketplace, proposal_id),
    FOREIGN KEY (hesap_key, marketplace) REFERENCES marketplaces(hesap_key, marketplace)
);

-- Durum raporlari
CREATE TABLE status_reports (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hesap_key TEXT NOT NULL,
    marketplace TEXT NOT NULL,
    report_date DATE NOT NULL,
    health_score INTEGER,
    health_status TEXT,
    pipeline_summary JSONB,
    error_summary JSONB,
    kpi_summary JSONB,
    anomaly_summary JSONB,
    segment_health JSONB,
    pending_proposals INTEGER DEFAULT 0,
    report_text TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(hesap_key, marketplace, report_date),
    FOREIGN KEY (hesap_key, marketplace) REFERENCES marketplaces(hesap_key, marketplace)
);

-- ==========================================
-- 8. HATA LOGLARI (90 gun retention)
-- ==========================================

-- Tum agent hata loglari (tek tablo)
CREATE TABLE error_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hesap_key TEXT NOT NULL,
    marketplace TEXT NOT NULL,
    agent TEXT NOT NULL,                      -- agent1, agent2, agent3, agent4
    session_id TEXT,
    timestamp TIMESTAMPTZ NOT NULL,
    error_type TEXT NOT NULL,                -- RateLimit, AuthError, ApiError, etc.
    error_message TEXT,
    step TEXT,                               -- collect_report, analyze, execute, etc.
    extra JSONB DEFAULT '{}',
    traceback TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Maestro pipeline hatalari (90 gun retention)
CREATE TABLE maestro_errors (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hesap_key TEXT NOT NULL,
    marketplace TEXT NOT NULL,
    session_id TEXT,
    timestamp TIMESTAMPTZ NOT NULL,
    error_type TEXT NOT NULL,
    error_message TEXT,
    step TEXT,
    agent TEXT,                              -- Hangi agent'ta hata oldu
    extra JSONB DEFAULT '{}',
    traceback TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ==========================================
-- 9. INDEXLER
-- ==========================================

-- Entity tablolari — hizli lookup
CREATE INDEX idx_campaigns_hesap_mp ON campaigns(hesap_key, marketplace);
CREATE INDEX idx_campaigns_type ON campaigns(ad_type);
CREATE INDEX idx_ad_groups_campaign ON ad_groups(campaign_id);
CREATE INDEX idx_keywords_campaign ON keywords(campaign_id);
CREATE INDEX idx_keywords_text ON keywords(keyword_text);
CREATE INDEX idx_targets_campaign ON targets(campaign_id);
CREATE INDEX idx_product_ads_asin ON product_ads(asin);

-- Rapor tablolari — tarih bazli sorgular
CREATE INDEX idx_targeting_reports_date ON targeting_reports(collection_date DESC);
CREATE INDEX idx_targeting_reports_lookup ON targeting_reports(hesap_key, marketplace, ad_type, collection_date);
CREATE INDEX idx_targeting_reports_campaign ON targeting_reports(campaign_id, collection_date);
CREATE INDEX idx_search_term_reports_date ON search_term_reports(collection_date DESC);
CREATE INDEX idx_search_term_reports_lookup ON search_term_reports(hesap_key, marketplace, ad_type, collection_date);
CREATE INDEX idx_search_term_reports_term ON search_term_reports(search_term);
CREATE INDEX idx_campaign_reports_date ON campaign_reports(collection_date DESC);

-- Agent 2 — analiz sorguları
CREATE INDEX idx_bid_recs_date ON bid_recommendations(analysis_date DESC);
CREATE INDEX idx_bid_recs_segment ON bid_recommendations(segment);
CREATE INDEX idx_bid_recs_decision ON bid_recommendations(decision);
CREATE INDEX idx_negative_cands_date ON negative_candidates(analysis_date DESC);
CREATE INDEX idx_harvesting_cands_date ON harvesting_candidates(analysis_date DESC);

-- Agent 3 — execution
CREATE INDEX idx_exec_plans_date ON execution_plans(plan_date DESC);
CREATE INDEX idx_exec_items_plan ON execution_items(plan_id);
CREATE INDEX idx_exec_items_type ON execution_items(item_type);

-- Agent 4 — optimizer
CREATE INDEX idx_decision_history_date ON decision_history(decision_date DESC);
CREATE INDEX idx_decision_history_target ON decision_history(targeting_id);
CREATE INDEX idx_decision_history_segment ON decision_history(segment);
CREATE INDEX idx_proposals_status ON proposals(status);

-- Hata loglari — zaman bazli sorgular
CREATE INDEX idx_error_logs_time ON error_logs(timestamp DESC);
CREATE INDEX idx_error_logs_agent ON error_logs(agent, error_type);
CREATE INDEX idx_error_logs_hesap ON error_logs(hesap_key, marketplace);
CREATE INDEX idx_maestro_errors_time ON maestro_errors(timestamp DESC);

-- Pipeline runs
CREATE INDEX idx_pipeline_runs_date ON pipeline_runs(pipeline_date DESC);
CREATE INDEX idx_pipeline_runs_session ON pipeline_runs(session_id);

-- ==========================================
-- 10. updated_at TRIGGER
-- ==========================================

CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_accounts_updated_at BEFORE UPDATE ON accounts
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();
CREATE TRIGGER tr_marketplaces_updated_at BEFORE UPDATE ON marketplaces
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();
CREATE TRIGGER tr_settings_updated_at BEFORE UPDATE ON settings
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();
CREATE TRIGGER tr_bid_functions_updated_at BEFORE UPDATE ON bid_functions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- ==========================================
-- 11. RETENTION POLICY (90 gun)
-- ==========================================
-- Supabase pg_cron ile calistirilacak (dashboard'dan ayarlanir)

-- Retention fonksiyonu: error_logs ve execution_items icin 90 gun
CREATE OR REPLACE FUNCTION cleanup_old_records()
RETURNS void AS $$
BEGIN
    -- Error logs: 90 gun
    DELETE FROM error_logs WHERE timestamp < NOW() - INTERVAL '90 days';
    -- Maestro errors: 90 gun
    DELETE FROM maestro_errors WHERE timestamp < NOW() - INTERVAL '90 days';
    -- Execution items: 90 gun
    DELETE FROM execution_items WHERE created_at < NOW() - INTERVAL '90 days';
    -- Execution plans: 90 gun (cascade ile items de silinir)
    DELETE FROM execution_plans WHERE created_at < NOW() - INTERVAL '90 days';
END;
$$ LANGUAGE plpgsql;

-- NOT: Bu fonksiyonu Supabase Dashboard > SQL Editor > pg_cron ile schedule edin:
-- SELECT cron.schedule('cleanup-old-records', '0 3 * * *', 'SELECT cleanup_old_records()');

-- ==========================================
-- 12. ROW LEVEL SECURITY (RLS)
-- ==========================================

-- Tum tablolarda RLS aktif et
ALTER TABLE accounts ENABLE ROW LEVEL SECURITY;
ALTER TABLE marketplaces ENABLE ROW LEVEL SECURITY;
ALTER TABLE pipeline_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE settings ENABLE ROW LEVEL SECURITY;
ALTER TABLE bid_functions ENABLE ROW LEVEL SECURITY;
ALTER TABLE portfolios ENABLE ROW LEVEL SECURITY;
ALTER TABLE campaigns ENABLE ROW LEVEL SECURITY;
ALTER TABLE ad_groups ENABLE ROW LEVEL SECURITY;
ALTER TABLE keywords ENABLE ROW LEVEL SECURITY;
ALTER TABLE targets ENABLE ROW LEVEL SECURITY;
ALTER TABLE product_ads ENABLE ROW LEVEL SECURITY;
ALTER TABLE negative_keywords ENABLE ROW LEVEL SECURITY;
ALTER TABLE negative_targets ENABLE ROW LEVEL SECURITY;
ALTER TABLE targeting_reports ENABLE ROW LEVEL SECURITY;
ALTER TABLE search_term_reports ENABLE ROW LEVEL SECURITY;
ALTER TABLE campaign_reports ENABLE ROW LEVEL SECURITY;
ALTER TABLE bid_recommendations ENABLE ROW LEVEL SECURITY;
ALTER TABLE negative_candidates ENABLE ROW LEVEL SECURITY;
ALTER TABLE harvesting_candidates ENABLE ROW LEVEL SECURITY;
ALTER TABLE execution_plans ENABLE ROW LEVEL SECURITY;
ALTER TABLE execution_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE verification_results ENABLE ROW LEVEL SECURITY;
ALTER TABLE decision_history ENABLE ROW LEVEL SECURITY;
ALTER TABLE asin_profiles ENABLE ROW LEVEL SECURITY;
ALTER TABLE segment_stats ENABLE ROW LEVEL SECURITY;
ALTER TABLE patterns ENABLE ROW LEVEL SECURITY;
ALTER TABLE anomalies ENABLE ROW LEVEL SECURITY;
ALTER TABLE proposals ENABLE ROW LEVEL SECURITY;
ALTER TABLE status_reports ENABLE ROW LEVEL SECURITY;
ALTER TABLE error_logs ENABLE ROW LEVEL SECURITY;
ALTER TABLE maestro_errors ENABLE ROW LEVEL SECURITY;

-- Service role icin full access (pipeline bu role ile yazacak)
CREATE POLICY "service_role_full_access" ON accounts FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_full_access" ON marketplaces FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_full_access" ON pipeline_runs FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_full_access" ON settings FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_full_access" ON bid_functions FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_full_access" ON portfolios FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_full_access" ON campaigns FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_full_access" ON ad_groups FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_full_access" ON keywords FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_full_access" ON targets FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_full_access" ON product_ads FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_full_access" ON negative_keywords FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_full_access" ON negative_targets FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_full_access" ON targeting_reports FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_full_access" ON search_term_reports FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_full_access" ON campaign_reports FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_full_access" ON bid_recommendations FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_full_access" ON negative_candidates FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_full_access" ON harvesting_candidates FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_full_access" ON execution_plans FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_full_access" ON execution_items FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_full_access" ON verification_results FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_full_access" ON decision_history FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_full_access" ON asin_profiles FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_full_access" ON segment_stats FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_full_access" ON patterns FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_full_access" ON anomalies FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_full_access" ON proposals FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_full_access" ON status_reports FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_full_access" ON error_logs FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_role_full_access" ON maestro_errors FOR ALL USING (true) WITH CHECK (true);

-- Anon role icin read-only (dashboard icin)
CREATE POLICY "anon_read_access" ON accounts FOR SELECT USING (true);
CREATE POLICY "anon_read_access" ON marketplaces FOR SELECT USING (true);
CREATE POLICY "anon_read_access" ON pipeline_runs FOR SELECT USING (true);
CREATE POLICY "anon_read_access" ON settings FOR SELECT USING (true);
CREATE POLICY "anon_read_access" ON bid_functions FOR SELECT USING (true);
CREATE POLICY "anon_read_access" ON portfolios FOR SELECT USING (true);
CREATE POLICY "anon_read_access" ON campaigns FOR SELECT USING (true);
CREATE POLICY "anon_read_access" ON ad_groups FOR SELECT USING (true);
CREATE POLICY "anon_read_access" ON keywords FOR SELECT USING (true);
CREATE POLICY "anon_read_access" ON targets FOR SELECT USING (true);
CREATE POLICY "anon_read_access" ON product_ads FOR SELECT USING (true);
CREATE POLICY "anon_read_access" ON negative_keywords FOR SELECT USING (true);
CREATE POLICY "anon_read_access" ON negative_targets FOR SELECT USING (true);
CREATE POLICY "anon_read_access" ON targeting_reports FOR SELECT USING (true);
CREATE POLICY "anon_read_access" ON search_term_reports FOR SELECT USING (true);
CREATE POLICY "anon_read_access" ON campaign_reports FOR SELECT USING (true);
CREATE POLICY "anon_read_access" ON bid_recommendations FOR SELECT USING (true);
CREATE POLICY "anon_read_access" ON negative_candidates FOR SELECT USING (true);
CREATE POLICY "anon_read_access" ON harvesting_candidates FOR SELECT USING (true);
CREATE POLICY "anon_read_access" ON execution_plans FOR SELECT USING (true);
CREATE POLICY "anon_read_access" ON execution_items FOR SELECT USING (true);
CREATE POLICY "anon_read_access" ON verification_results FOR SELECT USING (true);
CREATE POLICY "anon_read_access" ON decision_history FOR SELECT USING (true);
CREATE POLICY "anon_read_access" ON asin_profiles FOR SELECT USING (true);
CREATE POLICY "anon_read_access" ON segment_stats FOR SELECT USING (true);
CREATE POLICY "anon_read_access" ON patterns FOR SELECT USING (true);
CREATE POLICY "anon_read_access" ON anomalies FOR SELECT USING (true);
CREATE POLICY "anon_read_access" ON proposals FOR SELECT USING (true);
CREATE POLICY "anon_read_access" ON status_reports FOR SELECT USING (true);
CREATE POLICY "anon_read_access" ON error_logs FOR SELECT USING (true);
CREATE POLICY "anon_read_access" ON maestro_errors FOR SELECT USING (true);
