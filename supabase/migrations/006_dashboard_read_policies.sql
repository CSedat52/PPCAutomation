-- ============================================================
-- Migration 006: Dashboard Read Policies
-- ============================================================
-- 4 Mayis 2026'da 6 tabloda RLS acildi (kritik guvenlik fix).
-- Ancak policy yazilmadigi icin Lynor dashboard (anon key ile
-- baglanir) bu tablolari okuyamadi. Bu migration dashboard'in
-- okumasi gereken tablolara anon SELECT iznini geri verir.
--
-- Backend (Python, service_role) zaten BYPASSRLS oldugu icin
-- bu policy'lerden etkilenmez — sadece anon/authenticated icin
-- okuma kapisi acar. INSERT/UPDATE/DELETE hala kapali.
--
-- execution_queue istisnasi: Dashboard onay butonu INSERT yapar,
-- maestro UPDATE eder. O yuzden 3 policy birden gerekli.
-- ============================================================

-- ============================================================
-- pipeline_runs — agents sayfasi pipeline akis kutucuklari
-- ============================================================
GRANT SELECT ON public.pipeline_runs TO anon, authenticated;

DROP POLICY IF EXISTS "anon_read_pipeline_runs" ON public.pipeline_runs;
CREATE POLICY "anon_read_pipeline_runs" ON public.pipeline_runs
  FOR SELECT USING (true);

-- ============================================================
-- execution_queue — proposals sayfasi onay butonu
-- ============================================================
GRANT SELECT, INSERT, UPDATE ON public.execution_queue TO anon, authenticated;

DROP POLICY IF EXISTS "anon_read_execution_queue" ON public.execution_queue;
CREATE POLICY "anon_read_execution_queue" ON public.execution_queue
  FOR SELECT USING (true);

DROP POLICY IF EXISTS "anon_insert_execution_queue" ON public.execution_queue;
CREATE POLICY "anon_insert_execution_queue" ON public.execution_queue
  FOR INSERT WITH CHECK (true);

DROP POLICY IF EXISTS "anon_update_execution_queue" ON public.execution_queue;
CREATE POLICY "anon_update_execution_queue" ON public.execution_queue
  FOR UPDATE USING (true) WITH CHECK (true);

-- ============================================================
-- kpi_daily — Genel Bakis sayfasi KPI ozet
-- ============================================================
GRANT SELECT ON public.kpi_daily TO anon, authenticated;

DROP POLICY IF EXISTS "anon_read_kpi_daily" ON public.kpi_daily;
CREATE POLICY "anon_read_kpi_daily" ON public.kpi_daily
  FOR SELECT USING (true);

-- ============================================================
-- campaign_reports — varsa dashboard kampanya gorunumu
-- ============================================================
GRANT SELECT ON public.campaign_reports TO anon, authenticated;

DROP POLICY IF EXISTS "anon_read_campaign_reports" ON public.campaign_reports;
CREATE POLICY "anon_read_campaign_reports" ON public.campaign_reports
  FOR SELECT USING (true);

-- ============================================================
-- negative_candidates — proposals sayfasi negatif keyword listesi
-- ============================================================
GRANT SELECT ON public.negative_candidates TO anon, authenticated;

DROP POLICY IF EXISTS "anon_read_negative_candidates" ON public.negative_candidates;
CREATE POLICY "anon_read_negative_candidates" ON public.negative_candidates
  FOR SELECT USING (true);

-- ============================================================
-- DOGRULAMA — bu sorgu hepsinin RLS+policy durumunu gosterir
-- ============================================================
-- SELECT t.tablename, t.rowsecurity AS rls_acik,
--        COUNT(p.policyname) AS policy_sayisi
-- FROM pg_tables t
-- LEFT JOIN pg_policies p ON p.tablename = t.tablename
-- WHERE t.schemaname = 'public'
--   AND t.tablename IN ('pipeline_runs','execution_queue','kpi_daily',
--                       'campaign_reports','negative_candidates')
-- GROUP BY t.tablename, t.rowsecurity
-- ORDER BY t.tablename;
