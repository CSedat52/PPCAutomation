"""
LYNOR DASHBOARD — KAPSAMLI GUNCELLEME PROMPTU
==============================================
Bu dosya Claude Code'a verilecek TEK PROMPT'tur.
Tum Supabase + db_client + dashboard degisikliklerini kapsar.

Claude Code'a soyle:
  "Bu dosyadaki talimatlari sirasiyla uygula."
"""

# ================================================================
# BOLUM A: SUPABASE TABLO DEGISIKLIKLERI
# ================================================================
# Claude Code bu SQL'leri psycopg2 ile dogrudan calistiracak.
# Supabase SQL Editor'a girmene gerek yok.

SUPABASE_SQL = """

-- A1: kpi_daily yeniden olustur (13 kampanya tipi + portfolio destekli)
DROP TABLE IF EXISTS kpi_daily;
CREATE TABLE kpi_daily (
    id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    report_date     DATE NOT NULL,
    hesap_key       TEXT NOT NULL,
    marketplace     TEXT NOT NULL,
    campaign_type   TEXT NOT NULL,
    portfolio_id    TEXT,
    portfolio_name  TEXT,
    spend           NUMERIC(12,2) NOT NULL DEFAULT 0,
    sales           NUMERIC(12,2) NOT NULL DEFAULT 0,
    clicks          INTEGER NOT NULL DEFAULT 0,
    orders          INTEGER NOT NULL DEFAULT 0,
    impressions     BIGINT NOT NULL DEFAULT 0,
    units_sold      INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT kpi_daily_unique
        UNIQUE (report_date, hesap_key, marketplace, campaign_type, portfolio_id)
);
CREATE INDEX idx_kpi_daily_date ON kpi_daily (report_date);
CREATE INDEX idx_kpi_daily_account ON kpi_daily (hesap_key, marketplace);
CREATE INDEX idx_kpi_daily_type ON kpi_daily (campaign_type);
CREATE INDEX idx_kpi_daily_portfolio ON kpi_daily (portfolio_name) WHERE portfolio_name IS NOT NULL;

-- A2: bid_recommendations — portfolio + reason kolon ekle
ALTER TABLE bid_recommendations ADD COLUMN IF NOT EXISTS portfolio TEXT;
ALTER TABLE bid_recommendations ADD COLUMN IF NOT EXISTS reason TEXT;

-- A3: harvesting_candidates — portfolio + cvr + recommendation ekle
ALTER TABLE harvesting_candidates ADD COLUMN IF NOT EXISTS portfolio TEXT;
ALTER TABLE harvesting_candidates ADD COLUMN IF NOT EXISTS cvr NUMERIC;
ALTER TABLE harvesting_candidates ADD COLUMN IF NOT EXISTS recommendation TEXT;

-- A4: negative_candidates tablosu olustur
CREATE TABLE IF NOT EXISTS negative_candidates (
    id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    hesap_key       TEXT NOT NULL,
    marketplace     TEXT NOT NULL,
    analysis_date   DATE,
    ad_type         TEXT,
    campaign_name   TEXT,
    portfolio       TEXT,
    search_term     TEXT,
    match_type      TEXT,
    source          TEXT,
    impressions     INTEGER DEFAULT 0,
    clicks          INTEGER DEFAULT 0,
    cost            NUMERIC(12,2) DEFAULT 0,
    sales           NUMERIC(12,2) DEFAULT 0,
    cvr             NUMERIC,
    cpc             NUMERIC,
    reason          TEXT,
    decision        TEXT DEFAULT 'PENDING',
    decided_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- A5: proposals_system — risk/benefit rename
ALTER TABLE proposals_system RENAME COLUMN risk TO beklenen_sonuc;
ALTER TABLE proposals_system RENAME COLUMN benefit TO gerceklesen_sonuc;

-- A6: agent_logs — ulke filtresi + detay kolonlari ekle
ALTER TABLE agent_logs ADD COLUMN IF NOT EXISTS hesap_key TEXT;
ALTER TABLE agent_logs ADD COLUMN IF NOT EXISTS marketplace TEXT;
ALTER TABLE agent_logs ADD COLUMN IF NOT EXISTS session_id TEXT;
ALTER TABLE agent_logs ADD COLUMN IF NOT EXISTS traceback TEXT;

-- A7: pipeline_runs tablosu olustur
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id                    UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    session_id            TEXT NOT NULL,
    hesap_key             TEXT NOT NULL,
    marketplace           TEXT NOT NULL,
    current_step          TEXT,
    status                TEXT DEFAULT 'running',
    started_at            TIMESTAMPTZ DEFAULT NOW(),
    agent1_completed_at   TIMESTAMPTZ,
    agent2_completed_at   TIMESTAMPTZ,
    agent3_completed_at   TIMESTAMPTZ,
    agent4_completed_at   TIMESTAMPTZ,
    error_message         TEXT,
    updated_at            TIMESTAMPTZ DEFAULT NOW()
);
"""

# ================================================================
# BOLUM B: db_client.py GUNCELLEME
# ================================================================
# Eski upsert_kpi_daily metodunu (satir 957-1019 civari) SIL.
# Yerine asagidaki metodlari SupabaseClient sinifina ekle.
# 
# Bu metodlar:
#   - JSON dosyalarindan campaign raporlarini okur
#   - 13 kampanya tipini entity verilerinden tespit eder
#   - Portfolio eslestirmesi yapar
#   - kpi_daily tablosuna UPSERT eder
#
# Kampanya Tipleri (13):
#   SP (6): Auto, Broad, Exact, Phrase, ASIN, Category
#     - sp_campaigns.targetingType = AUTO → SP-Auto
#     - sp_keywords matchType cogunlugu → SP-Broad/Exact/Phrase
#     - sp_targets expression ASIN_SAME_AS/ASIN_EXPANDED_FROM → SP-ASIN
#     - sp_targets expression ASIN_CATEGORY_SAME_AS → SP-Category
#
#   SB (3): Keyword, ASIN, Category
#     - sb_keywords'te keyword varsa → SB-Keyword
#     - sb_targets expressions asinSameAs → SB-ASIN
#     - sb_targets expressions asinCategorySameAs → SB-Category
#
#   SD (4): Retargeting, Contextual, Audience, Product
#     - sd_targets: audience + audienceSameAs → SD-Audience
#     - sd_targets: purchases/views + exactProduct/relatedProduct + lookback → SD-Retargeting
#     - sd_targets: asinCategorySameAs (direkt veya views icinde) → SD-Contextual
#     - sd_targets: similarProduct → SD-Product
#     - Oncelik: Audience > Retargeting > Contextual > Product

DB_CLIENT_NEW_METHODS = """
    # ==========================================
    # KPI DAILY — 13 KAMPANYA TIPI DESTEKLI
    # ==========================================

    def upsert_kpi_daily(self, hesap_key, mp, date_str=None):
        from collections import Counter
        if not date_str:
            date_str = datetime.utcnow().strftime("%Y-%m-%d")
        data_dir = _project_root / "data" / f"{hesap_key}_{mp}"
        if not data_dir.exists():
            logger.error("KPI: data klasoru yok: %s", data_dir)
            return 0

        def _load(fn):
            fp = data_dir / fn
            if not fp.exists(): return []
            try:
                with open(fp,"r",encoding="utf-8") as f: d=json.load(f)
                return d if isinstance(d,list) else []
            except: return []

        logger.info("KPI SYNC: %s/%s (%s)", hesap_key, mp, date_str)
        sp_map = self._classify_sp(date_str, _load)
        sb_map = self._classify_sb(date_str, _load)
        sd_map = self._classify_sd(date_str, _load)

        # Portfolio eslestirme
        pf_names = {str(p.get("portfolioId","")): p["name"]
                    for p in _load(f"{date_str}_portfolios.json") if p.get("name")}
        pf_map = {}
        for pre in ("sp","sb","sd"):
            for c in _load(f"{date_str}_{pre}_campaigns.json"):
                cid,pid = str(c.get("campaignId","")), str(c.get("portfolioId",""))
                if cid and pid: pf_map[cid] = (pid, pf_names.get(pid))

        rows = []
        for r in _load(f"{date_str}_sp_campaign_report_14d.json"):
            cid = str(r.get("campaignId",""))
            pid,pn = pf_map.get(cid,(None,None))
            rows.append((r.get("date"),hesap_key,mp,sp_map.get(cid,"SP-Other"),pid,pn,
                self._safe_numeric(r.get("cost")),self._safe_numeric(r.get("sales14d")),
                self._safe_int(r.get("clicks")),self._safe_int(r.get("purchases14d")),
                self._safe_int(r.get("impressions")),self._safe_int(r.get("unitsSoldClicks14d"))))

        for r in _load(f"{date_str}_sb_campaign_report_14d.json"):
            cid = str(r.get("campaignId",""))
            pid,pn = pf_map.get(cid,(None,None))
            rows.append((r.get("date"),hesap_key,mp,sb_map.get(cid,"SB-Other"),pid,pn,
                self._safe_numeric(r.get("cost")),self._safe_numeric(r.get("sales")),
                self._safe_int(r.get("clicks")),
                self._safe_int(r.get("purchases") or r.get("purchasesClicks")),
                self._safe_int(r.get("impressions")),self._safe_int(r.get("unitsSold"))))

        for r in _load(f"{date_str}_sd_campaign_report_14d.json"):
            cid = str(r.get("campaignId",""))
            pid,pn = pf_map.get(cid,(None,None))
            rows.append((r.get("date"),hesap_key,mp,sd_map.get(cid,"SD-Other"),pid,pn,
                self._safe_numeric(r.get("cost")),self._safe_numeric(r.get("sales")),
                self._safe_int(r.get("clicks")),
                self._safe_int(r.get("purchases") or r.get("purchasesClicks")),
                self._safe_int(r.get("impressions")),self._safe_int(r.get("unitsSold"))))

        if not rows:
            logger.warning("KPI: veri yok"); return 0

        agg = {}
        for (rd,hk,m,ct,pid,pn,sp,sa,cl,od,im,un) in rows:
            key = (rd,hk,m,ct,pid)
            if key not in agg:
                agg[key] = [rd,hk,m,ct,pid,pn,0.0,0.0,0,0,0,0]
            a=agg[key]; a[6]+=sp; a[7]+=sa; a[8]+=cl; a[9]+=od; a[10]+=im; a[11]+=un
            if not a[5] and pn: a[5]=pn

        agg_rows = []
        for a in agg.values():
            a[6]=round(a[6],2); a[7]=round(a[7],2)
            agg_rows.append(tuple(a))

        logger.info("KPI: %d ham -> %d aggregate", len(rows), len(agg_rows))

        conn = self._conn()
        try:
            cur = conn.cursor()
            sql = \"\"\"
                INSERT INTO kpi_daily (
                    report_date,hesap_key,marketplace,campaign_type,
                    portfolio_id,portfolio_name,
                    spend,sales,clicks,orders,impressions,units_sold,updated_at
                ) VALUES %s
                ON CONFLICT (report_date,hesap_key,marketplace,campaign_type,portfolio_id)
                DO UPDATE SET
                    portfolio_name=EXCLUDED.portfolio_name,
                    spend=EXCLUDED.spend, sales=EXCLUDED.sales,
                    clicks=EXCLUDED.clicks, orders=EXCLUDED.orders,
                    impressions=EXCLUDED.impressions, units_sold=EXCLUDED.units_sold,
                    updated_at=NOW()
            \"\"\"
            execute_values(cur, sql, agg_rows,
                template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())", page_size=500)
            count = cur.rowcount; cur.close()
            logger.info("KPI: %d satir upsert (%s/%s)", count, hesap_key, mp)
            return count
        except Exception as e:
            logger.error("KPI upsert hatasi: %s", e); return 0
        finally:
            conn.close()

    def _classify_sp(self, date_str, _load):
        from collections import Counter
        SP_ASIN={"ASIN_SAME_AS","ASIN_EXPANDED_FROM"}
        SP_CAT={"ASIN_CATEGORY_SAME_AS"}
        SP_AUTO={"QUERY_HIGH_REL_MATCHES","QUERY_BROAD_REL_MATCHES",
                 "ASIN_ACCESSORY_RELATED","ASIN_SUBSTITUTE_RELATED"}
        camp_tt={str(c["campaignId"]):c.get("targetingType","MANUAL")
                 for c in _load(f"{date_str}_sp_campaigns.json") if c.get("campaignId")}
        camp_kw={}
        for k in _load(f"{date_str}_sp_keywords.json"):
            cid,mt=str(k.get("campaignId","")),k.get("matchType","")
            if cid and mt: camp_kw.setdefault(cid,Counter())[mt]+=1
        camp_tgt={}
        for t in _load(f"{date_str}_sp_targets.json"):
            cid=str(t.get("campaignId",""))
            for e in (t.get("expression") or []):
                if not isinstance(e,dict): continue
                et=e.get("type","")
                if et in SP_ASIN: camp_tgt.setdefault(cid,set()).add("ASIN")
                elif et in SP_CAT: camp_tgt.setdefault(cid,set()).add("CATEGORY")
                elif et in SP_AUTO: camp_tgt.setdefault(cid,set()).add("AUTO")
        result={}
        for cid,tt in camp_tt.items():
            if tt=="AUTO": result[cid]="SP-Auto"; continue
            kw=camp_kw.get(cid); tg=camp_tgt.get(cid,set())
            has_kw=kw and sum(kw.values())>0
            if has_kw and "ASIN" not in tg and "CATEGORY" not in tg:
                result[cid]=f"SP-{kw.most_common(1)[0][0].capitalize()}"
            elif "ASIN" in tg and not has_kw and "CATEGORY" not in tg: result[cid]="SP-ASIN"
            elif "CATEGORY" in tg and not has_kw and "ASIN" not in tg: result[cid]="SP-Category"
            elif "ASIN" in tg and "CATEGORY" in tg: result[cid]="SP-ASIN"
            elif has_kw: result[cid]=f"SP-{kw.most_common(1)[0][0].capitalize()}"
            elif "AUTO" in tg: result[cid]="SP-Auto"
            else: result[cid]="SP-Other"
        return result

    def _classify_sb(self, date_str, _load):
        kw_cids={str(k["campaignId"]) for k in _load(f"{date_str}_sb_keywords.json") if k.get("campaignId")}
        camp_tgt={}
        for t in _load(f"{date_str}_sb_targets.json"):
            cid=str(t.get("campaignId",""))
            for e in (t.get("expressions") or t.get("expression") or []):
                if not isinstance(e,dict): continue
                et=e.get("type","").lower()
                if "asinsameas" in et: camp_tgt.setdefault(cid,set()).add("ASIN")
                elif "asincategorysameas" in et: camp_tgt.setdefault(cid,set()).add("CATEGORY")
        all_cids={str(c["campaignId"]) for c in _load(f"{date_str}_sb_campaigns.json") if c.get("campaignId")}
        all_cids.update(kw_cids); all_cids.update(camp_tgt.keys())
        result={}
        for cid in all_cids:
            tg=camp_tgt.get(cid,set())
            if cid in kw_cids: result[cid]="SB-Keyword"
            elif "ASIN" in tg: result[cid]="SB-ASIN"
            elif "CATEGORY" in tg: result[cid]="SB-Category"
            else: result[cid]="SB-Other"
        return result

    def _classify_sd(self, date_str, _load):
        all_cids={str(c["campaignId"]) for c in _load(f"{date_str}_sd_campaigns.json") if c.get("campaignId")}
        camp_types={}
        for t in _load(f"{date_str}_sd_targets.json"):
            cid=str(t.get("campaignId","")); all_cids.add(cid)
            exprs=t.get("expression") or []
            if not exprs or not isinstance(exprs[0],dict): continue
            main=exprs[0].get("type",""); vals=exprs[0].get("value",[])
            subs=set()
            if isinstance(vals,list):
                for v in vals:
                    if isinstance(v,dict): subs.add(v.get("type",""))
            if main=="audience": camp_types.setdefault(cid,set()).add("AUDIENCE")
            elif main=="similarProduct": camp_types.setdefault(cid,set()).add("PRODUCT")
            elif main=="asinCategorySameAs": camp_types.setdefault(cid,set()).add("CONTEXTUAL")
            elif main in ("purchases","views"):
                if "asinCategorySameAs" in subs: camp_types.setdefault(cid,set()).add("CONTEXTUAL")
                elif "exactProduct" in subs or "relatedProduct" in subs: camp_types.setdefault(cid,set()).add("RETARGETING")
                elif "similarProduct" in subs: camp_types.setdefault(cid,set()).add("PRODUCT")
                else: camp_types.setdefault(cid,set()).add("RETARGETING")
        result={}
        for cid in all_cids:
            tg=camp_types.get(cid,set())
            if "AUDIENCE" in tg: result[cid]="SD-Audience"
            elif "RETARGETING" in tg: result[cid]="SD-Retargeting"
            elif "CONTEXTUAL" in tg: result[cid]="SD-Contextual"
            elif "PRODUCT" in tg: result[cid]="SD-Product"
            else: result[cid]="SD-Other"
        return result

    def upsert_kpi_daily_all(self, date_str=None):
        accounts_path = _project_root / "config" / "accounts.json"
        if not accounts_path.exists():
            logger.error("accounts.json bulunamadi"); return 0
        with open(accounts_path) as f: accounts = json.load(f)
        total = 0
        for hk, h in accounts.get("hesaplar", {}).items():
            for mp, cfg in h.get("marketplaces", {}).items():
                if cfg.get("aktif"):
                    total += (self.upsert_kpi_daily(hk, mp, date_str) or 0)
        logger.info("KPI TOPLAM: %d satir", total)
        return total
"""

# ================================================================
# BOLUM C: SILINECEK DOSYA
# ================================================================
# supabase/populate_kpi_daily.py SILINECEK
# Artik db_client.upsert_kpi_daily() ayni isi yapiyor.

# ================================================================
# BOLUM D: DOGRULAMA
# ================================================================
DOGRULAMA = """
# 1. Supabase tablo degisikliklerini dogrula
python -c "
import psycopg2, os
from dotenv import load_dotenv
load_dotenv('.env')
conn = psycopg2.connect(os.getenv('SUPABASE_DB_URL'))
cur = conn.cursor()

# Tablo listesi
cur.execute(\"\"\"SELECT table_name FROM information_schema.tables
WHERE table_schema='public' AND table_type='BASE TABLE' ORDER BY table_name\"\"\")
print('TABLOLAR:')
for r in cur.fetchall(): print(f'  {r[0]}')

# kpi_daily yapisi
cur.execute(\"\"\"SELECT column_name, data_type FROM information_schema.columns
WHERE table_name='kpi_daily' ORDER BY ordinal_position\"\"\")
print('\\nkpi_daily kolonlari:')
for r in cur.fetchall(): print(f'  {r[0]:20s} {r[1]}')

# Yeni kolonlar
for tbl,col in [('bid_recommendations','portfolio'),('bid_recommendations','reason'),
                ('harvesting_candidates','portfolio'),('harvesting_candidates','cvr'),
                ('harvesting_candidates','recommendation'),
                ('agent_logs','hesap_key'),('agent_logs','marketplace'),
                ('proposals_system','beklenen_sonuc'),('proposals_system','gerceklesen_sonuc')]:
    cur.execute(\"\"\"SELECT COUNT(*) FROM information_schema.columns
    WHERE table_name=%s AND column_name=%s\"\"\", (tbl,col))
    ok = cur.fetchone()[0] > 0
    print(f'  {tbl}.{col}: {\"OK\" if ok else \"EKSIK!\"}')

# Yeni tablolar
for tbl in ['negative_candidates','pipeline_runs']:
    cur.execute(\"\"\"SELECT COUNT(*) FROM information_schema.tables
    WHERE table_name=%s AND table_schema='public'\"\"\", (tbl,))
    ok = cur.fetchone()[0] > 0
    print(f'  {tbl}: {\"MEVCUT\" if ok else \"YOK!\"}')

conn.close()
"

# 2. KPI doldur (tek marketplace test)
python -c "
import sys; sys.path.insert(0,'.')
from supabase.db_client import SupabaseClient
db = SupabaseClient()
db.upsert_kpi_daily('vigowood_eu', 'UK', '2026-03-18')
"

# 3. Basariliysa tum marketplace'leri doldur
python -c "
import sys; sys.path.insert(0,'.')
from supabase.db_client import SupabaseClient
db = SupabaseClient()
db.upsert_kpi_daily_all('2026-03-18')
"

# 4. Kampanya tipi dagilimi kontrol
python -c "
import psycopg2, os
from dotenv import load_dotenv
load_dotenv('.env')
conn = psycopg2.connect(os.getenv('SUPABASE_DB_URL'))
cur = conn.cursor()
cur.execute('SELECT campaign_type, COUNT(*), SUM(spend)::numeric, SUM(sales)::numeric FROM kpi_daily GROUP BY campaign_type ORDER BY SUM(spend) DESC')
print('KAMPANYA TIPI DAGILIMI:')
for ct,cnt,sp,sa in cur.fetchall():
    acos = float(sp)/float(sa)*100 if sa and float(sa)>0 else 0
    print(f'  {ct:18s} {cnt:>5d} satir  spend={float(sp):>10.2f}  sales={float(sa):>10.2f}  ACoS={acos:>5.1f}%')
cur.execute('SELECT hesap_key, marketplace, COUNT(*) FROM kpi_daily GROUP BY hesap_key, marketplace ORDER BY hesap_key, marketplace')
print('\\nMARKETPLACE DAGILIMI:')
for hk,mp,cnt in cur.fetchall():
    print(f'  {hk}/{mp}: {cnt} satir')
cur.execute('SELECT COUNT(*) FROM kpi_daily')
print(f\"\\nTOPLAM: {cur.fetchone()[0]} satir\")
conn.close()
"
"""
