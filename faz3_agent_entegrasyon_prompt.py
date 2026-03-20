"""
FAZ 3: AGENT SUPABASE ENTEGRASYONU — KAPSAMLI GUNCELLEME PROMPTU
=================================================================
Bu dosya amazon-ppc-automation klasorunde Claude Code'a verilecek.

HEDEF: Tum agentlar Supabase'deki dogru tablolara dogru verileri yazacak.

ONEMLI: Once db_client.py'i oku ve mevcut insert/upsert metodlarini anla.
Sonra asagidaki degisiklikleri sirayla uygula.
"""

# ================================================================
# ONCEKI ADIMDA YAPILAN TABLO DEGISIKLIKLERI (referans)
# ================================================================
# kpi_daily: DROP+CREATE (13 kampanya tipi + portfolio_id/name, UNIQUE constraint)
# bid_recommendations: +portfolio TEXT, +reason TEXT
# harvesting_candidates: +portfolio TEXT, +cvr NUMERIC, +recommendation TEXT
# negative_candidates: YENI TABLO (campaign_name, ad_type, portfolio, search_term, match_type, source, impressions, clicks, cost, sales, cvr, cpc, reason, decision)
# proposals_system: risk -> beklenen_sonuc, benefit -> gerceklesen_sonuc
# agent_logs: +hesap_key TEXT, +marketplace TEXT, +session_id TEXT, +traceback TEXT
# pipeline_runs: YENI TABLO (session_id, hesap_key, marketplace, current_step, status, agentX_completed_at, error_message)

# ================================================================
# DEGISIKLIK 1: db_client.py — insert metodlarini guncelle
# ================================================================
"""
ONCE db_client.py'i oku (supabase/db_client.py).
Asagidaki metodlari bul ve yeni kolonlari destekleyecek sekilde guncelle:

1.1) insert_bid_recommendations(hesap_key, mp, date, data_list):
     data_list'teki her dict'te artik su yeni alanlar olacak:
       - "portfolio": portfolio adi (str veya None)
       - "reason": bid tavsiye sebebi (str veya None)
     Bu alanlari INSERT sorgusuna ekle:
       portfolio = item.get("portfolio"),
       reason = item.get("reason") veya item.get("sebep"),

1.2) insert_harvesting_candidates(hesap_key, mp, date, data_list):
     Yeni alanlar:
       - "portfolio": portfolio adi
       - "cvr": conversion rate (numeric)
       - "recommendation": oneri metni
     INSERT sorgusuna ekle.

1.3) insert_negative_candidates(hesap_key, mp, date, data_list):
     Bu metod VARSA guncelle, YOKSA OLUSTUR.
     negative_candidates tablosuna yazar:
       hesap_key, marketplace, analysis_date, ad_type, campaign_name,
       portfolio, search_term, match_type, source,
       impressions, clicks, cost, sales, cvr, cpc, reason, decision

1.4) upsert_proposal(hesap_key, mp, oneri_dict):
     oneri_dict'te su alanlar DEGISTI:
       ESKI: "risk" -> YENI: "beklenen_sonuc" (veya "risk" fallback)
       ESKI: "kazanim" -> YENI: "gerceklesen_sonuc" (veya "kazanim" fallback)
     proposals_system tablosundaki kolon adlari:
       beklenen_sonuc, gerceklesen_sonuc
     Metod iki formati da kabul etsin (geriye uyumluluk):
       beklenen = oneri.get("beklenen_sonuc") or oneri.get("risk", "")
       gerceklesen = oneri.get("gerceklesen_sonuc") or oneri.get("kazanim", "")

1.5) insert_agent_log(hesap_key, mp, agent_name, log_dict):
     Bu metod VARSA guncelle, YOKSA OLUSTUR.
     agent_logs tablosuna yazar:
       agent_id, level, message, error_type, hesap_key, marketplace,
       session_id, traceback
     log_dict ornek:
       {"level": "info", "message": "...", "error_type": "RateLimit",
        "session_id": "20260318_120000", "traceback": "..."}

1.6) YENI METOD: upsert_pipeline_run(session_id, hesap_key, mp, step, status, error_msg=None):
     pipeline_runs tablosuna INSERT ON CONFLICT (session_id, hesap_key, marketplace):
       session_id, hesap_key, marketplace, current_step, status,
       agentX_completed_at (step'e gore), error_message, updated_at
     step degerleri: "agent1", "agent2", "agent3_execute", "agent3_verify", "agent4", "completed"
     status: "running", "completed", "failed"
     Ornek: step="agent1", status="completed" -> agent1_completed_at = NOW()

1.7) YENI METOD: update_agent_status_detail(agent_name, status, health_detail=None):
     agent_status tablosunu gunceller:
       UPDATE agent_status SET status=X, last_run_at=NOW(),
       health_detail=JSONB, health_score=Y WHERE agent_name=Z
     health_detail JSONB: {"tasks": 150, "duration": "2m 30s", "errors_7d": 3,
       "last_3_runs": [{"date":"2026-03-18","status":"completed","duration":"2m","tasks":150}]}
"""

# ================================================================
# DEGISIKLIK 2: analyst.py — bid/harvesting/negative Supabase sync
# ================================================================
"""
agent2/analyst.py dosyasinda _sync_agent2_to_supabase fonksiyonunu guncelle:

2.1) bid_data dict'ine 2 yeni alan ekle:
     BID RESULTS'ta zaten portfolio_id ve sebep alanlari var.
     
     MEVCUT:
       bid_data.append({
           "reklam_tipi": r.get("reklam_tipi", "SP"),
           ...
           "karar_durumu": "PENDING",
       })
     
     EKLENECEK (karar_durumu'ndan ONCE):
       "portfolio": r.get("portfolio_id", ""),     # portfolio_id Agent 2'de mevcut
       "reason": r.get("sebep", ""),               # sebep Agent 2'de mevcut

2.2) harv_data dict'ine 3 yeni alan ekle:
     MEVCUT:
       harv_data.append({
           ...
           "karar_durumu": "PENDING",
       })
     
     EKLENECEK:
       "portfolio": h.get("portfolio_id", ""),
       "cvr": h.get("cvr", 0),
       "recommendation": h.get("oneri", "") or h.get("recommendation", ""),

2.3) neg_data dict'ine portfolio ekle:
     EKLENECEK:
       "portfolio": n.get("portfolio_id", ""),
       "cvr": n.get("cvr", 0),
       "cpc": n.get("cpc", 0),
"""

# ================================================================
# DEGISIKLIK 3: parallel_collector.py — kpi_daily otomatik sync
# ================================================================
"""
parallel_collector.py dosyasinda collect_marketplace fonksiyonu TAMAMLANDIKTAN SONRA
(await client.close() ONCE) kpi_daily sync ekle.

MEVCUT (collect_marketplace sonunda):
    await client.close()
    return R

YENI (client.close ONCE):
    # KPI Daily sync
    try:
        import sys as _sys
        _sys.path.insert(0, str(BASE_DIR))
        from supabase.db_client import SupabaseClient
        db = SupabaseClient()
        hesap_mp = label.split("/")  # label = "vigowood_eu/UK"
        if len(hesap_mp) == 2:
            db.upsert_kpi_daily(hesap_mp[0], hesap_mp[1], today)
            logger.info("[%s] KPI daily sync tamamlandi", label)
    except Exception as e:
        logger.warning("[%s] KPI daily sync hatasi (collector devam eder): %s", label, e)

    await client.close()
    return R

NOT: Bu degisiklik her marketplace veri toplama bittikten HEMEN SONRA
kpi_daily'yi doldurur. Ayri script calistirmaya gerek kalmaz.
"""

# ================================================================
# DEGISIKLIK 4: proposal_engine.py — risk/kazanim -> beklenen_sonuc/gerceklesen_sonuc
# ================================================================
"""
agent4/proposal_engine.py dosyasinda _olustur_oneri metodunu guncelle:

MEVCUT:
    def _olustur_oneri(self, today, kategori, ne, neden,
                        kanit, risk, kazanim, degisecek_dosya, degisecek_alan) -> dict:
        ...
        return {
            "id":               oneri_id,
            "tarih":            today,
            "kategori":         kategori,
            "durum":            "BEKLIYOR",
            "ne":               ne,
            "neden":            neden,
            "kanit":            kanit,
            "risk":             risk,
            "kazanim":          kazanim,
            ...
        }

YENI (parametre isimleri degismez — geriye uyumluluk icin):
    def _olustur_oneri(self, today, kategori, ne, neden,
                        kanit, risk, kazanim, degisecek_dosya, degisecek_alan) -> dict:
        ...
        return {
            "id":               oneri_id,
            "tarih":            today,
            "kategori":         kategori,
            "durum":            "BEKLIYOR",
            "ne":               ne,
            "neden":            neden,
            "kanit":            kanit,
            "beklenen_sonuc":   risk,       # Supabase kolon adi
            "gerceklesen_sonuc": kazanim,    # Supabase kolon adi
            "risk":             risk,        # Geriye uyumluluk (JSON dosyalari)
            "kazanim":          kazanim,     # Geriye uyumluluk (JSON dosyalari)
            ...
        }

Bu sayede hem JSON dosyalari (eski format) hem Supabase (yeni format) calisir.
"""

# ================================================================
# DEGISIKLIK 5: maestro_agent.py — pipeline_runs + agent_logs
# ================================================================
"""
maestro/maestro_agent.py dosyasinda Supabase entegrasyonu ekle.

5.1) Dosyanin basina (import blogu) Supabase helper ekle:

    def _get_sdb():
        \"\"\"Supabase client al (hata olursa None don).\"\"\"
        try:
            import sys as _sys
            _base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            if _base not in _sys.path:
                _sys.path.insert(0, _base)
            from supabase.db_client import SupabaseClient
            return SupabaseClient()
        except Exception:
            return None

5.2) start_pipeline fonksiyonunda (session olusturulduktan sonra) pipeline_runs INSERT:

    # Session olusturulduktan sonra:
    sdb = _get_sdb()
    if sdb:
        try:
            sdb.upsert_pipeline_run(session_id, hesap_key, marketplace, "starting", "running")
        except: pass

5.3) _run_agent1, _run_agent2, _run_agent3 fonksiyonlarinda her agent baslangic/bitis:

    # Agent basladikca:
    sdb = _get_sdb()
    if sdb:
        try:
            sdb.upsert_pipeline_run(session_id, hesap_key, marketplace, "agent1", "running")
        except: pass

    # Agent tamamlaninca:
    if sdb:
        try:
            sdb.upsert_pipeline_run(session_id, hesap_key, marketplace, "agent1", "completed")
        except: pass

    # Agent hata verince:
    if sdb:
        try:
            sdb.upsert_pipeline_run(session_id, hesap_key, marketplace, "agent1", "failed", error_msg=str(e))
        except: pass

5.4) Pipeline tamamlaninca (tum agentlar bittikten sonra):
    if sdb:
        try:
            sdb.upsert_pipeline_run(session_id, hesap_key, marketplace, "completed", "completed")
        except: pass

5.5) state_manager.update_agent_status cagrildigi her yerde, ayni zamanda
     Supabase agent_status ve agent_logs'a da yaz:
     
     # Ornek: Agent 1 tamamlandi
     state_manager.update_agent_status(state, "agent1", "completed", summary=result)
     if sdb:
         try:
             sdb.update_agent_status_detail("agent1", "completed", {
                 "tasks": result.get("basarili", 0) + result.get("basarisiz", 0),
                 "duration": result.get("sure", "?"),
                 "errors_7d": result.get("basarisiz", 0),
             })
             sdb.insert_agent_log(hesap_key, marketplace, "agent1", {
                 "level": "info",
                 "message": f"Agent 1 tamamlandi: {result.get('basarili',0)} basarili",
                 "session_id": session_id,
             })
         except: pass
     
     # Hata durumunda:
     if sdb:
         try:
             sdb.update_agent_status_detail("agent1", "failed")
             sdb.insert_agent_log(hesap_key, marketplace, "agent1", {
                 "level": "error",
                 "message": f"Agent 1 hatasi: {error_msg[:200]}",
                 "error_type": error_type,
                 "session_id": session_id,
                 "traceback": tb[:1000] if tb else None,
             })
         except: pass
"""

# ================================================================
# DEGISIKLIK 6: optimizer.py — proposals_system kolon eslestirme
# ================================================================
"""
agent4/optimizer.py dosyasindaki _sync_agent4_to_supabase fonksiyonunda:

Oneriler zaten sdb.upsert_proposal() ile yaziliyor.
proposal_engine.py'daki degisiklik (Degisiklik 4) sayesinde
oneri dict'inde artik beklenen_sonuc/gerceklesen_sonuc alanlari olacak.
db_client.py'daki upsert_proposal metodu da geriye uyumlu olacak (Degisiklik 1.4).
Bu yuzden optimizer.py'da ek degisiklik GEREKMEZ.

Sadece agent_status guncellemesi ekle (pipeline_runs zaten Maestro'dan geliyor):
Optimizer tamamlaninca sdb.update_agent_status_detail("agent4", "completed", {...}) cagir.

MEVCUT _sync_agent4_to_supabase sonuna ekle:
    # Agent 4 status guncelle
    try:
        sdb.update_agent_status_detail("agent4", "completed", {
            "tasks": len(kararlar) + len(oneriler),
            "duration": "?",
            "errors_7d": 0,
        })
    except: pass
"""

# ================================================================
# DEGISIKLIK 7: amazon_ads_mcp.py (Agent 1) — kpi_daily sync
# ================================================================
"""
agent1/amazon_ads_mcp.py dosyasindaki _sync_agent1_to_supabase fonksiyonuna
kpi_daily sync EKLE (paralel collector disinda tek marketplace calistiginda):

MEVCUT fonksiyonun SONUNA (tum entity sync bittikten sonra) ekle:
    # KPI Daily sync
    try:
        db.upsert_kpi_daily(hesap_key, marketplace, today)
        logger.info("Supabase: kpi_daily sync tamamlandi (%s/%s)", hesap_key, marketplace)
    except Exception as e:
        _log_sync_error("supabase_sync_kpi", "kpi_daily", e, traceback.format_exc())

Ayrica agent_status guncelle:
    try:
        db.update_agent_status_detail("agent1", "completed", {
            "tasks": R.get("basarili", 0) + R.get("basarisiz", 0) + R.get("atlanan", 0),
        })
    except: pass
"""

# ================================================================
# DOGRULAMA
# ================================================================
"""
Degisiklikler tamamlandiktan sonra dogrulama:

1. Syntax kontrol:
   python -c "import agent2.analyst"
   python -c "import agent4.optimizer"
   python -c "import agent4.proposal_engine"
   python -c "from supabase.db_client import SupabaseClient"

2. db_client metodlarini kontrol:
   python -c "
   from supabase.db_client import SupabaseClient
   db = SupabaseClient()
   # Metodlarin varligini kontrol et
   assert hasattr(db, 'insert_bid_recommendations')
   assert hasattr(db, 'insert_harvesting_candidates')
   assert hasattr(db, 'insert_negative_candidates')
   assert hasattr(db, 'upsert_proposal')
   assert hasattr(db, 'upsert_pipeline_run')
   assert hasattr(db, 'update_agent_status_detail')
   assert hasattr(db, 'insert_agent_log')
   assert hasattr(db, 'upsert_kpi_daily')
   print('Tum metodlar mevcut')
   "

3. proposal_engine cikti kontrolu:
   python -c "
   # Oneri dict'inde hem eski hem yeni alanlar var mi?
   from agent4.proposal_engine import ProposalEngine
   # Kontrol: _olustur_oneri metodunda beklenen_sonuc ve gerceklesen_sonuc var mi
   import inspect
   src = inspect.getsource(ProposalEngine._olustur_oneri)
   assert 'beklenen_sonuc' in src, 'beklenen_sonuc eksik!'
   assert 'gerceklesen_sonuc' in src, 'gerceklesen_sonuc eksik!'
   print('proposal_engine OK')
   "
"""
