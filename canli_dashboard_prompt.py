"""
CANLI DASHBOARD GUNCELLEME — 3 SORUN, 3 COZUM
================================================
Bu dosya 2 FARKLI klasorde uygulanacak:
  ADIM 1-3: amazon-ppc-automation klasorunde
  ADIM 4:   lynor-dashboard klasorunde

SORUNLAR:
  1. Sadece error seviyesi Supabase'e yaziliyor, info/warn yok
     → Agent calisiyor ama dashboard'da hicbir sey gorunmuyor
  2. pipeline_runs tablosu bos — Maestro guncellemiyot
     → "Henuz pipeline calismadi" gosteriliyor
  3. Dashboard auto-refresh yok — sayfa acilinca veri bir kez yukleniyor
     → Pipeline calisirken canli guncelleme yok
"""

# ================================================================
# ADIM 1: log_utils.py — Genel log fonksiyonu ekle (info/warn/error)
# ================================================================
"""
log_utils.py dosyasini oku.

save_error_log fonksiyonunun YANINA (altina) yeni bir fonksiyon ekle:

def save_log(level, message, agent_name, hesap_key=None, marketplace=None,
             session_id=None, error_type=None, traceback_str=None, extra=None):
    \"\"\"
    Genel log fonksiyonu — info, warn, error tum seviyeleri destekler.
    Sadece Supabase agent_logs tablosuna yazar (lokal JSON'a YAZMAZ).
    
    Lokal JSON sadece hatalar icin (save_error_log ile).
    Bu fonksiyon dashboard gorunurlugu icin.
    
    level: "info", "warn", "error"
    \"\"\"
    if not agent_name:
        return
    try:
        from pathlib import Path as _Path
        _project_root = str(_Path(__file__).parent)
        import sys as _sys
        if _project_root not in _sys.path:
            _sys.path.insert(0, _project_root)
        from supabase.db_client import SupabaseClient
        _sdb = SupabaseClient()
        _sdb._execute(
            \"\"\"INSERT INTO agent_logs (agent_id, level, message, error_type,
                hesap_key, marketplace, session_id, traceback, created_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())\"\"\",
            (agent_name, level, str(message)[:500], error_type,
             hesap_key, marketplace, session_id,
             str(traceback_str)[:1000] if traceback_str else None)
        )
    except Exception:
        pass  # Dashboard gorunurlugu icin — basarisiz olursa agent calismaya devam eder
"""

# ================================================================
# ADIM 2: maestro/maestro_agent.py — Pipeline events Supabase'e yaz
# ================================================================
"""
maestro/maestro_agent.py dosyasini oku.

2a) Dosyanin basinda log_utils importunu bul. _central_save_error_log zaten import ediliyor.
    Yanina save_log'u da ekle:

    from log_utils import save_error_log as _central_save_error_log, save_log as _save_log

2b) _get_sdb() fonksiyonu zaten var. Kontrol et.

2c) Simdi kritik noktalara pipeline_runs + agent_status + info log ekleyecegiz.
    Asagidaki PATTERN'i her agent calisma noktasina uygula.

    ONEMLI: Maestro kodunu dikkatlice oku. start_pipeline, _run_agent1, _run_agent2,
    _run_agent3 gibi fonksiyonlari bul. Her birinde asagidaki gibi ekleme yap.

    === PATTERN: Pipeline basladiginda ===
    start_pipeline veya run_pipeline fonksiyonunda, session olusturulduktan sonra:

    # Supabase: Pipeline basladi
    sdb = _get_sdb()
    if sdb:
        try:
            sdb.upsert_pipeline_run(session_id, hesap_key, marketplace, "starting", "running")
        except: pass
    _save_log("info", f"Pipeline basladi: {hesap_key}/{marketplace}",
              "maestro", hesap_key, marketplace, session_id)

    === PATTERN: Agent 1 basladiginda ===
    _run_agent1 fonksiyonunda, agent calistirilmadan ONCE:

    _save_log("info", f"Agent 1 basliyor: {hesap_key}/{marketplace}",
              "agent1", hesap_key, marketplace, session_id)
    sdb = _get_sdb()
    if sdb:
        try:
            sdb.upsert_pipeline_run(session_id, hesap_key, marketplace, "agent1", "running")
        except: pass

    === PATTERN: Agent 1 BASARIYLA tamamlandiginda ===
    state_manager.update_agent_status(..., "completed", ...) satirindan HEMEN SONRA:

    _save_log("info", f"Agent 1 tamamlandi: {result.get('basarili',0)} basarili, {result.get('basarisiz',0)} hata",
              "agent1", hesap_key, marketplace, session_id)
    sdb = _get_sdb()
    if sdb:
        try:
            sdb.upsert_pipeline_run(session_id, hesap_key, marketplace, "agent1", "completed")
            sdb.update_agent_status_detail("agent1", "completed", {
                "tasks": result.get("basarili", 0) + result.get("basarisiz", 0) + result.get("atlanan", 0),
                "duration": result.get("sure", "?"),
                "errors_7d": result.get("basarisiz", 0),
            })
        except: pass

    === PATTERN: Agent 1 HATA verdiginde ===
    state_manager.update_agent_status(..., "failed", ...) satirindan HEMEN SONRA:

    _save_log("error", f"Agent 1 hatasi: {error_msg[:200]}",
              "agent1", hesap_key, marketplace, session_id,
              error_type="AgentFailure")
    sdb = _get_sdb()
    if sdb:
        try:
            sdb.upsert_pipeline_run(session_id, hesap_key, marketplace, "agent1", "failed", str(error_msg)[:500])
            sdb.update_agent_status_detail("agent1", "failed")
        except: pass

    === AYNI PATTERN'I AGENT 2, 3, 4 ICIN DE UYGULA ===
    Her agent icin 3 nokta: baslangic, basari, hata.
    Agent adi ve result alanlari agent'a gore degisir.

    Agent 2 icin: agent_name="agent2", adim="agent2"
    Agent 3 execute icin: agent_name="agent3", adim="agent3_execute" 
    Agent 3 verify icin: agent_name="agent3", adim="agent3_verify"
    Agent 4 icin: agent_name="agent4", adim="agent4"

    === PATTERN: Pipeline TAMAMLANDIGINDA ===
    Tum agentlar bittikten sonra, pipeline_tamamlandi veya session tamamlandi noktasinda:

    _save_log("info", f"Pipeline tamamlandi: {hesap_key}/{marketplace}",
              "maestro", hesap_key, marketplace, session_id)
    sdb = _get_sdb()
    if sdb:
        try:
            sdb.upsert_pipeline_run(session_id, hesap_key, marketplace, "completed", "completed")
        except: pass

    === PATTERN: Excel onay bekleme basladiginda ===
    E-posta gonderilip onay beklenmeye basladiginda:

    _save_log("info", "Excel onay bekleniyor — e-posta gonderildi",
              "maestro", hesap_key, marketplace, session_id)
    sdb = _get_sdb()
    if sdb:
        try:
            sdb.upsert_pipeline_run(session_id, hesap_key, marketplace, "waiting_approval", "running")
        except: pass

    === PATTERN: Excel onayi alindiginda ===

    _save_log("info", "Excel onayi alindi — Agent 3 basliyor",
              "maestro", hesap_key, marketplace, session_id)

2d) ONEMLI: Her agent fonksiyonunda hesap_key, marketplace ve session_id
    degiskenlerinin mevcut oldugundan emin ol. Maestro'da bunlar genellikle
    fonksiyon parametreleri veya config.CURRENT_ACCOUNT'tan geliyor.
    
    session_id icin: state["current_session"]["session_id"] veya
    session olusturulurken kayit edilen degisken.
"""

# ================================================================
# ADIM 3: db_client.py — Eksik metodlari kontrol et ve ekle
# ================================================================
"""
supabase/db_client.py dosyasini oku.

Asagidaki metodlarin VAR OLDUGUNDAN emin ol. Yoksa ekle:

3a) upsert_pipeline_run(session_id, hesap_key, mp, step, status, error_msg=None):
    Kontrol: hasattr(SupabaseClient, 'upsert_pipeline_run')

    YOKSA EKLE:
    def upsert_pipeline_run(self, session_id, hesap_key, mp, step, status, error_msg=None):
        \"\"\"Pipeline adim durumunu gunceller veya olusturur.\"\"\"
        # Once mevcut kayit var mi bak
        conn = self._conn()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT id FROM pipeline_runs WHERE session_id=%s AND hesap_key=%s AND marketplace=%s",
                (session_id, hesap_key, mp))
            existing = cur.fetchone()
            
            if existing:
                # Guncelle
                update_parts = ["current_step=%s", "status=%s", "updated_at=NOW()"]
                params = [step, status]
                if error_msg:
                    update_parts.append("error_message=%s")
                    params.append(str(error_msg)[:500])
                # Agent completed_at zamanlari
                if status == "completed":
                    if step == "agent1": update_parts.append("agent1_completed_at=NOW()")
                    elif step == "agent2": update_parts.append("agent2_completed_at=NOW()")
                    elif step in ("agent3", "agent3_execute", "agent3_verify"): update_parts.append("agent3_completed_at=NOW()")
                    elif step == "agent4": update_parts.append("agent4_completed_at=NOW()")
                params.append(existing[0])
                cur.execute(f"UPDATE pipeline_runs SET {', '.join(update_parts)} WHERE id=%s", params)
            else:
                # Yeni kayit
                cur.execute(
                    \"\"\"INSERT INTO pipeline_runs (session_id, hesap_key, marketplace,
                        current_step, status, error_message)
                       VALUES (%s, %s, %s, %s, %s, %s)\"\"\",
                    (session_id, hesap_key, mp, step, status,
                     str(error_msg)[:500] if error_msg else None))
            cur.close()
        except Exception as e:
            logger.error("pipeline_runs hatasi: %s", e)
        finally:
            conn.close()

3b) update_agent_status_detail(agent_name, status, health_detail=None):
    Kontrol: hasattr(SupabaseClient, 'update_agent_status_detail')

    YOKSA EKLE:
    def update_agent_status_detail(self, agent_name, status, health_detail=None):
        \"\"\"agent_status tablosundaki agent durumunu gunceller.\"\"\"
        try:
            update_fields = "status=%s, last_run_at=NOW()"
            params = [status]
            if health_detail:
                import json as _json
                # Mevcut health_detail'i oku, last_3_runs'a ekle
                update_fields += ", health_detail=%s, health_score=%s"
                params.append(_json.dumps(health_detail))
                # Basit saglik skoru hesapla
                errors = health_detail.get("errors_7d", 0)
                score = max(50, 100 - errors * 5)
                params.append(score)
            params.append(agent_name)
            self._execute(f"UPDATE agent_status SET {update_fields} WHERE agent_name=%s", params)
        except Exception as e:
            logger.error("agent_status guncelleme hatasi: %s", e)
"""

# ================================================================
# ADIM 4: lynor-dashboard — Auto-refresh (polling)
# ================================================================
"""
BU ADIM lynor-dashboard KLASORUNDE UYGULANACAK.

4a) src/app/page.tsx (Genel Bakis) dosyasini oku.
    useEffect icindeki load() fonksiyonunu bul.

    Mevcut useEffect'e auto-refresh ekle:
    
    MEVCUT:
    useEffect(() => {
      let cancelled = false;
      async function load() { ... }
      load();
      return () => { cancelled = true; };
    }, [dateRange, customMode, dateFrom, dateTo, selA, ...]);

    YENI (load() cagrisindan SONRA, return'den ONCE):
    
    // Auto-refresh: her 30 saniyede bir guncelle
    const interval = setInterval(() => {
      if (!cancelled) load();
    }, 30000);
    
    return () => { cancelled = true; clearInterval(interval); };

4b) src/app/agents/page.tsx dosyasini oku.
    Ayni sekilde useEffect'e auto-refresh ekle (30 saniye).

4c) src/app/logs/page.tsx dosyasini oku.
    Ayni sekilde useEffect'e auto-refresh ekle (15 saniye — loglar daha sik).

4d) Build kontrol:
    cd lynor-dashboard && npx next build
"""

# ================================================================
# DOGRULAMA
# ================================================================
"""
amazon-ppc-automation klasorunde:

1. log_utils.py'da save_log fonksiyonu var mi:
   python -c "from log_utils import save_log; print('save_log OK')"

2. db_client metodlari var mi:
   python -c "
   from supabase.db_client import SupabaseClient
   db = SupabaseClient()
   assert hasattr(db, 'upsert_pipeline_run'), 'upsert_pipeline_run eksik!'
   assert hasattr(db, 'update_agent_status_detail'), 'update_agent_status_detail eksik!'
   print('db_client metodlari OK')
   "

3. maestro syntax kontrolu:
   python -c "import py_compile; py_compile.compile('maestro/maestro_agent.py', doraise=True); print('maestro OK')"

4. save_log test (Supabase'e yazabilme):
   python -c "
   from log_utils import save_log
   save_log('info', 'Test log - canli dashboard', 'maestro', 'vigowood_na', 'US', 'TEST_SESSION')
   print('Test log yazildi — dashboard /logs sayfasini kontrol et')
   "

lynor-dashboard klasorunde:
5. Build kontrolu:
   npx next build
"""
