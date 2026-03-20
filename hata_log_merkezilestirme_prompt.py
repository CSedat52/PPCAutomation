"""
HATA LOG MERKEZILESTIRME — SUPABASE DUAL-WRITE
================================================
Bu dosya amazon-ppc-automation klasorunde Claude Code'a verilecek.

SORUN:
  6 farkli dosyada 6 farkli save_error_log() fonksiyonu var.
  Her biri sadece lokal JSON'a yaziyor (agentX_errors.json).
  Hicbiri Supabase agent_logs tablosuna yazmiyor.
  Dashboard /logs sayfasi agent_logs'u okuyor → hatalar gorunmuyor.

COZUM:
  1. log_utils.py'daki save_error_log()'a Supabase dual-write ekle
  2. Her agent'taki kendi save_error_log'unu sil
  3. Yerine log_utils'tan import + ince wrapper koy (eski cagrilari bozmamak icin)

SONUC:
  42 hata noktasinin HEPSI hem lokale hem Supabase'e yazilir.
  Dashboard'da tum hatalar eksiksiz gorunur.

ONEMLI: Hicbir agent'in save_error_log() CAGRI yerleri degismeyecek.
Sadece fonksiyon TANIMLARI degisecek.
"""

# ================================================================
# ADIM 1: log_utils.py — Supabase dual-write ekle
# ================================================================
"""
log_utils.py dosyasini oku (proje kokunde).

save_error_log fonksiyonuna 2 yeni parametre ekle:
  - hesap_key (str, opsiyonel)
  - marketplace (str, opsiyonel)

Fonksiyonun SONUNA (lokal JSON yazdiktan sonra) Supabase yazma ekle.

MEVCUT imza:
  def save_error_log(hata_tipi, hata_mesaji, log_dir,
                     traceback_str=None, adim=None, extra=None,
                     session_id=None, agent_name=None, max_kayit=200):

YENI imza:
  def save_error_log(hata_tipi, hata_mesaji, log_dir,
                     traceback_str=None, adim=None, extra=None,
                     session_id=None, agent_name=None, max_kayit=200,
                     hesap_key=None, marketplace=None):

Fonksiyonun SONUNA (return True'dan ONCE) su blogu ekle:

    # ---- Supabase dual-write ----
    if hesap_key and agent_name:
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
                (agent_name, "error", str(hata_mesaji)[:500], hata_tipi,
                 hesap_key, marketplace, session_id,
                 str(traceback_str)[:1000] if traceback_str else None)
            )
        except Exception:
            pass  # Supabase yazamazsa lokal zaten yazildi, sessizce devam et

    return True

NOT: lazy import kullaniyoruz cunku log_utils her yerden import ediliyor.
try/except ile sariyoruz cunku Supabase baglantisi olmasa bile lokal log devam etmeli.
"""

# ================================================================
# ADIM 2: agent1/amazon_ads_mcp.py — kendi save_error_log'unu kaldir
# ================================================================
"""
agent1/amazon_ads_mcp.py dosyasini oku.

2a) Dosyanin basindaki import bolumune ekle:
    from log_utils import save_error_log as _central_save_error_log

2b) Dosyadaki "def save_error_log(" tanimini bul ve TAMAMEN su wrapper ile degistir:

def save_error_log(hata_tipi, hata_mesaji, data_dir, traceback_str=None, adim=None,
                   extra=None, session_id=None):
    \"\"\"Agent 1 hata logu — lokal + Supabase dual-write.\"\"\"
    from pathlib import Path
    log_dir = Path(data_dir) / "logs"
    # data_dir'den hesap_key ve marketplace cikar: data/vigowood_eu_UK -> vigowood_eu, UK
    dir_name = Path(data_dir).name  # "vigowood_eu_UK"
    parts = dir_name.rsplit("_", 1)
    hk = parts[0] if len(parts) == 2 else ""
    mp = parts[1] if len(parts) == 2 else ""
    return _central_save_error_log(
        hata_tipi, hata_mesaji, log_dir,
        traceback_str=traceback_str, adim=adim, extra=extra,
        session_id=session_id, agent_name="agent1",
        hesap_key=hk, marketplace=mp)

DIKKAT: Eski fonksiyonun TAMAMINI sil (def'ten sonraki tum body dahil).
Cagri yerleri DEGISMEYECEk — ayni parametre sirasi korunuyor.
"""

# ================================================================
# ADIM 3: parallel_collector.py — kendi save_error_log'unu kaldir
# ================================================================
"""
parallel_collector.py dosyasini oku.

3a) Dosyanin basindaki import bolumune ekle:
    from log_utils import save_error_log as _central_save_error_log

3b) Dosyadaki "def save_error_log(" tanimini bul ve TAMAMEN su wrapper ile degistir:

def save_error_log(data_dir, hata_tipi, hata_mesaji, adim=None, extra=None):
    \"\"\"Parallel collector hata logu — lokal + Supabase dual-write.\"\"\"
    from pathlib import Path
    log_dir = Path(data_dir) / "logs"
    dir_name = Path(data_dir).name
    parts = dir_name.rsplit("_", 1)
    hk = parts[0] if len(parts) == 2 else ""
    mp = parts[1] if len(parts) == 2 else ""
    return _central_save_error_log(
        hata_tipi, hata_mesaji, log_dir,
        adim=adim, extra=extra, agent_name="agent1",
        hesap_key=hk, marketplace=mp)

NOT: Bu dosyada parametre sirasi FARKLI (data_dir basta).
Wrapper bunu koruyor, cagri yerleri degismeyecek.
"""

# ================================================================
# ADIM 4: agent2/analyst.py — kendi save_error_log'unu kaldir
# ================================================================
"""
agent2/analyst.py dosyasini oku.

4a) Dosyanin basindaki import bolumune ekle:
    from log_utils import save_error_log as _central_save_error_log

4b) Bu dosyada data_dir parametre olarak GECMIYOR — global LOG_DIR kullaniliyor.
    Once LOG_DIR'in nasil tanimlandigini bul. Muhtemelen:
      LOG_DIR = data_dir / "logs"  (veya benzer bir path)
    data_dir ise muhtemelen global: BASE_DIR / "data" / f"{hesap_key}_{marketplace}"

    Dosyadaki "def save_error_log(" tanimini bul ve TAMAMEN su wrapper ile degistir:

def save_error_log(hata_tipi, hata_mesaji, traceback_str=None, adim=None,
                   extra=None, session_id=None):
    \"\"\"Agent 2 hata logu — lokal + Supabase dual-write.\"\"\"
    return _central_save_error_log(
        hata_tipi, hata_mesaji, LOG_DIR,
        traceback_str=traceback_str, adim=adim, extra=extra,
        session_id=session_id, agent_name="agent2",
        hesap_key=HESAP_KEY, marketplace=MARKETPLACE)

NOT: HESAP_KEY ve MARKETPLACE global degiskenler — analyst.py'nin basinda
CLI parametrelerinden set ediliyor (sys.argv[1] ve sys.argv[2]).
Bu globallerin ismini dosyadan oku ve wrapper'da kullan.
Eger farkli isimlerle tanimliysa (ornegin hesap_key, marketplace kucuk harf)
o isimleri kullan.
"""

# ================================================================
# ADIM 5: agent3/executor.py — kendi save_error_log'unu kaldir
# ================================================================
"""
agent3/executor.py dosyasini oku.

5a) Dosyanin basindaki import bolumune ekle:
    from log_utils import save_error_log as _central_save_error_log

5b) Agent 2 ile ayni yapilar. Bu dosyada da data_dir global.
    Dosyadaki "def save_error_log(" tanimini bul ve TAMAMEN su wrapper ile degistir:

def save_error_log(hata_tipi, hata_mesaji, traceback_str=None, adim=None,
                   extra=None, session_id=None):
    \"\"\"Agent 3 hata logu — lokal + Supabase dual-write.\"\"\"
    return _central_save_error_log(
        hata_tipi, hata_mesaji, LOG_DIR,
        traceback_str=traceback_str, adim=adim, extra=extra,
        session_id=session_id, agent_name="agent3",
        hesap_key=HESAP_KEY, marketplace=MARKETPLACE)

NOT: Agent 2 ile ayni durum — global degisken isimlerini dosyadan oku.
"""

# ================================================================
# ADIM 6: agent4/optimizer.py — kendi save_error_log'unu kaldir
# ================================================================
"""
agent4/optimizer.py dosyasini oku.

6a) Dosyanin basindaki import bolumune ekle:
    from log_utils import save_error_log as _central_save_error_log

6b) Bu dosyada data_dir parametre olarak GECILIYOR (agent1 gibi).
    Dosyadaki "def save_error_log(" tanimini bul ve TAMAMEN su wrapper ile degistir:

def save_error_log(hata_tipi, hata_mesaji, data_dir, traceback_str=None, adim=None,
                   extra=None, session_id=None):
    \"\"\"Agent 4 hata logu — lokal + Supabase dual-write.\"\"\"
    from pathlib import Path
    log_dir = Path(data_dir) / "logs"
    dir_name = Path(data_dir).name
    parts = dir_name.rsplit("_", 1)
    hk = parts[0] if len(parts) == 2 else ""
    mp = parts[1] if len(parts) == 2 else ""
    return _central_save_error_log(
        hata_tipi, hata_mesaji, log_dir,
        traceback_str=traceback_str, adim=adim, extra=extra,
        session_id=session_id, agent_name="agent4",
        hesap_key=hk, marketplace=mp)
"""

# ================================================================
# ADIM 7: maestro/maestro_agent.py — kendi save_error_log'unu kaldir
# ================================================================
"""
maestro/maestro_agent.py dosyasini oku.

7a) Dosyanin basindaki import bolumune ekle (maestro/ icinden ust dizine cikmali):
    import sys as _sys
    _sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from log_utils import save_error_log as _central_save_error_log

    NOT: Bu path ekleme zaten dosyada mevcut olabilir — kontrol et, tekrar ekleme.

7b) Maestro'nun save_error_log'unda data_dir YOK, log dosyasi farkli yere yaziliyor:
    maestro/logs/{hesap}_{mp}_maestro_errors.json

    Dosyadaki "def save_error_log(" tanimini bul ve TAMAMEN su wrapper ile degistir:

def save_error_log(hata_tipi, hata_mesaji, session_id=None, adim=None,
                   extra=None, traceback_str=None):
    \"\"\"Maestro hata logu — lokal + Supabase dual-write.\"\"\"
    from pathlib import Path
    log_dir = Path(config.LOG_DIR)
    # Maestro config'den hesap bilgisi
    current = config.CURRENT_ACCOUNT or {}
    hk = current.get("hesap_key", "")
    mp = current.get("marketplace", "")
    return _central_save_error_log(
        hata_tipi, hata_mesaji, log_dir,
        traceback_str=traceback_str, adim=adim, extra=extra,
        session_id=session_id, agent_name="maestro",
        hesap_key=hk, marketplace=mp)

DIKKAT: Maestro'da parametre sirasi farkli — session_id 3. sirada,
traceback_str en sonda. Wrapper bu sirayi KORUYOR.
"""

# ================================================================
# ADIM 8: DOGRULAMA
# ================================================================
"""
8a) Import kontrolu — hicbir dosyada syntax hatasi olmadigini dogrula:

    python -c "from log_utils import save_error_log; print('log_utils OK')"
    python -c "import agent1.amazon_ads_mcp; print('agent1 OK')"
    python -c "import parallel_collector; print('parallel_collector OK')"
    python -c "import agent2.analyst; print('agent2 OK')"
    python -c "import agent3.executor; print('agent3 OK')"
    python -c "import agent4.optimizer; print('agent4 OK')"

    NOT: maestro modulu relative import kullandigi icin dogrudan import edilemez.
    Dosyayi okuyup syntax kontrolu yap:
    python -c "import py_compile; py_compile.compile('maestro/maestro_agent.py', doraise=True); print('maestro OK')"

8b) Fonksiyon imza kontrolu — wrapper'larin eski cagrilarla uyumlu oldugunu dogrula:

    python -c "
    import inspect
    from log_utils import save_error_log
    sig = inspect.signature(save_error_log)
    params = list(sig.parameters.keys())
    assert 'hesap_key' in params, 'hesap_key eksik!'
    assert 'marketplace' in params, 'marketplace eksik!'
    assert 'agent_name' in params, 'agent_name eksik!'
    print(f'log_utils.save_error_log params: {params}')
    print('Imza kontrolu OK')
    "

8c) Eski save_error_log tanimlarinin silindigini dogrula:

    grep -rn "def save_error_log" agent1/ agent2/ agent3/ agent4/ maestro/ parallel_collector.py

    BEKLENEN: Her dosyada SADECE wrapper tanimini gormeli (def save_error_log ile baslayan).
    Uzun body'li eski tanimlar OLMAMALI.

8d) Hicbir dosyada "def save_error_log" icerisinde "json.dump" veya 
    "with open" OLMAMALI (lokal yazma artik log_utils'ta):

    grep -A5 "def save_error_log" agent1/amazon_ads_mcp.py agent2/analyst.py agent3/executor.py agent4/optimizer.py maestro/maestro_agent.py parallel_collector.py

    BEKLENEN: Her wrapper 5-10 satirdan fazla OLMAMALI.
    json.dump, with open, kayitlar.append gibi satirlar OLMAMALI.
"""

# ================================================================
# OZET: Degisen dosyalar ve satirlar
# ================================================================
"""
| Dosya                     | Degisiklik                                           |
|---------------------------|------------------------------------------------------|
| log_utils.py              | +15 satir (Supabase dual-write + 2 yeni parametre)   |
| agent1/amazon_ads_mcp.py  | -30 satir eski def, +12 satir wrapper                |
| parallel_collector.py     | -20 satir eski def, +10 satir wrapper                |
| agent2/analyst.py         | -25 satir eski def, +8 satir wrapper                 |
| agent3/executor.py        | -25 satir eski def, +8 satir wrapper                 |
| agent4/optimizer.py       | -25 satir eski def, +12 satir wrapper                |
| maestro/maestro_agent.py  | -25 satir eski def, +10 satir wrapper                |

Toplam: ~150 satir silme, ~75 satir ekleme = net -75 satir
42 hata noktasinin HICBIRI degismiyor — ayni save_error_log() cagrilari calismaya devam ediyor.
"""
