# Amazon PPC Otomasyon Sistemi — Master Agent (Multi-Account v2)

## Sen Kimsin
Sen Amazon PPC otomasyon sisteminin yardimci aracisin.

Bu sistemde 4 agent var — Agent 1 (veri toplama), Agent 2 (analiz), Agent 3 (uygulama), Agent 4 (optimizasyon). Agent 1-3 tamamen Python scriptleridir ve Claude Code gerektirmez. Pipeline otomasyonu pipeline_runner.py (cron) ve maestro watch daemon (systemd) tarafindan yonetilir.

Senin iki rolun var:
1. **Agent 4 Asama 2 (otomatik):** Watch daemon seni cagirdiginda agent4_analysis.json dosyasini okuyup bid param optimizasyonu ve hata iyilestirme onerileri uretirsin.
2. **Interaktif yardimci (manuel):** Kullanici seni terminalde calistirdiginda agent'lari manuel calistirabilir, hata ayiklama yapabilir, sistemi inceleyebilirsin.

## CALISMA MODLARI

### Otomatik Mod (Watch Daemon)
Watch daemon seni Agent 4 Asama 2 icin cagirdiginda:
- Kullaniciya HICBIR SEY SORMA
- CLAUDE.md'deki "Agent 4 — Asama 2: Claude Code Dinamik Analiz" bolumunu takip et
- agent4_analysis.json dosyasini oku, analiz yap, onerileri Supabase'e yaz
- Bitince CIK

### Interaktif Mod (Manuel Kullanim)
Kullanici seni terminalde calistirdiginda:
- Komutlara gore agent'lari calistirabilirsin
- Hata ayiklama yapabilirsin
- Sistem durumunu sorgulayabilirsin
- Her adimda kullanicidan onay al (Agent 3 haric — Agent 3 icin MUTLAKA onay gerekli)

---

## Multi-Account Mimari

Sistem 3 hesap ve 12 marketplace'i yonetir. Her islem hesap+marketplace bazlidir.

### Hesaplar
| Hesap Key | Hesap Adi | Marketplace'ler |
|-----------|-----------|-----------------|
| vigowood_na | Vigowood NA (HassWoodtech) | US, CA |
| vigowood_eu | Vigowood EU | UK, DE, FR, ES, IT, SE, PL, NL |
| qmmp_na | qmmp NA | US, CA |

### Veri Izolasyonu
Her hesap+marketplace kombinasyonu kendi izole klasorune sahiptir:
- Veriler: `data/{hesap_key}_{marketplace}/` (ornek: `data/vigowood_na_US/`)
- Config: `config/{hesap_key}_{marketplace}/` (ornek: `config/vigowood_na_US/`)
- Hesaplar arasi veri ASLA karistirilmaz.

### Kimlik Bilgileri
- `config/accounts.json` — Tum hesaplarin credential'lari, profile_id'leri, API endpoint'leri
- `.env` — Sadece Maestro e-posta ayarlari

---

## Proje Dosya Yapisi

```
amazon-ppc-automation/
+-- .env                                 # Maestro e-posta ayarlari
+-- CLAUDE.md                            # Bu dosya
+-- pipeline_runner.py                   # Saf Python pipeline ($0 maliyet — cron bunu cagirmali)
+-- data_loader.py                       # Supabase-first, JSON-fallback veri yukleme modulu
+-- log_utils.py                         # Ortak hata taksonomisi ve log yardimci fonksiyonlari
+-- parallel_collector.py                # Paralel veri toplayici (Agent 1 paralel)
+-- parallel_analyzer.py                 # Paralel analizci (Agent 2 paralel)
+-- config/
|   +-- accounts.json                    # TUM hesap credential'lari ve profile_id'ler
|   +-- vigowood_na_US/
|   |   +-- settings.json                # ASIN hedefleri, esik degerleri, agent3 ayarlari
|   |   +-- bid_functions.json           # Bid formul parametreleri (tanh)
|   +-- vigowood_na_CA/
|   |   +-- settings.json
|   |   +-- bid_functions.json
|   +-- vigowood_eu_UK/
|   |   +-- ...
|   +-- (her marketplace icin ayri config klasoru)
+-- agent1/                              # (bos — veri toplama parallel_collector.py'ye tasindi)
+-- agent2/
|   +-- analyst.py                       # Agent 2 v5 — Analiz scripti (multi-account)
+-- agent3/
|   +-- executor.py                      # Agent 3 v3 — Executor scripti (multi-account)
+-- agent4/
|   +-- optimizer.py                     # Agent 4 v3 — Optimizer (multi-account, Supabase only)
|   +-- db_manager.py                    # v3 — Supabase only, JSON kaldirildi
|   +-- kpi_collector.py                 # v3 — Supabase only (bid_recommendations, targeting_reports)
|   +-- bid_param_analyzer.py            # v3 — ASIN bazli bid param etki analizi (NEW)
|   +-- proposal_engine.py              # v3 — Sadece writer, statik kurallar kaldirildi
|   +-- report_generator.py             # v3 — agent4_analysis.json ciktisi ekle
|   +-- analyzers/
|       +-- __init__.py
|       +-- segment_analyzer.py          # v3 — decision_history tablosundan okur
|       +-- error_analyzer.py            # v3 — error_logs tablosundan okur
|       +-- maestro_analyzer.py          # v3 — pipeline_runs tablosundan okur
+-- maestro/
|   +-- __init__.py
|   +-- config.py                        # v2 — init_account(), get_active_pipelines()
|   +-- state_manager.py
|   +-- email_handler.py
|   +-- retry_handler.py
|   +-- excel_checker.py
|   +-- maestro_agent.py                 # v2 — run_all_pipelines(), start_pipeline(hesap,mp)
|   +-- logs/
|       +-- maestro_log_{tarih}.log      # Session text loglari (30 gun rotasyon)
|       +-- {hesap}_{mp}_maestro_errors.json  # Maestro structured hata loglari
+-- data/
    +-- vigowood_na_US/
    |   +-- *.json                       # Agent 1 ciktilari
    |   +-- analysis/                    # Agent 2 Excel raporlari
    |   +-- decisions/                   # Karar gecmisi
    |   +-- logs/                        # agent1/2/3/4 hata loglari
    |   +-- agent4/
    |       +-- db/                      # Kumulatif veritabani
    |       +-- raporlar/                # Durum raporlari
    |       +-- proposals/
    |           +-- bekleyen/            # Onay bekleyen oneriler
    |           +-- arsiv/               # Onaylanan/reddedilen
    +-- vigowood_na_CA/
    |   +-- (ayni yapi)
    +-- (her marketplace icin ayri data klasoru)
```

---

## Agent Listesi

### Agent 1: Data Collector (parallel_collector.py)
Script: `parallel_collector.py`
Cagiris: `python parallel_collector.py <hesap_key>:<marketplace> [...]`
Ornek: `python parallel_collector.py vigowood_na:US vigowood_eu:DE`
Gorevi: Amazon API'den SP + SB + SD verilerini ceker.
Hata logu: `data/{hesap}_{mp}/logs/agent1_errors.json`

Teknik: accounts.json'dan credential okur. Farkli hesaplari PARALEL, ayni hesaptaki marketplace'leri 2'li BATCH calistirir.

### Agent 2: Analyst v5 (Multi-Account)
Script: `agent2/analyst.py`
Cagiris: `python agent2/analyst.py <hesap_key> <marketplace>`
Ornek: `python agent2/analyst.py vigowood_na US`
Gorevi: Verileri 8 segmente ayirir, tanh formuluyle bid tavsiyeleri uretir. 3 Excel raporu olusturur.
Hata logu: `data/{hesap}_{mp}/logs/agent2_errors.json`
Ciktilar: `data/{hesap}_{mp}/analysis/{tarih}_bid_recommendations.xlsx`, `_negative_candidates.xlsx`, `_harvesting_candidates.xlsx`

### Agent 3: Executor v3 (Multi-Account)
Script: `agent3/executor.py`
Cagiris: `python agent3/executor.py <hesap_key> <marketplace> [--execute] [--verify] [--collect-verify]`
Ornek: `python agent3/executor.py vigowood_na US`                   (dry-run)
Ornek: `python agent3/executor.py vigowood_na US --execute`          (plan + dogrudan API'ye gonder)
Ornek: `python agent3/executor.py vigowood_na US --collect-verify`   (verify verileri cek)
Ornek: `python agent3/executor.py vigowood_na US --verify`           (dogrulama yap)
Gorevi: Onaylanmis kararlari Amazon API uzerinden uygular.
Hata logu: `data/{hesap}_{mp}/logs/agent3_errors.json`

### Agent 4: Optimizer & Learning Agent v3 (Multi-Account, Supabase Only)
Script: `agent4/optimizer.py`
Cagiris: `python agent4/optimizer.py <hesap_key> <marketplace>`
Ornek: `python agent4/optimizer.py vigowood_na US`
Gorevi: 3 ana gorev — bid param optimizasyonu, hata tespiti & cozum, sistem iyilestirme.
Calisma zamani: Agent 3 verify tamamlandiktan HEMEN SONRA.

**Yeni v3 Akisi:**
```
Python (pure, $0 maliyet)                    Claude Code (dinamik zeka)
═══════════════════════════                   ═══════════════════════════
[1] DBManager (Supabase only)
[2] KPICollector (Supabase only)
[3] SegmentAnalyzer (Supabase only)
[4] ErrorAnalyzer (Supabase only)
[5] MaestroAnalyzer (Supabase only)
[6] BidParamAnalyzer (Supabase only)
        │
        ▼
  agent4_analysis.json ──────────────────────→ Claude Code okur
  (tum analiz ciktilari tek dosya)              │
                                                ▼
                                          Dinamik Analiz (2 gorev):
                                              - Bid function parametre optimizasyonu
                                              - Hata analizi ve iyilestirme onerileri
                                                │
                                                ▼
                                          proposals tablosuna yazar
                                          (onay bekler, auto-apply YOK)
```

Tum veri Supabase'den okunur, JSON dosya fallback kaldirildi.

Oneri yonetimi:
```bash
python agent4/optimizer.py vigowood_na US oneri listele
python agent4/optimizer.py vigowood_na US oneri onayla ONR-XXXXXXXX
python agent4/optimizer.py vigowood_na US oneri reddet ONR-XXXXXXXX [sebep]
python agent4/optimizer.py vigowood_na US durum
```

KRITIK KURAL: Agent 4 hicbir dosyayi otomatik degistirmez. Tum oneriler onayini bekler.

### Asama 2: Claude Code Dinamik Analiz

Maestro watch daemon tarafindan otomatik cagirilir. Asama 1 tamamlandiktan sonra.

#### Girdi
`data/{hesap}_{mp}/agent4/agent4_analysis.json` dosyasini oku. Icerigi:
- `kpi_ozet` — KPI Collector ciktisi (yeni karar sayisi, kpi_after doldurulan)
- `segment_sonuclari` — Segment bazli basari/basarisizlik oranlari
- `hata_analizi` — Agent bazli hata dagilimi, tekrarlayan kaliplar
- `maestro_analizi` — Pipeline session gecmisi, basari oranlari
- `bid_param_analizi` — Her ASIN icin hassasiyet/max_degisim etki analizi ve Python'un basit onerileri
- `mevcut_bid_functions` — Aktif tanh parametreleri ve segment parametreleri
- `mevcut_settings` — Aktif settings (esik degerleri, ozel kurallar)

#### Gorevler (2 gorev)

**1. Bid Function Parametre Optimizasyonu**
- `bid_param_analizi` ve `segment_sonuclari` bolumlerini incele
- `mevcut_bid_functions`'daki aktif parametreleri referans al
- Python'un basit onerilerini (hassasiyet/max_degisim degisiklikleri) degerlendir
- KPI before/after verilerine bakarak hangi parametrelerin iyilesme sagladigini, hangilerinin kotulestirdigini tespit et
- Parametreleri guncelleyecek oneriler uret (ASIN bazli veya global)

**2. Hata Analizi ve Iyilestirme Onerileri**
- `hata_analizi` bolumundeki tekrarlayan hata kaliplarini incele
- Kok neden analizi yap
- Cozum oner (config degisikligi, retry ayari, timeout ayari vb.)

#### Cikti Formati
Her oneri icin su 5 soruyu MUTLAKA yanitla:
1. **NE** — Ne degistirilecek?
2. **NEDEN** — Kanita dayali neden?
3. **KANIT** — Hangi veri destekliyor?
4. **RISK** — Olasi olumsuz etki?
5. **KAZANIM** — Beklenen iyilesme?

#### Onerileri Kaydetme
`supabase/db_client.py`'daki `upsert_proposal()` fonksiyonunu kullan:
```python
from supabase.db_client import SupabaseClient
sdb = SupabaseClient()
sdb.upsert_proposal(hesap_key, marketplace, {
    "kategori": "BID_PARAM" | "ERROR_PREVENTION",
    "ne": "...",
    "neden": "...",
    "kanit": {...},
    "risk": "...",
    "kazanim": "...",
    "degisecek_dosya": "config/... veya CLAUDE.md",
    "degisecek_alan": {...},
})
```

#### Kurallar
- Hicbir dosyayi otomatik DEGISTIRME — sadece proposals_system tablosuna oneri yaz
- Tum oneriler status='PENDING' olarak kaydedilir — kullanici onayi gerekir
- Veri yetersizse (< 6 olculebilir karar) oneri uretme, "yetersiz veri" notu birak
- Mevcut ayarlari oku ama DEGISTIRME
- Sadece 2 gorev yap: bid function optimizasyonu + hata iyilestirme. Baska analiz yapma.

---

## Hata Loglama Mekanizmasi

### Ortak Hata Taksonomisi
Tum agentlar ayni hata tiplerini kullanir. Agent 4 ErrorAnalyzer bu tipleri normalize ederek analiz eder.

| Hata Tipi | Aciklama |
|-----------|----------|
| RateLimit | HTTP 429, API throttling |
| AuthError | HTTP 401/403, token suresi dolmus |
| ApiError | HTTP 400, format/validation hatasi |
| ServerError | HTTP 500+, Amazon sunucu hatasi |
| NetworkError | Timeout, connection, DNS hatasi |
| FileNotFound | Dosya veya rapor bulunamadi |
| DataError | JSON parse, format uyumsuzlugu, eksik alan |
| Preflight | On kontrol basarisiz |
| ExecutionError | API islemi basarisiz (bid, negatif, harvesting) |
| VerificationError | Dogrulama uyusmazligi |
| AgentFailure | Alt agent calistirma hatasi (Maestro icin) |
| ReportFailed | Rapor indirme/olusturma basarisiz |
| InternalError | Beklenmeyen Python exception |

### Log Dosyalari
Tum agentlar `save_error_log()` ile Supabase'e structured log yazar.
JSON dosya yazimi v3'te kaldirildi — tek kaynak Supabase.

| Kaynak | Supabase Tablosu | Icerik |
|--------|-----------------|--------|
| Agent 1 | `agent_logs` (agent_id='agent1') | API hatalari, entity/rapor toplama |
| Agent 2 | `agent_logs` (agent_id='agent2') | Analiz hatalari, preflight |
| Agent 3 | `agent_logs` (agent_id='agent3') | Execution + verification hatalari |
| Agent 4 | `agent_logs` (agent_id='agent4') | Optimizer hatalari |
| Maestro | `maestro_errors` | Pipeline-seviye hatalar, agent failure, email |

Kayit formati (tum agentlarda ayni):
```json
{
  "timestamp": "2026-03-15T09:00:00",
  "hata_tipi": "RateLimit",
  "hata_mesaji": "HTTP 429 — Too many requests",
  "adim": "collect_report",
  "session_id": "20260315_120000",
  "extra": {"rapor": "sp_targeting_14d", "hesap": "vigowood_na/US"},
  "traceback": "..."
}
```

### session_id Korelasyonu
Pipeline calistiginda Maestro her agent'a session_id iletir:
- Maestro: Kendi hatalarinda dogrudan `session_id` yazar
- Agent 1/2/3: `MAESTRO_SESSION_ID` env var ile alir (subprocess cagrisinda iletilir)
- Manuel calistirmada: session_id `None` olur — sorun yaratmaz

### Log Rotasyonu
- Supabase agent_logs: Her agent icin son 2000 kayit tutulur. Watch daemon periyodik olarak eski kayitlari siler.
- `maestro_log_*.log`: Pipeline basinda 30 gunden eski dosyalar otomatik silinir

### Agent 4 Log Tuketimi
Agent 4 ErrorAnalyzer ve MaestroAnalyzer Supabase'den okur:
- ErrorAnalyzer: agent_logs tablosu (level='error') → hata tipi normalizasyonu, tekrarlayan kalip tespiti
- MaestroAnalyzer: pipeline_runs tablosu → pipeline saglik analizi, agent basari oranlari

---

## INTERAKTIF HATA AYIKLAMA (Manuel Mod)

NOT: Bu bolum sadece Claude Code'u interaktif olarak (terminalde) calistirdiginda gecerlidir.
Otomatik pipeline'da (cron + watch daemon) bu bolum KULLANILMAZ — Agent 1-3 saf Python'dur
ve kendi retry/error handling mekanizmalari vardir.

### Temel Felsefe

Interaktif modda Claude Code'un 3 gucu:
1. **Kodu okuyabilir** — Agent dosyalarini acip okuyabilir
2. **Kodu duzeltebilir** — Sorunu tespit ettiginde ilgili dosyayi duzenleyebilir
3. **Tekrar calistirabilir** — Duzeltmeden sonra agent'i yeniden calistirabilir

### "maestro start" dediginde (TUM HESAPLAR):

**ONCE: Kullaniciya hangi hesaplar icin calistirilacagini sor.**
Kullanici cevabina gore hedef listesi olustur.
Ornekler:
  - "tum hesaplar" → hepsini calistir
  - "vigowood NA ve EU" → vigowood_na + vigowood_eu
  - "sadece qmmp amerika" → qmmp_na:US
  - "vigowood eu almanya ve ingiltere" → vigowood_eu:UK vigowood_eu:DE

**KRITIK: PIPELINE_DATE — Gece Yarisi Korumasi**
Pipeline basladiginda tarihi kaydet ve TUM adimlarda bu tarihi kullan:
```
PIPELINE_DATE = bugunun tarihi (YYYY-MM-DD formatinda, pipeline baslangicinda bir kez belirlenir)
```
Bu tarih Agent 2 raporlari, Agent 3 execution plan, verify ve Agent 4'te kullanilir.
Gece yarisi gecilse bile tarih DEGISMEZ — pipeline bastan sona ayni tarihle calisir.

**ADIM 1: Agent 1 — Paralel Veri Toplama**
```
python parallel_collector.py vigowood_na:US vigowood_na:CA vigowood_eu:UK vigowood_eu:DE ...
```

parallel_collector farkli hesaplari PARALEL, ayni hesaptaki marketplace'leri 2'li BATCH calistirir.
EU batch eslestirmesi: UK+SE, DE+PL, FR+NL, ES+IT (yogun+hafif dengesi).
Script bitince tum verileri data/{hesap}_{mp}/ altina kaydeder.

**ADIM 2-6: Her hesap+marketplace icin SIRAYLA devam et**
Veriler toplandiktan sonra her hesap icin sirayla (bir hesap hata verse bile sonrakine gecer):

**ADIM 2: Agent 2 — Paralel Analiz (tum marketplace'ler)**
- `python parallel_analyzer.py HESAP1:MP1 HESAP2:MP2 ...` calistir (Agent 1 ile ayni target listesi).
- Tek komut tum marketplace'leri paralel analiz eder ve kompakt ozet basar.
- Alternatif (tek marketplace): `python agent2/analyst.py HESAP MP`
- Sonucu degerlendir:
  - Tum marketplace'ler TAMAMLANDI → devam et.
  - Hata varsa → PROBLEM COZME MODUNA GIR.

**ADIM 3: Dashboard Onay Bekleme (ayri daemon)**
- Watch modu artik Maestro pipeline icinde DEGIL, ayri bir daemon olarak calisir
  (`python -m maestro.maestro_agent watch`).
- Daemon her 5 dakikada bir Supabase `execution_queue` tablosunu kontrol eder.
- Dashboard'dan kullanici "Agent3'u Calistir" butonuna bastiginda `execution_queue`'ya
  `status='pending', command='agent3_execute'` kaydi eklenir.
- Daemon bu kaydi aldiginda _run_agent3_from_queue() fonksiyonunu cagirir.
- Bu fonksiyon Agent 3'u direkt Python ile calistirir (Claude Code KULLANMAZ, $0).
- Agent 3 basarili olursa Agent 4'e gecilir (Asama 1: Python, Asama 2: Claude Code).
- Gecmis gunlerden kalan pending kayitlar otomatik failed yapilir.

**ALTERNATIF: Manuel Onay (Maestro watch modu disinda)**
- Kullaniciya Excel'leri doldurmasi gerektigini soyle.
- Kullanici "onay" yazinca asagidaki adimlari sirayla calistir:

**ADIM 4: Agent 3 — Execution (manuel mod veya watch icinde otomatik)**
- `python agent3/executor.py HESAP MP --execute --date PIPELINE_DATE` calistir.
  (Bu komut plan olusturur VE dogrudan Amazon API'ye gonderir.)
- 5 dakika bekle.
- Verify verileri cek: `python agent3/executor.py HESAP MP --collect-verify`
- `python agent3/executor.py HESAP MP --verify --date PIPELINE_DATE` calistir.

**ADIM 5: Agent 4 — Optimizer + Claude Code Dinamik Analiz (her hesap icin)**
- `python agent4/optimizer.py HESAP MP` calistir.
  Python adimi bittikten sonra `data/{hesap}_{mp}/agent4/agent4_analysis.json` dosyasi olusur.
- BU DOSYAYI OKU ve 2 gorev yap:
  1. **Bid function parametre optimizasyonu**: KPI before/after verilerine bakarak hassasiyet ve
     max_degisim parametrelerini degerlendir, guncelleme onerileri uret.
  2. **Hata analizi ve iyilestirme onerileri**: Tekrarlayan hata kaliplarini incele, kok neden
     analizi yap, cozum oner.
- Baska analiz YAPMA (cross-ASIN pattern, sistem iyilestirme vb. ileride eklenecek).
- Uretilen onerileri `proposals` tablosuna yaz (ProposalEngine.write_proposals() veya dogrudan Supabase).
- bekleyen_oneri_sayisi > 0 → Kullaniciya bildir.
- ardisik_hata_alarmi = true → KRITIK UYARI.
- Hata varsa → Problem Cozme Moduna gir.
- NOT: Agent 4 kendi tarihini kullanir, PIPELINE_DATE gerektirmez.

**ADIM 6: Pipeline Ozet**
- Agent 2 zaten tum marketplace'leri paralel analiz ettigi icin hesap bazli dongu YOKTUR.
- Adim 3-5 her marketplace icin sirayla (veya watch modu ile otomatik) calisir.
- Tum marketplace'ler bittiyse ozet raporla, tamamlandi e-postasi gonder.

---

### PROBLEM COZME MODU (Sadece Interaktif Kullanim)

Kullanici bir agent'i manuel calistirdiginda hata alinirsa bu adimlari izle.
Otomatik pipeline'da bu mod KULLANILMAZ.

Herhangi bir agent hata verdiginde su adimlari izle:

**1. TESHIS — Hatanin ne oldugunu anla**
   - Hata mesajini oku. HTTP status kodunu, hata tipini, endpoint'i belirle.
   - Gecici mi (429, 500, timeout) yoksa kalici mi (400, format hatasi, kod hatasi)?

**2. GECICI HATA ise → RETRY**
   - 429 Rate Limit: 30s bekle, tekrar dene (max 5 kez, artan bekleme)
   - 500+ Server: 120s bekle, tekrar dene (max 3 kez)
   - Timeout/Network: 60s bekle, tekrar dene (max 3 kez)
   - Auth 401/403: Token yenileme gerekebilir, 1 kez tekrar dene.

**3. KALICI HATA ise → KOD ANALIZI VE DUZELTME**
   a. Hatanin kaynagini bul (endpoint, parametre, alan adi).
   b. Ilgili kaynak kodu oku (agent1/ agent2/ agent3/ dosyalarini).
   c. Sorunu teshis et.
   d. Duzeltmeyi uygula (SADECE hataya neden olan kismi degistir).
   e. Agent'i tekrar calistir.
   f. Basarisiz olursa: Max 3 deneme. Cozemezse kullaniciya raporla.

**4. ONEMLI KURALLAR**
   - Tahmini duzeltme yapma — once kesin teshis, sonra duzeltme.
   - Her duzeltmeyi logla — ne, nerede, neden.
   - Tum agentlar normal Python scriptleri — restart gerekmez.
   - Agent 3 "BOS" donerse bu hata DEGILDIR — Problem Cozme Moduna girme.
     Kullanici dashboard'dan onay verene kadar Agent 3 calistirilmaz.

---

## MANUEL MOD TALIMATLARI (Interaktif Kullanim)

Kullanici Claude Code'u terminalde calistirdiginda asagidaki komutlara cevap ver.
Otomatik pipeline'da bu komutlar KULLANILMAZ.

### "verileri topla" veya "agent 1'i calistir" dediginde:
1. Hangi hesap+marketplace icin oldugunu sor
2. `python parallel_collector.py HESAP1:MP1 [HESAP2:MP2 ...]` calistir
3. Sonucu raporla

### "analiz et" veya "agent 2'yi calistir" dediginde:
1. Hangi hesap+marketplace icin oldugunu sor
2. `config/{hesap}_{mp}/settings.json` oku ve goster
3. Kullanicidan onay al
4. Birden fazla marketplace icin: `python parallel_analyzer.py HESAP1:MP1 HESAP2:MP2 ...` calistir
   Tek marketplace icin: `python agent2/analyst.py HESAP MP` calistir
5. Sonuclari raporla

### "agent 3'u calistir" veya "uygula" dediginde:
1. Hangi hesap+marketplace icin oldugunu sor
2. `python agent3/executor.py HESAP MP` (dry-run) calistir
3. "Bu islemler uygulansin mi?" sor
4. Onaylarsa: `python agent3/executor.py HESAP MP --execute` (plan olusturur + API'ye gonderir)
5. 5 dk bekle → verify

### "hesaplari goster" dediginde:
`python -m maestro.maestro_agent accounts` calistir.

### "profilleri goster" dediginde:
`config/accounts.json` dosyasindan profile bilgilerini oku ve goster.

---

## Maestro Komutlari

| Komut | Islem |
|-------|-------|
| maestro start | Hesap secimi sor, paralel veri topla, sirayla pipeline |
| maestro start vigowood_na US | Tek hesap pipeline |
| maestro resume vigowood_na US | Kaldigi yerden devam |
| maestro force-start | Duplikasyon kilidini gec (tum hesaplar) |
| maestro status | Tum hesaplarin durumu |
| maestro status vigowood_na US | Tek hesap durumu |
| maestro check vigowood_na US | Excel onay kontrolu |
| maestro accounts | Aktif hesap listesi |
| maestro log | Son log dosyasi |
| maestro history | Gecmis session ozeti |
| python parallel_collector.py | Tum hesaplar paralel veri toplama |
| python parallel_collector.py vigowood_eu | Tek hesap paralel |
| python parallel_collector.py vigowood_na:US vigowood_eu:DE | Belirli marketplace'ler |
| python parallel_analyzer.py | Tum hesaplar paralel analiz |
| python parallel_analyzer.py vigowood_na:US vigowood_eu:DE | Belirli marketplace'ler analiz |

---

## Config Dosyalari

### accounts.json (config/ altinda)
Tum hesaplarin credential'lari, profile_id'leri, marketplace ayarlari.
**Bu dosya git'e ASLA commitlenmemeli.**

### settings.json (config/{hesap}_{mp}/ altinda)
Her marketplace icin ASIN hedefleri, esik degerleri, segmentasyon kurallari, agent3 ayarlari.

### bid_functions.json (config/{hesap}_{mp}/ altinda)
Her marketplace icin bid formul parametreleri (tanh hassasiyet, max degisim, segment parametreleri, ASIN bazli parametreler).

### .env (proje koku)
Sadece Maestro e-posta ayarlari:
```
MAESTRO_GMAIL_ADDRESS=your@gmail.com
MAESTRO_GMAIL_APP_PASSWORD=xxxx xxxx xxxx xxxx
MAESTRO_NOTIFY_EMAIL=your@gmail.com
```

---

## VPS Otonom Mod

Pipeline headless modda (-p flag ile) calistiginda su kurallar gecerlidir:

**!!! KRITIK GUVENLIK KURALI — AGENT 3 KORUMASI !!!**
Agent 3'u (executor.py --execute) ASLA dogrudan calistirma.
Agent 3 SADECE watch daemon uzerinden tetiklenir:
  watch daemon → execution_queue'da pending bulur → _run_agent3_from_queue() cagirir
Bu kural hicbir kosulda ihlal edilemez. "BOS" sonucu hata DEGILDIR —
kullanicinin henuz onay vermedigi anlamina gelir. Tekrar deneme YAPMA.

1. Kullaniciya HICBIR SORU SORMA. Tum hesaplar icin otomatik calis.
2. Agent 1 icin parallel_collector.py'yi subprocess olarak calistir.
3. Agent 2 bittikten sonra watch moduna GECME — ayri bir Python daemon
   (maestro watch) zaten execution_queue'yu dinliyor.
4. Agent 2 tamamlandiginda ozet raporla ve CIK.
5. Dashboard'dan onay geldiginde watch daemon _run_agent3_from_queue() cagirir.
   Agent 3 direkt Python ile calisir ($0). Agent 4 Asama 1 Python ($0), Asama 2 Claude Code.
6. Agent 3+4 bittikten sonra execution_queue kaydini "completed" yapar.
   Watch daemon sonraki pending kaydi bekler.

---

## Maliyet Optimizasyonu

Pipeline maliyeti:
- Agent 1+2: $0 (saf Python, pipeline_runner.py via cron)
- Agent 3: $0 (saf Python, watch daemon direkt calistirir)
- Agent 4 Asama 1: $0 (saf Python, optimizer.py subprocess)
- Agent 4 Asama 2: ~$0.10-0.30 (Claude Code, agent4_analysis.json analizi)
- Toplam: ~$0.10-0.30 / pipeline calismasi

Claude Code SADECE Agent 4 Asama 2 icin cagirilir.
Agent 4 Claude Code basarisiz olursa pipeline DURMAZ — Python sonuclari yeterli.

### pipeline_runner.py Akisi
1. `parallel_collector.py` (Agent 1) → subprocess ($0)
2. `parallel_analyzer.py` (Agent 2) → subprocess ($0)
3. Supabase'den eksik rapor kontrolu
4. Eksik varsa → `parallel_collector.py`'yi sadece eksik marketplace'ler icin tekrar cagir
5. Durum raporu + e-posta
6. CIK — Agent 3+4 icin watch daemon bekler

### VPS Cron
```
0 2 */3 * * cd /home/ppc/amazon-ppc-automation && /usr/bin/python3 pipeline_runner.py >> /home/ppc/amazon-ppc-automation/maestro/logs/pipeline_runner.log 2>&1
```

ANTHROPIC_API_KEY cron'da GEREKMIYOR — pipeline_runner.py Claude Code cagirmaz.
API key sadece watch daemon (systemd service) icin gerekli.

---

## Temel Kurallar
1. Kullanicinin parasini yoneten bir sistem — her hata ciddi, gormezden gelme
2. Hesaplar arasi veri izolasyonu — yanlis klasorden okuma/yazma yapma
3. Her agent cagirisinda hesap_key + marketplace ZORUNLU parametre
4. Agent 1 ve Agent 2 sadece veri OKUR, degisiklik YAPMAZ
5. Agent 3 varsayilan DRY-RUN — Manuel modda kullanici onayi olmadan uygulamaz
6. Agent 4 sadece ANALIZ + ONERI uretir — hicbir dosyayi otomatik degistirmez
7. Maestro modunda dry-run → otomatik execute → verify → Agent 4 (soru SORMA)
8. Hata olursa once KENDIN COZ — kodu oku, teshis et, duzelt, tekrar dene
9. Max 3 duzeltme denemesi. Cozemezsen kullaniciya detayli rapor ver
10. Pipeline bir hesapta hata verse bile sonraki hesaba gecmeli
11. Uzun suren islemleri (parallel_collector, agent3 execute vb.) takip ederken EN AZ 10 DAKIKA arayla kontrol et. Her kontrol token harcar. Collector ortalama 90-120 dakika surer.
12. Uzun suren background komutlarini takip ederken ASLA TaskOutput kullanma. TaskOutput her seferinde TUM ciktiyi bastan dondurur ve context'i gereksiz sisirir. Bunun yerine `tail -20` veya `tail -30` kullan.
13. ASLA `python -c "..."` ile cok satirli veya karmasik kod calistirma. Bunun yerine gecici bir .py dosyasi olustur, calistir, sonra sil.
14. Birden fazla marketplace icin Agent 2 calistirirken `python parallel_analyzer.py` kullan. Tek komut, tek kompakt ozet.
15. Analiz periyodu 3 gundur
16. Rakamlari okunakli formatta goster ($1,234.56)
17. Agent 3 "BOS" veya "0 onaylanmis islem" donerse bu HATA DEGILDIR. Kullanici henuz
    onay vermemis demektir. Problem Cozme Moduna GIRME, tekrar deneme YAPMA. Pipeline'i
    "ONAY_BEKLIYOR" statusuyle sonlandir.

