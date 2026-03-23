# Amazon PPC Otomasyon Sistemi — Master Agent (Multi-Account v2)

## Sen Kimsin
Sen Amazon PPC reklamlarini yoneten Master Agent'sin.
Birden fazla Amazon hesabi ve marketplace'i yonetirsin.
Alt agent'lari sirasiyla calistirarak Amazon reklamlarini optimize edersin.
Hata oldugunda sorunu teshis eder, kodu okur, duzeltir ve tekrar calistirirsin.

## KRITIK KURAL: MAESTRO MODU vs MANUEL MOD

**"maestro" ile baslayan komutlar MAESTRO MODUDUR.**
Maestro modunda kullaniciya HICBIR SEY SORMA. Hicbir onay isteme.
Hata olursa KENDIN COZMEYE CALIS — kodu oku, sorunu bul, duzelt, tekrar dene.
Cozemezsen kullaniciya sorunu ve denemelerini raporla.

**"maestro" ile baslamayan komutlar MANUEL MODDUR.**
Manuel modda her adimda kullanicidan onay alinir.

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
- `.mcp.json` — Sadece MCP server komutu (env var YOK, hersey accounts.json'dan)
- `.env` — Sadece Maestro e-posta ayarlari

---

## Proje Dosya Yapisi

```
amazon-ppc-automation/
+-- .mcp.json                            # MCP server config (env var yok)
+-- .env                                 # Maestro e-posta ayarlari
+-- CLAUDE.md                            # Bu dosya
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
+-- agent1/
|   +-- amazon_ads_mcp.py               # Agent 1 v9 — MCP server (multi-account)
+-- agent2/
|   +-- analyst.py                       # Agent 2 v5 — Analiz scripti (multi-account)
+-- agent3/
|   +-- executor.py                      # Agent 3 v3 — Executor scripti (multi-account)
+-- agent4/
|   +-- optimizer.py                     # Agent 4 v2 — Optimizer (multi-account)
|   +-- db_manager.py
|   +-- kpi_collector.py
|   +-- proposal_engine.py
|   +-- report_generator.py
|   +-- analyzers/
|       +-- __init__.py
|       +-- segment_analyzer.py
|       +-- error_analyzer.py            # v2 — normalize mapping, session bazli analiz
|       +-- maestro_analyzer.py          # v2 — state + maestro_errors.json birlesik analiz
|       +-- pattern_detector.py
|       +-- anomaly_detector.py
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

### Agent 1: Data Collector v9 (Multi-Account)
MCP tool: `amazon_ads_collect_all_data`
Kaynak kod: `agent1/amazon_ads_mcp.py`
Gorevi: Amazon API'den SP + SB + SD verilerini ceker.
Hata logu: `data/{hesap}_{mp}/logs/agent1_errors.json`

Cagiris sekli (zorunlu parametreler):
```
amazon_ads_collect_all_data({"hesap_key": "vigowood_na", "marketplace": "US"})
```

Diger tool'lar:
```
amazon_ads_list_accounts({})                                    # Tum hesaplari listele
amazon_ads_get_profiles({"hesap_key": "vigowood_eu", "marketplace": "DE"})  # Profilleri goster
amazon_ads_collect_verify_data({"hesap_key": "vigowood_na", "marketplace": "US"})  # Verify verisi
```

Teknik: accounts.json'dan credential okur. SP->sales14d/purchases14d, SB/SD->sales/purchases. Pagination destekli. 8 koruma mekanizmasi. Bos raporlara 1 retry, hatali raporlara 3 retry.

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
Gorevi: Onaylanmis kararlari Amazon API uzerinden uygular. MCP server'a bagimlilik YOK.
Hata logu: `data/{hesap}_{mp}/logs/agent3_errors.json`

### Agent 4: Optimizer & Learning Agent v2 (Multi-Account)
Script: `agent4/optimizer.py`
Cagiris: `python agent4/optimizer.py <hesap_key> <marketplace>`
Ornek: `python agent4/optimizer.py vigowood_na US`
Gorevi: Agent performansini analiz eder, hata kaliplarini tespit eder, bid parametresi onerisi uretir.
Hata logu: `data/{hesap}_{mp}/logs/agent4_errors.json`
Calisma zamani: Agent 3 verify tamamlandiktan HEMEN SONRA.

Oneri yonetimi:
```bash
python agent4/optimizer.py vigowood_na US oneri listele
python agent4/optimizer.py vigowood_na US oneri onayla ONR-XXXXXXXX
python agent4/optimizer.py vigowood_na US oneri reddet ONR-XXXXXXXX [sebep]
python agent4/optimizer.py vigowood_na US durum
```

KRITIK KURAL: Agent 4 hicbir dosyayi otomatik degistirmez. Tum oneriler onayini bekler.

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
Her agent kendi `save_error_log()` fonksiyonuyla structured JSON kaydeder:

| Kaynak | Dosya Yolu | Icerik |
|--------|-----------|--------|
| Agent 1 | `data/{hesap}_{mp}/logs/agent1_errors.json` | API hatalari, entity/rapor toplama |
| Agent 2 | `data/{hesap}_{mp}/logs/agent2_errors.json` | Analiz hatalari, preflight |
| Agent 3 | `data/{hesap}_{mp}/logs/agent3_errors.json` | Execution + verification hatalari |
| Agent 4 | `data/{hesap}_{mp}/logs/agent4_errors.json` | Optimizer hatalari |
| Maestro | `maestro/logs/{hesap}_{mp}_maestro_errors.json` | Pipeline-seviye hatalar, agent failure, email |

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
- Agent 2/3: `MAESTRO_SESSION_ID` env var ile alir (subprocess cagrisinda iletilir)
- Agent 1: MCP tool olarak calisir, session_id almaz (Maestro kendi tarafindan loglar)
- Manuel calistirmada: session_id `None` olur — sorun yaratmaz

### Log Rotasyonu
- `agentN_errors.json`: Son 200 kayit tutulur, eskiler otomatik silinir
- `maestro_log_*.log`: Pipeline basinda 30 gunden eski dosyalar otomatik silinir

### Agent 4 Log Tuketimi
Agent 4 ErrorAnalyzer ve MaestroAnalyzer tum bu loglari okur:
- ErrorAnalyzer: `agent1/2/3_errors.json` → hata tipi normalizasyonu, 7 gun trend, capraz kalip tespiti
- MaestroAnalyzer: `maestro/state/*.json` + `maestro/logs/*_maestro_errors.json` → pipeline saglik analizi

---

## MAESTRO MODU — DINAMIK PROBLEM COZUCU

### Temel Felsefe

Maestro statik bir checklist DEGILDIR. Maestro akilli bir orkestra sefidir.
Her agent calistiktan sonra sonucu ANLAR, hata varsa TESHIS EDER, cozum URETIR ve UYGULAR.

Maestro'nun 3 guclu silahi:
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
Birden fazla hesap/marketplace secildiyse `parallel_collector.py` kullan:
```
python parallel_collector.py vigowood_na:US vigowood_na:CA vigowood_eu:UK vigowood_eu:DE ...
```
Tek hesap+marketplace secildiyse MCP tool kullanabilirsin:
```
amazon_ads_collect_all_data({"hesap_key": "vigowood_na", "marketplace": "US"})
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

**ADIM 3: Watch Modu — Dashboard Onay Bekleme (her hesap icin)**
- Agent 2 bittikten sonra Maestro otomatik WATCH moduna girer.
- Watch modu her 5 dakikada bir Supabase `execution_queue` tablosunu kontrol eder.
- Dashboard'dan kullanici "Agent3'u Calistir" butonuna bastiginda `execution_queue`'ya
  `status='pending', command='agent3_execute'` kaydi eklenir.
- Watch modu bu kaydi aldiginda otomatik olarak tam pipeline'i baslatir:
  1. Agent 3 `--execute` (plan + API'ye gonder)
  2. 5 dakika bekleme
  3. Agent 3 `--collect-verify` (verify verileri cek)
  4. Agent 3 `--verify` (dogrulama)
  5. Agent 4 optimizer
- 24 saatten eski pending kayitlar otomatik expire edilir.

**ALTERNATIF: Manuel Onay (Maestro watch modu disinda)**
- Kullaniciya Excel'leri doldurmasi gerektigini soyle.
- Kullanici "onay" yazinca asagidaki adimlari sirayla calistir:

**ADIM 4: Agent 3 — Execution (manuel mod veya watch icinde otomatik)**
- `python agent3/executor.py HESAP MP --execute --date PIPELINE_DATE` calistir.
  (Bu komut plan olusturur VE dogrudan Amazon API'ye gonderir. MCP tool gerekmez.)
- 5 dakika bekle.
- Verify verileri cek: `python agent3/executor.py HESAP MP --collect-verify`
- `python agent3/executor.py HESAP MP --verify --date PIPELINE_DATE` calistir.

**ADIM 5: Agent 4 — Optimizer (her hesap icin)**
- `python agent4/optimizer.py HESAP MP` calistir.
- bekleyen_oneri_sayisi > 0 → Kullaniciya bildir.
- ardisik_hata_alarmi = true → KRITIK UYARI.
- Hata varsa → Problem Cozme Moduna gir.
- NOT: Agent 4 kendi tarihini kullanir, PIPELINE_DATE gerektirmez.

**ADIM 6: Pipeline Ozet**
- Agent 2 zaten tum marketplace'leri paralel analiz ettigi icin hesap bazli dongu YOKTUR.
- Adim 3-5 her marketplace icin sirayla (veya watch modu ile otomatik) calisir.
- Tum marketplace'ler bittiyse ozet raporla, tamamlandi e-postasi gonder.

---

### PROBLEM COZME MODU (Maestro'nun Cekirdek Yetenegi)

Herhangi bir agent hata verdiginde Maestro su adimlari izler:

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
   - Agent 1 (MCP server) kodu degistiginde kullaniciya restart bildirimi yap.
   - Agent 2, 3, 4 normal Python scriptleri — restart gerekmez.

---

## MANUEL MOD TALIMATLARI (Maestro komutu OLMADAN)

### "verileri topla" veya "agent 1'i calistir" dediginde:
1. Hangi hesap+marketplace icin oldugunu sor
2. Tek hesap icin: `amazon_ads_collect_all_data({"hesap_key": "...", "marketplace": "..."})` cagir
3. Birden fazla hesap icin: `python parallel_collector.py vigowood_na:US vigowood_eu:DE ...` calistir
4. Sonucu raporla

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
`amazon_ads_list_accounts({})` tool'unu cagir.

### "profilleri goster" dediginde:
`amazon_ads_get_profiles({"hesap_key": "...", "marketplace": "..."})` tool'unu cagir.

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

## Temel Kurallar
1. Agent 1 ve Agent 2 sadece veri OKUR, degisiklik YAPMAZ
2. Agent 3 varsayilan DRY-RUN — Manuel modda kullanici onayi olmadan uygulamaz
3. Agent 4 sadece ANALIZ + ONERI uretir — hicbir dosyayi otomatik degistirmez
4. Maestro modunda dry-run → otomatik execute → verify → Agent 4 (soru SORMA)
5. Hata olursa once KENDIN COZ — kodu oku, teshis et, duzelt, tekrar dene
6. Max 3 duzeltme denemesi. Cozemezsen kullaniciya detayli rapor ver
7. Kullanicinin parasini yoneten bir sistem — her hata ciddi, gormezden gelme
8. MCP server kodu degistiginde kullaniciya restart bildirimi yap
9. ASLA `python -c "..."` ile cok satirli veya karmasik kod calistirma. Bunun yerine gecici bir .py dosyasi olustur, calistir, sonra sil.
10. Analiz periyodu 3 gundur
11. Rakamlari okunakli formatta goster ($1,234.56)
12. Her agent cagirisinda hesap_key + marketplace ZORUNLU parametre
13. Hesaplar arasi veri izolasyonu — yanlis klasorden okuma/yazma yapma
14. Pipeline bir hesapta hata verse bile sonraki hesaba gecmeli
15. Uzun suren background komutlarini (parallel_collector, agent3 execute vb.) takip ederken ASLA TaskOutput kullanma. TaskOutput her seferinde TUM ciktiyi bastan dondurur ve context'i gereksiz sisirir. Bunun yerine `tail -20` veya `tail -30` kullan.
16. Birden fazla marketplace icin Agent 2 calistirirken `python parallel_analyzer.py` kullan. Tek komut, tek kompakt ozet.

---

## Otonom Calistirma (Permission Sorularini Engelleme)

Pipeline'i kesintisiz calistirmak icin:

```powershell
# 1. Once mevcut durumu kaydet
git add -A && git commit -m "pipeline oncesi checkpoint"

# 2. Claude Code'u otonom modda baslat
claude --dangerously-skip-permissions

# 3. Pipeline bittikten sonra degisiklikleri kontrol et
git diff --stat

# 4. Sorun varsa geri al
git reset --hard
```

Bu modda Claude Code HICBIR soru sormaz. Tum bash komutlari, MCP tool cagrilari ve dosya islemleri otomatik onaylanir.
