# Amazon PPC Otomasyon Sistemi

3 hesap, 12 marketplace icin Amazon PPC kampanyalarini yoneten otomasyon sistemi.
Agent 1-3 saf Python ($0), Agent 4 Asama 2 Claude Code (~$0.01-0.02/calisma).

## Hesaplar

| Hesap Key | Marketplace'ler |
|-----------|-----------------|
| vigowood_na | US, CA |
| vigowood_eu | UK, DE, FR, ES, IT, SE, PL, NL |
| qmmp_na | US, CA |

Veri izolasyonu: `data/{hesap_key}_{marketplace}/`, `config/{hesap_key}_{marketplace}/`
Hesaplar arasi veri ASLA karistirilmaz.

## Dosya Yapisi

```
amazon-ppc-automation/
+-- pipeline_runner.py              # Cron giris noktasi (Agent 1+2 orkestrasyonu, $0)
+-- parallel_collector.py           # Agent 1 — Amazon API veri toplama (tum MP'ler paralel)
+-- parallel_analyzer.py            # Agent 2 — paralel analiz orkestrator
+-- data_loader.py                  # Supabase-first, JSON-fallback veri yukleme
+-- log_utils.py                    # Ortak hata taksonomisi + log fonksiyonlari
+-- config/
|   +-- accounts.json               # Amazon API credential'lari (git'e COMMITLENMEZ)
+-- agent2/analyst.py               # Agent 2 — segmentasyon, tanh bid hesaplama, Excel rapor
+-- agent3/executor.py              # Agent 3 — Amazon API execution + decision_history yazma
+-- agent4/
|   +-- optimizer.py                # Agent 4 orkestrator (4 adim)
|   +-- kpi_collector.py            # decision_history kpi_after doldurma (targeting_reports'tan)
|   +-- bid_param_analyzer.py       # Gap closure regresyon (scipy curve_fit)
|   +-- proposal_engine.py          # proposals_system tablosuna yazar + CLI
|   +-- report_generator.py         # agent4_analysis.json + error_data.json + konsol ozeti
|   +-- analyzers/
|       +-- error_analyzer.py       # agent_logs'dan hata kalip tespiti
|       +-- maestro_analyzer.py     # pipeline_runs'dan pipeline saglik analizi
+-- maestro/
|   +-- maestro_agent.py            # Watch daemon + Agent 3/4 orkestrator
|   +-- config.py                   # init_account(), get_active_pipelines()
|   +-- state_manager.py, email_handler.py, retry_handler.py
+-- supabase/
|   +-- db_client.py                # PostgreSQL client (psycopg2)
|   +-- migrations/                 # Schema migration dosyalari (001-005)
+-- data/{hesap}_{mp}/
    +-- *.json                      # Agent 1 ciktilari
    +-- analysis/                   # Agent 2 Excel raporlari
    +-- agent4/
        +-- agent4_analysis.json    # Agent 4 analiz ciktisi
        +-- agent4_error_data.json  # Claude Code Phase 2 girdi dosyasi
```

## Konfigurasyon

Tum ayarlar Supabase'den okunur (JSON fallback yok):
- `settings` tablosu: ASIN hedefleri, esik degerleri, segmentasyon kurallari
- `bid_functions` tablosu: tanh formul parametreleri, segment parametreleri
- `accounts.json`: Amazon API credential'lari (lokal dosya, Supabase'e tasinmaz)
- `.env`: Maestro e-posta ayarlari (GMAIL_ADDRESS, APP_PASSWORD, NOTIFY_EMAIL)

## Pipeline Akisi

```
Cron (02:00 UTC, 3 gunde bir)
  |
  +-> pipeline_runner.py
        |-> parallel_collector.py (Agent 1) — Amazon API'den veri ceker
        |-> parallel_analyzer.py (Agent 2) — Analiz + Excel rapor uretir
        |-> Eksik rapor varsa retry
        +-> CIK — watch daemon devralir

Watch Daemon (systemd: ppc-watcher.service, 7/24 calisiyor)
  |
  +-> execution_queue tablosunu dinler (5 dk aralik)
        |-> Dashboard'dan onay gelince:
              |-> Agent 3 — Amazon API'ye uygular + decision_history'ye yazar ($0)
              |-> Agent 4 Asama 1 — Python analiz ($0)
              |     [1/4] KPICollector: decision_history kpi_after doldurur
              |     [2/4] ErrorAnalyzer: agent_logs hata kalip tespiti
              |     [3/4] MaestroAnalyzer: pipeline saglik analizi
              |     [4/4] BidParamAnalyzer: gap closure regresyon
              +-> Agent 4 Asama 2 — Claude Code hata analizi (~$0.01-0.02)
```

maestro_agent.py CLI: `watch [dakika]`, `status [hesap mp]`, `accounts`

## Bid Param Optimizasyon Dongusu

Bid formulu (Agent 2):
```
bid_degisim = -tanh(acos_fark_orani × hassasiyet) × max_degisim
acos_fark_orani = (mevcut_acos - hedef_acos) / hedef_acos
```

Veri akisi:
```
Agent 3 bid uygular → decision_history (status=APPLIED, before metrikleri)
    ↓ (3 gun sonra)
Agent 1 yeni targeting_reports toplar
    ↓
Agent 4 KPICollector → kpi_after doldurur, gap_closure hesaplar (status=VERIFIED)
    ↓ (20+ veri noktasi biriktikten sonra)
Agent 4 BidParamAnalyzer → (ASIN × targeting_type) bazinda regresyon
    ↓
scipy curve_fit: ideal_bid_degisim = -tanh(x × h_optimal) × m_optimal
    ↓
bid_param_regressions + regression_data_points tablolarina yazar
    ↓
Dashboard: regresyon grafigi (noktalar + mevcut egri + optimal egri)
```

Targeting tipleri: KEYWORD (SP/SB keyword) ve PRODUCT_TARGET (SP/SB/SD target)
Her ASIN icin iki ayri regresyon yapilir.

## Agent 4 Asama 2 — Claude Code Gorevi

Watch daemon tarafindan otomatik cagirilir. Tek gorev:
1. `agent4_error_data.json` dosyasini oku
2. Tekrarlayan hata kaliplarini analiz et, kok neden tespiti yap
3. Cozum onerileri uret (config degisikligi, retry ayari, timeout vb.)
4. Onerileri `proposals_system` tablosuna PENDING olarak yaz
5. Bid param optimizasyonu YAPMA — Python tarafindan yapiliyor
6. Bitince CIK

## Kritik Kurallar

1. **Agent 3 guvenlik**: Agent 3 SADECE watch daemon uzerinden tetiklenir. ASLA dogrudan calistirma.
2. **Veri izolasyonu**: Her agent cagirisinda hesap_key + marketplace ZORUNLU.
3. **Maliyet**: Agent 1-3 saf Python ($0). Claude Code SADECE Agent 4 Asama 2.
4. **Hata yonetimi**: Agent 4 Claude Code basarisiz olursa pipeline DURMAZ.
5. **Onay**: Agent 3 "BOS" donerse hata DEGIL — kullanici henuz onay vermemis.
6. **Log**: Tum hatalar Supabase `agent_logs` tablosuna yazilir (JSON log yok).

## Hata Taksonomisi

Tum agentlar ayni tipleri kullanir: RateLimit, AuthError, ApiError, ServerError,
NetworkError, FileNotFound, DataError, Preflight, ExecutionError, VerificationError,
AgentFailure, ReportFailed, InternalError.

## VPS Bilgileri

- Sunucu: Hetzner CPX22, Ubuntu 24.04, IP 116.203.46.163
- Kullanici: `ppc` (sudo yok), `root` (systemd islemleri)
- Cron: `0 2 */3 * * cd /home/ppc/amazon-ppc-automation && /usr/bin/python3 pipeline_runner.py`
- Service: `ppc-watcher.service` (auto-restart, boot persistence)
- Firewall: UFW — inbound 22/tcp, outbound 443, 80, 53, 5432
- DB: Supabase PostgreSQL (free tier)
- Dashboard: lynor-dashboard.vercel.app (Next.js/TypeScript)
- GitHub: CSedat52/PPCAutomation, CSedat52/lynor-dashboard
