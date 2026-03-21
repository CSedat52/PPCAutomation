# SB Targets + SB Themes Pipeline Fix — Claude Code Talimatı

## ÖZET

Bu talimat pipeline'daki 4 sorunu tek seferde düzeltiyor:

**Sorun A — SB Targets (Kampanya 1):** `sb_targets.json` dosyası Agent 1 tarafından toplanıyor ama `build_targeting_lookup()` sadece SP ve SD targets okuyor. SB ASIN targeting kampanyaları (ör: "DYKOS - SB - ASIN Target 1") bid güncellemesi yapamıyor.

**Sorun B — SB Themes (Kampanya 2):** Amazon SB theme-based targeting (`keywords-related-to-your-brand`, `keywords-related-to-your-landing-pages`) tamamen ayrı bir API endpoint'i kullanıyor (`/sb/themes`). Pipeline bu entity tipini hiç bilmiyor — ne toplanıyor ne de yönetiliyor.

**Sorun C — DATA_DIR Parse:** Agent 3 Supabase fonksiyonlarında `hesap_key` ve `marketplace` bilgisi `DATA_DIR.name.rsplit("_", 1)` ile parse ediliyor. Kırılgan yaklaşım, global değişkenlere taşınmalı.

**Sorun D — MCP Bağlantı Kopması:** Uzun pipeline'larda (44dk+ veri toplama) MCP server idle kalıyor ve bağlantı kopuyor. Execution aşamasında MCP tool çağrılamıyor, geçici script yazmak zorunda kalınıyor. Çözüm: `executor.py --apply` ile doğrudan API execution, MCP bağımlılığı kaldır.

---

## DEĞİŞİKLİK LİSTESİ (10 madde, 5 dosya)

### SB Targets + SB Themes (Kampanya 1 & 2 fix)
| # | Dosya | Fonksiyon/Bölüm | Değişiklik |
|---|-------|-----------------|------------|
| 1 | `parallel_collector.py` | entity collection listesi | `sb_themes` collection ekle |
| 2 | `agent1/amazon_ads_mcp.py` | entity collection listesi | `sb_themes` collection ekle |
| 3 | `agent1/amazon_ads_mcp.py` | `WRITE_ENDPOINTS` | `sb_target_bid_update` ve `sb_theme_bid_update` ekle |
| 4 | `agent1/amazon_ads_mcp.py` | verify collection | `sb_themes` + `sb_targets` verify collection ekle |
| 5 | `agent3/executor.py` | `build_targeting_lookup()` | `sb_targets.json` ve `sb_themes.json` oku |
| 6 | `agent3/executor.py` | `prepare_bid_change()` | SB TARGET ve THEME entity payload case'leri ekle |
| 7 | `agent3/executor.py` | `load_verify_actual_data()` | `sb_themes` verify parse ekle |
| 8 | `agent1/amazon_ads_mcp.py` | `execute_plan` rollback | `themeId` entity_type tespiti ekle |

### Pipeline Yapısal Düzeltmeler
| # | Dosya | Sorun | Değişiklik |
|---|-------|-------|------------|
| 9 | `agent3/executor.py` | DATA_DIR parse kırılgan | `HESAP_KEY` ve `MARKETPLACE` global'lere taşı |
| 11 | `agent3/executor.py` + `CLAUDE.md` | MCP bağlantı kopması | `--apply` flag: doğrudan API execution, MCP bağımlılığı kaldır |

---

## DEĞİŞİKLİK 1: parallel_collector.py — SB Themes Collection

Entity collection listesinde `sb_negative_keywords` satırından SONRA şu satırı ekle:

```python
# SB Themes (theme-based targeting: keywords-related-to-brand/landing-pages)
collect_list_throttled("sb_themes", "/sb/themes/list", ["enabled", "paused"], "themes",
                       content_type="application/json",
                       accept="application/vnd.sbthemeslistresponse.v3+json",
                       custom_body={"stateFilter": {"include": ["enabled", "paused"]}, "maxResults": 100}),
```

NOT: `sb_targets` zaten mevcut, dokunma.

---

## DEĞİŞİKLİK 2: agent1/amazon_ads_mcp.py — SB Themes Collection

Entity collection listesinde `sb_negative_keywords` satırından SONRA aynı şekilde ekle:

```python
# SB Themes
collect_list_throttled("sb_themes", "/sb/themes/list",
                       ["enabled", "paused"], "themes",
                       content_type="application/json",
                       accept="application/vnd.sbthemeslistresponse.v3+json",
                       custom_body={
                           "stateFilter": {"include": ["enabled", "paused"]},
                           "maxResults": 100
                       }),
```

Ayrıca entity_map ve Supabase sync listelerine de `sb_themes` eklenecekse ekle (isteğe bağlı, öncelik değil).

---

## DEĞİŞİKLİK 3: agent1/amazon_ads_mcp.py — WRITE_ENDPOINTS

`WRITE_ENDPOINTS` dict'ine şu iki endpoint'i ekle (mevcut `sd_target_bid_update` satırından SONRA):

```python
"sb_target_bid_update": {
    "method": "PUT", "path": "/sb/targets",
    "content_type": "application/json",
    "accept": "*/*",
    "wrapper_key": None,
},
"sb_theme_bid_update": {
    "method": "PUT", "path": "/sb/themes",
    "content_type": "application/json",
    "accept": "application/vnd.sbthemesupdateresponse.v3+json",
    "wrapper_key": "themes",
},
```

KRITIK: `sb_theme_bid_update` wrapper_key = "themes". API'ye giden payload şu formatta olmalı:
```json
{"themes": [{"themeId": "...", "campaignId": "...", "adGroupId": "...", "state": "enabled", "bid": 1.5}]}
```

`sb_target_bid_update` wrapper_key = None. API'ye array gider:
```json
[{"targetId": "...", "campaignId": "...", "adGroupId": "...", "state": "enabled", "bid": 1.5}]
```

---

## DEĞİŞİKLİK 4: agent1/amazon_ads_mcp.py — Verify Collection

`amazon_ads_collect_verify_data` fonksiyonunda, `sb_kws` (SB keywords) satırlarından SONRA şu blokları ekle:

```python
# SB targets (SB ASIN targeting dogrulama icin)
sb_targets = await collect_verify("sb_targets", "/sb/targets/list",
                                    ["enabled", "paused"], "targets",
                                    content_type="application/json",
                                    accept=SB_CONTENT_TYPES["targets"],
                                    custom_body={
                                        "filters": [{"filterType": "TARGETING_STATE",
                                                     "values": ["enabled", "paused"]}],
                                        "maxResults": 100
                                    })
R["sb_target_sayisi"] = len(sb_targets)

# SB themes (theme-based targeting dogrulama icin)
sb_themes = await collect_verify("sb_themes", "/sb/themes/list",
                                   ["enabled", "paused"], "themes",
                                   content_type="application/json",
                                   accept="application/vnd.sbthemeslistresponse.v3+json",
                                   custom_body={
                                       "stateFilter": {"include": ["enabled", "paused"]},
                                       "maxResults": 100
                                   })
R["sb_theme_sayisi"] = len(sb_themes)
```

---

## DEĞİŞİKLİK 5: agent3/executor.py — build_targeting_lookup()

### 5A: SB Targets'ı TARGET loop'una ekle

Mevcut:
```python
for prefix, ad_type, id_field in [
    ("sp_targets", "SP", "targetId"),
    ("sd_targets", "SD", "targetId"),
]:
```

Yeni:
```python
for prefix, ad_type, id_field in [
    ("sp_targets", "SP", "targetId"),
    ("sb_targets", "SB", "targetId"),
    ("sd_targets", "SD", "targetId"),
]:
```

### 5B: Expression parse — SB farkını ele al

Aynı loop'un içindeki expression parse kısmında, SB targets `expressions` (çoğul) kullanıyor, SP/SD `expression` (tekil) kullanıyor.

Mevcut satırı bul:
```python
expression = e.get("expression", e.get("targetingExpression", ""))
```

Şununla değiştir:
```python
expression = e.get("expression", e.get("expressions", e.get("targetingExpression", "")))
```

### 5C: SB Themes için yeni bir blok ekle

TARGET loop'undan SONRA, fonksiyonun sonuna (return lookup'tan ÖNCE) şu bloğu ekle:

```python
# ---- SB THEME ENTITY'LER ----
THEME_TYPE_TO_REPORT = {
    "KEYWORDS_RELATED_TO_YOUR_BRAND": "keywords-related-to-your-brand",
    "KEYWORDS_RELATED_TO_YOUR_LANDING_PAGES": "keywords-related-to-your-landing-pages",
}

sb_themes_path = DATA_DIR / f"{today}_sb_themes.json"
if sb_themes_path.exists():
    with open(sb_themes_path, "r", encoding="utf-8") as f:
        themes = json.load(f)
    for t in themes:
        tid = str(t.get("themeId", ""))
        cid = str(t.get("campaignId", ""))
        agid = str(t.get("adGroupId", ""))
        theme_type = t.get("themeType", "")
        report_text = THEME_TYPE_TO_REPORT.get(theme_type, theme_type.lower())

        entity_info = {
            "entity_id": tid,
            "entity_type": "THEME",
            "ad_type": "SB",
            "campaign_id": cid,
            "ad_group_id": agid,
            "state": t.get("state", "enabled"),
            "bid": t.get("bid", 0),
        }

        # Birincil key: (campaign_id, report_text, "THEME")
        key = (cid, report_text, "THEME")
        lookup[key] = entity_info
        # Ikincil key: TARGETING ile de bulunabilsin
        key2 = (cid, report_text, "TARGETING")
        if key2 not in lookup:
            lookup[key2] = entity_info

    logger.info("SB Themes: %d theme entity yuklendi", len(themes))
```

---

## DEĞİŞİKLİK 6: agent3/executor.py — prepare_bid_change()

### 6A: SB TARGET case'i ekle

Mevcut `elif entity_type == "TARGET":` bloğunda SD'den sonra SB ekle:

```python
elif entity_type == "TARGET":
    if ad_type == "SP":
        result["api_endpoint"] = "sp_target_bid_update"
        result["api_payload"] = {
            "targetId": entity["entity_id"],
            "bid": round(yeni_bid, 2),
        }
    elif ad_type == "SB":
        result["api_endpoint"] = "sb_target_bid_update"
        sb_state = entity.get("state", "enabled").lower()
        result["api_payload"] = {
            "targetId": entity["entity_id"],
            "campaignId": entity["campaign_id"],
            "adGroupId": entity["ad_group_id"],
            "state": sb_state,
            "bid": round(yeni_bid, 2),
        }
    elif ad_type == "SD":
        result["api_endpoint"] = "sd_target_bid_update"
        result["api_payload"] = {
            "targetId": entity["entity_id"],
            "bid": round(yeni_bid, 2),
        }
```

### 6B: THEME case'i ekle

TARGET bloğundan SONRA yeni bir elif ekle:

```python
elif entity_type == "THEME":
    result["api_endpoint"] = "sb_theme_bid_update"
    sb_state = entity.get("state", "enabled").lower()
    result["api_payload"] = {
        "themeId": entity["entity_id"],
        "campaignId": entity["campaign_id"],
        "adGroupId": entity["ad_group_id"],
        "state": sb_state,
        "bid": round(yeni_bid, 2),
    }
```

---

## DEĞİŞİKLİK 7: agent3/executor.py — load_verify_actual_data()

### 7A: SB Targets

SD targets bloğundan SONRA şu bloğu ekle (mevcut `actual["targets"]` dict'ine ekleniyor, SP/SD targets ile aynı yapı):

NOT: SB targets zaten "targets" dict'ine eklenecek çünkü entity_type = TARGET. Ama targetId çakışma riski yok (SP/SB/SD target ID'leri farklı).

Aslında bu ekleme gerekmeyebilir — `sb_targets.json` verify dosyası `collect_verify` ile zaten toplanıyor (Değişiklik 4). `load_verify_actual_data()` mevcut kodda sadece `sp_targets` ve `sd_targets` okuyor, `sb_targets` da eklenmeli:

```python
# SB Targets
sb_tgt_path = DATA_DIR / f"{prefix}sb_targets.json"
if sb_tgt_path.exists():
    try:
        with open(sb_tgt_path, "r", encoding="utf-8") as f:
            for t in json.load(f):
                tid = str(t.get("targetId", ""))
                if tid:
                    actual["targets"][tid] = {"bid": t.get("bid", 0)}
    except (json.JSONDecodeError, IOError):
        pass
```

### 7B: SB Themes

`actual` dict'ine yeni bir key ekle. Önce fonksiyonun başındaki `actual = {...}` dict'ine ekle:

```python
actual = {
    "keywords": {},
    "targets": {},
    "themes": {},          # ← YENİ
    "negative_keywords": {},
    "negative_targets": {},
    "campaigns": {},
}
```

Sonra fonksiyonun sonuna (campaigns bloğundan sonra) ekle:

```python
# SB Themes
theme_path = DATA_DIR / f"{prefix}sb_themes.json"
if theme_path.exists():
    try:
        with open(theme_path, "r", encoding="utf-8") as f:
            for t in json.load(f):
                tid = str(t.get("themeId", ""))
                if tid:
                    actual["themes"][tid] = {"bid": t.get("bid", 0)}
    except (json.JSONDecodeError, IOError):
        pass
```

### 7C: process_verification_results() — THEME doğrulama

`process_verification_results()` fonksiyonunda bid doğrulama bölümünde (bölüm 1), `source` seçimini güncellemek gerekiyor. Mevcut:

```python
source = keywords if entity_type == "KEYWORD" else targets
```

Yeni:

```python
if entity_type == "KEYWORD":
    source = keywords
elif entity_type == "THEME":
    source = actual_data.get("themes", {})
else:
    source = targets
```

---

## DEĞİŞİKLİK 8 (EK): Rollback log'da ad_type tespiti

`amazon_ads_mcp.py`'deki `execute_plan` fonksiyonunda rollback kaydı oluştururken `_ad_type` tespiti yapılıyor. Mevcut:

```python
if ep_name.startswith("sp_"):
    _ad_type = "SP"
elif ep_name.startswith("sb_"):
    _ad_type = "SB"
elif ep_name.startswith("sd_"):
    _ad_type = "SD"
```

Bu zaten SB'yi kapsıyor. Ama `_entity_type` tespiti sadece `keywordId` ve `targetId` kontrol ediyor, `themeId` eklenmeli:

```python
if "keywordId" in payload:
    _entity_id = str(payload["keywordId"])
    _entity_type = "KEYWORD"
elif "targetId" in payload:
    _entity_id = str(payload["targetId"])
    _entity_type = "TARGET"
elif "themeId" in payload:
    _entity_id = str(payload["themeId"])
    _entity_type = "THEME"
```

---

## DEĞİŞİKLİK 9: Agent 3 — DATA_DIR parse fix'ini kaldır, global'e taşı

### Sorun
Supabase fonksiyonlarında `hesap_key` ve `marketplace` bilgisi `DATA_DIR.name.rsplit("_", 1)` ile parse ediliyor. Bu kırılgan — ileride farklı isimlendirme olursa patlar.

### Çözüm
`init_paths()` fonksiyonunda zaten `hesap_key` ve `marketplace` parametreleri geliyor. Bunları modül seviyesinde global olarak sakla.

**executor.py — Global değişkenler (satır ~99):**

Mevcut:
```python
DATA_DIR = None
ANALYSIS_DIR = None
LOG_DIR = None
CONFIG_DIR = None
```

Yeni:
```python
DATA_DIR = None
ANALYSIS_DIR = None
LOG_DIR = None
CONFIG_DIR = None
HESAP_KEY = None
MARKETPLACE = None
```

**executor.py — init_paths() fonksiyonu (satır ~108):**

Mevcut:
```python
def init_paths(hesap_key, marketplace):
    global DATA_DIR, ANALYSIS_DIR, LOG_DIR, CONFIG_DIR
```

Yeni:
```python
def init_paths(hesap_key, marketplace):
    global DATA_DIR, ANALYSIS_DIR, LOG_DIR, CONFIG_DIR, HESAP_KEY, MARKETPLACE
    HESAP_KEY = hesap_key
    MARKETPLACE = marketplace
```

**executor.py — 3 Supabase fonksiyonundaki DATA_DIR parse bloklarını değiştir:**

Her yerde şu blok var (satır ~308, ~427, ~579):
```python
dir_name = DATA_DIR.name if DATA_DIR else ""
parts = dir_name.rsplit("_", 1)
hk = parts[0] if len(parts) == 2 else ""
mp = parts[1] if len(parts) == 2 else ""
```

Hepsini şununla değiştir:
```python
hk = HESAP_KEY or ""
mp = MARKETPLACE or ""
```

---

## DEĞİŞİKLİK 10: executor.py — Doğrudan API Execution (`--apply` flag)

### Sorun
`executor.py --execute` sadece execution plan dosyası oluşturuyor. Plan'ı Amazon API'ye göndermek için MCP tool (`amazon_ads_execute_plan`) gerekiyor. Ama uzun pipeline'larda (44dk+ veri toplama) MCP server bağlantısı kopuyor ve execution yapılamıyor. Claude Code geçici script yazmak zorunda kalıyor.

### Kök Neden
`parallel_collector.py` uzun süre bash subprocess olarak çalışırken MCP server idle kalıyor. Claude Code'un MCP bağlantısı timeout'a uğruyor. Bu Claude Code'un bilinen bir davranışı — uzun idle dönemlerinde MCP stdio bağlantısı kaybediliyor.

### Çözüm
`executor.py`'ye `--apply` flag'i ekle. Plan oluşturulduktan sonra, aynı process içinde doğrudan Amazon API'ye bağlanıp işlemleri göndersin. MCP'ye bağımlılık kalksin. Tıpkı `parallel_collector.py`'nin veri toplama için MCP kullanmaması gibi.

### Uygulama

**11A: executor.py — `apply_execution_plan()` fonksiyonu ekle**

`_run_executor_impl()` fonksiyonundan SONRA, yeni bir fonksiyon ekle:

```python
async def apply_execution_plan(hesap_key, marketplace, plan_path):
    """
    Execution plan dosyasini okuyup dogrudan Amazon API'ye gonderir.
    MCP server'a bagimlilik olmadan calisir.
    parallel_collector.py'deki AmazonAdsClient'i kullanir.
    """
    import asyncio
    sys.path.insert(0, str(BASE_DIR))
    from parallel_collector import AmazonAdsClient, load_accounts

    accounts = load_accounts()
    lwa = accounts["lwa_app"]
    hesap = accounts["hesaplar"][hesap_key]
    mp_config = hesap["marketplaces"][marketplace]

    config = {
        "client_id": lwa["client_id"],
        "client_secret": lwa["client_secret"],
        "refresh_token": hesap["refresh_token"],
        "marketplace": marketplace,
        "profile_id": mp_config["profile_id"],
        "account_id": hesap["account_id"],
        "api_endpoint": hesap["api_endpoint"],
        "token_endpoint": hesap["token_endpoint"],
    }
    client = AmazonAdsClient(config)

    with open(plan_path, "r", encoding="utf-8") as f:
        plan = json.load(f)

    # Plan dosya adindan tarihi cikar
    fname = Path(plan_path).name
    today = fname[:10] if len(fname) >= 10 and fname[4] == "-" else datetime.utcnow().strftime("%Y-%m-%d")

    # WRITE_ENDPOINTS — executor icinde tanimla (amazon_ads_mcp'ye bagimlilik olmasin)
    WRITE_ENDPOINTS = {
        "sp_keyword_bid_update": {"method": "PUT", "path": "/sp/keywords",
            "content_type": "application/vnd.spKeyword.v3+json",
            "accept": "application/vnd.spKeyword.v3+json", "wrapper_key": "keywords"},
        "sp_target_bid_update": {"method": "PUT", "path": "/sp/targets",
            "content_type": "application/vnd.spTargetingClause.v3+json",
            "accept": "application/vnd.spTargetingClause.v3+json", "wrapper_key": "targetingClauses"},
        "sb_keyword_bid_update": {"method": "PUT", "path": "/sb/keywords",
            "content_type": "application/json", "accept": "*/*", "wrapper_key": None},
        "sb_target_bid_update": {"method": "PUT", "path": "/sb/targets",
            "content_type": "application/json", "accept": "*/*", "wrapper_key": None},
        "sb_theme_bid_update": {"method": "PUT", "path": "/sb/themes",
            "content_type": "application/json",
            "accept": "application/vnd.sbthemesupdateresponse.v3+json", "wrapper_key": "themes"},
        "sd_target_bid_update": {"method": "PUT", "path": "/sd/targets",
            "content_type": "application/json", "accept": "application/json", "wrapper_key": None},
        "sp_negative_keyword_add": {"method": "POST", "path": "/sp/negativeKeywords",
            "content_type": "application/vnd.spNegativeKeyword.v3+json",
            "accept": "application/vnd.spNegativeKeyword.v3+json", "wrapper_key": "negativeKeywords"},
        "sp_campaign_negative_keyword_add": {"method": "POST", "path": "/sp/campaignNegativeKeywords",
            "content_type": "application/vnd.spCampaignNegativeKeyword.v3+json",
            "accept": "application/vnd.spCampaignNegativeKeyword.v3+json", "wrapper_key": "campaignNegativeKeywords"},
        "sp_negative_target_add": {"method": "POST", "path": "/sp/negativeTargets",
            "content_type": "application/vnd.spNegativeTargetingClause.v3+json",
            "accept": "application/vnd.spNegativeTargetingClause.v3+json", "wrapper_key": "negativeTargetingClauses"},
    }

    results = {
        "tarih": today, "durum": "BASLATILDI",
        "bid_sonuclari": [], "negatif_sonuclari": [], "hatalar": [],
        "rollback_log": [],
        "ozet": {"basarili": 0, "basarisiz": 0, "atlanan": 0},
    }

    EXECUTE_DELAY = 0.3  # Islemler arasi bekleme (saniye)

    async def execute_single(ep_name, payload):
        ep = WRITE_ENDPOINTS.get(ep_name)
        if not ep:
            return False, {}, f"Bilinmeyen endpoint: {ep_name}"
        wrapper = ep["wrapper_key"]
        body = {wrapper: [payload]} if wrapper else [payload]
        try:
            resp = await client._request_with_retry(
                ep["method"], ep["path"], body,
                content_type=ep["content_type"], accept=ep["accept"]
            )
            # Hata kontrolu
            if wrapper and isinstance(resp, dict):
                inner = resp.get(wrapper, resp)
                if isinstance(inner, dict) and inner.get("error"):
                    return False, resp, f"API error: {json.dumps(inner['error'])[:500]}"
                return True, resp, None
            if isinstance(resp, list):
                for item in resp:
                    if isinstance(item, dict) and "code" in item:
                        if str(item.get("code", "")).upper() not in ("SUCCESS", "200"):
                            return False, resp, f"API error: {json.dumps(item)[:500]}"
                return True, resp, None
            return True, resp, None
        except Exception as e:
            return False, {}, f"Exception: {str(e)[:300]}"

    # --- BID DEGISIKLIKLERI ---
    logger.info("--- Faz 1: Bid Degisiklikleri (%d islem) ---", len(plan.get("bid_islemleri", [])))
    for op in plan.get("bid_islemleri", []):
        if op.get("status") != "HAZIR":
            results["ozet"]["atlanan"] += 1
            continue

        ep_name = op.get("api_endpoint", "")
        payload = op.get("api_payload", {})
        success, resp, error = await execute_single(ep_name, payload)

        if success:
            results["ozet"]["basarili"] += 1
            # Entity bilgileri
            _entity_id, _entity_type, _ad_type = "", "", ""
            if "keywordId" in payload: _entity_id, _entity_type = str(payload["keywordId"]), "KEYWORD"
            elif "targetId" in payload: _entity_id, _entity_type = str(payload["targetId"]), "TARGET"
            elif "themeId" in payload: _entity_id, _entity_type = str(payload["themeId"]), "THEME"
            if ep_name.startswith("sp_"): _ad_type = "SP"
            elif ep_name.startswith("sb_"): _ad_type = "SB"
            elif ep_name.startswith("sd_"): _ad_type = "SD"

            results["rollback_log"].append({
                "tip": "BID_DEGISIKLIGI",
                "kampanya": op.get("kampanya", ""), "hedefleme": op.get("hedefleme", ""),
                "eski_bid": op.get("eski_bid"), "yeni_bid": op.get("yeni_bid"),
                "entity_id": _entity_id, "entity_type": _entity_type,
                "ad_type": _ad_type, "campaign_id": str(payload.get("campaignId", "")),
                "api_endpoint": ep_name, "api_payload": payload,
                "rollback": f"Bid'i {op.get('eski_bid')} olarak geri al",
            })
            logger.info("  BASARILI: %s — %s (%.2f)", op.get("kampanya",""), op.get("hedefleme",""), payload.get("bid",0))
        else:
            results["ozet"]["basarisiz"] += 1
            results["hatalar"].append({"faz": "BID", "hata": error})
            logger.error("  BASARISIZ: %s — %s", op.get("hedefleme",""), error)

        await asyncio.sleep(EXECUTE_DELAY)

    # --- NEGATIF EKLEMELER ---
    logger.info("--- Faz 2: Negatif Eklemeler (%d islem) ---", len(plan.get("negatif_islemleri", [])))
    for op in plan.get("negatif_islemleri", []):
        if op.get("status") != "HAZIR":
            results["ozet"]["atlanan"] += 1
            continue

        ep_name = op.get("api_endpoint", "")
        payload = op.get("api_payload", {})
        success, resp, error = await execute_single(ep_name, payload)

        if success:
            results["ozet"]["basarili"] += 1
            results["rollback_log"].append({
                "tip": op.get("tip", "NEGATIF"),
                "kampanya": op.get("kampanya", ""), "hedefleme": op.get("hedefleme", ""),
                "campaign_id": str(payload.get("campaignId", "")),
                "api_endpoint": ep_name, "api_payload": payload,
            })
            logger.info("  BASARILI: %s — %s", op.get("kampanya",""), op.get("hedefleme",""))
        else:
            results["ozet"]["basarisiz"] += 1
            results["hatalar"].append({"faz": "NEGATIF", "hata": error})
            logger.error("  BASARISIZ: %s — %s", op.get("hedefleme",""), error)

        await asyncio.sleep(EXECUTE_DELAY)

    # --- ROLLBACK LOG KAYDET ---
    logs_dir = LOG_DIR
    logs_dir.mkdir(parents=True, exist_ok=True)
    rollback = {
        "tarih": today,
        "olusturma_zamani": datetime.utcnow().isoformat(),
        "islemler": results["rollback_log"],
    }
    rollback_path = logs_dir / f"{today}_rollback.json"
    with open(rollback_path, "w", encoding="utf-8") as f:
        json.dump(rollback, f, indent=2, ensure_ascii=False)

    results["durum"] = "TAMAMLANDI"
    results["rollback_dosyasi"] = str(rollback_path)

    ozet = results["ozet"]
    logger.info("=== EXECUTION TAMAMLANDI: %d basarili, %d basarisiz, %d atlanan ===",
                ozet["basarili"], ozet["basarisiz"], ozet["atlanan"])

    await client.close()
    return results
```

**11B: executor.py — CLI'da `--apply` desteği ekle (satır ~2839):**

Mevcut:
```python
else:
    force = "--execute" in sys.argv
    result = run_executor(hesap_key, marketplace, today=custom_date, force_execute=force)
    print(json.dumps(result, indent=2, ensure_ascii=False))
```

Yeni:
```python
else:
    force = "--execute" in sys.argv
    apply_direct = "--apply" in sys.argv
    result = run_executor(hesap_key, marketplace, today=custom_date, force_execute=force)
    print(json.dumps(result, indent=2, ensure_ascii=False))

    # --apply: Plan olusturulduktan sonra dogrudan Amazon API'ye gonder
    if apply_direct and force:
        plan_path = result.get("plan_dosyasi")
        if plan_path and Path(plan_path).exists():
            import asyncio
            logger.info("=== APPLY: Execution plan Amazon API'ye gonderiliyor ===")
            apply_result = asyncio.run(apply_execution_plan(hesap_key, marketplace, plan_path))
            print(json.dumps(apply_result, indent=2, ensure_ascii=False))
        else:
            logger.error("Plan dosyasi bulunamadi, --apply uygulanamadi")
```

**11C: CLAUDE.md güncellemesi**

ADIM 4'teki talimatı güncelle. Mevcut MCP tool çağrısı yerine:

```
# Eski yontem (MCP bagimli):
# amazon_ads_execute_plan({"hesap_key": "HESAP", "marketplace": "MP", "plan_file": "..."})

# Yeni yontem (MCP bagimsiz):
python agent3/executor.py HESAP MP --execute --apply --date PIPELINE_DATE
```

Bu tek komutla hem plan oluşur hem doğrudan Amazon API'ye gönderilir.

Maestro modunda artık `execute_plan` MCP tool'una gerek yok. Tüm execution `executor.py --execute --apply` ile yapılır.

MCP tool (`amazon_ads_execute_plan`) olduğu gibi kalır — manuel mod ve tek işlem için kullanılabilir.

**11D: Verify için de aynı yaklaşım**

Verify verileri de MCP tool ile toplanıyordu. Aynı mantıkla `executor.py --verify` moduna doğrudan API'den veri çekme yeteneği ekle:

```python
async def collect_verify_data(hesap_key, marketplace, data_date):
    """Verify icin gereken entity verilerini dogrudan Amazon API'den ceker."""
    # parallel_collector'dan AmazonAdsClient import et
    # SP keywords, SP targets, SB keywords, SB targets, SB themes,
    # SD targets, SP neg keywords, SP neg targets topla
    # {data_date}_verify_{entity}.json olarak kaydet
    ...
```

Bu fonksiyon `amazon_ads_collect_verify_data` MCP tool'unun yaptığının aynısını yapar ama MCP'ye bağımlı olmadan.

---


## TEST PLANI

Değişiklikler yapıldıktan sonra doğrulama:

1. **Agent 1 entity toplama testi:**
   ```
   python parallel_collector.py vigowood_eu:UK
   ```
   Kontrol: `data/vigowood_eu_UK/` altında `{tarih}_sb_themes.json` dosyası oluşmalı.

2. **Agent 3 dry-run testi:**
   ```
   python agent3/executor.py vigowood_eu UK
   ```
   Kontrol: Daha önce "entity bulunamadı" hatası veren 4 satırın artık HAZIR status'te olması lazım:
   - B07FPR2SPC (SB ASIN Target) → sb_target_bid_update
   - B07TXSWR33 (SB ASIN Target) → sb_target_bid_update
   - B00ENNKTMO (SB ASIN Target) → sb_target_bid_update
   - keywords-related-to-your-landing-pages (SB Theme) → sb_theme_bid_update

3. **Agent 3 --execute --apply testi (tek hesap):**
   ```
   python agent3/executor.py vigowood_eu UK --execute --apply --date 2026-03-21
   ```
   Kontrol: Plan oluşturulduktan sonra doğrudan Amazon API'ye gönderilmeli. MCP tool çağrısı gerekmemeli.

4. **DATA_DIR fix doğrulama:**
   Agent 3 çalıştırıldığında Supabase hataları olmamalı. `HESAP_KEY` ve `MARKETPLACE` global'lerden okunmalı.

5. **MCP server restart gerekli:** `amazon_ads_mcp.py` değiştiği için MCP server restart edilmeli.

---


## ÖNCELİK SIRASI

1. **Değişiklik 9** — DATA_DIR global fix (5 dakika, 3 yerde find-replace)
2. **Değişiklik 3** — WRITE_ENDPOINTS (sb_target + sb_theme endpoint tanımları)
3. **Değişiklik 5** — build_targeting_lookup (sb_targets + sb_themes entity resolution)
4. **Değişiklik 6** — prepare_bid_change (SB TARGET + THEME payload case'leri)
5. **Değişiklik 10** — executor.py --apply (doğrudan API execution, MCP bağımlılığı kaldır)
6. **Değişiklik 1+2** — parallel_collector + amazon_ads_mcp collection (sb_themes toplama)
7. **Değişiklik 4+7+8** — verify + rollback (doğrulama desteği)

---

---


## DİKKAT EDİLECEK NOKTALAR

- SB API'de keyword/target PUT payload'ları array formatında gider (`[{...}]`), wrapper_key = None
- SB Themes PUT payload'u wrapper ile gider: `{"themes": [{...}]}`, wrapper_key = "themes"
- SB targets'ta expression key'i `expressions` (çoğul, array), SP/SD'de `expression` (tekil)
- SB targets'ta expression type `asinSameAs`, raporda format `asin="B07FPR2SPC"` — `_extract_value_from_targeting()` tırnaklar arasını çıkarır ve lowercase yapar → `b07fpr2spc` — entity lookup'taki value da lowercase
- Theme targeting raporda `targetingType: "THEME"`, `matchType: "THEME"`, `keywordText: "keywords-related-to-your-landing-pages"` olarak geliyor
- `keywords-related-to-your-brand` şu an 0 tıklama (UK'da), bid tavsiyesi üretilmeyebilir — sorun değil
