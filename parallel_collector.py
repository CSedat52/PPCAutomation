"""
Paralel Veri Toplayici — Tum Hesaplar
========================================
Tum hesaplarin Agent 1 verilerini paralel toplar.

Strateji:
  - Farkli hesaplar (vigowood_na, vigowood_eu, qmmp_na) TAMAMEN PARALEL
  - Ayni hesaptaki marketplace'ler de TAMAMEN PARALEL (batch YOK)
    vigowood_na: US + CA ayni anda
    vigowood_eu: UK, DE, FR, ES, IT, SE, PL, NL hepsi ayni anda
    qmmp_na: US + CA ayni anda

Kullanim:
  python parallel_collector.py                                        → tum hesaplar
  python parallel_collector.py vigowood_eu                            → tek hesap
  python parallel_collector.py vigowood_na:US vigowood_eu:DE          → belirli marketplace'ler
  python parallel_collector.py vigowood_na:US vigowood_eu:UK vigowood_eu:DE qmmp_na:US  → karisik

Gereksinimler: pip install httpx --break-system-packages
"""

import os
import sys
import json
import time
import gzip
import asyncio
import logging
from datetime import datetime, timedelta
from pathlib import Path

try:
    import httpx
except ImportError:
    print("httpx gerekli: pip install httpx --break-system-packages")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("parallel_collector")

BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR))
from log_utils import save_error_log as _central_save_error_log, save_log as _save_log


def _dashboard_status(agent_name, status, health_detail=None):
    """Dashboard agent_status tablosunu gunceller."""
    try:
        from supabase.db_client import SupabaseClient
        db = SupabaseClient()
        db.update_agent_status_detail(agent_name, status, health_detail)
    except Exception:
        pass


def _dashboard_pipeline(session_id, hesap_key, marketplace, step, status, error_msg=None):
    """Dashboard pipeline_runs tablosunu gunceller."""
    if not session_id:
        return
    try:
        from supabase.db_client import SupabaseClient
        db = SupabaseClient()
        db.upsert_pipeline_run(session_id, hesap_key, marketplace, step, status, error_msg)
    except Exception:
        pass

# ============================================================================
# ACCOUNTS.JSON
# ============================================================================

def load_accounts():
    for p in [BASE_DIR / "config" / "accounts.json", BASE_DIR / "accounts.json"]:
        if p.exists():
            with open(p, "r", encoding="utf-8") as f:
                return json.load(f)
    raise FileNotFoundError("accounts.json bulunamadi")


def get_data_dir(hesap_key, marketplace):
    d = BASE_DIR / "data" / f"{hesap_key}_{marketplace}"
    d.mkdir(parents=True, exist_ok=True)
    return d


# ============================================================================
# AMAZON ADS CLIENT (agent1 ile ayni mantik)
# ============================================================================

RETRY_MAX = 3
RETRY_RATE_LIMIT_WAIT = 60
RETRY_TIMEOUT_WAIT = 15
REPORT_MAX_WAIT = 1200
REPORT_POLL_INTERVAL = 45     # v12: 90 → 45 sn (1200s'de 27 poll, 429 riski dusuk)
REPORTING_CONTENT_TYPE = "application/vnd.createasyncreportrequest.v3+json"


class AmazonAdsClient:
    def __init__(self, config):
        self.client_id = config["client_id"]
        self.client_secret = config["client_secret"]
        self.refresh_token = config["refresh_token"]
        self.marketplace = config["marketplace"].upper()
        self.profile_id = config["profile_id"]
        self.account_id = config["account_id"]
        self.base_url = config["api_endpoint"]
        self.token_endpoint = config["token_endpoint"]
        self._access_token = None
        self._token_expires_at = 0
        self._http = None

    async def get_http(self):
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(timeout=180)
        return self._http

    async def close(self):
        if self._http and not self._http.is_closed:
            await self._http.aclose()

    async def _ensure_token(self):
        if self._access_token and time.time() < (self._token_expires_at - 60):
            return
        http = await self.get_http()
        resp = await http.post(self.token_endpoint, data={
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        })
        resp.raise_for_status()
        data = resp.json()
        self._access_token = data["access_token"]
        self._token_expires_at = time.time() + data.get("expires_in", 3600)

    async def _headers(self, content_type="application/json", accept="application/json",
                       include_account_id=False):
        await self._ensure_token()
        h = {
            "Authorization": f"Bearer {self._access_token}",
            "Amazon-Advertising-API-ClientId": self.client_id,
            "Content-Type": content_type,
            "Accept": accept,
        }
        if self.profile_id:
            h["Amazon-Advertising-API-Scope"] = self.profile_id
        if include_account_id and self.account_id:
            h["Amazon-Ads-AccountId"] = self.account_id
        return h

    async def _request_with_retry(self, method, endpoint, payload=None,
                                    content_type="application/json", accept="application/json",
                                    include_account_id=False):
        last_error = None
        token_refreshed = False
        for attempt in range(RETRY_MAX + 1):
            try:
                http = await self.get_http()
                headers = await self._headers(content_type=content_type, accept=accept,
                                              include_account_id=include_account_id)
                if method == "GET":
                    resp = await http.get(f"{self.base_url}{endpoint}", headers=headers)
                elif method == "PUT":
                    resp = await http.put(f"{self.base_url}{endpoint}", headers=headers, json=payload)
                else:
                    resp = await http.post(f"{self.base_url}{endpoint}", headers=headers, json=payload)

                if resp.status_code == 429:
                    if attempt < RETRY_MAX:
                        wait = RETRY_RATE_LIMIT_WAIT * (attempt + 1)
                        logger.warning("Rate limit (429). %ds bekleniyor...", wait)
                        await asyncio.sleep(wait)
                        continue
                    resp.raise_for_status()

                if resp.status_code == 401 and not token_refreshed:
                    self._access_token = None
                    self._token_expires_at = 0
                    await self._ensure_token()
                    token_refreshed = True
                    continue

                if resp.status_code >= 400:
                    logger.error("API Hata %d — %s", resp.status_code, endpoint)
                resp.raise_for_status()
                return resp.json()

            except httpx.TimeoutException:
                if attempt < RETRY_MAX:
                    wait = RETRY_TIMEOUT_WAIT * (attempt + 1)
                    logger.warning("Timeout. %ds bekleniyor...", wait)
                    await asyncio.sleep(wait)
                    continue
                raise
            except httpx.HTTPStatusError:
                raise
            except Exception as e:
                last_error = e
                if attempt < RETRY_MAX:
                    await asyncio.sleep(5 * (attempt + 1))
                    continue
                raise
        raise last_error or Exception("Max retry asildi")

    async def get(self, endpoint, accept="application/json", include_account_id=False):
        return await self._request_with_retry("GET", endpoint, accept=accept,
                                              include_account_id=include_account_id)

    async def post(self, endpoint, payload, content_type="application/json",
                   accept="application/json", include_account_id=False):
        return await self._request_with_retry("POST", endpoint, payload, content_type, accept,
                                              include_account_id=include_account_id)

    # ------------------------------------------------------------------
    # FIRE-ALL-THEN-POLL: Rapor islemini 3 fazaya ayir
    # ------------------------------------------------------------------

    async def create_report_request(self, payload):
        """Faz 1: Rapor olusturma istegi gonder, reportId dondur."""
        import re
        try:
            data = await self.post("/reporting/reports", payload,
                                    content_type=REPORTING_CONTENT_TYPE,
                                    include_account_id=True)
        except httpx.HTTPStatusError as e:
            error_body = e.response.text[:500] if e.response else ""
            if e.response and e.response.status_code == 425:
                match = re.search(r'duplicate of\s*:\s*([0-9a-f-]{36})', error_body)
                if match:
                    return {"reportId": match.group(1)}
                else:
                    return {"error": f"HTTP 425", "details": error_body}
            else:
                return {"error": f"HTTP {e.response.status_code}", "details": error_body}

        report_id = data.get("reportId")
        if not report_id:
            return {"error": "Report ID yok"}
        return {"reportId": report_id}

    async def poll_and_download_report(self, report_id):
        """Faz 2+3: Raporu poll et, hazir olunca indir."""
        elapsed = 0
        while elapsed < REPORT_MAX_WAIT:
            try:
                status_data = await self.get(f"/reporting/reports/{report_id}",
                                             include_account_id=True)
            except httpx.HTTPStatusError:
                await asyncio.sleep(REPORT_POLL_INTERVAL)
                elapsed += REPORT_POLL_INTERVAL
                continue

            status = status_data.get("status")
            if status == "COMPLETED":
                url = status_data.get("url")
                if not url:
                    return {"error": "Download URL yok"}
                http = await self.get_http()
                resp = await http.get(url)
                resp.raise_for_status()
                try:
                    decompressed = gzip.decompress(resp.content)
                    result = json.loads(decompressed)
                except gzip.BadGzipFile:
                    result = resp.json()
                rows = result if isinstance(result, list) else result.get("rows", result.get("data", []))
                return rows
            elif status == "FAILED":
                return {"error": f"Rapor basarisiz: {status_data.get('failureReason', '?')}"}

            await asyncio.sleep(REPORT_POLL_INTERVAL)
            elapsed += REPORT_POLL_INTERVAL

        return {"error": f"Zaman asimi ({REPORT_MAX_WAIT}s)"}

    async def download_report(self, payload):
        """Eski uyumluluk: create + poll + download tek cagri (retry icin kullanilir)."""
        result = await self.create_report_request(payload)
        if is_error(result):
            return result
        return await self.poll_and_download_report(result["reportId"])


# ============================================================================
# SABITLER (agent1 ile ayni)
# ============================================================================

SP_CT = {
    "campaigns": "application/vnd.spCampaign.v3+json",
    "adGroups": "application/vnd.spAdGroup.v3+json",
    "productAds": "application/vnd.spProductAd.v3+json",
    "keywords": "application/vnd.spKeyword.v3+json",
    "targets": "application/vnd.spTargetingClause.v3+json",
    "negativeKeywords": "application/vnd.spNegativeKeyword.v3+json",
    "negativeTargets": "application/vnd.spNegativeTargetingClause.v3+json",
    "campaignNegativeKeywords": "application/vnd.spCampaignNegativeKeyword.v3+json",
}
SB_CT = {
    "campaigns": "application/vnd.sbcampaignresource.v4+json",
    "keywords": "application/vnd.sbkeyword.v3.2+json",
    "targets": "application/vnd.sblisttargetsresponse.v3.2+json",
}
PORTFOLIO_CT = "application/vnd.spPortfolio.v3+json"

SP_TARGETING_COLS = [
    "campaignName", "campaignId", "adGroupName", "adGroupId",
    "keywordId", "keyword", "matchType", "targeting",
    "adKeywordStatus", "keywordBid", "keywordType",
    "impressions", "clicks", "cost", "costPerClick", "clickThroughRate",
    "purchases14d", "sales14d", "unitsSoldClicks14d",
    "acosClicks14d", "roasClicks14d", "startDate", "endDate",
]
SP_SEARCH_TERM_COLS = [
    "searchTerm", "campaignName", "campaignId", "adGroupName", "adGroupId",
    "keywordId", "keyword", "matchType", "keywordType", "targeting",
    "adKeywordStatus", "impressions", "clicks", "cost", "costPerClick",
    "clickThroughRate", "purchases14d", "sales14d", "unitsSoldClicks14d",
    "acosClicks14d", "roasClicks14d", "startDate", "endDate",
]
SB_TARGETING_COLS = [
    "campaignName", "campaignId", "adGroupName", "adGroupId",
    "keywordText", "matchType", "targetingExpression", "targetingType",
    "adKeywordStatus", "keywordBid", "impressions", "clicks", "cost",
    "purchases", "purchasesClicks", "sales", "salesClicks", "unitsSold",
    "addToCart", "addToCartClicks", "newToBrandPurchases", "newToBrandSales",
    "startDate", "endDate",
]
SB_SEARCH_TERM_COLS = [
    "campaignName", "campaignId", "adGroupName", "adGroupId",
    "keywordText", "matchType", "searchTerm", "keywordBid", "keywordId",
    "impressions", "clicks", "cost", "purchases", "purchasesClicks",
    "sales", "salesClicks", "unitsSold", "startDate", "endDate",
]
SD_TARGETING_COLS = [
    "campaignName", "campaignId", "adGroupName", "adGroupId",
    "targetingExpression", "impressions", "clicks", "cost",
    "purchases", "purchasesClicks", "sales", "salesClicks",
    "unitsSold", "unitsSoldClicks", "addToCart", "addToCartClicks",
    "addToCartViews", "viewClickThroughRate", "startDate", "endDate",
]

# Campaign raporlari — Dashboard KPI icin (timeUnit: DAILY → her satir 1 gun)
SP_CAMPAIGN_COLS = [
    "campaignId", "campaignName", "campaignStatus", "campaignBudgetAmount",
    "impressions", "clicks", "cost", "costPerClick", "clickThroughRate",
    "purchases14d", "sales14d", "unitsSoldClicks14d",
    "acosClicks14d", "roasClicks14d",
    "date",
]
SB_CAMPAIGN_COLS = [
    "campaignId", "campaignName", "campaignStatus", "campaignBudgetAmount",
    "impressions", "clicks", "cost",
    "purchases", "purchasesClicks", "sales",
    "unitsSold",
    "date",
]
SD_CAMPAIGN_COLS = [
    "campaignId", "campaignName", "campaignStatus", "campaignBudgetAmount",
    "impressions", "clicks", "cost",
    "purchases", "purchasesClicks", "sales",
    "unitsSold",
    "addToCartViews",
    "date",
]


# ============================================================================
# YARDIMCI FONKSIYONLAR
# ============================================================================

def save_json(filename, data, data_dir):
    data_dir.mkdir(parents=True, exist_ok=True)
    filepath = data_dir / filename
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    count = len(data) if isinstance(data, list) else 1
    logger.info("  Kaydedildi: %s (%d kayit)", filepath.name, count)
    return str(filepath)


def file_exists_today(filename, data_dir):
    filepath = data_dir / filename
    if filepath.exists():
        mod_time = datetime.fromtimestamp(filepath.stat().st_mtime)
        if mod_time.date() == datetime.utcnow().date():
            return True
    return False


def load_existing(filename, data_dir):
    with open(data_dir / filename, "r", encoding="utf-8") as f:
        return json.load(f)


def is_error(result):
    return isinstance(result, dict) and "error" in result


def save_error_log(data_dir, hata_tipi, hata_mesaji, adim=None, extra=None):
    """Parallel collector hata logu — lokal + Supabase dual-write."""
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


def build_report_payload(ad_product, report_type, group_by, columns, days_back,
                         time_unit="SUMMARY"):
    end = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    start = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    return {
        "name": f"{report_type}_{days_back}d_{end}",
        "startDate": start, "endDate": end,
        "configuration": {
            "adProduct": ad_product, "groupBy": group_by,
            "columns": columns, "reportTypeId": report_type,
            "timeUnit": time_unit, "format": "GZIP_JSON",
        },
    }


# ============================================================================
# TEK MARKETPLACE VERI TOPLAMA
# ============================================================================

async def collect_marketplace(client, data_dir, label):
    """Tek bir marketplace icin tum verileri toplar. Agent 1 ile ayni mantik."""
    today = datetime.utcnow().strftime("%Y-%m-%d")
    R = {"basarili": 0, "basarisiz": 0, "atlanan": 0, "hatalar": []}

    # Dashboard: Marketplace bazli pipeline takibi
    _parts = label.split("/")
    _hk = _parts[0] if len(_parts) == 2 else ""
    _mp = _parts[1] if len(_parts) == 2 else ""
    _session_id = f"parallel_{today}_{_hk}_{_mp}"
    _dashboard_pipeline(_session_id, _hk, _mp, "agent1", "running")
    _save_log("info", f"Agent 1 basliyor (parallel): {label}", "agent1", _hk, _mp, _session_id)

    async def collect_list(name, endpoint, state_filter, extract_key,
                           method="POST", content_type="application/json",
                           accept="application/json", max_count=1000,
                           custom_body=None):
        fname = f"{today}_{name}.json"
        if file_exists_today(fname, data_dir):
            R["atlanan"] += 1
            return load_existing(fname, data_dir)
        try:
            all_items = []
            if method == "GET":
                query = f"?stateFilter={','.join(state_filter)}&count={max_count}"
                resp = await client.get(endpoint + query, accept=accept)
                items = resp.get(extract_key, resp) if isinstance(resp, dict) else resp
                if not isinstance(items, list):
                    items = [items] if items else []
                all_items = items
            else:
                body = custom_body.copy() if custom_body else {"stateFilter": {"include": state_filter}}
                if "maxResults" not in body:
                    body["maxResults"] = max_count
                while True:
                    resp = await client.post(endpoint, body, content_type=content_type, accept=accept)
                    items = resp.get(extract_key, resp) if isinstance(resp, dict) else resp
                    if not isinstance(items, list):
                        items = [items] if items else []
                    all_items.extend(items)
                    next_token = resp.get("nextToken") if isinstance(resp, dict) else None
                    if next_token and len(items) > 0:
                        body["nextToken"] = next_token
                        await asyncio.sleep(0.5)
                    else:
                        break
            save_json(fname, all_items, data_dir)
            R["basarili"] += 1
            return all_items
        except httpx.HTTPStatusError as e:
            status_code = e.response.status_code if e.response else 0
            error_body = e.response.text[:500] if e.response else ""
            if status_code == 400 and "do not have access" in error_body:
                # Marketplace bu reklam tipini desteklemiyor — normal durum.
                # Bos liste kaydet, uyari logla, hata SAYMA.
                logger.info("[%s] %s: marketplace desteklemiyor (400), atlaniyor", label, name)
                save_json(fname, [], data_dir)
                R["basarili"] += 1
                return []
            else:
                # Gercek hata (yanlis payload, format vs.)
                R["hatalar"].append(f"{name}: HTTP {status_code} — {error_body[:150]}")
                R["basarisiz"] += 1
                save_error_log(data_dir, f"HTTP_{status_code}", f"{name}: {error_body[:200]}",
                               adim="collect_list", extra={"entity": name, "status": status_code,
                                                           "body_snippet": error_body[:300]})
                return []
        except Exception as e:
            R["hatalar"].append(f"{name}: {str(e)[:200]}")
            R["basarisiz"] += 1
            save_error_log(data_dir, type(e).__name__, str(e)[:300],
                           adim="collect_list", extra={"entity": name})
            return []

    async def collect_report(name, ad_product_enum, report_type, group_by, columns, days,
                             time_unit="SUMMARY"):
        fname = f"{today}_{name}_{days}d.json"
        key = f"{name}_{days}d"
        if file_exists_today(fname, data_dir):
            R["atlanan"] += 1
            return key, "CACHE"
        try:
            payload = build_report_payload(ad_product_enum, report_type, group_by, columns, days, time_unit)
            rows = await client.download_report(payload)
            if is_error(rows):
                R["hatalar"].append(f"{key}: {rows.get('error', '?')}")
                R["basarisiz"] += 1
                save_error_log(data_dir, "ReportFailed", rows.get("error", "?"),
                               adim="collect_report", extra={"rapor": key})
                return key, "HATA"
            save_json(fname, rows, data_dir)
            R["basarili"] += 1
            if isinstance(rows, list) and len(rows) == 0:
                return key, "BOS"
            return key, "DOLU"
        except Exception as e:
            R["hatalar"].append(f"{key}: {str(e)[:200]}")
            R["basarisiz"] += 1
            save_error_log(data_dir, type(e).__name__, str(e)[:300],
                           adim="collect_report", extra={"rapor": key})
            return key, "HATA"

    logger.info("[%s] Entity toplama basliyor (paralel, max 4 esanli)...", label)

    # ---- OPTIMIZASYON 1: Entity'leri paralel topla (Semaphore ile max 4 esanli) ----
    entity_sem = asyncio.Semaphore(4)

    async def collect_list_throttled(*args, **kwargs):
        async with entity_sem:
            return await collect_list(*args, **kwargs)

    entity_coros = [
        # Portfolios
        collect_list_throttled("portfolios", "/portfolios/list", ["ENABLED"], "portfolios",
                               content_type=PORTFOLIO_CT, accept=PORTFOLIO_CT,
                               custom_body={"stateFilter": {"include": ["ENABLED"]}}),
        # SP Entities (8)
        collect_list_throttled("sp_campaigns", "/sp/campaigns/list", ["ENABLED", "PAUSED"], "campaigns",
                               content_type=SP_CT["campaigns"], accept=SP_CT["campaigns"]),
        collect_list_throttled("sp_ad_groups", "/sp/adGroups/list", ["ENABLED", "PAUSED"], "adGroups",
                               content_type=SP_CT["adGroups"], accept=SP_CT["adGroups"]),
        collect_list_throttled("sp_product_ads", "/sp/productAds/list", ["ENABLED", "PAUSED"], "productAds",
                               content_type=SP_CT["productAds"], accept=SP_CT["productAds"]),
        collect_list_throttled("sp_keywords", "/sp/keywords/list", ["ENABLED", "PAUSED"], "keywords",
                               content_type=SP_CT["keywords"], accept=SP_CT["keywords"]),
        collect_list_throttled("sp_targets", "/sp/targets/list", ["ENABLED", "PAUSED"], "targetingClauses",
                               content_type=SP_CT["targets"], accept=SP_CT["targets"]),
        collect_list_throttled("sp_negative_keywords", "/sp/negativeKeywords/list", ["ENABLED"], "negativeKeywords",
                               content_type=SP_CT["negativeKeywords"], accept=SP_CT["negativeKeywords"]),
        collect_list_throttled("sp_campaign_negative_keywords", "/sp/campaignNegativeKeywords/list",
                               ["ENABLED"], "campaignNegativeKeywords",
                               content_type=SP_CT["campaignNegativeKeywords"], accept=SP_CT["campaignNegativeKeywords"]),
        collect_list_throttled("sp_negative_targets", "/sp/negativeTargets/list", ["ENABLED"], "negativeTargetingClauses",
                               content_type=SP_CT["negativeTargets"], accept=SP_CT["negativeTargets"]),
        # SB Entities (5)
        collect_list_throttled("sb_campaigns", "/sb/v4/campaigns/list", ["ENABLED", "PAUSED"], "campaigns",
                               content_type=SB_CT["campaigns"], accept=SB_CT["campaigns"], max_count=100),
        collect_list_throttled("sb_ad_groups", "/sb/v4/adGroups/list", ["ENABLED", "PAUSED"], "adGroups",
                               content_type=SB_CT["campaigns"], accept=SB_CT["campaigns"], max_count=100),
        collect_list_throttled("sb_keywords", "/sb/keywords", ["enabled", "paused"], "keywords",
                               method="GET", accept=SB_CT["keywords"]),
        collect_list_throttled("sb_targets", "/sb/targets/list", ["enabled", "paused"], "targets",
                               content_type="application/json", accept=SB_CT["targets"],
                               custom_body={"filters": [{"filterType": "TARGETING_STATE",
                                                         "values": ["enabled", "paused"]}], "maxResults": 100}),
        collect_list_throttled("sb_negative_keywords", "/sb/negativeKeywords", ["enabled"], "negativeKeywords",
                               method="GET", accept="application/vnd.sbnegativekeyword.v3.2+json"),
        # SD Entities (3)
        collect_list_throttled("sd_campaigns", "/sd/campaigns", ["enabled", "paused"], "campaigns", method="GET"),
        collect_list_throttled("sd_ad_groups", "/sd/adGroups", ["enabled", "paused"], "adGroups", method="GET"),
        collect_list_throttled("sd_targets", "/sd/targets", ["enabled", "paused"], "targets", method="GET"),
    ]

    entity_results = await asyncio.gather(*entity_coros)

    # Entity sonuclarindan kampanya sayilarini cikar (smart-skip icin)
    entity_names = [
        "portfolios",
        "sp_campaigns", "sp_ad_groups", "sp_product_ads", "sp_keywords", "sp_targets",
        "sp_negative_keywords", "sp_campaign_negative_keywords", "sp_negative_targets",
        "sb_campaigns", "sb_ad_groups", "sb_keywords", "sb_targets", "sb_negative_keywords",
        "sd_campaigns", "sd_ad_groups", "sd_targets",
    ]
    entity_map = dict(zip(entity_names, entity_results))
    sp_count = len(entity_map.get("sp_campaigns", []))
    sb_count = len(entity_map.get("sb_campaigns", []))
    sd_count = len(entity_map.get("sd_campaigns", []))

    logger.info("[%s] Entity tamamlandi. Kampanya sayilari: SP=%d, SB=%d, SD=%d",
                label, sp_count, sb_count, sd_count)

    # ---- OPTIMIZASYON 2: FIRE-ALL-THEN-POLL ----
    # Faz 1 (FIRE): Tum rapor isteklerini 2'li batch'ler halinde gonder
    #   → Amazon hepsini AYNI ANDA uretmeye baslar
    # Faz 2 (POLL): Tum raporlari birlikte poll et
    #   → Toplam sure = max(rapor suresi), sum degil!

    # ---- OPTIMIZASYON 3: SMART-SKIP ----
    # Kampanyasi olmayan reklam tiplerinin raporlarini ATLA.
    # sb_campaigns=0 → SB raporlari bosuna istenmesin.
    # sd_campaigns=0 → SD raporlari bosuna istenmesin.
    # Marketplace basina 30-60 dk tasarruf (bos rapor poll + retry suresi).

    all_report_tasks = [
        # SP: targeting_14d + search_term_30d
        ("sp_targeting_report", "SPONSORED_PRODUCTS", "spTargeting", ["targeting"], SP_TARGETING_COLS, 14, "SUMMARY"),
        ("sp_search_term_report", "SPONSORED_PRODUCTS", "spSearchTerm", ["searchTerm"], SP_SEARCH_TERM_COLS, 30, "SUMMARY"),
        # SB: targeting_14d + search_term_30d
        ("sb_targeting_report", "SPONSORED_BRANDS", "sbTargeting", ["targeting"], SB_TARGETING_COLS, 14, "SUMMARY"),
        ("sb_search_term_report", "SPONSORED_BRANDS", "sbSearchTerm", ["searchTerm"], SB_SEARCH_TERM_COLS, 30, "SUMMARY"),
        # SD: targeting_14d + targeting_30d
        ("sd_targeting_report", "SPONSORED_DISPLAY", "sdTargeting", ["targeting"], SD_TARGETING_COLS, 14, "SUMMARY"),
        ("sd_targeting_report", "SPONSORED_DISPLAY", "sdTargeting", ["targeting"], SD_TARGETING_COLS, 30, "SUMMARY"),
        # Campaign raporlari — Dashboard KPI (timeUnit: DAILY)
        ("sp_campaign_report", "SPONSORED_PRODUCTS", "spCampaigns", ["campaign"], SP_CAMPAIGN_COLS, 14, "DAILY"),
        ("sb_campaign_report", "SPONSORED_BRANDS", "sbCampaigns", ["campaign"], SB_CAMPAIGN_COLS, 14, "DAILY"),
        ("sd_campaign_report", "SPONSORED_DISPLAY", "sdCampaigns", ["campaign"], SD_CAMPAIGN_COLS, 14, "DAILY"),
    ]

    # Kampanyasi olmayan tiplerin raporlarini filtrele
    skip_prefixes = []
    if sp_count == 0:
        skip_prefixes.append("sp_")
        logger.info("[%s] SMART-SKIP: SP kampanya yok → SP raporlari atlaniyor", label)
    if sb_count == 0:
        skip_prefixes.append("sb_")
        logger.info("[%s] SMART-SKIP: SB kampanya yok → SB raporlari atlaniyor", label)
    if sd_count == 0:
        skip_prefixes.append("sd_")
        logger.info("[%s] SMART-SKIP: SD kampanya yok → SD raporlari atlaniyor", label)

    if skip_prefixes:
        report_tasks = [t for t in all_report_tasks if not any(t[0].startswith(p) for p in skip_prefixes)]
        skipped_count = len(all_report_tasks) - len(report_tasks)
        logger.info("[%s] SMART-SKIP: %d/%d rapor atlanacak, %d rapor cekilecek",
                    label, skipped_count, len(all_report_tasks), len(report_tasks))
    else:
        report_tasks = all_report_tasks

    logger.info("[%s] Raporlar basliyor (fire-all-then-poll, %d rapor)...", label, len(report_tasks))

    failed_tasks = []
    empty_tasks = []

    # --- FAZ 1: FIRE — rapor olusturma isteklerini gonder (2'li batch, rate limit koruma) ---
    pending = {}   # key -> (report_id, task_tuple)
    skipped = []   # cache'den gelen raporlar

    for i in range(0, len(report_tasks), 2):
        batch = report_tasks[i:i+2]
        fire_coros = []
        fire_keys = []

        for name, ap, rt, gb, cols, days, tunit in batch:
            key = f"{name}_{days}d"
            fname = f"{today}_{key}.json"

            if file_exists_today(fname, data_dir):
                R["atlanan"] += 1
                skipped.append(key)
                continue

            payload = build_report_payload(ap, rt, gb, cols, days, tunit)
            fire_coros.append(client.create_report_request(payload))
            fire_keys.append((key, (name, ap, rt, gb, cols, days, tunit)))

        if fire_coros:
            results = await asyncio.gather(*fire_coros)
            for (key, task_tuple), result in zip(fire_keys, results):
                if is_error(result):
                    logger.warning("[%s] Rapor olusturulamadi: %s — %s", label, key, result.get("error"))
                    R["hatalar"].append(f"{key}: {result.get('error', '?')}")
                    R["basarisiz"] += 1
                    failed_tasks.append(task_tuple)
                    save_error_log(data_dir, "ReportCreateFailed", result.get("error", "?"),
                                   adim="fire_report", extra={"rapor": key})
                else:
                    report_id = result["reportId"]
                    logger.info("[%s] Rapor istendi: %s → %s", label, key, report_id)
                    pending[key] = (report_id, task_tuple)

        # 2'li batch arasi bekleme (rate limit koruma — ayni BATCH_SIZE=2 kurali)
        if i + 2 < len(report_tasks) and fire_coros:
            await asyncio.sleep(10)

    if skipped:
        logger.info("[%s] %d rapor cache'den (atlanildi): %s", label, len(skipped), ", ".join(skipped))

    # --- FAZ 2: POLL — tum raporlari AYNI ANDA poll et ---
    if pending:
        logger.info("[%s] %d rapor poll ediliyor (paralel)...", label, len(pending))

        async def poll_single(key, report_id, task_tuple):
            """Tek bir raporu poll et, indir, kaydet."""
            try:
                rows = await client.poll_and_download_report(report_id)
                if is_error(rows):
                    R["hatalar"].append(f"{key}: {rows.get('error', '?')}")
                    R["basarisiz"] += 1
                    save_error_log(data_dir, "ReportPollFailed", rows.get("error", "?"),
                                   adim="poll_report", extra={"rapor": key, "report_id": report_id})
                    return key, "HATA", task_tuple
                fname = f"{today}_{key}.json"
                save_json(fname, rows, data_dir)
                R["basarili"] += 1
                if isinstance(rows, list) and len(rows) == 0:
                    return key, "BOS", task_tuple
                return key, "DOLU", task_tuple
            except Exception as e:
                R["hatalar"].append(f"{key}: {str(e)[:200]}")
                R["basarisiz"] += 1
                save_error_log(data_dir, type(e).__name__, str(e)[:300],
                               adim="poll_report", extra={"rapor": key, "report_id": report_id})
                return key, "HATA", task_tuple

        poll_coros = [
            poll_single(key, report_id, task_tuple)
            for key, (report_id, task_tuple) in pending.items()
        ]
        poll_results = await asyncio.gather(*poll_coros)

        for key, status, task_tuple in poll_results:
            if status == "HATA":
                failed_tasks.append(task_tuple)
            elif status == "BOS":
                empty_tasks.append(task_tuple)

    # ----- RETRY: Bos raporlar 1 kez, hatali raporlar 3 kez -----
    RETRY_DELAYS = [120, 240, 300]

    if empty_tasks:
        logger.info("[%s] BOS RETRY: %d rapor 1 kez tekrar deneniyor...", label, len(empty_tasks))
        await asyncio.sleep(120)
        for name, ap, rt, gb, cols, days, tunit in empty_tasks:
            key = f"{name}_{days}d"
            fname = f"{today}_{key}.json"
            old_file = data_dir / fname
            if old_file.exists():
                old_file.unlink()
            R["basarili"] -= 1  # onceki bos kaydi duzelt
            _, status = await collect_report(name, ap, rt, gb, cols, days, tunit)
            if status == "DOLU":
                logger.info("[%s] BOS RETRY BASARILI: %s", label, key)
            else:
                logger.info("[%s] BOS RETRY: %s yine bos — gecerli kabul", label, key)

    if failed_tasks:
        logger.info("[%s] HATA RETRY: %d rapor tekrar deneniyor (max 3)...", label, len(failed_tasks))
        for name, ap, rt, gb, cols, days, tunit in failed_tasks:
            key = f"{name}_{days}d"
            resolved = False
            for attempt in range(len(RETRY_DELAYS)):
                delay = RETRY_DELAYS[attempt]
                logger.info("[%s] RETRY %d/3: %s — %ds bekleniyor...", label, attempt+1, key, delay)
                await asyncio.sleep(delay)

                fname = f"{today}_{key}.json"
                old_file = data_dir / fname
                if old_file.exists():
                    old_file.unlink()

                R["basarisiz"] -= 1
                R["hatalar"] = [h for h in R["hatalar"] if key not in h]

                _, status = await collect_report(name, ap, rt, gb, cols, days, tunit)
                if status in ("DOLU", "BOS"):
                    logger.info("[%s] RETRY %d/3 BASARILI: %s", label, attempt+1, key)
                    resolved = True
                    break
                else:
                    logger.warning("[%s] RETRY %d/3 BASARISIZ: %s", label, attempt+1, key)

            if not resolved:
                logger.error("[%s] RETRY TUKENDI: %s", label, key)

    toplam = R["basarili"] + R["basarisiz"] + R["atlanan"]
    logger.info("[%s] TAMAMLANDI — %d/%d basarili, %d hata, %d cache",
                label, R["basarili"], toplam, R["basarisiz"], R["atlanan"])

    if R["hatalar"]:
        for h in R["hatalar"]:
            logger.warning("[%s] Hata: %s", label, h)

    # KPI Daily sync
    try:
        import sys as _sys
        _base = str(BASE_DIR)
        if _base not in _sys.path:
            _sys.path.insert(0, _base)
        from supabase.db_client import SupabaseClient
        db = SupabaseClient()
        hesap_mp = label.split("/")  # label = "vigowood_eu/UK"
        if len(hesap_mp) == 2:
            db.upsert_kpi_daily(hesap_mp[0], hesap_mp[1], today)
            logger.info("[%s] KPI daily sync tamamlandi", label)
    except Exception as e:
        logger.warning("[%s] KPI daily sync hatasi (collector devam eder): %s", label, e)

    # Dashboard: Marketplace bazli pipeline tamamlandi
    _final = "completed" if R["basarisiz"] == 0 else "failed" if R["basarili"] == 0 else "completed"
    _dashboard_pipeline(_session_id, _hk, _mp, "agent1", _final)
    _save_log("info", f"Agent 1 tamamlandi (parallel): {label} — {R['basarili']} basarili, {R['basarisiz']} hata",
              "agent1", _hk, _mp, _session_id)

    await client.close()
    return R


# ============================================================================
# HESAP BAZLI PARALEL CALISTIRMA
# ============================================================================

async def run_account(hesap_key, hesap, lwa, mp_list):
    """Bir hesabin TUM marketplace'lerini PARALEL calistirir (batch YOK)."""
    results = {}
    coros = []
    mp_codes = []

    for mp_code in mp_list:
        mp_config = hesap["marketplaces"][mp_code]
        config = {
            "client_id": lwa["client_id"],
            "client_secret": lwa["client_secret"],
            "refresh_token": hesap["refresh_token"],
            "marketplace": mp_code,
            "profile_id": mp_config["profile_id"],
            "account_id": hesap["account_id"],
            "api_endpoint": hesap["api_endpoint"],
            "token_endpoint": hesap["token_endpoint"],
        }
        client = AmazonAdsClient(config)
        data_dir = get_data_dir(hesap_key, mp_code)
        label = f"{hesap_key}/{mp_code}"
        coros.append(collect_marketplace(client, data_dir, label))
        mp_codes.append(mp_code)

    all_results = await asyncio.gather(*coros, return_exceptions=True)

    for mp_code, result in zip(mp_codes, all_results):
        if isinstance(result, Exception):
            logger.error("[%s/%s] EXCEPTION: %s", hesap_key, mp_code, result)
            results[mp_code] = {"basarisiz": 1, "hata": str(result)}
        else:
            results[mp_code] = result

    return results


async def run_all(targets=None):
    """
    Hesaplari paralel calistirir.

    Args:
        targets: None → tum aktif hesaplar
                 ["vigowood_na"] → tek hesabin tum marketplace'leri
                 ["vigowood_na:US", "vigowood_eu:DE", "vigowood_eu:UK"] → belirli kombinasyonlar
    """
    accounts = load_accounts()
    lwa = accounts["lwa_app"]

    # Hedef marketplace'leri belirle
    # target_map: {hesap_key: [mp1, mp2, ...]}
    target_map = {}

    if targets:
        for t in targets:
            if ":" in t:
                hk, mp = t.split(":", 1)
                target_map.setdefault(hk, []).append(mp)
            else:
                # Sadece hesap adi verilmis → tum marketplace'leri
                hesap = accounts["hesaplar"].get(t, {})
                aktif = [mp for mp, cfg in hesap.get("marketplaces", {}).items() if cfg.get("aktif")]
                if aktif:
                    target_map[t] = aktif
    else:
        # Tum aktif hesaplar
        for hk, hesap in accounts["hesaplar"].items():
            aktif = [mp for mp, cfg in hesap.get("marketplaces", {}).items() if cfg.get("aktif")]
            if aktif:
                target_map[hk] = aktif

    if not target_map:
        logger.info("Calistirilacak hesap yok.")
        return

    # Hesap bazli marketplace listesi (batch YOK — tum marketplace'ler paralel)
    account_tasks = {}

    for hesap_key, mp_list in target_map.items():
        hesap = accounts["hesaplar"].get(hesap_key)
        if not hesap:
            logger.warning("Hesap bulunamadi: %s", hesap_key)
            continue
        account_tasks[hesap_key] = (hesap, mp_list)

    if not account_tasks:
        logger.info("Calistirilacak hesap yok.")
        return

    logger.info("=" * 60)
    logger.info("  PARALEL VERI TOPLAYICI")
    logger.info("=" * 60)
    logger.info("  %d hesap, toplam marketplace'ler:", len(account_tasks))
    for hk, (hesap, mp_list) in account_tasks.items():
        logger.info("    %s: %s (tumu paralel)", hk, ", ".join(mp_list))
    logger.info("=" * 60)

    # Dashboard: Pipeline basladi — agent1 running (tum marketplace'ler bitene kadar)
    _dashboard_status("agent1", "running")
    _dashboard_status("maestro", "running")
    _save_log("info", f"Parallel collector basladi: {len(account_tasks)} hesap", "maestro")

    start_time = time.time()

    # Her hesap icin paralel task olustur
    coros = []
    hesap_keys = []
    for hesap_key, (hesap, mp_list) in account_tasks.items():
        coros.append(run_account(hesap_key, hesap, lwa, mp_list))
        hesap_keys.append(hesap_key)

    all_results = await asyncio.gather(*coros, return_exceptions=True)

    elapsed = time.time() - start_time
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)

    # Ozet
    logger.info("\n" + "=" * 60)
    logger.info("  TAMAMLANDI — %d dk %d sn", minutes, seconds)
    logger.info("=" * 60)

    toplam_basarili = 0
    toplam_hata = 0

    for hesap_key, result in zip(hesap_keys, all_results):
        if isinstance(result, Exception):
            logger.error("  %s: EXCEPTION — %s", hesap_key, result)
            toplam_hata += 1
        else:
            for mp, r in result.items():
                b = r.get("basarili", 0)
                f = r.get("basarisiz", 0)
                a = r.get("atlanan", 0)
                t = b + f + a
                status = "OK" if f == 0 else f"HATA({f})"
                logger.info("  %s/%s: %d/%d basarili [%s]", hesap_key, mp, b, t, status)
                toplam_basarili += b
                toplam_hata += f

    logger.info("  Toplam: %d basarili, %d hata", toplam_basarili, toplam_hata)
    logger.info("=" * 60)

    # Dashboard: Tum marketplace'ler bitti — agent1 durumunu guncelle
    _agent1_final = "completed" if toplam_hata == 0 else "failed" if toplam_basarili == 0 else "completed"
    _dashboard_status("agent1", _agent1_final, {
        "tasks": toplam_basarili + toplam_hata,
        "errors_7d": toplam_hata,
    })
    _dashboard_status("maestro", "ONLINE")
    _save_log("info", f"Parallel collector tamamlandi: {toplam_basarili} basarili, {toplam_hata} hata ({minutes}dk {seconds}sn)", "maestro")


# ============================================================================
# CLI
# ============================================================================

if __name__ == "__main__":
    # Kullanim:
    #   python parallel_collector.py                          → tum hesaplar
    #   python parallel_collector.py vigowood_eu              → tek hesabin tum mp'leri
    #   python parallel_collector.py vigowood_na:US vigowood_eu:DE vigowood_eu:UK  → belirli kombinasyonlar
    targets = sys.argv[1:] if len(sys.argv) > 1 else None
    asyncio.run(run_all(targets))
