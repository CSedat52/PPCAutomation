"""
Amazon Advertising MCP Server — Data Collector Agent (v9 Multi-Account)
=========================================================================
DEGISIKLIKLER (v8 -> v9):
  1. Multi-account: accounts.json'dan hesap+marketplace bazli calisir.
  2. AmazonAdsClient config dict alir (env var okuma KALDIRILDI).
  3. Tum tool'lar hesap_key + marketplace zorunlu parametre alir.
  4. Veri izolasyonu: data/{hesap_key}_{marketplace}/ altina yazar.
  5. Yeni tool: amazon_ads_list_accounts.
  6. Geriye uyumluluk YOK — tek yol accounts.json.

DEGISIKLIKLER (v7.1 -> v8):
  1. save_error_log(): Yeni fonksiyon — basarisiz her API islemi
     data/logs/agent1_errors.json dosyasina eklenir.
  2. collect_all_data() sonunda R["hatalar"] listesi doluysa
     her hata kaydi icin save_error_log() cagirilir.
  3. Hata tipi mesajdan otomatik cikarilir:
     RateLimit / AuthError / ApiError_400 / ServerError / Timeout / ReportFailed
  4. Agent 4 (Learning Agent) agent1_errors.json'u okuyarak
     hangi endpoint'lerin tekrar eden sorunlar yasadigini analiz eder.
  5. Son 200 hata kaydi tutulur (eski kayitlar otomatik temizlenir).

SP + SB + SD destegi. Agent 3 icin ad_groups ve product_ads eklendi.

Reklam Tipleri:
  - Sponsored Products (SP): Kampanya, Ad Group, Product Ad, Keyword, ASIN Target
  - Sponsored Brands (SB v4): Kampanya, Ad Group, Keyword, ASIN Target
  - Sponsored Display (SD): Kampanya, Ad Group, ASIN Target

KRITIK METRIK FARKLARI:
  SP: sales14d, purchases14d, unitsSoldClicks14d (attribution window belirtilir)
  SB: sales, purchases, unitsSold (14 gun sabit, window eki YOK)
  SD: sales, purchases, unitsSold (14 gun sabit, window eki YOK)

Cekilen Veriler (19 dosya):
  Entity Listeleri (12+1):
    SP: campaigns, ad_groups, product_ads, keywords, targets,
        negative_keywords, campaign_negative_keywords, negative_targets
    SB: campaigns, ad_groups, keywords, targets, negative_keywords
    SD: campaigns, ad_groups, targets
    Portfolios: portfolios

  Performans Raporlari (6):
    SP: targeting_14d, search_term_30d
    SB: targeting_14d, search_term_30d
    SD: targeting_14d, targeting_30d

Kaldirilan Veriler (v6 → v7):
  Entity: sp_negative_keywords, sp_negative_targets, sb_negative_keywords (Agent 2 kullanmiyor)
  Raporlar: Tum campaign raporlari (targeting raporundan hesaplanabilir)
  Raporlar: Tum 1d raporlari (Agent 2 kullanmiyor)
  Raporlar: Tum 14d search_term (Agent 2 sadece 30d kullaniyor)
  Raporlar: sp/sb targeting_30d (search_term_30d daha detayli veriyor)

8 Teknik Koruma:
  1. Rate limit (429) → 30 sn bekle, 3 kez daha dene (artan bekleme)
  2. Timeout → 15 sn bekle, tekrar dene (artan bekleme)
  3. Token dolmus (401) → Token yenile, tekrar dene
  4. Bos rapor (0 satir) → Uyari olarak bildir
  5. Ayni gunun verisi varsa → Tekrar cekme
  6. Amazon-Ads-AccountId header'i (raporlama icin ZORUNLU)
  7. Sirayla rapor cekme (2'li batch — kararlilik icin)
  8. Basarisiz raporlari sonunda tekrar dene (retry turu)
"""

import os
import json
import time
import gzip
import asyncio
import logging
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from contextlib import asynccontextmanager

import httpx
from pydantic import BaseModel, ConfigDict
from mcp.server.fastmcp import FastMCP, Context

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("amazon_ads_mcp")

# ============================================================================
# YAPILANDIRMA
# ============================================================================

MARKETPLACE_CONFIG = {
    "US": {
        "advertising_endpoint": "https://advertising-api.amazon.com",
        "token_endpoint": "https://api.amazon.com/auth/o2/token",
    },
    "CA": {
        "advertising_endpoint": "https://advertising-api.amazon.com",
        "token_endpoint": "https://api.amazon.com/auth/o2/token",
    },
    "UK": {
        "advertising_endpoint": "https://advertising-api-eu.amazon.com",
        "token_endpoint": "https://api.amazon.co.uk/auth/o2/token",
    },
    "DE": {
        "advertising_endpoint": "https://advertising-api-eu.amazon.com",
        "token_endpoint": "https://api.amazon.co.uk/auth/o2/token",
    },
    "FR": {
        "advertising_endpoint": "https://advertising-api-eu.amazon.com",
        "token_endpoint": "https://api.amazon.co.uk/auth/o2/token",
    },
    "ES": {
        "advertising_endpoint": "https://advertising-api-eu.amazon.com",
        "token_endpoint": "https://api.amazon.co.uk/auth/o2/token",
    },
    "IT": {
        "advertising_endpoint": "https://advertising-api-eu.amazon.com",
        "token_endpoint": "https://api.amazon.co.uk/auth/o2/token",
    },
    "JP": {
        "advertising_endpoint": "https://advertising-api-fe.amazon.com",
        "token_endpoint": "https://api.amazon.co.jp/auth/o2/token",
    },
    "AU": {
        "advertising_endpoint": "https://advertising-api-fe.amazon.com",
        "token_endpoint": "https://api.amazon.com.au/auth/o2/token",
    },
}

REPORT_MAX_WAIT = 900
REPORT_POLL_INTERVAL = 15  # v7: 10 → 15 sn (Amazon server yukunu azaltir)
RETRY_MAX = 3              # v7: 2 → 3 (daha fazla sans)
RETRY_RATE_LIMIT_WAIT = 30
RETRY_TIMEOUT_WAIT = 15    # v7: 10 → 15 sn
BATCH_SIZE = 2             # v7: 3 → 2 (daha az paralel = daha az rate limit)
BATCH_DELAY = 5            # v7: Batch arasi bekleme suresi (sn)

# ============================================================================
# CONTENT-TYPE HARITASI
# ============================================================================

REPORTING_CONTENT_TYPE = "application/vnd.createasyncreportrequest.v3+json"

SP_CONTENT_TYPES = {
    "campaigns": "application/vnd.spCampaign.v3+json",
    "adGroups": "application/vnd.spAdGroup.v3+json",
    "productAds": "application/vnd.spProductAd.v3+json",
    "keywords": "application/vnd.spKeyword.v3+json",
    "targets": "application/vnd.spTargetingClause.v3+json",
    "negativeKeywords": "application/vnd.spNegativeKeyword.v3+json",
    "negativeTargets": "application/vnd.spNegativeTargetingClause.v3+json",
    "campaignNegativeKeywords": "application/vnd.spCampaignNegativeKeyword.v3+json",
}

SB_CONTENT_TYPES = {
    "campaigns": "application/vnd.sbcampaignresource.v4+json",
    "keywords": "application/vnd.sbkeyword.v3.2+json",
    "targets": "application/vnd.sblisttargetsresponse.v3.2+json",
}

# ============================================================================
# WRITE ENDPOINTS HARITASI (Agent 3 Executor icin)
# ============================================================================

WRITE_ENDPOINTS = {
    # --- BID UPDATES (PUT) ---
    "sp_keyword_bid_update": {
        "method": "PUT", "path": "/sp/keywords",
        "content_type": SP_CONTENT_TYPES["keywords"],
        "accept": SP_CONTENT_TYPES["keywords"],
        "wrapper_key": "keywords",
    },
    "sp_target_bid_update": {
        "method": "PUT", "path": "/sp/targets",
        "content_type": SP_CONTENT_TYPES["targets"],
        "accept": SP_CONTENT_TYPES["targets"],
        "wrapper_key": "targetingClauses",
    },
    "sb_keyword_bid_update": {
        "method": "PUT", "path": "/sb/keywords",
        "content_type": "application/json",
        "accept": "*/*",
        "wrapper_key": None,
    },
    "sd_target_bid_update": {
        "method": "PUT", "path": "/sd/targets",
        "content_type": "application/json",
        "accept": "application/json",
        "wrapper_key": None,
    },
    # --- NEGATIVE ADDITIONS (POST) ---
    "sp_negative_keyword_add": {
        "method": "POST", "path": "/sp/negativeKeywords",
        "content_type": SP_CONTENT_TYPES["negativeKeywords"],
        "accept": SP_CONTENT_TYPES["negativeKeywords"],
        "wrapper_key": "negativeKeywords",
    },
    "sb_negative_keyword_add": {
        "method": "POST", "path": "/sb/negativeKeywords",
        "content_type": "application/json",
        "accept": "application/json",
        "wrapper_key": None,
    },
    "sp_negative_target_add": {
        "method": "POST", "path": "/sp/negativeTargets",
        "content_type": SP_CONTENT_TYPES["negativeTargets"],
        "accept": SP_CONTENT_TYPES["negativeTargets"],
        "wrapper_key": "negativeTargetingClauses",
    },
    # --- ENTITY CREATION (POST, Harvesting) ---
    "sp_campaign_create": {
        "method": "POST", "path": "/sp/campaigns",
        "content_type": SP_CONTENT_TYPES["campaigns"],
        "accept": SP_CONTENT_TYPES["campaigns"],
        "wrapper_key": "campaigns",
    },
    "sp_ad_group_create": {
        "method": "POST", "path": "/sp/adGroups",
        "content_type": SP_CONTENT_TYPES["adGroups"],
        "accept": SP_CONTENT_TYPES["adGroups"],
        "wrapper_key": "adGroups",
    },
    "sp_product_ad_create": {
        "method": "POST", "path": "/sp/productAds",
        "content_type": SP_CONTENT_TYPES["productAds"],
        "accept": SP_CONTENT_TYPES["productAds"],
        "wrapper_key": "productAds",
    },
    "sp_keyword_create": {
        "method": "POST", "path": "/sp/keywords",
        "content_type": SP_CONTENT_TYPES["keywords"],
        "accept": SP_CONTENT_TYPES["keywords"],
        "wrapper_key": "keywords",
    },
    "sp_target_create": {
        "method": "POST", "path": "/sp/targets",
        "content_type": SP_CONTENT_TYPES["targets"],
        "accept": SP_CONTENT_TYPES["targets"],
        "wrapper_key": "targetingClauses",
    },
}

EXECUTE_DELAY_BETWEEN_OPS = 1  # saniye, rate limit koruma

# ============================================================================
# RAPOR KOLON KONFIGURASYONLARI
# KRITIK: SP vs SB vs SD FARKLI METRIK ISIMLERI KULLANIYOR!
# ============================================================================

SP_TARGETING_COLS = [
    "campaignName", "campaignId", "adGroupName", "adGroupId",
    "keywordId", "keyword", "matchType", "targeting",
    "adKeywordStatus", "keywordBid", "keywordType",
    "impressions", "clicks", "cost", "costPerClick", "clickThroughRate",
    "purchases14d", "sales14d", "unitsSoldClicks14d",
    "acosClicks14d", "roasClicks14d",
    "startDate", "endDate",
]

SP_SEARCH_TERM_COLS = [
    "searchTerm", "campaignName", "campaignId", "adGroupName", "adGroupId",
    "keywordId", "keyword", "matchType", "keywordType", "targeting",
    "adKeywordStatus",
    "impressions", "clicks", "cost", "costPerClick", "clickThroughRate",
    "purchases14d", "sales14d", "unitsSoldClicks14d",
    "acosClicks14d", "roasClicks14d",
    "startDate", "endDate",
]

SB_TARGETING_COLS = [
    "campaignName", "campaignId", "adGroupName", "adGroupId",
    "keywordText", "matchType", "targetingExpression", "targetingType",
    "adKeywordStatus", "keywordBid",
    "impressions", "clicks", "cost",
    "purchases", "purchasesClicks", "sales", "salesClicks",
    "unitsSold",
    "addToCart", "addToCartClicks",
    "newToBrandPurchases", "newToBrandSales",
    "startDate", "endDate",
]

SB_SEARCH_TERM_COLS = [
    "campaignName", "campaignId", "adGroupName", "adGroupId",
    "keywordText", "matchType", "searchTerm",
    "keywordBid", "keywordId",
    "impressions", "clicks", "cost",
    "purchases", "purchasesClicks", "sales", "salesClicks",
    "unitsSold",
    "startDate", "endDate",
]

SD_TARGETING_COLS = [
    "campaignName", "campaignId", "adGroupName", "adGroupId",
    "targetingExpression",
    "impressions", "clicks", "cost",
    "purchases", "purchasesClicks", "sales", "salesClicks",
    "unitsSold", "unitsSoldClicks",
    "addToCart", "addToCartClicks", "addToCartViews",
    "viewClickThroughRate",
    "startDate", "endDate",
]

BASE_DIR = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ============================================================================
# ACCOUNTS.JSON YUKLEYICI
# ============================================================================

_ACCOUNTS_CACHE = None

def load_accounts():
    """accounts.json yukler ve cache'ler."""
    global _ACCOUNTS_CACHE
    if _ACCOUNTS_CACHE is not None:
        return _ACCOUNTS_CACHE
    for p in [BASE_DIR / "config" / "accounts.json", BASE_DIR / "accounts.json"]:
        if p.exists():
            with open(p, "r", encoding="utf-8") as f:
                _ACCOUNTS_CACHE = json.load(f)
            logger.info("accounts.json yuklendi: %s", p)
            return _ACCOUNTS_CACHE
    raise FileNotFoundError("accounts.json bulunamadi (config/ veya proje koku)")


def build_client_config(hesap_key, marketplace):
    """Hesap+marketplace icin AmazonAdsClient config dict'i olusturur."""
    accounts = load_accounts()
    lwa = accounts["lwa_app"]
    hesap = accounts["hesaplar"][hesap_key]
    mp = hesap["marketplaces"][marketplace]
    return {
        "client_id": lwa["client_id"],
        "client_secret": lwa["client_secret"],
        "refresh_token": hesap["refresh_token"],
        "marketplace": marketplace,
        "profile_id": mp["profile_id"],
        "account_id": hesap["account_id"],
        "api_endpoint": hesap["api_endpoint"],
        "token_endpoint": hesap["token_endpoint"],
    }


def get_data_dir(hesap_key, marketplace):
    """Hesap+marketplace icin data dizini. Yoksa olusturur."""
    d = BASE_DIR / "data" / f"{hesap_key}_{marketplace}"
    d.mkdir(parents=True, exist_ok=True)
    return d


def save_error_log(hata_tipi, hata_mesaji, data_dir, traceback_str=None, adim=None,
                   extra=None, session_id=None):
    """
    Agent 1 hatalarini data/logs/agent1_errors.json dosyasina ekler.
    Agent 4 (Learning Agent) bu dosyayi okuyarak tekrar eden API hata
    kaliplarini analiz eder ve Maestro CLAUDE.md guncellemesi onerir.

    Parametreler:
        hata_tipi    : Hata kategorisi — ortak taksonomi:
                       RateLimit, AuthError, ApiError, ServerError, NetworkError,
                       FileNotFound, DataError, ReportFailed, InternalError
        hata_mesaji  : Hata aciklamasi (HTTP status, endpoint, mesaj)
        data_dir     : Hesap bazli data klasoru (Path)
        traceback_str: traceback.format_exc() — beklenmeyen Python hatalarinda
        adim         : Hatanin gerceklestigi adim (orn. "collect_list", "collect_report")
        extra        : Ek baglam dict (orn. {"endpoint": "/sp/campaigns", "status_code": 429})
        session_id   : Pipeline session ID'si (Maestro korelasyonu icin)
    """
    log_dir = data_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "agent1_errors.json"

    try:
        with open(log_path, "r", encoding="utf-8") as f:
            kayitlar = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        kayitlar = []

    kayit = {
        "timestamp": datetime.utcnow().isoformat(),
        "hata_tipi": hata_tipi,
        "hata_mesaji": str(hata_mesaji)[:500],
        "adim": adim or "bilinmiyor",
    }
    if traceback_str:
        kayit["traceback"] = str(traceback_str)[:1000]
    if extra:
        kayit["extra"] = extra
    if session_id:
        kayit["session_id"] = session_id

    kayitlar.append(kayit)

    # Son 200 kaydi tut
    if len(kayitlar) > 200:
        kayitlar = kayitlar[-200:]

    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(kayitlar, f, indent=2, ensure_ascii=False)

    logger.info("Hata logu kaydedildi: %s", log_path)


def classify_error_from_message(hata_mesaji):
    """HTTP/API hata mesajindan ortak hata tipini belirler."""
    msg = str(hata_mesaji).lower()
    if "429" in msg or "rate limit" in msg:
        return "RateLimit"
    elif "401" in msg or "403" in msg or "unauthorized" in msg:
        return "AuthError"
    elif "400" in msg or "bad request" in msg:
        return "ApiError"
    elif any(k in msg for k in ["500", "502", "503", "504", "internal server"]):
        return "ServerError"
    elif any(k in msg for k in ["timeout", "connection", "network"]):
        return "NetworkError"
    elif "rapor basarisiz" in msg or "report failed" in msg:
        return "ReportFailed"
    elif "not found" in msg or "bulunamadi" in msg:
        return "FileNotFound"
    else:
        return "InternalError"


# ============================================================================
# AMAZON API ISTEMCISI (8 KORUMA DAHIL)
# ============================================================================

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
        await self._refresh_access_token()

    async def _refresh_access_token(self):
        http = await self.get_http()
        resp = await http.post(
            self.token_endpoint,
            data={
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        self._access_token = data["access_token"]
        self._token_expires_at = time.time() + data.get("expires_in", 3600)
        logger.info("Access token alindi/yenilendi.")

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
        """HTTP istegi yapar. Koruma mekanizmalari:
        1. Rate limit (429) → artan bekleme ile tekrar dene
        2. Timeout → artan bekleme ile tekrar dene
        3. Token dolmus (401) → Token yenile, tekrar dene
        """
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

                # Koruma 1: Rate limit — artan bekleme
                if resp.status_code == 429:
                    if attempt < RETRY_MAX:
                        wait = RETRY_RATE_LIMIT_WAIT * (attempt + 1)
                        logger.warning("Rate limit (429). %ds bekleniyor... (%d/%d)",
                                       wait, attempt + 1, RETRY_MAX)
                        await asyncio.sleep(wait)
                        continue
                    resp.raise_for_status()

                # Koruma 3: Token dolmus
                if resp.status_code == 401 and not token_refreshed:
                    logger.warning("Token dolmus (401). Yenileniyor...")
                    self._access_token = None
                    self._token_expires_at = 0
                    await self._refresh_access_token()
                    token_refreshed = True
                    continue

                if resp.status_code >= 400:
                    logger.error("API Hata %d — %s\nPayload: %s\nResponse: %s",
                                 resp.status_code, endpoint,
                                 json.dumps(payload, indent=2)[:500] if payload else "N/A",
                                 resp.text[:1000])
                resp.raise_for_status()
                return resp.json()

            except httpx.TimeoutException as e:
                # Koruma 2: Timeout — artan bekleme
                last_error = e
                if attempt < RETRY_MAX:
                    wait = RETRY_TIMEOUT_WAIT * (attempt + 1)
                    logger.warning("Timeout. %ds bekleniyor... (%d/%d)",
                                   wait, attempt + 1, RETRY_MAX)
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

        raise last_error or Exception("Maksimum deneme sayisina ulasildi")

    async def get(self, endpoint, accept="application/json", include_account_id=False):
        return await self._request_with_retry("GET", endpoint, accept=accept,
                                              include_account_id=include_account_id)

    async def post(self, endpoint, payload, content_type="application/json",
                   accept="application/json", include_account_id=False):
        return await self._request_with_retry("POST", endpoint, payload, content_type, accept,
                                              include_account_id=include_account_id)

    async def put(self, endpoint, payload, content_type="application/json",
                  accept="application/json"):
        return await self._request_with_retry("PUT", endpoint, payload, content_type, accept)

    async def download_report(self, payload):
        """Rapor olustur, bekle, indir. Amazon-Ads-AccountId header'i EKLENIR."""
        try:
            data = await self.post(
                "/reporting/reports", payload,
                content_type=REPORTING_CONTENT_TYPE,
                accept="application/json",
                include_account_id=True,
            )
        except httpx.HTTPStatusError as e:
            error_body = e.response.text[:500] if e.response else "N/A"
            # HTTP 425: Amazon duplicate request — extract existing report ID and poll it
            if e.response and e.response.status_code == 425:
                import re
                match = re.search(r'duplicate of\s*:\s*([0-9a-f-]{36})', error_body)
                if match:
                    dup_report_id = match.group(1)
                    logger.info("HTTP 425 duplike — mevcut rapor ID kullaniliyor: %s", dup_report_id)
                    data = {"reportId": dup_report_id}
                else:
                    logger.error("HTTP 425 ama rapor ID parse edilemedi: %s", error_body)
                    return {"error": f"HTTP {e.response.status_code}", "details": error_body}
            else:
                logger.error("Rapor olusturma hatasi (%d): %s | ReportType: %s",
                            e.response.status_code, error_body,
                            payload.get("configuration", {}).get("reportTypeId", "?"))
                return {"error": f"HTTP {e.response.status_code}", "details": error_body}

        report_id = data.get("reportId")
        if not report_id:
            logger.error("Report ID alinamadi: %s", data)
            return {"error": "Report ID yok", "details": str(data)[:500]}

        logger.info("Rapor olusturuldu: %s (%s)",
                    report_id,
                    payload.get("configuration", {}).get("reportTypeId", "?"))

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
                    return {"error": "Download URL yok", "details": str(status_data)[:500]}
                http = await self.get_http()
                resp = await http.get(url)
                resp.raise_for_status()
                try:
                    decompressed = gzip.decompress(resp.content)
                    result = json.loads(decompressed)
                except gzip.BadGzipFile:
                    result = resp.json()
                # DEBUG: Ham rapor yapisini logla
                if isinstance(result, dict):
                    logger.info("DEBUG rapor yapisi: type=dict, keys=%s, ilk 500 char=%s",
                                list(result.keys()), str(result)[:500])
                else:
                    logger.info("DEBUG rapor yapisi: type=%s, len=%s, ilk 500 char=%s",
                                type(result).__name__, len(result) if hasattr(result, '__len__') else '?',
                                str(result)[:500])
                rows = result if isinstance(result, list) else result.get("rows", result.get("data", []))
                logger.info("Rapor tamamlandi: %s (%d satir)", report_id, len(rows))
                return rows
            elif status == "FAILED":
                reason = status_data.get("failureReason", "Bilinmiyor")
                logger.error("Rapor basarisiz: %s — %s", report_id, reason)
                return {"error": f"Rapor basarisiz: {reason}"}

            await asyncio.sleep(REPORT_POLL_INTERVAL)
            elapsed += REPORT_POLL_INTERVAL

        logger.error("Rapor zaman asimi: %s", report_id)
        return {"error": f"Zaman asimi ({REPORT_MAX_WAIT}s)"}


# ============================================================================
# YARDIMCI FONKSIYONLAR
# ============================================================================

def _save_json(filename, data, data_dir):
    data_dir.mkdir(parents=True, exist_ok=True)
    filepath = data_dir / filename
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    count = len(data) if isinstance(data, list) else 1
    logger.info("Kaydedildi: %s (%d kayit)", filepath, count)
    return str(filepath)


def _file_exists_today(filename, data_dir):
    """Koruma 5: Ayni gunun verisi varsa tekrar cekme."""
    filepath = data_dir / filename
    if filepath.exists():
        mod_time = datetime.fromtimestamp(filepath.stat().st_mtime)
        if mod_time.date() == datetime.utcnow().date():
            logger.info("Dosya zaten mevcut (bugun): %s", filepath)
            return True
    return False


def _load_existing_json(filename, data_dir):
    filepath = data_dir / filename
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def _is_error(result):
    return isinstance(result, dict) and "error" in result


def _build_report_payload(ad_product, report_type, group_by, columns, days_back):
    # Son gun (bugun) haric tutulur — Amazon'da bugunun verisi eksik/tamamlanmamis olur
    end = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")  # dun
    start = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    return {
        "name": f"{report_type}_{days_back}d_{end}",
        "startDate": start,
        "endDate": end,
        "configuration": {
            "adProduct": ad_product,
            "groupBy": group_by,
            "columns": columns,
            "reportTypeId": report_type,
            "timeUnit": "SUMMARY",
            "format": "GZIP_JSON",
        },
    }


# ============================================================================
# MCP SERVER
# ============================================================================

@asynccontextmanager
async def app_lifespan(app):
    accounts = load_accounts()
    aktif = sum(
        1 for h in accounts.get("hesaplar", {}).values()
        for m in h.get("marketplaces", {}).values()
        if m.get("aktif")
    )
    logger.info("MCP Server v9 baslatildi — %d aktif marketplace", aktif)
    yield {"accounts": accounts}

mcp = FastMCP("amazon_ads_mcp", lifespan=app_lifespan)


class AccountInput(BaseModel):
    """Tum tool'lar icin zorunlu hesap parametreleri."""
    model_config = ConfigDict(extra="forbid")
    hesap_key: str
    marketplace: str


class ExecutePlanInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    hesap_key: str
    marketplace: str
    plan_file: str | None = None


class EmptyInput(BaseModel):
    model_config = ConfigDict(extra="forbid")


# ============================================================================
# ANA TOOL: TUM VERILERI TOPLA (SP + SB + SD) — v7 Optimized
# ============================================================================

@mcp.tool(
    name="amazon_ads_collect_all_data",
    annotations={
        "title": "Tum Verileri Topla (Agent 1 v9)",
        "readOnlyHint": True, "destructiveHint": False,
        "idempotentHint": False, "openWorldHint": True,
    },
)
async def amazon_ads_collect_all_data(params: AccountInput, ctx: Context = None) -> str:
    """Data Collector Agent v9 — SP + SB + SD reklam verilerini ceker ve kaydeder.

    Kullanim: amazon_ads_collect_all_data({"hesap_key": "vigowood_na", "marketplace": "US"})
    Veriler data/{hesap_key}_{marketplace}/ altina kaydedilir.
    """
    config = build_client_config(params.hesap_key, params.marketplace)
    client = AmazonAdsClient(config)
    data_dir = get_data_dir(params.hesap_key, params.marketplace)
    today = datetime.utcnow().strftime("%Y-%m-%d")
    account_label = f"{params.hesap_key}/{params.marketplace}"
    R = {
        "tarih": today, "hesap": account_label,
        "dosyalar": {}, "hatalar": [], "uyarilar": [],
        "basarili": 0, "basarisiz": 0, "atlanan": 0,
        "rapor_detay": {},
    }

    # ---- YARDIMCI: Liste cek veya mevcut kullan (PAGINATION DESTEKLI) ----
    async def collect_list(name, endpoint, state_filter, extract_key,
                           method="POST", content_type="application/json",
                           accept="application/json", max_count=1000,
                           custom_body=None):
        """
        Entity listesi ceker. Pagination ile TUM entity'leri toplar.
        
        Amazon API'leri varsayilan olarak en fazla 1000 kayit doner.
        Daha fazla entity varsa response'da 'nextToken' bulunur.
        Bu fonksiyon nextToken oldugu surece istekleri tekrarlar.
        
        POST endpoint'leri (SP/SD):
          Request body: {"stateFilter": {"include": [...]}, "maxResults": 1000}
          Response: {"targetingClauses": [...], "nextToken": "abc123"}
          Sonraki sayfa: body'ye "nextToken": "abc123" eklenir
          
        GET endpoint'leri (SB keywords):
          Query: ?stateFilter=enabled,paused&count=1000
          Response: [items] (nextToken yok, tek sayfada gelir)
          
        SB targets/campaigns (POST, farkli body formati):
          Request body: {"filters": [...], "maxResults": 1000}
          Response: {"targets": [...], "nextToken": "abc123"}
          Sonraki sayfa: body'ye "nextToken": "abc123" eklenir
        """
        fname = f"{today}_{name}.json"
        if _file_exists_today(fname, data_dir):
            data = _load_existing_json(fname, data_dir)
            R["dosyalar"][name] = str(data_dir / fname)
            R["atlanan"] += 1
            return data
        try:
            all_items = []

            if method == "GET":
                # GET endpoint'leri (SB keywords) — pagination yok
                query = f"?stateFilter={','.join(state_filter)}&count={max_count}"
                resp = await client.get(endpoint + query, accept=accept)
                items = resp.get(extract_key, resp) if isinstance(resp, dict) else resp
                if not isinstance(items, list):
                    items = [items] if items else []
                all_items = items
            else:
                # POST endpoint'leri — PAGINATION ile tum sayfalari cek
                body = custom_body.copy() if custom_body else {
                    "stateFilter": {"include": state_filter}
                }
                # maxResults ekle (custom_body'de yoksa)
                if "maxResults" not in body:
                    body["maxResults"] = max_count

                page = 1
                while True:
                    resp = await client.post(endpoint, body,
                                             content_type=content_type, accept=accept)

                    # Response'dan items cikar
                    items = resp.get(extract_key, resp) if isinstance(resp, dict) else resp
                    if not isinstance(items, list):
                        items = [items] if items else []
                    all_items.extend(items)

                    # nextToken kontrolu
                    next_token = None
                    if isinstance(resp, dict):
                        next_token = resp.get("nextToken")

                    if next_token and len(items) > 0:
                        page += 1
                        body["nextToken"] = next_token
                        logger.info("%s: Sayfa %d tamamlandi (%d kayit). Sonraki sayfa...",
                                    name, page - 1, len(items))
                        # Rate limit koruması — sayfalar arasi kisa bekleme
                        await asyncio.sleep(0.5)
                    else:
                        if page > 1:
                            logger.info("%s: Toplam %d sayfa, %d kayit toplandı.",
                                        name, page, len(all_items))
                        break

            R["dosyalar"][name] = _save_json(fname, all_items, data_dir)
            R["basarili"] += 1
            if len(all_items) == 0:
                R["uyarilar"].append(f"{name}: Bos liste (0 kayit).")
            return all_items
        except Exception as e:
            hata = str(e)[:300]
            if hasattr(e, 'response') and e.response is not None:
                hata += f" | Body: {e.response.text[:500]}"
            R["hatalar"].append({"rapor": name, "hata": hata})
            R["basarisiz"] += 1
            # Aninda logla — collect_all_data sonuna ulasilamazsa bile kayit olsun
            save_error_log(
                hata_tipi=classify_error_from_message(hata),
                hata_mesaji=hata,
                data_dir=data_dir,
                adim="collect_list",
                extra={"entity": name, "tarih": today, "hesap": account_label}
            )
            return []

    async def collect_report(name, ad_product_enum, report_type, group_by, columns,
                             days, ad_product_label):
        fname = f"{today}_{name}_{days}d.json"
        key = f"{name}_{days}d"
        if _file_exists_today(fname, data_dir):
            rows = _load_existing_json(fname, data_dir)
            R["dosyalar"][key] = str(data_dir / fname)
            R["atlanan"] += 1
            satir = len(rows) if isinstance(rows, list) else 0
            R["rapor_detay"][key] = {"durum": "CACHE", "satir": satir}
            return key, rows
        try:
            payload = _build_report_payload(ad_product_enum, report_type, group_by, columns, days)
            rows = await client.download_report(payload)
            if _is_error(rows):
                R["hatalar"].append({"rapor": key, "hata": rows.get("error", "?"),
                                     "detay": rows.get("details", "")[:300]})
                R["basarisiz"] += 1
                R["rapor_detay"][key] = {"durum": "HATA", "hata": rows.get("error", "?")}
                return key, []
            R["dosyalar"][key] = _save_json(fname, rows, data_dir)
            R["basarili"] += 1
            satir = len(rows)
            R["rapor_detay"][key] = {"durum": "DOLU" if satir > 0 else "BOS", "satir": satir}
            if satir == 0:
                R["uyarilar"].append(f"{key}: Bos rapor (0 satir).")
            return key, rows
        except Exception as e:
            hata = str(e)[:300]
            if hasattr(e, 'response') and e.response is not None:
                hata += f" | Body: {e.response.text[:500]}"
            R["hatalar"].append({"rapor": key, "hata": hata})
            R["basarisiz"] += 1
            R["rapor_detay"][key] = {"durum": "HATA", "hata": hata[:200]}
            # Aninda logla — collect_all_data sonuna ulasilamazsa bile kayit olsun
            save_error_log(
                hata_tipi=classify_error_from_message(hata),
                hata_mesaji=hata,
                data_dir=data_dir,
                adim="collect_report",
                extra={"rapor": key, "tarih": today, "hesap": account_label}
            )
            return key, []

    # ==================================================================
    # BOLUM 0: PORTFOLIO LISTESI (v3 API — tum reklam tipleri icin ortak)
    # ==================================================================
    logger.info("=== PORTFOLYOLAR ===")

    PORTFOLIO_CT = "application/vnd.spPortfolio.v3+json"
    portfolios = await collect_list(
        "portfolios", "/portfolios/list",
        state_filter=["ENABLED"], extract_key="portfolios",
        content_type=PORTFOLIO_CT, accept=PORTFOLIO_CT,
        custom_body={"stateFilter": {"include": ["ENABLED"]}}
    )
    R["portfolio_sayisi"] = len(portfolios)

    # ==================================================================
    # BOLUM 1: SPONSORED PRODUCTS (SP) — Entity Listeleri
    # ==================================================================
    logger.info("=== SPONSORED PRODUCTS ===")

    sp_camps = await collect_list("sp_campaigns", "/sp/campaigns/list",
                                   ["ENABLED", "PAUSED"], "campaigns",
                                   content_type=SP_CONTENT_TYPES["campaigns"],
                                   accept=SP_CONTENT_TYPES["campaigns"])
    R["sp_kampanya_sayisi"] = len(sp_camps)
    R["sp_kampanya_aktif"] = sum(1 for c in sp_camps if c.get("state", "").upper() == "ENABLED")

    # SP Ad Group'lar (Agent 3 icin gerekli: kampanya → ad_group eslestirme)
    sp_ad_groups = await collect_list("sp_ad_groups", "/sp/adGroups/list",
                                      ["ENABLED", "PAUSED"], "adGroups",
                                      content_type=SP_CONTENT_TYPES["adGroups"],
                                      accept=SP_CONTENT_TYPES["adGroups"])
    R["sp_ad_group_sayisi"] = len(sp_ad_groups)

    # SP Product Ads (Agent 3 icin gerekli: hedeflenen ASIN bilgisi)
    sp_product_ads = await collect_list("sp_product_ads", "/sp/productAds/list",
                                        ["ENABLED", "PAUSED"], "productAds",
                                        content_type=SP_CONTENT_TYPES["productAds"],
                                        accept=SP_CONTENT_TYPES["productAds"])
    R["sp_product_ad_sayisi"] = len(sp_product_ads)

    sp_kws = await collect_list("sp_keywords", "/sp/keywords/list",
                                 ["ENABLED", "PAUSED"], "keywords",
                                 content_type=SP_CONTENT_TYPES["keywords"],
                                 accept=SP_CONTENT_TYPES["keywords"])
    R["sp_keyword_sayisi"] = len(sp_kws)

    sp_targets = await collect_list("sp_targets", "/sp/targets/list",
                                     ["ENABLED", "PAUSED"], "targetingClauses",
                                     content_type=SP_CONTENT_TYPES["targets"],
                                     accept=SP_CONTENT_TYPES["targets"])
    R["sp_target_sayisi"] = len(sp_targets)

    # SP Negatif Keyword'ler (ad group seviyesi)
    sp_neg_kws = await collect_list("sp_negative_keywords", "/sp/negativeKeywords/list",
                                     ["ENABLED"], "negativeKeywords",
                                     content_type=SP_CONTENT_TYPES["negativeKeywords"],
                                     accept=SP_CONTENT_TYPES["negativeKeywords"])
    R["sp_negatif_keyword_sayisi"] = len(sp_neg_kws)

    # SP Kampanya Negatif Keyword'ler (kampanya seviyesi)
    sp_camp_neg_kws = await collect_list("sp_campaign_negative_keywords",
                                          "/sp/campaignNegativeKeywords/list",
                                          ["ENABLED"], "campaignNegativeKeywords",
                                          content_type=SP_CONTENT_TYPES["campaignNegativeKeywords"],
                                          accept=SP_CONTENT_TYPES["campaignNegativeKeywords"])
    R["sp_kampanya_negatif_keyword_sayisi"] = len(sp_camp_neg_kws)

    # SP Negatif Targeting (ASIN/kategori negatif)
    sp_neg_targets = await collect_list("sp_negative_targets", "/sp/negativeTargets/list",
                                         ["ENABLED"], "negativeTargetingClauses",
                                         content_type=SP_CONTENT_TYPES["negativeTargets"],
                                         accept=SP_CONTENT_TYPES["negativeTargets"])
    R["sp_negatif_target_sayisi"] = len(sp_neg_targets)

    # ==================================================================
    # BOLUM 2: SPONSORED BRANDS (SB v4) — Entity Listeleri
    # ==================================================================
    logger.info("=== SPONSORED BRANDS ===")

    sb_camps = await collect_list("sb_campaigns", "/sb/v4/campaigns/list",
                                   ["ENABLED", "PAUSED"], "campaigns",
                                   content_type=SB_CONTENT_TYPES["campaigns"],
                                   accept=SB_CONTENT_TYPES["campaigns"],
                                   max_count=100)  # SB v4: maxResults max 100
    R["sb_kampanya_sayisi"] = len(sb_camps)
    R["sb_kampanya_aktif"] = sum(1 for c in sb_camps if str(c.get("state", c.get("status", ""))).upper() == "ENABLED")

    # SB Ad Group'lar (Agent 3 icin gerekli)
    sb_ad_groups = await collect_list("sb_ad_groups", "/sb/v4/adGroups/list",
                                      ["ENABLED", "PAUSED"], "adGroups",
                                      content_type=SB_CONTENT_TYPES["campaigns"],
                                      accept=SB_CONTENT_TYPES["campaigns"],
                                      max_count=100)  # SB v4: maxResults max 100
    R["sb_ad_group_sayisi"] = len(sb_ad_groups)

    sb_kws = await collect_list("sb_keywords", "/sb/keywords",
                                 ["enabled", "paused"], "keywords",
                                 method="GET",
                                 accept=SB_CONTENT_TYPES["keywords"])
    R["sb_keyword_sayisi"] = len(sb_kws)

    sb_targets = await collect_list("sb_targets", "/sb/targets/list",
                                     ["enabled", "paused"], "targets",
                                     content_type="application/json",
                                     accept=SB_CONTENT_TYPES["targets"],
                                     custom_body={
                                         "filters": [{"filterType": "TARGETING_STATE",
                                                      "values": ["enabled", "paused"]}],
                                         "maxResults": 100  # SB: maxResults max 100
                                     })
    R["sb_target_sayisi"] = len(sb_targets)

    # SB Negatif Keyword'ler
    sb_neg_kws = await collect_list("sb_negative_keywords", "/sb/negativeKeywords",
                                     ["enabled"], "negativeKeywords",
                                     method="GET",
                                     accept="application/vnd.sbnegativekeyword.v3.2+json")
    R["sb_negatif_keyword_sayisi"] = len(sb_neg_kws)

    # ==================================================================
    # BOLUM 3: SPONSORED DISPLAY (SD) — Entity Listeleri
    # ==================================================================
    logger.info("=== SPONSORED DISPLAY ===")

    sd_camps = await collect_list("sd_campaigns", "/sd/campaigns",
                                   ["enabled", "paused"], "campaigns",
                                   method="GET")
    R["sd_kampanya_sayisi"] = len(sd_camps)

    # SD Ad Group'lar (Agent 3 icin gerekli)
    sd_ad_groups = await collect_list("sd_ad_groups", "/sd/adGroups",
                                      ["enabled", "paused"], "adGroups",
                                      method="GET")
    R["sd_ad_group_sayisi"] = len(sd_ad_groups)

    sd_targets = await collect_list("sd_targets", "/sd/targets",
                                     ["enabled", "paused"], "targets",
                                     method="GET")
    R["sd_target_sayisi"] = len(sd_targets)

    # ==================================================================
    # BOLUM 4: PERFORMANS RAPORLARI (v7: 6 rapor, 2'li batch)
    # ==================================================================
    logger.info("=== PERFORMANS RAPORLARI (6 rapor) ===")

    report_tasks = [
        # SP: targeting_14d (bid tavsiyeleri) + search_term_30d (harvesting)
        ("sp_targeting_report", "SPONSORED_PRODUCTS", "spTargeting",
         ["targeting"], SP_TARGETING_COLS, 14, "SP"),
        ("sp_search_term_report", "SPONSORED_PRODUCTS", "spSearchTerm",
         ["searchTerm"], SP_SEARCH_TERM_COLS, 30, "SP"),

        # SB: targeting_14d (bid tavsiyeleri) + search_term_30d (harvesting)
        ("sb_targeting_report", "SPONSORED_BRANDS", "sbTargeting",
         ["targeting"], SB_TARGETING_COLS, 14, "SB"),
        ("sb_search_term_report", "SPONSORED_BRANDS", "sbSearchTerm",
         ["searchTerm"], SB_SEARCH_TERM_COLS, 30, "SB"),

        # SD: targeting_14d (bid tavsiyeleri) + targeting_30d (harvesting)
        ("sd_targeting_report", "SPONSORED_DISPLAY", "sdTargeting",
         ["targeting"], SD_TARGETING_COLS, 14, "SD"),
        ("sd_targeting_report", "SPONSORED_DISPLAY", "sdTargeting",
         ["targeting"], SD_TARGETING_COLS, 30, "SD"),
    ]

    # v7: 2'li batch + batch arasi bekleme
    failed_tasks = []  # Hata alan raporlar
    empty_tasks = []   # Bos donen raporlar

    for i in range(0, len(report_tasks), BATCH_SIZE):
        batch = report_tasks[i:i+BATCH_SIZE]
        coros = []
        for name, ad_prod_enum, rtype, gby, cols, days, ad_label in batch:
            coros.append(collect_report(name, ad_prod_enum, rtype, gby, cols, days, ad_label))

        batch_results = await asyncio.gather(*coros)

        for idx, (name, ad_prod_enum, rtype, gby, cols, days, ad_label) in enumerate(batch):
            key, rows = batch_results[idx]
            if not rows:
                # Hata mi bos mu ayir
                detay = R["rapor_detay"].get(key, {})
                if detay.get("durum") == "BOS":
                    empty_tasks.append((name, ad_prod_enum, rtype, gby, cols, days, ad_label))
                elif detay.get("durum") == "HATA":
                    failed_tasks.append((name, ad_prod_enum, rtype, gby, cols, days, ad_label))

        # Batch arasi bekleme (son batch haric)
        if i + BATCH_SIZE < len(report_tasks):
            logger.info("Batch arasi %ds bekleniyor...", BATCH_DELAY)
            await asyncio.sleep(BATCH_DELAY)

    # ==================================================================
    # BOLUM 5: RETRY TURU
    # Hatali raporlar: 3 deneme (artan bekleme)
    # Bos raporlar: 1 deneme (bossa gecerli kabul et)
    # ==================================================================
    RETRY_DELAYS = [60, 120, 240]
    MAX_RETRIES = len(RETRY_DELAYS)

    # --- 5a: Bos raporlari 1 kez tekrar dene ---
    if empty_tasks:
        logger.info("=== BOS RAPOR RETRY: %d rapor 1 kez tekrar deneniyor ===", len(empty_tasks))
        await asyncio.sleep(60)

        for name, ad_prod_enum, rtype, gby, cols, days, ad_label in empty_tasks:
            key_name = f"{name}_{days}d"

            # Onceki sayaclari duzelt
            R["basarili"] -= 1
            R["uyarilar"] = [u for u in R["uyarilar"] if key_name not in u]

            # Eski dosyayi sil
            fname = f"{today}_{key_name}.json"
            old_file = data_dir / fname
            if old_file.exists():
                old_file.unlink()

            key, rows = await collect_report(name, ad_prod_enum, rtype, gby, cols, days, ad_label)
            detay = R["rapor_detay"].get(key, {})

            if detay.get("durum") == "DOLU":
                logger.info("BOS RETRY BASARILI: %s (%d satir)", key, detay.get("satir", 0))
            else:
                logger.info("BOS RETRY: %s yine bos — gecerli kabul edildi", key)

            R["rapor_detay"][key]["retry"] = True
            R["rapor_detay"][key]["retry_deneme"] = 1

    # --- 5b: Hatali raporlari 3 kez tekrar dene ---
    if failed_tasks:
        logger.info("=== HATA RETRY: %d rapor tekrar deneniyor — max %d deneme ===",
                    len(failed_tasks), MAX_RETRIES)

        for name, ad_prod_enum, rtype, gby, cols, days, ad_label in failed_tasks:
            key_name = f"{name}_{days}d"
            resolved = False

            for attempt in range(MAX_RETRIES):
                delay = RETRY_DELAYS[attempt]
                logger.info("RETRY %d/%d: %s — %ds bekleniyor...",
                            attempt + 1, MAX_RETRIES, key_name, delay)
                await asyncio.sleep(delay)

                R["hatalar"] = [h for h in R["hatalar"] if h.get("rapor") != key_name]
                old_detay = R["rapor_detay"].get(key_name, {})
                if old_detay.get("durum") == "HATA":
                    R["basarisiz"] -= 1

                fname = f"{today}_{key_name}.json"
                old_file = data_dir / fname
                if old_file.exists():
                    old_file.unlink()

                key, rows = await collect_report(name, ad_prod_enum, rtype, gby, cols, days, ad_label)
                detay = R["rapor_detay"].get(key, {})

                if detay.get("durum") in ("DOLU", "BOS"):
                    logger.info("RETRY %d/%d BASARILI: %s", attempt + 1, MAX_RETRIES, key)
                    R["rapor_detay"][key]["retry"] = True
                    R["rapor_detay"][key]["retry_deneme"] = attempt + 1
                    resolved = True
                    break
                else:
                    logger.warning("RETRY %d/%d BASARISIZ: %s", attempt + 1, MAX_RETRIES, key)

            if not resolved:
                R["rapor_detay"][key_name]["retry"] = True
                R["rapor_detay"][key_name]["retry_deneme"] = MAX_RETRIES
                R["rapor_detay"][key_name]["retry_sonuc"] = "basarisiz"
                logger.error("RETRY TUKENDI: %s — %d denemede de basarisiz", key_name, MAX_RETRIES)

    # ==================================================================
    # SONUC OZETI — Sadece teknik detaylar (performans ozeti YOK)
    # ==================================================================
    toplam = R["basarili"] + R["basarisiz"] + R["atlanan"]

    if R["basarisiz"] == 0:
        R["durum"] = "BASARILI"
    elif R["basarili"] > 0:
        R["durum"] = "KISMI_BASARILI"
    else:
        R["durum"] = "BASARISIZ"

    # Teknik rapor detaylari
    dolu_raporlar = []
    bos_raporlar = []
    hatali_raporlar = []
    retry_raporlar = []
    cache_raporlar = []

    for key, detay in R["rapor_detay"].items():
        durum = detay.get("durum", "?")
        retry = detay.get("retry", False)
        retry_sonuc = detay.get("retry_sonuc", "")

        if retry:
            retry_raporlar.append(key)

        if durum == "DOLU":
            dolu_raporlar.append(f"{key} ({detay.get('satir', 0)} satir)")
        elif durum == "BOS":
            bos_raporlar.append(f"{key}{' [retry sonrasi yine bos]' if retry_sonuc == 'yine_bos' else ''}")
        elif durum == "HATA":
            hatali_raporlar.append(f"{key}: {detay.get('hata', '?')[:100]}")
        elif durum == "CACHE":
            cache_raporlar.append(f"{key} ({detay.get('satir', 0)} satir)")

    # Entity istatistikleri
    entity_ozet = (
        f"Portfolio: {R.get('portfolio_sayisi', 0)} | "
        f"SP: {R.get('sp_kampanya_sayisi', 0)} kampanya({R.get('sp_kampanya_aktif', 0)} aktif), "
        f"{R.get('sp_ad_group_sayisi', 0)} ad group, {R.get('sp_product_ad_sayisi', 0)} product ad, "
        f"{R.get('sp_keyword_sayisi', 0)} keyword, {R.get('sp_target_sayisi', 0)} target, "
        f"{R.get('sp_negatif_keyword_sayisi', 0)} neg.kw, {R.get('sp_kampanya_negatif_keyword_sayisi', 0)} camp.neg.kw, "
        f"{R.get('sp_negatif_target_sayisi', 0)} neg.target | "
        f"SB: {R.get('sb_kampanya_sayisi', 0)} kampanya, {R.get('sb_ad_group_sayisi', 0)} ad group, "
        f"{R.get('sb_keyword_sayisi', 0)} keyword, "
        f"{R.get('sb_target_sayisi', 0)} target, {R.get('sb_negatif_keyword_sayisi', 0)} neg.kw | "
        f"SD: {R.get('sd_kampanya_sayisi', 0)} kampanya, {R.get('sd_ad_group_sayisi', 0)} ad group, "
        f"{R.get('sd_target_sayisi', 0)} target"
    )

    R["teknik_ozet"] = {
        "toplam_islem": f"{toplam}/23",
        "basarili": R["basarili"],
        "basarisiz": R["basarisiz"],
        "cache_kullanildi": R["atlanan"],
        "entity_bilgileri": entity_ozet,
        "dolu_raporlar": dolu_raporlar,
        "bos_raporlar": bos_raporlar,
        "hatali_raporlar": hatali_raporlar,
        "retry_yapilanlar": retry_raporlar,
        "cache_raporlar": cache_raporlar,
    }

    R["ozet_mesaj"] = (
        f"Agent 1 v7 tamamlandi. "
        f"Toplam: {toplam}/23 | Basarili: {R['basarili']} | Basarisiz: {R['basarisiz']} | Cache: {R['atlanan']}. "
        f"Dolu rapor: {len(dolu_raporlar)} | Bos rapor: {len(bos_raporlar)} | Hatali: {len(hatali_raporlar)} | "
        f"Retry yapilan: {len(retry_raporlar)}. "
        f"Entity: {entity_ozet}"
    )

    logger.info(R["ozet_mesaj"])

    # Basarisiz islemler varsa agent1_errors.json'a kaydet
    if R["basarisiz"] > 0:
        for hata_kaydi in R["hatalar"]:
            rapor_adi = hata_kaydi.get("rapor", "bilinmiyor")
            hata_mesaji = hata_kaydi.get("hata", "")
            detay = hata_kaydi.get("detay", "")

            hata_tipi = classify_error_from_message(hata_mesaji)

            save_error_log(
                hata_tipi=hata_tipi,
                hata_mesaji=hata_mesaji + (f" | {detay}" if detay else ""),
                data_dir=data_dir,
                adim="collect_all_data",
                extra={"rapor": rapor_adi, "tarih": today, "hesap": account_label}
            )

    await client.close()
    return json.dumps(R, indent=2, ensure_ascii=False)


# ============================================================================
# VERIFY TOOL: Dogrulama icin guncel entity verilerini ceker (Agent 3 verify)
# ============================================================================

class VerifyInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    hesap_key: str
    marketplace: str
    data_date: str | None = None

@mcp.tool(
    name="amazon_ads_collect_verify_data",
    annotations={
        "title": "Verify icin Guncel Entity Verileri Cek",
        "readOnlyHint": True, "destructiveHint": False,
        "idempotentHint": False, "openWorldHint": True,
    },
)
async def amazon_ads_collect_verify_data(params: VerifyInput, ctx: Context = None) -> str:
    """Agent 3 verify icin guncel entity listelerini ceker."""
    config = build_client_config(params.hesap_key, params.marketplace)
    client = AmazonAdsClient(config)
    data_dir = get_data_dir(params.hesap_key, params.marketplace)
    today = datetime.utcnow().strftime("%Y-%m-%d")
    prefix = params.data_date or today

    R = {
        "tarih": today, "verify_prefix": prefix,
        "hesap": f"{params.hesap_key}/{params.marketplace}",
        "dosyalar": {}, "hatalar": [], "uyarilar": [],
        "basarili": 0, "basarisiz": 0,
    }

    async def collect_verify(name, endpoint, state_filter, extract_key,
                              method="POST", content_type="application/json",
                              accept="application/json", max_count=1000,
                              custom_body=None):
        fname = f"{prefix}_verify_{name}.json"
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
                body = custom_body.copy() if custom_body else {
                    "stateFilter": {"include": state_filter}
                }
                if "maxResults" not in body:
                    body["maxResults"] = max_count
                page = 1
                while True:
                    resp = await client.post(endpoint, body,
                                             content_type=content_type, accept=accept)
                    items = resp.get(extract_key, resp) if isinstance(resp, dict) else resp
                    if not isinstance(items, list):
                        items = [items] if items else []
                    all_items.extend(items)
                    next_token = resp.get("nextToken") if isinstance(resp, dict) else None
                    if next_token and len(items) > 0:
                        page += 1
                        body["nextToken"] = next_token
                        await asyncio.sleep(0.5)
                    else:
                        break

            R["dosyalar"][name] = _save_json(fname, all_items, data_dir)
            R["basarili"] += 1
            return all_items
        except Exception as e:
            hata = str(e)[:300]
            R["hatalar"].append({"entity": name, "hata": hata})
            R["basarisiz"] += 1
            return []

    # --- Verify icin gereken entity'ler ---
    logger.info("=== VERIFY: Entity verileri cekiliyor ===")

    # SP entities (bid dogrulama icin)
    sp_camps = await collect_verify("sp_campaigns", "/sp/campaigns/list",
                                     ["ENABLED", "PAUSED"], "campaigns",
                                     content_type=SP_CONTENT_TYPES["campaigns"],
                                     accept=SP_CONTENT_TYPES["campaigns"])
    R["sp_kampanya_sayisi"] = len(sp_camps)

    sp_kws = await collect_verify("sp_keywords", "/sp/keywords/list",
                                   ["ENABLED", "PAUSED"], "keywords",
                                   content_type=SP_CONTENT_TYPES["keywords"],
                                   accept=SP_CONTENT_TYPES["keywords"])
    R["sp_keyword_sayisi"] = len(sp_kws)

    sp_targets = await collect_verify("sp_targets", "/sp/targets/list",
                                       ["ENABLED", "PAUSED"], "targetingClauses",
                                       content_type=SP_CONTENT_TYPES["targets"],
                                       accept=SP_CONTENT_TYPES["targets"])
    R["sp_target_sayisi"] = len(sp_targets)

    # SP Negatif (negatif ekleme dogrulamasi icin)
    sp_neg_kws = await collect_verify("sp_negative_keywords", "/sp/negativeKeywords/list",
                                       ["ENABLED"], "negativeKeywords",
                                       content_type=SP_CONTENT_TYPES["negativeKeywords"],
                                       accept=SP_CONTENT_TYPES["negativeKeywords"])
    R["sp_negatif_keyword_sayisi"] = len(sp_neg_kws)

    sp_neg_targets = await collect_verify("sp_negative_targets", "/sp/negativeTargets/list",
                                           ["ENABLED"], "negativeTargetingClauses",
                                           content_type=SP_CONTENT_TYPES["negativeTargets"],
                                           accept=SP_CONTENT_TYPES["negativeTargets"])
    R["sp_negatif_target_sayisi"] = len(sp_neg_targets)

    # SB keywords (SB bid dogrulama icin)
    sb_kws = await collect_verify("sb_keywords", "/sb/keywords",
                                   ["enabled", "paused"], "keywords",
                                   method="GET",
                                   accept=SB_CONTENT_TYPES["keywords"])
    R["sb_keyword_sayisi"] = len(sb_kws)

    # SD targets
    sd_targets = await collect_verify("sd_targets", "/sd/targets",
                                       ["enabled", "paused"], "targets",
                                       method="GET")
    R["sd_target_sayisi"] = len(sd_targets)

    toplam = R["basarili"] + R["basarisiz"]
    R["durum"] = "BASARILI" if R["basarisiz"] == 0 else "KISMI_BASARILI"
    R["ozet_mesaj"] = (
        f"Verify verileri cekild: {R['basarili']}/{toplam} basarili. "
        f"Dosyalar {prefix}_verify_*.json olarak kaydedildi."
    )
    logger.info(R["ozet_mesaj"])
    await client.close()
    return json.dumps(R, indent=2, ensure_ascii=False)


# ============================================================================
# EXECUTOR TOOL: EXECUTION PLAN UYGULA (Agent 3)
# ============================================================================

@mcp.tool(
    name="amazon_ads_execute_plan",
    annotations={
        "title": "Execution Plan Uygula (Agent 3)",
        "readOnlyHint": False, "destructiveHint": True,
        "idempotentHint": False, "openWorldHint": True,
    },
)
async def amazon_ads_execute_plan(params: ExecutePlanInput, ctx: Context = None) -> str:
    """Agent 3 execution plan JSON'unu okur ve Amazon API uzerinden uygular."""
    config = build_client_config(params.hesap_key, params.marketplace)
    client = AmazonAdsClient(config)
    data_dir = get_data_dir(params.hesap_key, params.marketplace)
    today = datetime.utcnow().strftime("%Y-%m-%d")

    # --- 1. Execution plan dosyasini bul ve oku ---
    if params.plan_file:
        plan_path = Path(params.plan_file)
        # Plan dosya adindan tarihi cikar (gece yarisi korumasi)
        fname = plan_path.name
        if len(fname) >= 10 and fname[4] == "-" and fname[7] == "-":
            today = fname[:10]
    else:
        plan_path = data_dir / "logs" / f"{today}_execution_plan.json"
    if not plan_path.exists():
        return json.dumps({"durum": "HATA", "mesaj": f"Plan dosyasi bulunamadi: {plan_path}"})

    with open(plan_path, "r", encoding="utf-8") as f:
        plan = json.load(f)

    logger.info("Execution plan yuklendi: %s", plan_path)

    # --- 2. Sonuc ve rollback yapilari ---
    results = {
        "tarih": today,
        "durum": "BASLATILDI",
        "bid_sonuclari": [],
        "negatif_sonuclari": [],
        "harvesting_sonuclari": [],
        "rollback_log": [],
        "hatalar": [],
        "ozet": {"basarili": 0, "basarisiz": 0, "atlanan": 0},
    }

    # --- HELPER: Tek bir API cagrisini calistir ---
    async def execute_single(api_endpoint_name, payload):
        ep = WRITE_ENDPOINTS.get(api_endpoint_name)
        if not ep:
            return False, {}, f"Bilinmeyen endpoint: {api_endpoint_name}"

        method = ep["method"]
        path = ep["path"]
        ct = ep["content_type"]
        accept = ep["accept"]
        wrapper = ep["wrapper_key"]

        if wrapper:
            body = {wrapper: [payload]}
        else:
            body = [payload]

        try:
            resp = await client._request_with_retry(
                method, path, body, content_type=ct, accept=accept
            )

            # SP v3 response: {"wrapper": {"success": [...], "error": [...]}}
            if wrapper and isinstance(resp, dict):
                inner = resp.get(wrapper, resp)
                if isinstance(inner, dict):
                    errors = inner.get("error", [])
                    if errors:
                        return False, resp, f"API error: {json.dumps(errors, ensure_ascii=False)[:500]}"
                    return True, resp, None

            # SB/SD response: array or simple object
            if isinstance(resp, list):
                for item in resp:
                    if isinstance(item, dict) and "code" in item:
                        if str(item.get("code", "")).upper() not in ("SUCCESS", "200"):
                            return False, resp, f"API error: {json.dumps(item, ensure_ascii=False)[:500]}"
                return True, resp, None

            return True, resp, None

        except httpx.HTTPStatusError as e:
            error_body = e.response.text[:500] if e.response else "N/A"
            return False, {"http_status": e.response.status_code}, f"HTTP {e.response.status_code}: {error_body}"
        except Exception as e:
            return False, {}, f"Exception: {str(e)[:300]}"

    # --- HELPER: SP v3 response'tan entity ID cikar ---
    def extract_entity_id(response, wrapper_key, id_field):
        if not isinstance(response, dict):
            return None
        inner = response.get(wrapper_key, response)
        if isinstance(inner, dict):
            successes = inner.get("success", [])
            if successes and isinstance(successes[0], dict):
                # ID dogrudan success objesinde veya ic ice entity'de olabilir
                val = successes[0].get(id_field)
                if val:
                    return str(val)
                # Nested: {"success": [{"campaign": {"campaignId": "123"}}]}
                for v in successes[0].values():
                    if isinstance(v, dict) and id_field in v:
                        return str(v[id_field])
        if isinstance(response, dict) and id_field in response:
            return str(response[id_field])
        return None

    # ==========================================================
    # FAZ 1: BID DEGISIKLIKLERI
    # ==========================================================
    logger.info("--- Faz 1: Bid Degisiklikleri (%d islem) ---", len(plan.get("bid_islemleri", [])))
    for op in plan.get("bid_islemleri", []):
        if op.get("status") != "HAZIR":
            results["ozet"]["atlanan"] += 1
            continue

        ep_name = op.get("api_endpoint", "")
        payload = op.get("api_payload", {})

        logger.info("Bid guncelleme: %s — %s ($%.2f -> $%.2f)",
                     op.get("kampanya", ""), op.get("hedefleme", ""),
                     op.get("eski_bid", 0), op.get("yeni_bid", 0))

        success, resp, error = await execute_single(ep_name, payload)

        sonuc = {
            "kampanya": op.get("kampanya", ""),
            "hedefleme": op.get("hedefleme", ""),
            "eski_bid": op.get("eski_bid"),
            "yeni_bid": op.get("yeni_bid"),
            "durum": "BASARILI" if success else "BASARISIZ",
            "hata": error,
        }
        results["bid_sonuclari"].append(sonuc)

        if success:
            results["ozet"]["basarili"] += 1
            _entity_id = ""
            _entity_type = ""
            _ad_type = ""
            _campaign_id = str(payload.get("campaignId", ""))
            if "keywordId" in payload:
                _entity_id = str(payload["keywordId"])
                _entity_type = "KEYWORD"
            elif "targetId" in payload:
                _entity_id = str(payload["targetId"])
                _entity_type = "TARGET"
            if ep_name.startswith("sp_"):
                _ad_type = "SP"
            elif ep_name.startswith("sb_"):
                _ad_type = "SB"
            elif ep_name.startswith("sd_"):
                _ad_type = "SD"

            results["rollback_log"].append({
                "tip": "BID_DEGISIKLIGI",
                "kampanya": op.get("kampanya", ""),
                "hedefleme": op.get("hedefleme", ""),
                "eski_bid": op.get("eski_bid"),
                "yeni_bid": op.get("yeni_bid"),
                "entity_id": _entity_id,
                "entity_type": _entity_type,
                "ad_type": _ad_type,
                "campaign_id": _campaign_id,
                "api_endpoint": ep_name,
                "api_payload": payload,
                "rollback": f"Bid'i {op.get('eski_bid')} olarak geri al",
            })
            logger.info("  -> BASARILI")
        else:
            results["ozet"]["basarisiz"] += 1
            results["hatalar"].append({"faz": "BID", "detay": sonuc, "hata": error})
            logger.error("  -> BASARISIZ: %s", error)

        await asyncio.sleep(EXECUTE_DELAY_BETWEEN_OPS)

    # ==========================================================
    # FAZ 2: NEGATIF EKLEMELER
    # ==========================================================
    logger.info("--- Faz 2: Negatif Eklemeler (%d islem) ---", len(plan.get("negatif_islemleri", [])))
    for op in plan.get("negatif_islemleri", []):
        if op.get("status") != "HAZIR":
            results["ozet"]["atlanan"] += 1
            continue

        ep_name = op.get("api_endpoint", "")
        payload = op.get("api_payload", {})

        logger.info("Negatif ekleme: %s — %s (%s)",
                     op.get("kampanya", ""), op.get("hedefleme", ""), op.get("tip", ""))

        success, resp, error = await execute_single(ep_name, payload)

        sonuc = {
            "kampanya": op.get("kampanya", ""),
            "hedefleme": op.get("hedefleme", ""),
            "tip": op.get("tip", ""),
            "durum": "BASARILI" if success else "BASARISIZ",
            "hata": error,
        }
        results["negatif_sonuclari"].append(sonuc)

        if success:
            results["ozet"]["basarili"] += 1
            results["rollback_log"].append({
                "tip": op.get("tip", "NEGATIF"),
                "kampanya": op.get("kampanya", ""),
                "hedefleme": op.get("hedefleme", ""),
                "campaign_id": str(payload.get("campaignId", "")),
                "api_endpoint": ep_name,
                "api_payload": payload,
            })
            logger.info("  -> BASARILI")
        else:
            results["ozet"]["basarisiz"] += 1
            results["hatalar"].append({"faz": "NEGATIF", "detay": sonuc, "hata": error})
            logger.error("  -> BASARISIZ: %s", error)

        await asyncio.sleep(EXECUTE_DELAY_BETWEEN_OPS)

    # ==========================================================
    # FAZ 3: HARVESTING (sub_operations with chain dependency)
    # ==========================================================
    logger.info("--- Faz 3: Harvesting (%d islem) ---", len(plan.get("harvesting_islemleri", [])))
    for harvest in plan.get("harvesting_islemleri", []):
        if harvest.get("status") != "HAZIR":
            results["ozet"]["atlanan"] += 1
            continue

        sub_ops = harvest.get("sub_operations", [])
        logger.info("Harvesting: %s — %s (%d sub-op)",
                     harvest.get("hedefleme", ""), harvest.get("tip", ""), len(sub_ops))

        harvest_result = {
            "hedefleme": harvest.get("hedefleme", ""),
            "tip": harvest.get("tip", ""),
            "kampanya_adi": harvest.get("kampanya_adi", ""),
            "sub_sonuclar": [],
            "durum": "BASARILI",
        }

        new_campaign_id = None
        new_ad_group_id = None
        chain_broken = False

        for sub_op in sub_ops:
            if chain_broken:
                harvest_result["sub_sonuclar"].append({
                    "op": sub_op.get("op", ""),
                    "durum": "ATLANDI",
                    "hata": "Onceki islem basarisiz (chain broken)",
                })
                continue

            ep_name = sub_op.get("api_endpoint", "")
            payload = dict(sub_op.get("api_payload", {}))

            # --- Placeholder degistir ---
            placeholder_fail = False
            for key in list(payload.keys()):
                val = payload[key]
                if val == "__YENI_KAMPANYA_ID__":
                    if new_campaign_id:
                        payload[key] = new_campaign_id
                    else:
                        placeholder_fail = True
                        chain_broken = True
                        harvest_result["sub_sonuclar"].append({
                            "op": sub_op.get("op", ""),
                            "durum": "BASARISIZ",
                            "hata": "Kampanya ID henuz mevcut degil",
                        })
                        break
                elif val == "__YENI_AD_GROUP_ID__":
                    if new_ad_group_id:
                        payload[key] = new_ad_group_id
                    else:
                        placeholder_fail = True
                        chain_broken = True
                        harvest_result["sub_sonuclar"].append({
                            "op": sub_op.get("op", ""),
                            "durum": "BASARISIZ",
                            "hata": "Ad Group ID henuz mevcut degil",
                        })
                        break

            if placeholder_fail:
                continue

            logger.info("  Sub-op: %s — %s", sub_op.get("op", ""), ep_name)
            success, resp, error = await execute_single(ep_name, payload)

            sub_sonuc = {
                "op": sub_op.get("op", ""),
                "durum": "BASARILI" if success else "BASARISIZ",
                "hata": error,
            }

            if success:
                ep_info = WRITE_ENDPOINTS.get(ep_name, {})
                wrapper = ep_info.get("wrapper_key")

                if sub_op.get("op") == "KAMPANYA_OLUSTUR" and wrapper:
                    extracted = extract_entity_id(resp, wrapper, "campaignId")
                    if extracted:
                        new_campaign_id = extracted
                        sub_sonuc["campaignId"] = extracted
                        logger.info("    -> Yeni kampanya ID: %s", extracted)
                    else:
                        chain_broken = True
                        sub_sonuc["durum"] = "BASARISIZ"
                        sub_sonuc["hata"] = f"Kampanya olusturuldu ama ID alinamadi. Response: {json.dumps(resp, ensure_ascii=False)[:300]}"
                        logger.error("    -> ID alinamadi: %s", json.dumps(resp, ensure_ascii=False)[:300])

                elif sub_op.get("op") == "AD_GROUP_OLUSTUR" and wrapper:
                    extracted = extract_entity_id(resp, wrapper, "adGroupId")
                    if extracted:
                        new_ad_group_id = extracted
                        sub_sonuc["adGroupId"] = extracted
                        logger.info("    -> Yeni ad group ID: %s", extracted)
                    else:
                        chain_broken = True
                        sub_sonuc["durum"] = "BASARISIZ"
                        sub_sonuc["hata"] = f"Ad group olusturuldu ama ID alinamadi. Response: {json.dumps(resp, ensure_ascii=False)[:300]}"
                        logger.error("    -> ID alinamadi: %s", json.dumps(resp, ensure_ascii=False)[:300])
                else:
                    logger.info("    -> BASARILI")
            else:
                chain_broken = True
                logger.error("    -> BASARISIZ: %s", error)

            harvest_result["sub_sonuclar"].append(sub_sonuc)
            await asyncio.sleep(EXECUTE_DELAY_BETWEEN_OPS)

        # Harvesting ozet durumu
        failed_subs = [s for s in harvest_result["sub_sonuclar"] if s["durum"] != "BASARILI"]
        if failed_subs:
            harvest_result["durum"] = "KISMI_BASARISIZ" if any(
                s["durum"] == "BASARILI" for s in harvest_result["sub_sonuclar"]
            ) else "BASARISIZ"
            results["ozet"]["basarisiz"] += 1
        else:
            results["ozet"]["basarili"] += 1

        results["harvesting_sonuclari"].append(harvest_result)
        results["rollback_log"].append({
            "tip": harvest.get("tip", "HARVEST"),
            "hedefleme": harvest.get("hedefleme", ""),
            "yeni_kampanya_adi": harvest.get("kampanya_adi", ""),
            "kaynak_kampanya": harvest.get("kaynak_kampanya", ""),
            "source_campaign_id": str(harvest.get("source_campaign_id", "")),
            "new_campaign_id": new_campaign_id,
            "new_ad_group_id": new_ad_group_id,
            "sub_operations": harvest_result["sub_sonuclar"],
        })

    # ==========================================================
    # ROLLBACK LOG KAYDET
    # ==========================================================
    logs_dir = data_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    rollback = {
        "tarih": today,
        "olusturma_zamani": datetime.utcnow().isoformat(),
        "islemler": results["rollback_log"],
    }
    rollback_path = logs_dir / f"{today}_rollback.json"
    with open(rollback_path, "w", encoding="utf-8") as f:
        json.dump(rollback, f, indent=2, ensure_ascii=False)

    # ==========================================================
    # SONUC
    # ==========================================================
    total = results["ozet"]["basarili"] + results["ozet"]["basarisiz"] + results["ozet"]["atlanan"]
    if results["ozet"]["basarisiz"] == 0 and results["ozet"]["basarili"] > 0:
        results["durum"] = "BASARILI"
    elif results["ozet"]["basarili"] > 0:
        results["durum"] = "KISMI_BASARILI"
    else:
        results["durum"] = "BASARISIZ"

    results["ozet_mesaj"] = (
        f"Execution tamamlandi. "
        f"Toplam: {total} | Basarili: {results['ozet']['basarili']} | "
        f"Basarisiz: {results['ozet']['basarisiz']} | Atlanan: {results['ozet']['atlanan']}"
    )
    results["rollback_dosyasi"] = str(rollback_path)

    logger.info("=== AGENT 3 EXECUTION TAMAMLANDI: %s ===", results["durum"])
    logger.info(results["ozet_mesaj"])
    await client.close()
    return json.dumps(results, indent=2, ensure_ascii=False)


# ============================================================================
# PROFIL TOOL
# ============================================================================

@mcp.tool(
    name="amazon_ads_get_profiles",
    annotations={"title": "Profil Listesi", "readOnlyHint": True,
                 "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def amazon_ads_get_profiles(params: AccountInput, ctx: Context = None) -> str:
    """Amazon Advertising API profillerini listeler."""
    try:
        config = build_client_config(params.hesap_key, params.marketplace)
        client = AmazonAdsClient(config)
        profiles = await client.get("/v2/profiles")
        await client.close()
        if isinstance(profiles, list):
            for p in profiles:
                acc_info = p.get("accountInfo", {})
                p["_account_id_hint"] = acc_info.get("id", "N/A")
                p["_marketplace_hint"] = acc_info.get("marketplaceStringId", "N/A")
        return json.dumps(profiles, indent=2, ensure_ascii=False)
    except Exception as e:
        if isinstance(e, httpx.HTTPStatusError):
            return f"API Hatasi ({e.response.status_code}): {e.response.text[:500]}"
        return f"Hata: {str(e)}"


# ============================================================================
# HESAP LISTESI TOOL
# ============================================================================

@mcp.tool(
    name="amazon_ads_list_accounts",
    annotations={"title": "Hesap Listesi", "readOnlyHint": True,
                 "destructiveHint": False, "idempotentHint": True, "openWorldHint": False},
)
async def amazon_ads_list_accounts(params: EmptyInput, ctx: Context = None) -> str:
    """accounts.json'daki tum aktif hesap+marketplace kombinasyonlarini listeler."""
    accounts = load_accounts()
    result = {"hesaplar": [], "toplam_aktif": 0}
    for hesap_key, hesap in accounts.get("hesaplar", {}).items():
        for mp_code, mp_config in hesap.get("marketplaces", {}).items():
            aktif = mp_config.get("aktif", False)
            result["hesaplar"].append({
                "hesap_key": hesap_key,
                "marketplace": mp_code,
                "hesap_adi": hesap.get("hesap_adi", hesap_key),
                "profile_id": mp_config.get("profile_id", ""),
                "aktif": aktif,
            })
            if aktif:
                result["toplam_aktif"] += 1
    return json.dumps(result, indent=2, ensure_ascii=False)


# ============================================================================
# BASLAT
# ============================================================================

if __name__ == "__main__":
    mcp.run()
