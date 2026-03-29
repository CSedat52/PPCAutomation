"""
Data Loader — Supabase-First, JSON-Fallback Veri Yükleme
==========================================================
Tüm pipeline bileşenleri (Agent 2, 3, 4, Maestro, pipeline_runner)
bu modülü kullanarak veri okur.

Strateji:
  1. Supabase raw_data kolonundan oku (orijinal JSON formatı korunmuş)
  2. Supabase başarısız olursa JSON dosyasına fallback
  3. JSON dosyası da yoksa boş liste/dict dön

raw_data kolonu sayesinde downstream kodda field erişimi değişmiyor:
  row["campaignId"], row["keywordBid"], row["sales14d"] vb. aynen çalışır.
"""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger("data_loader")


# ============================================================================
# SUPABASE BAĞLANTI
# ============================================================================

def _get_sdb():
    from supabase.db_client import SupabaseClient
    return SupabaseClient()


def _fetch_raw_data(table, hesap_key, marketplace,
                    extra_where="", extra_params=()):
    """
    Supabase tablosundan raw_data kolonunu çek.
    Returns: list[dict] veya None (Supabase başarısız).
    """
    try:
        sdb = _get_sdb()
        sql = (f"SELECT raw_data FROM {table} "
               f"WHERE hesap_key = %s AND marketplace = %s {extra_where}")
        params = (hesap_key, marketplace) + tuple(extra_params)
        rows = sdb._fetch_all(sql, params)
        if rows is None:
            return None
        result = []
        for row in rows:
            rd = row[0]
            if rd is None:
                continue
            if isinstance(rd, dict):
                result.append(rd)
            elif isinstance(rd, str):
                result.append(json.loads(rd))
        return result
    except Exception as e:
        logger.debug("Supabase raw_data okunamadi (%s): %s", table, e)
        return None


def _fetch_count(table, hesap_key, marketplace,
                 extra_where="", extra_params=()):
    """Supabase tablosundan kayıt sayısı dön. -1 = hata."""
    try:
        sdb = _get_sdb()
        sql = (f"SELECT COUNT(*) FROM {table} "
               f"WHERE hesap_key = %s AND marketplace = %s {extra_where}")
        params = (hesap_key, marketplace) + tuple(extra_params)
        row = sdb._fetch_one(sql, params)
        return row[0] if row else 0
    except Exception:
        return -1


# ============================================================================
# JSON FALLBACK
# ============================================================================

def _json_load(data_dir, filename):
    """JSON dosyasından oku. Yoksa None dön."""
    path = Path(data_dir) / filename
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _json_load_with_date(data_dir, prefix, date=None):
    """
    {date}_{prefix}.json dosyasını oku.
    Bulunamazsa bir önceki günü dene.
    """
    if date:
        result = _json_load(data_dir, f"{date}_{prefix}.json")
        if result is not None:
            return result
        # Bir önceki gün
        try:
            onceki = (datetime.strptime(date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
            result = _json_load(data_dir, f"{onceki}_{prefix}.json")
            if result is not None:
                logger.info("JSON fallback: %s bugunun dosyasi yok, onceki gun kullaniliyor", prefix)
                return result
        except Exception:
            pass

    # Tarihsiz: en son dosyayı bul
    data_path = Path(data_dir)
    candidates = sorted(data_path.glob(f"*_{prefix}.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if candidates:
        try:
            with open(candidates[0], "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return []


# ============================================================================
# ENTITY LOADERS (Agent 1 çıktıları)
# ============================================================================

def load_campaigns(hesap_key, marketplace, ad_type, data_dir, date=None):
    """Kampanya listesi. Supabase-first, JSON fallback."""
    data = _fetch_raw_data("campaigns", hesap_key, marketplace,
                           "AND ad_type = %s", (ad_type,))
    if data is not None:
        return data
    prefix = f"{ad_type.lower()}_campaigns"
    return _json_load_with_date(data_dir, prefix, date) or []


def load_keywords(hesap_key, marketplace, ad_type, data_dir, date=None):
    """Keyword listesi."""
    data = _fetch_raw_data("keywords", hesap_key, marketplace,
                           "AND ad_type = %s", (ad_type,))
    if data is not None:
        return data
    prefix = f"{ad_type.lower()}_keywords"
    return _json_load_with_date(data_dir, prefix, date) or []


def load_targets(hesap_key, marketplace, ad_type, data_dir, date=None):
    """Target listesi."""
    data = _fetch_raw_data("targets", hesap_key, marketplace,
                           "AND ad_type = %s", (ad_type,))
    if data is not None:
        return data
    prefix = f"{ad_type.lower()}_targets"
    return _json_load_with_date(data_dir, prefix, date) or []


def load_negative_keywords(hesap_key, marketplace, ad_type, data_dir,
                           scope="AD_GROUP", date=None):
    """Negatif keyword listesi."""
    data = _fetch_raw_data("negative_keywords", hesap_key, marketplace,
                           "AND ad_type = %s AND scope = %s", (ad_type, scope))
    if data is not None:
        return data
    if scope == "CAMPAIGN":
        prefix = f"{ad_type.lower()}_campaign_negative_keywords"
    else:
        prefix = f"{ad_type.lower()}_negative_keywords"
    return _json_load_with_date(data_dir, prefix, date) or []


def load_negative_targets(hesap_key, marketplace, data_dir, date=None):
    """SP negatif target listesi."""
    data = _fetch_raw_data("negative_targets", hesap_key, marketplace)
    if data is not None:
        return data
    return _json_load_with_date(data_dir, "sp_negative_targets", date) or []


def load_portfolios(hesap_key, marketplace, data_dir, date=None):
    """Portfolio listesi."""
    data = _fetch_raw_data("portfolios", hesap_key, marketplace)
    if data is not None:
        return data
    return _json_load_with_date(data_dir, "portfolios", date) or []


def load_product_ads(hesap_key, marketplace, data_dir, date=None):
    """SP product ads (ASIN + SKU)."""
    data = _fetch_raw_data("product_ads", hesap_key, marketplace)
    if data is not None:
        return data
    return _json_load_with_date(data_dir, "sp_product_ads", date) or []


def load_themes(hesap_key, marketplace, data_dir, date=None):
    """SB themes listesi."""
    data = _fetch_raw_data("themes", hesap_key, marketplace)
    if data is not None:
        return data
    return _json_load_with_date(data_dir, "sb_themes", date) or []


# ============================================================================
# REPORT LOADERS
# ============================================================================

def load_targeting_report(hesap_key, marketplace, ad_type, period,
                          data_dir, date):
    """Targeting raporu. period: '14d' veya '30d'."""
    data = _fetch_raw_data("targeting_reports", hesap_key, marketplace,
                           "AND ad_type = %s AND report_period = %s AND collection_date = %s",
                           (ad_type, period, date))
    if data is not None:
        return data
    prefix = f"{ad_type.lower()}_targeting_report_{period}"
    return _json_load_with_date(data_dir, prefix, date) or []


def load_search_term_report(hesap_key, marketplace, ad_type, period,
                            data_dir, date):
    """Search term raporu."""
    data = _fetch_raw_data("search_term_reports", hesap_key, marketplace,
                           "AND ad_type = %s AND collection_date = %s",
                           (ad_type, date))
    if data is not None:
        return data
    prefix = f"{ad_type.lower()}_search_term_report_{period}"
    return _json_load_with_date(data_dir, prefix, date) or []


def load_campaign_report(hesap_key, marketplace, ad_type, period,
                         data_dir, date):
    """Campaign raporu."""
    data = _fetch_raw_data("campaign_reports", hesap_key, marketplace,
                           "AND ad_type = %s AND collection_date = %s",
                           (ad_type, date))
    if data is not None:
        return data
    prefix = f"{ad_type.lower()}_campaign_report_{period}"
    return _json_load_with_date(data_dir, prefix, date) or []


# ============================================================================
# TOPLU YÜKLEME (Agent 2 uyumlu nested dict)
# ============================================================================

def load_all_agent1_data(hesap_key, marketplace, data_dir, date=None):
    """
    Tüm Agent 1 entity ve rapor verilerini Supabase-first yükle.
    Agent 2 analyst.py'nin beklediği nested dict formatını döner.

    Returns:
        {
            "portfolios": [...],
            "sp": {"campaigns": [...], "keywords": [...], ...},
            "sb": {"campaigns": [...], "keywords": [...], ...},
            "sd": {"campaigns": [...], "targets": [...]},
            "reports": {
                "sp_targeting_report_14d": [...],
                "sp_search_term_report_30d": [...],
                ...
            }
        }
    """
    data = {
        "portfolios": load_portfolios(hesap_key, marketplace, data_dir, date),
        "sp": {
            "campaigns": load_campaigns(hesap_key, marketplace, "SP", data_dir, date),
            "keywords": load_keywords(hesap_key, marketplace, "SP", data_dir, date),
            "targets": load_targets(hesap_key, marketplace, "SP", data_dir, date),
            "negative_keywords": load_negative_keywords(hesap_key, marketplace, "SP", data_dir, "AD_GROUP", date),
            "campaign_negative_keywords": load_negative_keywords(hesap_key, marketplace, "SP", data_dir, "CAMPAIGN", date),
            "negative_targets": load_negative_targets(hesap_key, marketplace, data_dir, date),
        },
        "sb": {
            "campaigns": load_campaigns(hesap_key, marketplace, "SB", data_dir, date),
            "keywords": load_keywords(hesap_key, marketplace, "SB", data_dir, date),
            "targets": load_targets(hesap_key, marketplace, "SB", data_dir, date),
            "negative_keywords": load_negative_keywords(hesap_key, marketplace, "SB", data_dir, "AD_GROUP", date),
        },
        "sd": {
            "campaigns": load_campaigns(hesap_key, marketplace, "SD", data_dir, date),
            "targets": load_targets(hesap_key, marketplace, "SD", data_dir, date),
        },
        "reports": {
            "sp_targeting_report_14d": load_targeting_report(hesap_key, marketplace, "SP", "14d", data_dir, date),
            "sp_search_term_report_30d": load_search_term_report(hesap_key, marketplace, "SP", "30d", data_dir, date),
            "sb_targeting_report_14d": load_targeting_report(hesap_key, marketplace, "SB", "14d", data_dir, date),
            "sb_search_term_report_30d": load_search_term_report(hesap_key, marketplace, "SB", "30d", data_dir, date),
            "sd_targeting_report_14d": load_targeting_report(hesap_key, marketplace, "SD", "14d", data_dir, date),
            "sd_targeting_report_30d": load_targeting_report(hesap_key, marketplace, "SD", "30d", data_dir, date),
        },
    }

    # Kaynak bilgisi logla
    sp_camp_count = len(data["sp"]["campaigns"])
    report_count = sum(len(v) for v in data["reports"].values())
    logger.info("Agent 1 verileri yuklendi: SP kampanya=%d, rapor satirlari=%d",
                sp_camp_count, report_count)
    return data


# ============================================================================
# KAMPANYA SAYISI (smart-skip için)
# ============================================================================

def count_campaigns(hesap_key, marketplace, ad_type, data_dir, date=None):
    """
    Kampanya sayısını dön. Smart-skip kararı için.
    Returns: int (0 = kampanya yok, >0 = var, -1 = belirlenemedi)
    """
    count = _fetch_count("campaigns", hesap_key, marketplace,
                         "AND ad_type = %s", (ad_type,))
    if count >= 0:
        return count

    # JSON fallback
    prefix = f"{ad_type.lower()}_campaigns"
    data = _json_load_with_date(data_dir, prefix, date)
    if data is not None:
        return len(data) if isinstance(data, list) else 0
    return -1


# ============================================================================
# RAPOR VARLIK KONTROLÜ (pipeline_runner için)
# ============================================================================

def check_report_exists(hesap_key, marketplace, table, ad_type, date):
    """Supabase'de belirli tarihte rapor var mı kontrol et."""
    try:
        sdb = _get_sdb()
        row = sdb._fetch_one(
            f"SELECT 1 FROM {table} WHERE hesap_key = %s AND marketplace = %s "
            f"AND collection_date = %s AND ad_type = %s LIMIT 1",
            (hesap_key, marketplace, date, ad_type)
        )
        return row is not None
    except Exception:
        return None  # Belirlenemedi


# ============================================================================
# MAESTRO STATE (pipeline_runs Supabase-first)
# ============================================================================

def load_pipeline_sessions(hesap_key, marketplace, limit=50):
    """Pipeline session geçmişini Supabase'den yükle."""
    try:
        sdb = _get_sdb()
        rows = sdb._fetch_all("""
            SELECT session_id, status, agent1_status, agent2_status,
                   agent3_status, agent4_status, started_at, completed_at,
                   summary, current_step, error_message
            FROM pipeline_runs
            WHERE hesap_key = %s AND marketplace = %s
            ORDER BY started_at DESC LIMIT %s
        """, (hesap_key, marketplace, limit))
        if rows:
            cols = ["session_id", "status", "agent1_status", "agent2_status",
                    "agent3_status", "agent4_status", "started_at", "completed_at",
                    "summary", "current_step", "error_message"]
            return [dict(zip(cols, r)) for r in rows]
    except Exception as e:
        logger.debug("Pipeline sessions Supabase'den okunamadi: %s", e)
    return None


# ============================================================================
# VERIFY DOSYALARI (ephemeral — sadece JSON, Supabase'e gitmez)
# ============================================================================

def load_verify_file(data_dir, date, entity_prefix):
    """Verify dosyası oku. Bu dosyalar ephemeral — sadece JSON."""
    return _json_load(data_dir, f"{date}_verify_{entity_prefix}.json") or []
