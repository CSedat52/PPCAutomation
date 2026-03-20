"""
Shared Log Utilities — Ortak Hata Loglama Altyapisi
=====================================================
Tum agentlar ve Maestro bu modulu kullanarak tutarli
hata kayitlari olusturur.

Kullanim:
  from log_utils import save_error_log, classify_error_type, ERROR_TYPES

Dosya konumu: Proje koku (BASE_DIR)
"""

import json
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("log_utils")

# ============================================================================
# ORTAK HATA TAKSONOMISI
# ============================================================================

ERROR_TYPES = {
    "RateLimit":         "HTTP 429, API throttling",
    "AuthError":         "HTTP 401/403, token suresi dolmus",
    "ApiError":          "HTTP 400, format/validation hatasi",
    "ServerError":       "HTTP 500+, Amazon sunucu hatasi",
    "NetworkError":      "Timeout, connection, DNS hatasi",
    "FileNotFound":      "Dosya veya rapor bulunamadi",
    "DataError":         "JSON parse, format uyumsuzlugu, eksik alan",
    "Preflight":         "On kontrol basarisiz",
    "ExecutionError":    "API islemi basarisiz (bid, negatif, harvesting)",
    "VerificationError": "Dogrulama uyusmazligi",
    "AgentFailure":      "Alt agent calistirma hatasi",
    "ReportFailed":      "Rapor indirme/olusturma basarisiz",
    "InternalError":     "Beklenmeyen Python exception",
}

# Python exception -> ortak tip eslestirme
_EXCEPTION_MAP = {
    "FileNotFoundError": "FileNotFound",
    "PermissionError":   "FileNotFound",
    "json.JSONDecodeError": "DataError",
    "JSONDecodeError":   "DataError",
    "KeyError":          "DataError",
    "ValueError":        "DataError",
    "TypeError":         "DataError",
    "IndexError":        "DataError",
    "ConnectionError":   "NetworkError",
    "TimeoutError":      "NetworkError",
    "OSError":           "NetworkError",
    "IOError":           "NetworkError",
}


def classify_error_type(error_str):
    """
    Hata mesajindan veya exception sinif adindan ortak hata tipini belirler.

    Args:
        error_str: Exception sinif adi veya hata mesaji

    Returns:
        str: ERROR_TYPES'taki degerlerden biri
    """
    error_lower = str(error_str).lower()

    # HTTP status kodlarina gore
    if any(k in error_lower for k in ["429", "rate limit", "too many requests", "throttl"]):
        return "RateLimit"

    if any(k in error_lower for k in ["401", "403", "unauthorized", "forbidden", "token expired"]):
        return "AuthError"

    if any(k in error_lower for k in ["400", "bad request", "validation", "malformed"]):
        return "ApiError"

    if any(k in error_lower for k in ["500", "502", "503", "504", "internal server",
                                       "bad gateway", "service unavail", "gateway timeout"]):
        return "ServerError"

    if any(k in error_lower for k in ["timeout", "connection", "network", "dns",
                                       "refused", "reset", "broken pipe", "eof", "ssl"]):
        return "NetworkError"

    if any(k in error_lower for k in ["not found", "no such file", "filenotfound",
                                       "dosya bulunamadi", "bulunamadi"]):
        return "FileNotFound"

    if any(k in error_lower for k in ["parse", "decode", "invalid", "format",
                                       "keyerror", "valueerror", "typeerror"]):
        return "DataError"

    if any(k in error_lower for k in ["preflight", "on kontrol", "on_kontrol"]):
        return "Preflight"

    if any(k in error_lower for k in ["rapor basarisiz", "report failed", "report error"]):
        return "ReportFailed"

    # Python exception sinif adi eslestirme
    exc_name = str(error_str).split(":")[0].strip().split(".")[-1]
    if exc_name in _EXCEPTION_MAP:
        return _EXCEPTION_MAP[exc_name]

    return "InternalError"


def normalize_error_type(hata_tipi):
    """
    Eski formattaki hata tiplerini yeni taksonomiye normalize eder.
    Geriye uyumluluk icin — Agent 4 ErrorAnalyzer bu fonksiyonu kullanir.

    Args:
        hata_tipi: Eski veya yeni formattaki hata tipi string

    Returns:
        str: Normalize edilmis hata tipi
    """
    # Zaten yeni taksonomideyse aynen don
    if hata_tipi in ERROR_TYPES:
        return hata_tipi

    # Eski Agent 1 formatlari
    mapping = {
        "ApiError_400":  "ApiError",
        "Timeout":       "NetworkError",
        # retry_handler formatlari (kucuk harf + underscore)
        "rate_limit":    "RateLimit",
        "auth_error":    "AuthError",
        "data_error":    "DataError",
        "network":       "NetworkError",
        "server_error":  "ServerError",
        "file_not_found": "FileNotFound",
    }

    if hata_tipi in mapping:
        return mapping[hata_tipi]

    # Python exception isimleri
    if hata_tipi in _EXCEPTION_MAP:
        return _EXCEPTION_MAP[hata_tipi]

    return hata_tipi


# ============================================================================
# ORTAK save_error_log FONKSIYONU
# ============================================================================

def save_error_log(hata_tipi, hata_mesaji, log_dir,
                   traceback_str=None, adim=None, extra=None,
                   session_id=None, agent_name=None,
                   max_kayit=200,
                   hesap_key=None, marketplace=None):
    """
    Tum agentlar icin birlesik hata log fonksiyonu.

    Parametreler:
        hata_tipi     : ERROR_TYPES'taki degerlerden biri
        hata_mesaji   : Hata aciklamasi (max 500 char)
        log_dir       : Log dosyasinin yazilacagi klasor (Path)
                        Dosya adi: {agent_name}_errors.json
        traceback_str : traceback.format_exc() ciktisi (opsiyonel, max 1000 char)
        adim          : Hatanin gerceklestigi adim (orn. "collect_list", "preflight")
        extra         : Ek baglam dict (orn. {"endpoint": "/sp/campaigns", "status": 429})
        session_id    : Pipeline session ID'si (korelasyon icin)
        agent_name    : Agent adi (dosya adi icin: "agent1", "agent2", "agent3", "agent4", "maestro")
        max_kayit     : Dosyadaki max kayit sayisi (varsayilan 200)

    Returns:
        bool: Basarili ise True
    """
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    # Dosya adi belirle
    if agent_name:
        filename = f"{agent_name}_errors.json"
    else:
        filename = "errors.json"

    log_path = log_dir / filename

    try:
        with open(log_path, "r", encoding="utf-8") as f:
            kayitlar = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        kayitlar = []

    # Hata tipini normalize et
    hata_tipi = normalize_error_type(hata_tipi)

    kayit = {
        "timestamp":  datetime.utcnow().isoformat(),
        "hata_tipi":  hata_tipi,
        "hata_mesaji": str(hata_mesaji)[:500],
        "adim":       adim or "bilinmiyor",
    }

    if traceback_str:
        kayit["traceback"] = str(traceback_str)[:1000]
    if extra:
        kayit["extra"] = extra
    if session_id:
        kayit["session_id"] = session_id

    kayitlar.append(kayit)

    # Eski kayitlari temizle
    if len(kayitlar) > max_kayit:
        kayitlar = kayitlar[-max_kayit:]

    try:
        with open(log_path, "w", encoding="utf-8") as f:
            json.dump(kayitlar, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error("Hata logu yazilamadi %s: %s", log_path, e)
        return False

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
                """INSERT INTO agent_logs (agent_id, level, message, error_type,
                    hesap_key, marketplace, session_id, traceback, created_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())""",
                (agent_name, "error", str(hata_mesaji)[:500], hata_tipi,
                 hesap_key, marketplace, session_id,
                 str(traceback_str)[:1000] if traceback_str else None)
            )
        except Exception:
            pass  # Supabase yazamazsa lokal zaten yazildi, sessizce devam et

    return True


def save_log(level, message, agent_name, hesap_key=None, marketplace=None,
             session_id=None, error_type=None, traceback_str=None, extra=None):
    """
    Genel log fonksiyonu — info, warn, error tum seviyeleri destekler.
    Sadece Supabase agent_logs tablosuna yazar (lokal JSON'a YAZMAZ).
    Dashboard gorunurlugu icin.

    level: "info", "warn", "error"
    """
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
            """INSERT INTO agent_logs (agent_id, level, message, error_type,
                hesap_key, marketplace, session_id, traceback, created_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())""",
            (agent_name, level, str(message)[:500], error_type,
             hesap_key, marketplace, session_id,
             str(traceback_str)[:1000] if traceback_str else None)
        )
    except Exception:
        pass  # Dashboard gorunurlugu icin — basarisiz olursa agent calismaya devam eder


# ============================================================================
# LOG ROTASYONU
# ============================================================================

def rotate_text_logs(log_dir, pattern="*.log", max_age_days=30):
    """
    Belirtilen gun sayisindan eski text log dosyalarini siler.

    Args:
        log_dir: Log klasoru
        pattern: Dosya patterni (varsayilan *.log)
        max_age_days: Maks gun (varsayilan 30)

    Returns:
        int: Silinen dosya sayisi
    """
    log_dir = Path(log_dir)
    if not log_dir.exists():
        return 0

    now = datetime.utcnow()
    silinen = 0

    for f in log_dir.glob(pattern):
        try:
            # Dosya yas kontrolu
            mtime = datetime.utcfromtimestamp(f.stat().st_mtime)
            age_days = (now - mtime).days
            if age_days > max_age_days:
                f.unlink()
                silinen += 1
                logger.info("Eski log silindi: %s (%d gun)", f.name, age_days)
        except Exception:
            continue

    return silinen
