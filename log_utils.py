"""
Shared Log Utilities — Ortak Hata Loglama Altyapisi
=====================================================
Tum agentlar ve Maestro bu modulu kullanarak tutarli
hata kayitlari olusturur.

Kullanim:
  from log_utils import save_error_log, classify_error_type, ERROR_TYPES

Strateji (v4):
  1. Supabase agent_logs tablosuna yaz (birincil)
  2. Supabase basarisiz olursa lokal JSON dosyasina fallback yaz
  3. Her iki yontem de basarisiz olursa stderr'e yaz (son care)

Dosya konumu: Proje koku (BASE_DIR)
"""

import json
import logging
import sys
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("log_utils")

# Fallback log dizini (proje koku / logs / error_fallback)
_FALLBACK_DIR = Path(__file__).parent / "logs" / "error_fallback"


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
    """
    error_lower = str(error_str).lower()

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

    exc_name = str(error_str).split(":")[0].strip().split(".")[-1]
    if exc_name in _EXCEPTION_MAP:
        return _EXCEPTION_MAP[exc_name]

    return "InternalError"


def normalize_error_type(hata_tipi):
    """Eski formattaki hata tiplerini yeni taksonomiye normalize eder."""
    if hata_tipi in ERROR_TYPES:
        return hata_tipi

    mapping = {
        "ApiError_400":  "ApiError",
        "Timeout":       "NetworkError",
        "rate_limit":    "RateLimit",
        "auth_error":    "AuthError",
        "data_error":    "DataError",
        "network":       "NetworkError",
        "server_error":  "ServerError",
        "file_not_found": "FileNotFound",
    }

    if hata_tipi in mapping:
        return mapping[hata_tipi]

    if hata_tipi in _EXCEPTION_MAP:
        return _EXCEPTION_MAP[hata_tipi]

    return hata_tipi


# ============================================================================
# JSON FALLBACK YAZICI
# ============================================================================

def _write_json_fallback(kayit, agent_name=None, log_dir=None):
    """
    Supabase basarisiz olunca lokal JSON dosyasina yaz.
    Dosya: {log_dir}/{agent_name}_errors_fallback.json veya
           logs/error_fallback/{agent_name}_errors.json
    max 500 kayit, FIFO.
    """
    try:
        if log_dir:
            fallback_dir = Path(log_dir)
        else:
            fallback_dir = _FALLBACK_DIR
        fallback_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{agent_name or 'unknown'}_errors_fallback.json"
        filepath = fallback_dir / filename

        # Mevcut kayitlari oku
        kayitlar = []
        if filepath.exists():
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    kayitlar = json.load(f)
            except (json.JSONDecodeError, IOError):
                kayitlar = []

        kayitlar.append(kayit)

        # Max 500 kayit tut
        if len(kayitlar) > 500:
            kayitlar = kayitlar[-500:]

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(kayitlar, f, indent=2, ensure_ascii=False)

        return True
    except Exception as e:
        print(f"[ERROR] JSON fallback da yazilamadi: {e}", file=sys.stderr)
        return False


# ============================================================================
# ORTAK save_error_log FONKSIYONU
# ============================================================================

def save_error_log(hata_tipi, hata_mesaji, log_dir=None,
                   traceback_str=None, adim=None, extra=None,
                   session_id=None, agent_name=None,
                   max_kayit=200,
                   hesap_key=None, marketplace=None):
    """
    Tum agentlar icin birlesik hata log fonksiyonu.
    Supabase birincil, JSON fallback, stderr son care (v4).

    Returns:
        bool: Herhangi bir yere yazildiysa True
    """
    hata_tipi = normalize_error_type(hata_tipi)

    # Kayit objesi (Supabase ve JSON icin ortak)
    kayit = {
        "timestamp":   datetime.utcnow().isoformat(),
        "hata_tipi":   hata_tipi,
        "hata_mesaji":  str(hata_mesaji)[:500],
        "adim":        adim or "bilinmiyor",
        "agent":       agent_name or "unknown",
        "hesap_key":   hesap_key or "",
        "marketplace": marketplace or "",
        "session_id":  session_id,
    }
    if traceback_str:
        kayit["traceback"] = str(traceback_str)[:1000]
    if extra:
        kayit["extra"] = extra

    # 1. Supabase'e yaz (birincil)
    if hesap_key and agent_name:
        try:
            _project_root = str(Path(__file__).parent)
            if _project_root not in sys.path:
                sys.path.insert(0, _project_root)
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
            return True
        except Exception as e:
            logger.warning("save_error_log Supabase basarisiz: %s — JSON fallback", e)

    # 2. JSON fallback (Supabase basarisiz veya hesap_key eksik)
    json_ok = _write_json_fallback(kayit, agent_name, log_dir)
    if json_ok:
        return True

    # 3. Son care: stderr
    print(f"[ERROR] Hata kaydi KAYIP — {hata_tipi}: {str(hata_mesaji)[:300]} "
          f"(agent={agent_name}, hesap={hesap_key}/{marketplace})", file=sys.stderr)
    return False


def save_log(level, message, agent_name, hesap_key=None, marketplace=None,
             session_id=None, error_type=None, traceback_str=None, extra=None):
    """
    Genel log fonksiyonu — info, warn, error tum seviyeleri destekler.
    Supabase birincil, stderr fallback (v4).
    """
    if not agent_name:
        return
    try:
        _project_root = str(Path(__file__).parent)
        if _project_root not in sys.path:
            sys.path.insert(0, _project_root)
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
    except Exception as e:
        # Sessiz yutma DEGIL — stderr'e yaz
        logger.warning("save_log Supabase basarisiz: %s — %s: %s",
                        e, agent_name, str(message)[:100])


# ============================================================================
# LOG ROTASYONU
# ============================================================================

def rotate_text_logs(log_dir, pattern="*.log", max_age_days=30):
    """Belirtilen gun sayisindan eski text log dosyalarini siler."""
    log_dir = Path(log_dir)
    if not log_dir.exists():
        return 0

    now = datetime.utcnow()
    silinen = 0

    for f in log_dir.glob(pattern):
        try:
            mtime = datetime.utcfromtimestamp(f.stat().st_mtime)
            age_days = (now - mtime).days
            if age_days > max_age_days:
                f.unlink()
                silinen += 1
                logger.info("Eski log silindi: %s (%d gun)", f.name, age_days)
        except Exception as e:
            logger.warning("Log rotasyon hatasi (%s): %s", f.name, e)
            continue

    return silinen
