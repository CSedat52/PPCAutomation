"""
Maestro Agent — Retry Handler
================================
Hata siniflandirma ve retry mekanizmasi.
Hata tipine gore farkli strateji uygular.
"""

import time
import logging

from . import config

logger = logging.getLogger("maestro.retry")


# ============================================================================
# HATA SINIFLANDIRMA
# ============================================================================

def classify_error(error):
    """
    Hatayi siniflandirir ve uygun retry stratejisini doner.
    
    Args:
        error: Exception veya error string
    
    Returns:
        (error_type: str, retry_config: dict)
    """
    error_str = str(error).lower()

    # Rate Limit (429)
    if any(k in error_str for k in ["429", "rate limit", "too many requests", "throttl"]):
        return "rate_limit", config.RETRY_CONFIG["rate_limit"]

    # Auth/Token (401, 403)
    if any(k in error_str for k in ["401", "403", "unauthorized", "forbidden", "token", "auth"]):
        return "auth_error", config.RETRY_CONFIG["auth_error"]

    # Network/Timeout
    if any(k in error_str for k in ["timeout", "connection", "network", "dns", "refused",
                                      "reset", "broken pipe", "eof", "ssl"]):
        return "network", config.RETRY_CONFIG["network"]

    # File Not Found
    if any(k in error_str for k in ["not found", "no such file", "filenotfound", "dosya bulunamadi"]):
        return "file_not_found", config.RETRY_CONFIG["file_not_found"]

    # Data/Format Error (400)
    if any(k in error_str for k in ["400", "bad request", "invalid", "malformed",
                                      "validation", "format", "parse"]):
        return "data_error", config.RETRY_CONFIG["data_error"]

    # Server Error (500+)
    if any(k in error_str for k in ["500", "502", "503", "504", "internal server",
                                      "bad gateway", "service unavail", "gateway timeout"]):
        return "server_error", config.RETRY_CONFIG["server_error"]

    # Bilinmeyen hata — guvenli tarafta kal, retry yap
    logger.warning("Bilinmeyen hata tipi, server_error olarak siniflandiriliyor: %s", error_str[:200])
    return "server_error", config.RETRY_CONFIG["server_error"]


def calculate_wait_time(retry_config, attempt):
    """
    Bekleme suresini hesaplar.
    
    Args:
        retry_config: Retry konfigurasyonu
        attempt: Kacinci deneme (0-indexed)
    
    Returns:
        Bekleme suresi (saniye)
    """
    backoff = retry_config.get("backoff", "none")
    base_wait = retry_config.get("base_wait", 0)

    if backoff == "exponential":
        return base_wait * (2 ** attempt)
    elif backoff == "fixed":
        return base_wait
    else:
        return 0


def should_retry(retry_config, current_attempt):
    """Tekrar denenmeli mi kontrolu."""
    max_retries = retry_config.get("max_retries", 0)
    return current_attempt < max_retries


# ============================================================================
# RETRY EXECUTOR
# ============================================================================

def execute_with_retry(func, agent_name, *args, **kwargs):
    """
    Fonksiyonu retry mekanizmasiyla calistirir.
    
    Args:
        func: Calistirilacak fonksiyon
        agent_name: Log icin agent adi
        *args, **kwargs: Fonksiyona gecilecek arguman
    
    Returns:
        (success: bool, result: any, error_info: dict or None)
    """
    attempt = 0
    last_error = None
    last_error_type = None
    last_retry_config = None

    while True:
        try:
            result = func(*args, **kwargs)

            # Fonksiyon dict donduruyorsa durum kontrolu yap
            if isinstance(result, dict):
                durum = result.get("durum", result.get("status", "")).upper()
                if durum in ("BASARISIZ", "FAILED", "ON_KONTROL_BASARISIZ"):
                    error_msg = result.get("hata", result.get("mesaj", result.get("message", "Bilinmeyen hata")))
                    raise RuntimeError(error_msg)

            logger.info("%s: Basarili (deneme %d)", agent_name, attempt + 1)
            return True, result, None

        except Exception as e:
            last_error = str(e)
            error_type, retry_cfg = classify_error(e)
            last_error_type = error_type
            last_retry_config = retry_cfg

            logger.warning(
                "%s: Hata [%s] (deneme %d/%d): %s",
                agent_name, error_type, attempt + 1,
                retry_cfg.get("max_retries", 0) + 1,
                last_error[:300]
            )

            if should_retry(retry_cfg, attempt):
                wait = calculate_wait_time(retry_cfg, attempt)
                if wait > 0:
                    logger.info("%s: %d saniye bekleniyor...", agent_name, wait)
                    time.sleep(wait)
                attempt += 1
                continue
            else:
                logger.error(
                    "%s: Retry limiti asildi [%s]. Son hata: %s",
                    agent_name, error_type, last_error[:500]
                )
                return False, None, {
                    "error_type": last_error_type,
                    "error_message": last_error,
                    "attempts": attempt + 1,
                    "retry_config": last_retry_config,
                }


def get_error_suggestion(error_type):
    """Hata tipine gore cozum onerisi doner."""
    suggestions = {
        "rate_limit": (
            "Amazon API rate limit'e takildi. Bu genellikle gecici bir sorundur. "
            "Birkac dakika bekleyip 'maestro resume' ile devam edebilirsiniz."
        ),
        "auth_error": (
            "Amazon API kimlik dogrulama hatasi. "
            "config/accounts.json dosyasindaki AMAZON_REFRESH_TOKEN'in gecerli oldugundan emin olun. "
            "Token sureleri dolmus olabilir — yeniden olusturmaniz gerekebilir."
        ),
        "network": (
            "Ag baglantisi sorunu. Internet baglantisinizi kontrol edin. "
            "Amazon API sunucularina erisim oldugundan emin olun."
        ),
        "data_error": (
            "Veri formati hatasi. Bu genellikle bir kod hatasidir veya Amazon API'nin beklenen formati degismistir. "
            "Hata detayini inceleyip ilgili agent'in kodunu kontrol edin."
        ),
        "server_error": (
            "Amazon sunucu hatasi. Bu Amazon tarafinda gecici bir sorundur. "
            "Birkac dakika bekleyip 'maestro resume' ile devam edebilirsiniz."
        ),
        "file_not_found": (
            "Gerekli dosya bulunamadi. Bu genellikle onceki asamanin tamamlanmamis olmasindan kaynaklanir. "
            "Pipeline'in dogru sirayla calistirildigindan emin olun."
        ),
    }
    return suggestions.get(error_type, "Bilinmeyen hata. Log dosyasini inceleyerek detaylari kontrol edin.")


# ============================================================================
# ORTAK TAKSONOMI ESLESTIRME
# ============================================================================

# retry_handler ic isimleri -> ortak taksonomi eslestirmesi
# Agent 4 ve log analizi icin kullanilir
_NORMALIZE_MAP = {
    "rate_limit":    "RateLimit",
    "auth_error":    "AuthError",
    "network":       "NetworkError",
    "data_error":    "DataError",
    "server_error":  "ServerError",
    "file_not_found": "FileNotFound",
}


def normalize_error_type(error_type):
    """retry_handler hata tipini ortak taksonomiye cevirir."""
    return _NORMALIZE_MAP.get(error_type, error_type)
