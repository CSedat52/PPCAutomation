"""
Maestro Agent — Retry Handler
================================
Hata siniflandirma ve retry mekanizmasi.
Hata tipine gore farkli strateji uygular.
"""

import time
import logging
import os
import sys

from . import config

_base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _base not in sys.path:
    sys.path.insert(0, _base)
from log_utils import classify_error_type

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
    error_type = classify_error_type(str(error))

    # log_utils taksonomisini retry config key'e maple
    type_mapping = {
        "RateLimit": "rate_limit",
        "AuthError": "auth_error",
        "NetworkError": "network",
        "FileNotFound": "file_not_found",
        "ApiError": "data_error",
        "DataError": "data_error",
        "ServerError": "server_error",
        "InternalError": "server_error",
        "ExecutionError": "server_error",
        "VerificationError": "server_error",
        "AgentFailure": "server_error",
        "ReportFailed": "server_error",
        "Preflight": "data_error",
    }
    config_key = type_mapping.get(error_type, "server_error")
    return error_type, config.RETRY_CONFIG.get(config_key, config.RETRY_CONFIG["server_error"])


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
