"""
Maestro Agent — Konfigurasyon (v2 Multi-Account)
==================================================
Statik ayarlar + hesap bazli dinamik path'ler.
init_account(hesap_key, marketplace) cagrildiktan sonra
hesaba ozel path'ler kullanilabilir.
"""

import os
import json

try:
    from dotenv import load_dotenv
    _base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    _env_path = os.path.join(_base, ".env")
    if os.path.exists(_env_path):
        load_dotenv(_env_path)
except ImportError:
    pass

# ============================================================================
# ZAMANLAMA
# ============================================================================

SCHEDULE_INTERVAL_DAYS = 3
SCHEDULE_TIME_HOUR_UTC = 9

# ============================================================================
# E-POSTA (Gmail SMTP + IMAP)
# ============================================================================

GMAIL_ADDRESS = os.environ.get("MAESTRO_GMAIL_ADDRESS", "")
GMAIL_APP_PASSWORD = os.environ.get("MAESTRO_GMAIL_APP_PASSWORD", "")
NOTIFY_EMAIL = os.environ.get("MAESTRO_NOTIFY_EMAIL", "")

if not NOTIFY_EMAIL:
    NOTIFY_EMAIL = GMAIL_ADDRESS

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587
IMAP_HOST = "imap.gmail.com"
IMAP_PORT = 993

# ============================================================================
# ONAY BEKLEME
# ============================================================================

APPROVAL_CHECK_INTERVAL_MINUTES = 60
APPROVAL_REMINDER_AFTER_HOURS = 6
APPROVAL_KEYWORDS = ["onay", "approved", "onayla", "tamam", "ok"]

# ============================================================================
# RETRY STRATEJISI
# ============================================================================

RETRY_CONFIG = {
    "rate_limit": {"max_retries": 5, "backoff": "exponential", "base_wait": 30},
    "auth_error": {"max_retries": 1, "backoff": "none", "base_wait": 0},
    "network": {"max_retries": 3, "backoff": "fixed", "base_wait": 60},
    "data_error": {"max_retries": 0, "backoff": "none", "base_wait": 0},
    "server_error": {"max_retries": 3, "backoff": "fixed", "base_wait": 120},
    "file_not_found": {"max_retries": 0, "backoff": "none", "base_wait": 0},
}

# ============================================================================
# STATIK DOSYA YOLLARI (Maestro'nun kendi dosyalari)
# ============================================================================

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

LOG_DIR = os.path.join(BASE_DIR, "maestro", "logs")
STATE_DIR = os.path.join(BASE_DIR, "maestro", "state")

AGENT2_SCRIPT = os.path.join(BASE_DIR, "agent2", "analyst.py")
AGENT3_SCRIPT = os.path.join(BASE_DIR, "agent3", "executor.py")

ACCOUNTS_FILE = os.path.join(BASE_DIR, "config", "accounts.json")
if not os.path.exists(ACCOUNTS_FILE):
    ACCOUNTS_FILE = os.path.join(BASE_DIR, "accounts.json")

# ============================================================================
# HESAP BAZLI DINAMIK PATH'LER
# init_account() ile set edilir
# ============================================================================

CURRENT_ACCOUNT = None       # {"hesap_key": ..., "marketplace": ..., "label": ...}
ACCOUNT_DATA_DIR = None      # data/{hesap_key}_{marketplace}/
ACCOUNT_CONFIG_DIR = None    # config/{hesap_key}_{marketplace}/
RECOMMENDATION_EXCEL_DIR = None
SETTINGS_FILE = None
STATE_FILE = None


def init_account(hesap_key, marketplace):
    """Hesap bazli path'leri set eder. Pipeline basinda cagirilir."""
    global CURRENT_ACCOUNT, ACCOUNT_DATA_DIR, ACCOUNT_CONFIG_DIR
    global RECOMMENDATION_EXCEL_DIR, SETTINGS_FILE, STATE_FILE

    dir_name = f"{hesap_key}_{marketplace}"
    CURRENT_ACCOUNT = {
        "hesap_key": hesap_key,
        "marketplace": marketplace,
        "dir_name": dir_name,
        "label": f"{hesap_key}/{marketplace}",
    }
    ACCOUNT_DATA_DIR = os.path.join(BASE_DIR, "data", dir_name)
    ACCOUNT_CONFIG_DIR = os.path.join(BASE_DIR, "config", dir_name)
    RECOMMENDATION_EXCEL_DIR = os.path.join(ACCOUNT_DATA_DIR, "analysis")
    SETTINGS_FILE = os.path.join(ACCOUNT_CONFIG_DIR, "settings.json")

    # State dosyasi hesap bazli
    os.makedirs(STATE_DIR, exist_ok=True)
    STATE_FILE = os.path.join(STATE_DIR, f"{dir_name}_state.json")


def load_accounts():
    """Hesap bilgilerini Supabase'den yukler. Basarisizsa accounts.json'a fallback."""
    try:
        from supabase.db_client import SupabaseClient
        db = SupabaseClient()
        conn = db._conn()
        cur = conn.cursor()
        cur.execute("SELECT hesap_key, marketplace, aktif FROM marketplaces WHERE aktif = true")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        if rows:
            # accounts.json formatina donustur (geriye uyumluluk)
            hesaplar = {}
            for hk, mp, aktif in rows:
                if hk not in hesaplar:
                    hesaplar[hk] = {"marketplaces": {}}
                hesaplar[hk]["marketplaces"][mp] = {"aktif": True}
            return {"hesaplar": hesaplar}
    except Exception:
        pass

    with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def get_active_pipelines():
    """Aktif hesap+marketplace kombinasyonlarini Supabase'den sirasiyla doner."""
    # Once Supabase'den dene
    try:
        from supabase.db_client import SupabaseClient
        db = SupabaseClient()
        conn = db._conn()
        cur = conn.cursor()
        cur.execute("SELECT hesap_key, marketplace FROM marketplaces WHERE aktif = true ORDER BY hesap_key, marketplace")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        if rows:
            return [{"hesap_key": hk, "marketplace": mp} for hk, mp in rows]
    except Exception:
        pass

    # Fallback: accounts.json
    accounts = load_accounts()
    sira = accounts.get("pipeline_ayarlari", {}).get("calisma_sirasi", [])
    if sira:
        result = []
        for item in sira:
            hesap_key, mp = item.split(":")
            hesap = accounts["hesaplar"].get(hesap_key, {})
            mp_config = hesap.get("marketplaces", {}).get(mp, {})
            if mp_config.get("aktif", False):
                result.append({"hesap_key": hesap_key, "marketplace": mp})
        return result
    result = []
    for hesap_key, hesap in accounts.get("hesaplar", {}).items():
        for mp_code, mp_config in hesap.get("marketplaces", {}).items():
            if mp_config.get("aktif", False):
                result.append({"hesap_key": hesap_key, "marketplace": mp_code})
    return result


# ============================================================================
# E-POSTA SUBJECT SABLONLARI
# ============================================================================

EMAIL_SUBJECTS = {
    "excel_ready": "[Maestro] PPC Rapor Hazir - {account_label} - Session {session_id}",
    "error": "[Maestro] HATA - {account_label} - {agent_name} - Session {session_id}",
    "completed": "[Maestro] Tamamlandi - {account_label} - Session {session_id}",
    "reminder": "[Maestro] HATIRLATMA - {account_label} - Session {session_id}",
}

# ============================================================================
# GENEL
# ============================================================================

SESSION_ID_FORMAT = "%Y%m%d_%H%M%S"
DATE_FORMAT = "%Y-%m-%d"
