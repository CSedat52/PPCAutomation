"""
Maestro Agent — State Manager
===============================
JSON state dosyasi ve log dosyalari yonetimi.
Pipeline'in anlik durumunu takip eder ve detayli log tutar.
"""

import os
import json
import logging
from datetime import datetime
from pathlib import Path

from . import config

logger = logging.getLogger("maestro.state")


# ============================================================================
# STATE YONETIMI
# ============================================================================

def _ensure_dirs():
    """Gerekli klasorleri olusturur."""
    Path(config.LOG_DIR).mkdir(parents=True, exist_ok=True)
    Path(os.path.dirname(config.STATE_FILE)).mkdir(parents=True, exist_ok=True)


def load_state():
    """State'i Supabase'den yukler. Basarisizsa bos state doner."""
    _ensure_dirs()
    acc = config.CURRENT_ACCOUNT
    if acc:
        try:
            from supabase.db_client import SupabaseClient
            db = SupabaseClient()
            conn = db._conn()
            cur = conn.cursor()
            cur.execute("SELECT state_data FROM maestro_state WHERE hesap_key = %s AND marketplace = %s",
                        (acc["hesap_key"], acc["marketplace"]))
            row = cur.fetchone()
            cur.close()
            conn.close()
            if row and row[0]:
                data = row[0] if isinstance(row[0], dict) else json.loads(row[0])
                logger.debug("State Supabase'den yuklendi (%s)", acc["label"])
                return data
        except Exception as e:
            logger.warning("State Supabase'den okunamadi: %s — bos state olusturuluyor", e)
    return _empty_state()


def save_state(state):
    """State'i Supabase'e kaydeder. JSON fallback KALDIRILDI."""
    acc = config.CURRENT_ACCOUNT
    if acc:
        try:
            from supabase.db_client import SupabaseClient
            from psycopg2.extras import Json
            db = SupabaseClient()
            conn = db._conn()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO maestro_state (hesap_key, marketplace, state_data, updated_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (hesap_key, marketplace) DO UPDATE SET state_data = EXCLUDED.state_data, updated_at = NOW()
            """, (acc["hesap_key"], acc["marketplace"], Json(state)))
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            logger.error("State Supabase'e yazilamadi: %s", e)


def _empty_state():
    return {
        "last_run_date": None,
        "current_session": None,
        "history": [],
    }


def create_session(state):
    """Yeni session olusturur ve state'e ekler."""
    now = datetime.utcnow()
    session_id = now.strftime(config.SESSION_ID_FORMAT)
    today = now.strftime(config.DATE_FORMAT)

    session = {
        "session_id": session_id,
        "date": today,
        "status": "starting",
        "started_at": now.isoformat(),
        "completed_at": None,
        "agent1": _empty_agent_state(),
        "agent2": _empty_agent_state(),
        "agent3": _empty_agent_state(),
        "approval": {
            "email_sent_at": None,
            "reminder_sent_at": None,
            "approved_at": None,
            "approval_method": None,
        },
        "errors": [],
    }

    state["current_session"] = session
    state["last_run_date"] = today
    save_state(state)
    return session


def _empty_agent_state():
    return {
        "status": "pending",
        "started_at": None,
        "completed_at": None,
        "summary": None,
        "errors": [],
        "retry_count": 0,
    }


def update_agent_status(state, agent_name, status, summary=None, errors=None):
    """Agent durumunu gunceller. agent_name: 'agent1', 'agent2', 'agent3'"""
    session = state.get("current_session")
    if not session:
        return

    now = datetime.utcnow().isoformat()
    agent = session[agent_name]

    if status == "running" and agent["started_at"] is None:
        agent["started_at"] = now

    agent["status"] = status

    if status in ("completed", "failed"):
        agent["completed_at"] = now

    if summary:
        agent["summary"] = summary

    if errors:
        agent["errors"].extend(errors if isinstance(errors, list) else [errors])

    # Ust seviye session status guncelle
    if status == "failed":
        session["status"] = "error"
        session["errors"].append({
            "agent": agent_name,
            "timestamp": now,
            "details": errors,
        })

    save_state(state)


def update_session_status(state, status):
    """Session durumunu gunceller."""
    session = state.get("current_session")
    if not session:
        return
    session["status"] = status
    if status in ("completed", "error"):
        session["completed_at"] = datetime.utcnow().isoformat()
    save_state(state)


def update_approval_status(state, field, value=None):
    """Onay bilgisini gunceller. field: email_sent_at, reminder_sent_at, approved_at, approval_method"""
    session = state.get("current_session")
    if not session:
        return
    if value is None:
        value = datetime.utcnow().isoformat()
    session["approval"][field] = value
    save_state(state)


def archive_session(state):
    """Mevcut session'i history'ye tasir."""
    session = state.get("current_session")
    if not session:
        return
    state["history"].append(session)
    # Son 500 session'i tut
    if len(state["history"]) > 500:
        state["history"] = state["history"][-500:]
    state["current_session"] = None
    save_state(state)


def is_already_run_today(state):
    """Bugun zaten calistirildi mi kontrolu."""
    today = datetime.utcnow().strftime(config.DATE_FORMAT)
    return state.get("last_run_date") == today


def get_last_completed_step(state):
    """Hata sonrasi kaldigi yeri bulur. Resume icin kullanilir."""
    session = state.get("current_session")
    if not session:
        return None

    if session["agent3"]["status"] == "completed":
        return "agent3"
    elif session["agent3"]["status"] in ("running", "failed"):
        return "agent2"  # Agent 3'ten devam
    elif session["agent2"]["status"] == "completed":
        return "agent2"
    elif session["agent2"]["status"] in ("running", "failed"):
        return "agent1"  # Agent 2'den devam
    elif session["agent1"]["status"] == "completed":
        return "agent1"
    elif session["agent1"]["status"] in ("running", "failed"):
        return None  # Agent 1'den devam (bastan)
    return None


# ============================================================================
# LOG YONETIMI
# ============================================================================

_log_file_handler = None


def setup_session_log(session_id):
    """Session icin ayri log dosyasi olusturur."""
    global _log_file_handler
    _ensure_dirs()

    today = datetime.utcnow().strftime(config.DATE_FORMAT)
    log_path = os.path.join(config.LOG_DIR, f"maestro_log_{today}.log")

    # Onceki handler'i kaldir
    root_logger = logging.getLogger("maestro")
    if _log_file_handler:
        root_logger.removeHandler(_log_file_handler)

    _log_file_handler = logging.FileHandler(log_path, encoding="utf-8")
    _log_file_handler.setFormatter(
        logging.Formatter("[%(asctime)s] [%(levelname)s] %(name)s — %(message)s",
                          datefmt="%Y-%m-%d %H:%M:%S")
    )
    root_logger.addHandler(_log_file_handler)
    root_logger.setLevel(logging.INFO)

    # Console handler (yoksa ekle)
    if not any(isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
               for h in root_logger.handlers):
        console = logging.StreamHandler()
        console.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s",
                                                datefmt="%H:%M:%S"))
        root_logger.addHandler(console)

    logger.info("=" * 60)
    logger.info("SESSION START — %s", session_id)
    logger.info("=" * 60)
    return log_path


def get_latest_log_path():
    """En son log dosyasinin yolunu doner."""
    log_dir = Path(config.LOG_DIR)
    if not log_dir.exists():
        return None
    logs = sorted(log_dir.glob("maestro_log_*.log"), reverse=True)
    return str(logs[0]) if logs else None
