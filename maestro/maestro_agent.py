"""
Maestro Agent — Watch Daemon + Agent 3/4 Orkestrator
=======================================================
Dashboard execution_queue'yu izler, onay gelince Agent 3+4 calistirir.

Calisma Modlari:
  - watch [dakika]                 : Execution queue izle (varsayilan 5dk)
  - status                         : Pipeline durumu
  - status <hesap_key> <marketplace> : Tek hesap durumu
  - accounts                       : Aktif hesap listesi

Agent 1+2 orkestrasyonu pipeline_runner.py tarafindan yapilir (cron).
"""

import os
import sys
import json
import time
import logging
import subprocess
from datetime import datetime
from pathlib import Path

from . import config
from . import state_manager
from . import email_handler
from . import retry_handler

logger = logging.getLogger("maestro.agent")

# Ust dizini path'e ekle (log_utils icin)
_maestro_base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _maestro_base not in sys.path:
    sys.path.insert(0, _maestro_base)
from log_utils import save_error_log as _central_save_error_log, save_log as _save_log


def _extract_outer_json(text):
    """Son } karakterini bulup geriye dogru eslesme yaparak en distaki JSON objesini cikarir.
    rfind('{') ic ice JSON'larda yanlis pozisyon buluyordu — bu yontem dogru eslestirir."""
    end = text.rfind("}")
    if end < 0:
        return -1
    depth = 0
    in_string = False
    escape_next = False
    for i in range(end, -1, -1):
        c = text[i]
        if escape_next:
            escape_next = False
            continue
        # Geriye dogru taramada bir onceki karakter backslash ise escape
        if i > 0 and text[i - 1] == '\\' and in_string:
            continue
        if c == '"' and not escape_next:
            in_string = not in_string
        if in_string:
            continue
        if c == '}':
            depth += 1
        elif c == '{':
            depth -= 1
            if depth == 0:
                return (i, end)
    return (-1, -1)


def _get_sdb():
    """Supabase client al (hata olursa None don)."""
    try:
        import sys as _sys
        _base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if _base not in _sys.path:
            _sys.path.insert(0, _base)
        from supabase.db_client import SupabaseClient
        return SupabaseClient()
    except Exception:
        return None


# ============================================================================
# MAESTRO HATA LOGLAMA
# ============================================================================

def save_error_log(hata_tipi, hata_mesaji, session_id=None, adim=None,
                   extra=None, traceback_str=None):
    """Maestro hata logu — lokal + Supabase dual-write."""
    log_dir = Path(config.LOG_DIR)
    current = config.CURRENT_ACCOUNT or {}
    hk = current.get("hesap_key", "")
    mp = current.get("marketplace", "")
    # Eski dosya adi formati: {dir_name}_maestro_errors.json (hesap bazli)
    # MaestroAnalyzer "*_maestro_errors.json" glob ile okuyor — bunu korumamiz lazim
    dir_name = current.get("dir_name", "")
    log_agent_name = f"{dir_name}_maestro" if dir_name else "maestro"
    return _central_save_error_log(
        hata_tipi, hata_mesaji, log_dir,
        traceback_str=traceback_str, adim=adim, extra=extra,
        session_id=session_id, agent_name=log_agent_name,
        hesap_key=hk, marketplace=mp)


def _rotate_old_logs():
    """30 gunden eski maestro log dosyalarini siler."""
    log_dir = Path(config.LOG_DIR)
    if not log_dir.exists():
        return
    now = datetime.utcnow()
    for f in log_dir.glob("maestro_log_*.log"):
        try:
            mtime = datetime.utcfromtimestamp(f.stat().st_mtime)
            if (now - mtime).days > 30:
                f.unlink()
                logger.info("Eski log silindi: %s", f.name)
        except Exception as e:
            logger.warning("Log rotasyon hatasi: %s", e)
            continue


# ============================================================================
# PIPELINE — ANA AKIS
# ============================================================================

# ============================================================================
# AGENT CALISTIRICILARI
# ============================================================================

def _run_agent3(state, session_id, hesap_key, marketplace):
    """Agent 3'u calistirir (dry-run + execute + verify)."""
    logger.info("--- AGENT 3: Execution (%s/%s) ---", hesap_key, marketplace)
    state_manager.update_agent_status(state, "agent3", "running")

    _save_log("info", f"Agent 3 basliyor: {hesap_key}/{marketplace}",
              "agent3", hesap_key, marketplace, session_id)
    sdb = _get_sdb()
    if sdb:
        try:
            sdb.upsert_pipeline_run(session_id, hesap_key, marketplace, "agent3_execute", "running")
        except Exception as e:
            logger.warning("Supabase yazim hatasi: %s", e)

    # Adim 1: Dry-run
    logger.info("Agent 3 — Adim 1: Dry-run")
    def run_agent3_dryrun():
        env = {**os.environ,
               "MAESTRO_SESSION_ID": session_id,
               "HESAP_KEY": hesap_key,
               "MARKETPLACE": marketplace}
        result = subprocess.run(
            [sys.executable, config.AGENT3_SCRIPT, hesap_key, marketplace],
            capture_output=True, text=True, timeout=300,
            cwd=config.BASE_DIR, env=env,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Agent 3 dry-run hatasi (code {result.returncode}): {result.stderr[:500]}")
        output = result.stdout.strip()
        json_start, json_end = _extract_outer_json(output)
        if json_start >= 0:
            json_str = output[json_start:json_end+1]
            try:
                return json.loads(json_str)
            except json.JSONDecodeError as e:
                try:
                    decoder = json.JSONDecoder()
                    result, _ = decoder.raw_decode(json_str.strip())
                    logger.warning("Agent 3 dry-run: Extra data atildi, ilk JSON nesnesi kullanildi")
                    return result
                except json.JSONDecodeError:
                    logger.error("Agent 3 dry-run JSON parse basarisiz. Raw output (ilk 500 char): %s",
                                 json_str[:500])
                    raise RuntimeError(f"Agent 3 dry-run JSON parse hatasi: {e}")
        raise RuntimeError(f"Agent 3 dry-run JSON ciktisi alinamadi. Output: {output[:500]}")

    success, dry_result, error_info = retry_handler.execute_with_retry(
        run_agent3_dryrun, "Agent 3 (dry-run)"
    )

    if not success:
        error_msg = error_info.get("error_message", "Bilinmeyen hata") if error_info else "Bilinmeyen hata"
        error_type = error_info.get("error_type", "server_error") if error_info else "server_error"
        state_manager.update_agent_status(state, "agent3", "failed", errors=[error_msg])
        save_error_log("AgentFailure", error_msg, session_id=session_id,
                       adim="run_agent3_dryrun",
                       extra={"agent": "agent3", "phase": "dry-run",
                              "error_type": error_type,
                              "attempts": error_info.get("attempts", 0) if error_info else 0})
        _save_log("error", f"Agent 3 dry-run hatasi: {error_msg[:200]}",
                  "agent3", hesap_key, marketplace, session_id, error_type=error_type)
        if sdb:
            try:
                sdb.upsert_pipeline_run(session_id, hesap_key, marketplace, "agent3_execute", "failed", error_msg=error_msg)
                sdb.update_agent_status_detail("agent3", "failed")
            except Exception as e:
                logger.warning("Supabase yazim hatasi: %s", e)
        suggestion = retry_handler.get_error_suggestion(error_type)
        email_handler.send_error(session_id, "Agent 3 (dry-run)", error_msg, suggestion)
        return False

    # Dry-run sonucunu logla
    if isinstance(dry_result, dict):
        ozet = dry_result.get("ozet", {})
        logger.info("Dry-run sonucu: Bid=%s, Negatif=%s, Harvesting=%s",
                     ozet.get("bid_degisiklikleri", {}),
                     ozet.get("negatif_eklemeler", {}),
                     ozet.get("harvesting", {}))

        # Islem yoksa dur
        durum = dry_result.get("durum", "")
        if durum == "BOS":
            logger.info("Onaylanmis islem yok. Agent 3 tamamlandi (bos).")
            state_manager.update_agent_status(
                state, "agent3", "completed",
                summary="Onaylanmis islem yok — execution atlanildi."
            )
            return True

    # Adim 2: Execute
    logger.info("Agent 3 — Adim 2: Execution")
    def run_agent3_execute():
        env = {**os.environ,
               "MAESTRO_SESSION_ID": session_id,
               "HESAP_KEY": hesap_key,
               "MARKETPLACE": marketplace}
        result = subprocess.run(
            [sys.executable, config.AGENT3_SCRIPT, hesap_key, marketplace, "--execute"],
            capture_output=True, text=True, timeout=600,
            cwd=config.BASE_DIR, env=env,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Agent 3 execution hatasi (code {result.returncode}): {result.stderr[:500]}")
        output = result.stdout.strip()
        json_start, json_end = _extract_outer_json(output)
        if json_start >= 0:
            return json.loads(output[json_start:json_end+1])
        raise RuntimeError(f"Agent 3 execution JSON ciktisi alinamadi.")

    success, exec_result, error_info = retry_handler.execute_with_retry(
        run_agent3_execute, "Agent 3 (execute)"
    )

    if not success:
        error_msg = error_info.get("error_message", "Bilinmeyen hata") if error_info else "Bilinmeyen hata"
        error_type = error_info.get("error_type", "server_error") if error_info else "server_error"
        state_manager.update_agent_status(state, "agent3", "failed", errors=[error_msg])
        save_error_log("AgentFailure", error_msg, session_id=session_id,
                       adim="run_agent3_execute",
                       extra={"agent": "agent3", "phase": "execute",
                              "error_type": error_type,
                              "attempts": error_info.get("attempts", 0) if error_info else 0})
        _save_log("error", f"Agent 3 execute hatasi: {error_msg[:200]}",
                  "agent3", hesap_key, marketplace, session_id, error_type=error_type)
        if sdb:
            try:
                sdb.upsert_pipeline_run(session_id, hesap_key, marketplace, "agent3_execute", "failed", error_msg=error_msg)
                sdb.update_agent_status_detail("agent3", "failed")
            except Exception as e:
                logger.warning("Supabase yazim hatasi: %s", e)
        email_handler.send_error(session_id, "Agent 3 (execute)", error_msg,
                                  retry_handler.get_error_suggestion(error_type))
        return False

    # Execution basarili — pipeline_runs guncelle
    if sdb:
        try:
            sdb.upsert_pipeline_run(session_id, hesap_key, marketplace, "agent3_execute", "completed")
        except Exception as e:
            logger.warning("Supabase yazim hatasi: %s", e)

    # Adim 3: 5 dk bekle + verify
    logger.info("Agent 3 — Adim 3: 5 dakika bekleniyor (dogrulama icin)...")
    time.sleep(300)

    logger.info("Agent 3 — Adim 4: Dogrulama")
    def run_agent3_verify():
        env = {**os.environ,
               "MAESTRO_SESSION_ID": session_id,
               "HESAP_KEY": hesap_key,
               "MARKETPLACE": marketplace}
        result = subprocess.run(
            [sys.executable, config.AGENT3_SCRIPT, hesap_key, marketplace, "--verify"],
            capture_output=True, text=True, timeout=300,
            cwd=config.BASE_DIR, env=env,
        )
        output = result.stdout.strip()
        json_start, json_end = _extract_outer_json(output)
        if json_start >= 0:
            return json.loads(output[json_start:json_end+1])
        return {"durum": "DOGRULAMA_BILGI_YOK"}

    _, verify_result, _ = retry_handler.execute_with_retry(
        run_agent3_verify, "Agent 3 (verify)"
    )

    # Execution sonuclarini kaydet
    exec_summary = ""
    if isinstance(exec_result, dict):
        exec_summary = exec_result.get("ozet_mesaj", json.dumps(exec_result.get("ozet", {})))

    state_manager.update_agent_status(
        state, "agent3", "completed",
        summary=exec_summary or "Execution tamamlandi"
    )

    # Verify sonucunu da session'a ekle
    session = state.get("current_session", {})
    session["_agent3_exec_result"] = exec_result
    session["_agent3_verify_result"] = verify_result
    state_manager.save_state(state)

    _save_log("info", f"Agent 3 tamamlandi: {exec_summary[:100]}",
              "agent3", hesap_key, marketplace, session_id)
    if sdb:
        try:
            sdb.upsert_pipeline_run(session_id, hesap_key, marketplace, "agent3_verify", "completed")
            sdb.update_agent_status_detail("agent3", "completed", {
                "tasks": exec_result.get("ozet", {}).get("toplam", 0) if isinstance(exec_result, dict) else 0,
            })
        except Exception as e:
            logger.warning("Supabase yazim hatasi: %s", e)

    logger.info("Agent 3 tamamlandi: %s", exec_summary)
    return True


# ============================================================================
# DURUM SORGULAMA
# ============================================================================

def get_status(hesap_key=None, marketplace=None):
    """Mevcut session durumunu doner."""
    if hesap_key and marketplace:
        config.init_account(hesap_key, marketplace)
        state = state_manager.load_state()
        session = state.get("current_session")
        account_label = f"{hesap_key}/{marketplace}"

        if not session:
            last_run = state.get("last_run_date", "Hic calismamis")
            return {
                "hesap": account_label,
                "aktif_session": False,
                "son_calisma": last_run,
                "mesaj": f"Aktif session yok ({account_label}). Son calisma: {last_run}",
            }

        return {
            "hesap": account_label,
            "aktif_session": True,
            "session_id": session["session_id"],
            "status": session["status"],
            "agent1": session["agent1"]["status"],
            "agent2": session["agent2"]["status"],
            "agent3": session["agent3"]["status"],
            "onay": session["approval"],
            "hatalar": session.get("errors", []),
        }
    else:
        # Tum hesaplarin ozet durumu
        pipelines = config.get_active_pipelines()
        results = []
        for p in pipelines:
            config.init_account(p["hesap_key"], p["marketplace"])
            state = state_manager.load_state()
            session = state.get("current_session")
            results.append({
                "hesap": f"{p['hesap_key']}/{p['marketplace']}",
                "son_calisma": state.get("last_run_date", "-"),
                "aktif_session": bool(session),
                "status": session["status"] if session else "-",
            })
        return {"hesaplar": results}


def get_history(hesap_key=None, marketplace=None, limit=10):
    """Gecmis session'larin ozetini doner."""
    if hesap_key and marketplace:
        config.init_account(hesap_key, marketplace)
        state = state_manager.load_state()
        history = state.get("history", [])
        result = []
        for s in history[-limit:]:
            result.append({
                "hesap": f"{hesap_key}/{marketplace}",
                "session_id": s["session_id"],
                "date": s["date"],
                "status": s["status"],
                "agent1": s["agent1"]["status"],
                "agent2": s["agent2"]["status"],
                "agent3": s["agent3"]["status"],
            })
        return result
    else:
        # Tum hesaplarin son session'lari
        pipelines = config.get_active_pipelines()
        result = []
        for p in pipelines:
            config.init_account(p["hesap_key"], p["marketplace"])
            state = state_manager.load_state()
            history = state.get("history", [])
            if history:
                s = history[-1]
                result.append({
                    "hesap": f"{p['hesap_key']}/{p['marketplace']}",
                    "session_id": s["session_id"],
                    "date": s["date"],
                    "status": s["status"],
                })
        return result


# ============================================================================
# EXECUTION QUEUE — Dashboard'dan Agent3 tetikleme
# ============================================================================

def poll_execution_queue(filter_hesap=None, filter_marketplace=None):
    """
    Supabase execution_queue tablosundaki pending komutlari kontrol eder.
    Dashboard'dan 'Agent3'u Calistir' butonuna basildiginda buraya komut duser.

    Opsiyonel filtre: sadece belirli hesap/marketplace icin islem yap.
    24 saatten eski pending kayitlar otomatik expire edilir.
    """
    sdb = _get_sdb()
    if not sdb:
        return []

    try:
        # Kural: Gecmis gunlerden kalan pending → failed
        sdb._execute(
            "UPDATE execution_queue SET status='failed', completed_at=NOW(), "
            "result='{\"error\": \"previous_day_pending\"}' "
            "WHERE status='pending' AND requested_at::date < CURRENT_DATE")

        # Kural: Stuck processing temizligi (1 saatten eski):
        sdb._execute(
            "UPDATE execution_queue SET status='failed', completed_at=NOW(), "
            "result='{\"error\": \"stuck_processing_timeout\"}' "
            "WHERE status='processing' AND started_at < NOW() - INTERVAL '1 hour'")

        # Kural: Eski completed/failed kayitlari temizle (7 gunden eski):
        sdb._execute(
            "DELETE FROM execution_queue "
            "WHERE status IN ('completed','failed','expired') "
            "AND completed_at < NOW() - INTERVAL '7 days'")

        # Kural: 7 gunden eski verify snapshot'lari sil:
        sdb._execute(
            "DELETE FROM verify_snapshots "
            "WHERE verify_date < CURRENT_DATE - INTERVAL '7 days'")

        # Kural: Agent log kayitlarini son 2000 ile sinirla (her agent icin):
        for agent_id in ['agent1', 'agent2', 'agent3', 'agent4', 'maestro', 'pipeline_runner']:
            sdb._execute(
                "DELETE FROM agent_logs WHERE id IN ("
                "  SELECT id FROM agent_logs WHERE agent_id = %s "
                "  ORDER BY created_at DESC OFFSET 2000"
                ")", (agent_id,))

        # Pending komutlari cek
        if filter_hesap and filter_marketplace:
            rows = sdb._fetch_all(
                "SELECT id, hesap_key, marketplace, command FROM execution_queue "
                "WHERE status='pending' AND hesap_key=%s AND marketplace=%s ORDER BY requested_at",
                (filter_hesap, filter_marketplace))
        elif filter_hesap:
            rows = sdb._fetch_all(
                "SELECT id, hesap_key, marketplace, command FROM execution_queue "
                "WHERE status='pending' AND hesap_key=%s ORDER BY requested_at",
                (filter_hesap,))
        else:
            rows = sdb._fetch_all(
                "SELECT id, hesap_key, marketplace, command FROM execution_queue "
                "WHERE status='pending' ORDER BY requested_at")

        if not rows:
            return []

        logger.info("Execution queue: %d bekleyen komut bulundu", len(rows))
        results = []

        for row in rows:
            q_id, hesap_key, marketplace, command = row[0], row[1], row[2], row[3]
            logger.info("Queue isleniyor: %s/%s — %s", hesap_key, marketplace, command)

            # Status: processing
            sdb._execute("UPDATE execution_queue SET status='processing', started_at=NOW() WHERE id=%s", (q_id,))
            _save_log("info", f"Queue komutu alindi: {command} — {hesap_key}/{marketplace}",
                      "maestro", hesap_key, marketplace)

            if command == "agent3_execute":
                try:
                    success = _run_agent3_from_queue(hesap_key, marketplace)
                    status = "completed" if success else "failed"
                    sdb._execute(
                        "UPDATE execution_queue SET status=%s, completed_at=NOW(), result=%s WHERE id=%s",
                        (status, json.dumps({"success": success}), q_id))
                    results.append({"hesap": f"{hesap_key}/{marketplace}", "status": status})
                except Exception as e:
                    sdb._execute(
                        "UPDATE execution_queue SET status='failed', completed_at=NOW(), result=%s WHERE id=%s",
                        (json.dumps({"error": str(e)[:500]}), q_id))
                    results.append({"hesap": f"{hesap_key}/{marketplace}", "status": "failed", "error": str(e)[:200]})
            else:
                sdb._execute("UPDATE execution_queue SET status='unknown_command', completed_at=NOW() WHERE id=%s", (q_id,))

        return results
    except Exception as e:
        logger.error("Execution queue hatasi: %s", e)
        return []


def _run_agent3_from_queue(hesap_key, marketplace):
    """
    Dashboard onaylarindan Agent 3 + Agent 4'u calistirir.
    Agent 3: direkt Python (_run_agent3 fonksiyonu).
    Agent 4 Asama 1: Python (optimizer.py subprocess).
    Agent 4 Asama 2: Dogrudan Anthropic API (Claude Code kaldirildi).
    """
    config.init_account(hesap_key, marketplace)
    account_label = f"{hesap_key}/{marketplace}"
    logger.info("=" * 60)
    logger.info("  QUEUE: AGENT 3+4 BASLATILIYOR — %s", account_label)
    logger.info("=" * 60)

    _save_log("info", f"Dashboard onayi alindi, Agent 3+4 baslatiliyor: {account_label}",
              "maestro", hesap_key, marketplace)

    queue_session_id = f"queue_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{hesap_key}_{marketplace}"
    sdb = _get_sdb()

    # ===== AGENT 3 (direkt Python — Claude Code YOK) =====
    state = state_manager.load_state()
    if not state.get("current_session"):
        session = state_manager.create_session(state)
        session_id = session["session_id"]
    else:
        session_id = state["current_session"]["session_id"]

    if sdb:
        try:
            sdb.upsert_pipeline_run(queue_session_id, hesap_key, marketplace, "agent3_execute", "running")
        except Exception:
            pass

    agent3_success = _run_agent3(state, session_id, hesap_key, marketplace)

    if not agent3_success:
        logger.error("Agent 3 basarisiz: %s", account_label)
        if sdb:
            try:
                sdb.upsert_pipeline_run(queue_session_id, hesap_key, marketplace, "agent3_execute", "failed",
                                         error_msg="Agent 3 basarisiz")
            except Exception:
                pass
        return False

    logger.info("Agent 3 tamamlandi: %s. Agent 4'e geciliyor...", account_label)

    # ===== AGENT 4 ASAMA 1: Python Analiz ($0) =====
    if sdb:
        try:
            sdb.upsert_pipeline_run(queue_session_id, hesap_key, marketplace, "agent4", "running")
            sdb.update_agent_status_detail("agent4", "running")
        except Exception:
            pass

    agent4_python_ok = False
    try:
        agent4_cmd = [
            sys.executable,
            os.path.join(config.BASE_DIR, "agent4", "optimizer.py"),
            hesap_key, marketplace
        ]
        result = subprocess.run(
            agent4_cmd,
            capture_output=True, text=True, timeout=600,
            cwd=config.BASE_DIR,
            env={**os.environ, "MAESTRO_SESSION_ID": session_id,
                 "HESAP_KEY": hesap_key, "MARKETPLACE": marketplace},
        )
        if result.returncode == 0:
            agent4_python_ok = True
            logger.info("Agent 4 Asama 1 (Python) basarili: %s", account_label)
        else:
            logger.error("Agent 4 Python hata (exit %d): %s", result.returncode, result.stderr[-300:])
    except subprocess.TimeoutExpired:
        logger.error("Agent 4 Python timeout (10 dk): %s", account_label)
    except Exception as e:
        logger.error("Agent 4 Python calistirilamadi: %s — %s", account_label, e)

    # ===== AGENT 4 ASAMA 2: Dogrudan Anthropic API ile Hata Analizi =====
    # Claude Code KALDIRILDI — dogrudan API cagrisi ile %99 token tasarrufu.
    # Basarisiz olursa pipeline DURMAZ — Python sonuclari yeterli.
    agent4_claude_ok = False
    if agent4_python_ok:
        logger.info("Agent 4 Asama 2 (API) baslatiliyor: %s", account_label)
        try:
            from maestro.agent4_api_caller import run_agent4_phase2
            agent4_claude_ok = run_agent4_phase2(hesap_key, marketplace, config.BASE_DIR)
            if agent4_claude_ok:
                logger.info("Agent 4 Asama 2 (API) basarili: %s", account_label)
            else:
                logger.warning("Agent 4 API sonuc yok — Python sonuclari yeterli")
        except Exception as e:
            logger.warning("Agent 4 API hatasi: %s — Python sonuclari yeterli", e)

    # Agent 4 status guncelle
    if sdb:
        try:
            a4_status = "completed" if agent4_python_ok else "failed"
            sdb.upsert_pipeline_run(queue_session_id, hesap_key, marketplace, "agent4", a4_status)
            sdb.update_agent_status_detail("agent4", a4_status, {
                "python_ok": agent4_python_ok,
                "claude_ok": agent4_claude_ok,
            })
        except Exception:
            pass

    # Pipeline tamamlandi
    _save_log("info",
              f"Agent 3+4 tamamlandi: {account_label} "
              f"(A3={'OK' if agent3_success else 'FAIL'}, "
              f"A4-Python={'OK' if agent4_python_ok else 'FAIL'}, "
              f"A4-Claude={'OK' if agent4_claude_ok else 'SKIP'})",
              "maestro", hesap_key, marketplace)

    return agent3_success  # Agent 4 hatasi pipeline'i FAIL yapmaz


def watch_queue(interval_minutes=5):
    """
    Execution queue'yu belirli araliklarla kontrol eder.
    Kullanim: python -m maestro.maestro_agent watch
    """
    logger.info("=" * 60)
    logger.info("  EXECUTION QUEUE WATCH — her %d dakikada bir kontrol", interval_minutes)
    logger.info("=" * 60)

    while True:
        try:
            results = poll_execution_queue()
            if results:
                for r in results:
                    logger.info("  %s: %s", r.get("hesap"), r.get("status"))
        except Exception as e:
            logger.error("Watch dongusu hatasi: %s", e)

        time.sleep(interval_minutes * 60)


# ============================================================================
# CLI GIRIS NOKTASI
# ============================================================================

def main():
    """Komut satirindan calistirma."""
    if len(sys.argv) < 2:
        print("Kullanim: python -m maestro.maestro_agent <komut> [hesap_key marketplace]")
        print("Komutlar:")
        print("  watch [dakika]                 : Dashboard execution queue izle (varsayilan 5dk)")
        print("  status                         : Tum hesaplarin durumu")
        print("  status <hesap_key> <marketplace> : Tek hesap durumu")
        print("  accounts                       : Aktif hesap listesi")
        return

    command = sys.argv[1].lower().replace("-", "_")
    args = sys.argv[2:]

    if command == "status":
        if len(args) >= 2:
            result = get_status(args[0], args[1])
        else:
            result = get_status()
        print(json.dumps(result, indent=2, ensure_ascii=False))

    elif command == "accounts":
        pipelines = config.get_active_pipelines()
        print(f"\nAktif hesaplar ({len(pipelines)}):")
        for i, p in enumerate(pipelines, 1):
            print(f"  {i:2d}. {p['hesap_key']}/{p['marketplace']}")
        print()

    elif command == "watch":
        interval = int(args[0]) if args else 5
        watch_queue(interval)

    else:
        print(f"Bilinmeyen komut: {command}")
        print("Gecerli komutlar: watch, status, accounts")


if __name__ == "__main__":
    main()
