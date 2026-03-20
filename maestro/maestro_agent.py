"""
Maestro Agent — Ana Orkestrasyon Motoru (v2 Multi-Account)
============================================================
Tum hesap+marketplace kombinasyonlari icin pipeline calistirir.

Calisma Modlari:
  - start     : Tum aktif hesaplar icin pipeline'i bastan baslatir
  - start <hesap_key> <marketplace> : Tek hesap icin pipeline
  - resume <hesap_key> <marketplace> : Hata sonrasi devam
  - status    : Tum hesaplarin durumunu gosterir
  - status <hesap_key> <marketplace> : Tek hesap durumu
  - check <hesap_key> <marketplace>  : Excel onay kontrolu
  - log       : Son log dosyasini gosterir
  - history   : Gecmis session'larin ozetini gosterir
  - accounts  : Aktif hesap listesini gosterir

Pipeline Akisi (her hesap+marketplace icin):
  1. config.init_account(hesap_key, marketplace)
  2. Agent 1 (Veri Toplama — MCP tool)
  3. Agent 2 (Analiz — Python script)
  4. Onay bekleme (E-posta + Excel kontrol dongusu)
  5. Agent 3 (Execution — Python script + MCP tools)
  6. Ozet rapor e-postasi
"""

import os
import sys
import json
import time
import logging
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from . import config
from . import state_manager
from . import email_handler
from . import retry_handler
from . import excel_checker

logger = logging.getLogger("maestro.agent")

# Ust dizini path'e ekle (log_utils icin)
_maestro_base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _maestro_base not in sys.path:
    sys.path.insert(0, _maestro_base)
from log_utils import save_error_log as _central_save_error_log, save_log as _save_log


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
        except Exception:
            continue


# ============================================================================
# PIPELINE — ANA AKIS
# ============================================================================

def start_pipeline(hesap_key, marketplace, force=False):
    """
    Tek hesap+marketplace icin pipeline baslatir.
    """
    config.init_account(hesap_key, marketplace)
    account_label = config.CURRENT_ACCOUNT["label"]
    state = state_manager.load_state()

    # Log rotasyonu — 30 gunden eski dosyalari temizle
    _rotate_old_logs()

    # 1. Duplikasyon kontrolu
    if not force and state_manager.is_already_run_today(state):
        msg = (
            f"Pipeline {account_label} bugun ({datetime.utcnow().strftime(config.DATE_FORMAT)}) zaten calistirildi. "
            f"Tekrar calistirmak icin force=True kullanin."
        )
        logger.warning(msg)
        return {"durum": "ENGELLENDI", "hesap": account_label, "mesaj": msg}

    # 2. Yeni session olustur
    session = state_manager.create_session(state)
    session_id = session["session_id"]
    log_path = state_manager.setup_session_log(session_id)

    logger.info("Pipeline baslatiliyor — %s — Session: %s", account_label, session_id)

    # Supabase pipeline_runs kaydi
    sdb = _get_sdb()
    if sdb:
        try:
            sdb.upsert_pipeline_run(session_id, hesap_key, marketplace, "starting", "running")
        except Exception:
            pass
    _save_log("info", f"Pipeline basladi: {hesap_key}/{marketplace}",
              "maestro", hesap_key, marketplace, session_id)

    # 3. Agent 1 calistir
    success = _run_agent1(state, session_id, hesap_key, marketplace)
    if not success:
        return _build_error_result(state, session_id, "Agent 1", account_label)

    # 4. Agent 2 calistir
    success = _run_agent2(state, session_id, hesap_key, marketplace)
    if not success:
        return _build_error_result(state, session_id, "Agent 2", account_label)

    # 5. Agent 2 tamamlandi — Dashboard onay bekleme + execution_queue watch
    logger.info("Agent 2 tamamlandi. Dashboard'dan onay bekleniyor...")
    state_manager.update_session_status(state, "waiting_approval")
    _save_log("info", "Dashboard'dan onay bekleniyor — execution_queue izleniyor",
              "maestro", hesap_key, marketplace, session_id)
    if sdb:
        try:
            sdb.upsert_pipeline_run(session_id, hesap_key, marketplace, "waiting_approval", "running")
        except Exception:
            pass

    # Otomatik watch modu — 5 dakikada bir execution_queue kontrol et
    logger.info("=" * 60)
    logger.info("  WATCH MODU AKTIF — Dashboard'dan Agent3 komutu bekleniyor")
    logger.info("  Her 5 dakikada bir execution_queue kontrol edilecek")
    logger.info("=" * 60)

    watch_interval = 5 * 60  # 5 dakika
    while True:
        try:
            results = poll_execution_queue()
            if results:
                # Agent3 calistirildi — pipeline'i tamamla
                for r in results:
                    logger.info("  Queue sonucu: %s — %s", r.get("hesap"), r.get("status"))

                # Pipeline tamamlandi
                state_manager.update_session_status(state, "completed")
                logger.info("=" * 60)
                logger.info("PIPELINE TAMAMLANDI — %s — Session: %s", account_label, session_id)
                logger.info("=" * 60)

                if sdb:
                    try:
                        sdb.upsert_pipeline_run(session_id, hesap_key, marketplace, "completed", "completed")
                    except Exception:
                        pass
                _save_log("info", f"Pipeline tamamlandi: {hesap_key}/{marketplace}",
                          "maestro", hesap_key, marketplace, session_id)

                _send_completion_email(state, session_id)
                state_manager.archive_session(state)

                return {
                    "durum": "TAMAMLANDI",
                    "hesap": account_label,
                    "session_id": session_id,
                    "mesaj": f"Pipeline basariyla tamamlandi ({account_label}).",
                }
        except Exception as e:
            logger.error("Watch dongusu hatasi: %s", e)

        time.sleep(watch_interval)


def run_all_pipelines(force=False):
    """
    Tum aktif hesaplar icin pipeline'i sirayla calistirir.
    accounts.json'daki pipeline_ayarlari.calisma_sirasi kullanilir.
    """
    pipelines = config.get_active_pipelines()
    logger.info("=" * 60)
    logger.info("MAESTRO MULTI-ACCOUNT — %d pipeline baslatiliyor", len(pipelines))
    logger.info("=" * 60)

    results = []
    for p in pipelines:
        hesap_key = p["hesap_key"]
        marketplace = p["marketplace"]
        logger.info("\n--- Pipeline: %s/%s ---", hesap_key, marketplace)

        result = start_pipeline(hesap_key, marketplace, force=force)
        results.append({
            "hesap_key": hesap_key,
            "marketplace": marketplace,
            "durum": result.get("durum", "?"),
            "session_id": result.get("session_id", ""),
        })

        # Hata durumunda sonraki hesaba gec (pipeline durmasin)
        if result.get("durum") == "HATA":
            logger.warning("Pipeline %s/%s basarisiz — sonraki hesaba geciliyor",
                          hesap_key, marketplace)

    # Ozet
    basarili = sum(1 for r in results if r["durum"] == "TAMAMLANDI")
    hatali = sum(1 for r in results if r["durum"] == "HATA")
    logger.info("\n" + "=" * 60)
    logger.info("MAESTRO TAMAMLANDI — %d/%d basarili, %d hata",
                basarili, len(results), hatali)
    logger.info("=" * 60)

    return {"pipelines": results, "basarili": basarili, "hatali": hatali}


def resume_pipeline(hesap_key, marketplace):
    """
    Hata sonrasi kaldigi yerden devam eder.
    """
    config.init_account(hesap_key, marketplace)
    account_label = config.CURRENT_ACCOUNT["label"]
    state = state_manager.load_state()
    session = state.get("current_session")

    if not session:
        return {"durum": "BOS", "hesap": account_label,
                "mesaj": f"Devam edilecek aktif session yok ({account_label}). 'maestro start {hesap_key} {marketplace}' kullanin."}

    session_id = session["session_id"]
    state_manager.setup_session_log(session_id)
    logger.info("Pipeline devam ettiriliyor — %s — Session: %s", account_label, session_id)

    last_step = state_manager.get_last_completed_step(state)

    if last_step is None:
        logger.info("Agent 1'den baslaniyor...")
        success = _run_agent1(state, session_id, hesap_key, marketplace)
        if not success:
            return _build_error_result(state, session_id, "Agent 1", account_label)
        last_step = "agent1"

    if last_step == "agent1":
        success = _run_agent2(state, session_id, hesap_key, marketplace)
        if not success:
            return _build_error_result(state, session_id, "Agent 2", account_label)
        last_step = "agent2"

    if last_step == "agent2":
        current_status = session.get("status", "")
        if current_status == "waiting_approval":
            approval_result = _wait_for_approval(state, session_id)
            if not approval_result:
                return _build_waiting_result(state, session_id, account_label)
        elif session["agent3"]["status"] != "completed":
            state_manager.update_session_status(state, "waiting_approval")
            approval_result = _wait_for_approval(state, session_id)
            if not approval_result:
                return _build_waiting_result(state, session_id, account_label)

    if session["agent3"]["status"] != "completed":
        success = _run_agent3(state, session_id, hesap_key, marketplace)
        if not success:
            return _build_error_result(state, session_id, "Agent 3", account_label)

    state_manager.update_session_status(state, "completed")
    logger.info("PIPELINE TAMAMLANDI (resume) — %s — Session: %s", account_label, session_id)
    _send_completion_email(state, session_id)
    state_manager.archive_session(state)

    return {
        "durum": "TAMAMLANDI",
        "hesap": account_label,
        "session_id": session_id,
        "mesaj": f"Pipeline basariyla tamamlandi - resume ({account_label}).",
    }


# ============================================================================
# AGENT CALISTIRICILARI
# ============================================================================

def _run_agent1(state, session_id, hesap_key, marketplace):
    """Agent 1'i calistirir (MCP tool)."""
    logger.info("--- AGENT 1: Veri Toplama (%s/%s) ---", hesap_key, marketplace)
    state_manager.update_agent_status(state, "agent1", "running")

    _save_log("info", f"Agent 1 basliyor: {hesap_key}/{marketplace}",
              "agent1", hesap_key, marketplace, session_id)
    sdb = _get_sdb()
    if sdb:
        try:
            sdb.upsert_pipeline_run(session_id, hesap_key, marketplace, "agent1", "running")
        except Exception:
            pass

    today = datetime.utcnow().strftime(config.DATE_FORMAT)
    data_dir = Path(config.ACCOUNT_DATA_DIR)

    kritik_dosya = data_dir / f"{today}_sp_campaigns.json"

    if kritik_dosya.exists():
        logger.info("Agent 1 verileri zaten mevcut (tarih: %s). Devam ediliyor.", today)
        state_manager.update_agent_status(
            state, "agent1", "completed",
            summary=f"Veriler mevcut (tarih: {today})"
        )
        _save_log("info", f"Agent 1 tamamlandi (cached, tarih: {today})",
                  "agent1", hesap_key, marketplace, session_id)
        if sdb:
            try:
                sdb.upsert_pipeline_run(session_id, hesap_key, marketplace, "agent1", "completed")
                sdb.update_agent_status_detail("agent1", "completed", {"tasks": 0, "duration": "cached"})
            except Exception:
                pass
        return True

    logger.info("Agent 1 verileri bulunamadi. MCP tool cagiriliyor...")
    logger.info("TALIMAT: Claude Code 'amazon_ads_collect_all_data({\"hesap_key\": \"%s\", \"marketplace\": \"%s\"})' cagirmali.",
                hesap_key, marketplace)

    # Bekleme dongusu — Claude Code MCP tool'u cagirdiktan sonra dosyalar olusur
    max_wait_minutes = 30
    check_interval = 30  # saniye
    waited = 0

    while waited < max_wait_minutes * 60:
        if kritik_dosya.exists():
            # Dosya sayisini kontrol et
            files = list(data_dir.glob(f"{today}_*.json"))
            logger.info("Agent 1 verileri algilandi: %d dosya", len(files))
            state_manager.update_agent_status(
                state, "agent1", "completed",
                summary=f"{len(files)} dosya toplandi"
            )
            _save_log("info", f"Agent 1 tamamlandi: {len(files)} dosya toplandi",
                      "agent1", hesap_key, marketplace, session_id)
            if sdb:
                try:
                    sdb.upsert_pipeline_run(session_id, hesap_key, marketplace, "agent1", "completed")
                    sdb.update_agent_status_detail("agent1", "completed", {"tasks": len(files)})
                except Exception:
                    pass
            return True

        time.sleep(check_interval)
        waited += check_interval
        if waited % 120 == 0:
            logger.info("Agent 1 bekleniyor... (%d dk)", waited // 60)

    # Timeout
    error_msg = f"Agent 1 {max_wait_minutes} dakika icinde tamamlanmadi."
    logger.error(error_msg)
    state_manager.update_agent_status(state, "agent1", "failed", errors=[error_msg])
    save_error_log("AgentFailure", error_msg, session_id=session_id,
                   adim="run_agent1",
                   extra={"agent": "agent1", "sebep": "timeout",
                          "bekleme_dk": max_wait_minutes})
    _save_log("error", f"Agent 1 hatasi: {error_msg[:200]}",
              "agent1", hesap_key, marketplace, session_id, error_type="AgentFailure")
    if sdb:
        try:
            sdb.upsert_pipeline_run(session_id, hesap_key, marketplace, "agent1", "failed", error_msg=error_msg)
            sdb.update_agent_status_detail("agent1", "failed")
        except Exception:
            pass
    email_handler.send_error(
        session_id, "Agent 1", error_msg,
        suggestion="Amazon API baglantisini kontrol edin. 'maestro resume' ile tekrar deneyin."
    )
    return False


def _run_agent2(state, session_id, hesap_key, marketplace):
    """Agent 2'yi calistirir (Python script)."""
    logger.info("--- AGENT 2: Analiz (%s/%s) ---", hesap_key, marketplace)
    state_manager.update_agent_status(state, "agent2", "running")

    _save_log("info", f"Agent 2 basliyor: {hesap_key}/{marketplace}",
              "agent2", hesap_key, marketplace, session_id)
    sdb = _get_sdb()
    if sdb:
        try:
            sdb.upsert_pipeline_run(session_id, hesap_key, marketplace, "agent2", "running")
        except Exception:
            pass

    def run_agent2_script():
        env = {**os.environ, "MAESTRO_SESSION_ID": session_id}
        result = subprocess.run(
            [sys.executable, config.AGENT2_SCRIPT, hesap_key, marketplace],
            capture_output=True, text=True, timeout=600,
            cwd=config.BASE_DIR, env=env,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Agent 2 hata ile cikti (code {result.returncode}): {result.stderr[:500]}")

        # stdout'tan JSON sonucu parse et
        output = result.stdout.strip()
        # Son satirdan itibaren JSON bul
        json_start = output.rfind("{")
        if json_start >= 0:
            return json.loads(output[json_start:])
        raise RuntimeError(f"Agent 2 JSON ciktisi alinamadi. Output: {output[:500]}")

    success, result, error_info = retry_handler.execute_with_retry(
        run_agent2_script, "Agent 2"
    )

    if success:
        summary = result if isinstance(result, dict) else {}
        state_manager.update_agent_status(
            state, "agent2", "completed",
            summary=_format_agent2_summary(summary)
        )

        # Agent 2 summary'yi session'a kaydet (e-posta icin)
        session = state.get("current_session", {})
        session["_agent2_full_summary"] = summary

        logger.info("Agent 2 tamamlandi: %s", state["current_session"]["agent2"]["summary"])
        _save_log("info", f"Agent 2 tamamlandi: {summary.get('toplam_hedef', 0)} hedef analiz edildi",
                  "agent2", hesap_key, marketplace, session_id)
        if sdb:
            try:
                sdb.upsert_pipeline_run(session_id, hesap_key, marketplace, "agent2", "completed")
                sdb.update_agent_status_detail("agent2", "completed", {
                    "tasks": summary.get("toplam_hedef", 0) if isinstance(summary, dict) else 0,
                })
            except Exception:
                pass
        return True
    else:
        error_msg = error_info.get("error_message", "Bilinmeyen hata") if error_info else "Bilinmeyen hata"
        error_type = error_info.get("error_type", "server_error") if error_info else "server_error"
        attempts = error_info.get("attempts", 0) if error_info else 0
        state_manager.update_agent_status(state, "agent2", "failed", errors=[error_msg])

        save_error_log("AgentFailure", error_msg, session_id=session_id,
                       adim="run_agent2",
                       extra={"agent": "agent2", "error_type": error_type,
                              "attempts": attempts})

        _save_log("error", f"Agent 2 hatasi: {error_msg[:200]}",
                  "agent2", hesap_key, marketplace, session_id, error_type=error_type)
        if sdb:
            try:
                sdb.upsert_pipeline_run(session_id, hesap_key, marketplace, "agent2", "failed", error_msg=error_msg)
                sdb.update_agent_status_detail("agent2", "failed")
            except Exception:
                pass

        suggestion = retry_handler.get_error_suggestion(error_type)
        email_handler.send_error(session_id, "Agent 2", error_msg, suggestion)
        return False


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
        except Exception:
            pass

    # Adim 1: Dry-run
    logger.info("Agent 3 — Adim 1: Dry-run")
    def run_agent3_dryrun():
        env = {**os.environ, "MAESTRO_SESSION_ID": session_id}
        result = subprocess.run(
            [sys.executable, config.AGENT3_SCRIPT, hesap_key, marketplace],
            capture_output=True, text=True, timeout=300,
            cwd=config.BASE_DIR, env=env,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Agent 3 dry-run hatasi (code {result.returncode}): {result.stderr[:500]}")
        output = result.stdout.strip()
        json_start = output.rfind("{")
        if json_start >= 0:
            return json.loads(output[json_start:])
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
            except Exception:
                pass
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
        env = {**os.environ, "MAESTRO_SESSION_ID": session_id}
        result = subprocess.run(
            [sys.executable, config.AGENT3_SCRIPT, hesap_key, marketplace, "--execute"],
            capture_output=True, text=True, timeout=600,
            cwd=config.BASE_DIR, env=env,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Agent 3 execution hatasi (code {result.returncode}): {result.stderr[:500]}")
        output = result.stdout.strip()
        json_start = output.rfind("{")
        if json_start >= 0:
            return json.loads(output[json_start:])
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
            except Exception:
                pass
        email_handler.send_error(session_id, "Agent 3 (execute)", error_msg,
                                  retry_handler.get_error_suggestion(error_type))
        return False

    # Adim 3: 5 dk bekle + verify
    logger.info("Agent 3 — Adim 3: 5 dakika bekleniyor (dogrulama icin)...")
    time.sleep(300)

    logger.info("Agent 3 — Adim 4: Dogrulama")
    def run_agent3_verify():
        env = {**os.environ, "MAESTRO_SESSION_ID": session_id}
        result = subprocess.run(
            [sys.executable, config.AGENT3_SCRIPT, hesap_key, marketplace, "--verify"],
            capture_output=True, text=True, timeout=300,
            cwd=config.BASE_DIR, env=env,
        )
        output = result.stdout.strip()
        json_start = output.rfind("{")
        if json_start >= 0:
            return json.loads(output[json_start:])
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
        except Exception:
            pass

    logger.info("Agent 3 tamamlandi: %s", exec_summary)
    return True


# ============================================================================
# ONAY BEKLEME DONGUSU
# ============================================================================

def _wait_for_approval(state, session_id):
    """
    E-posta gonderir ve onay bekler.
    Her APPROVAL_CHECK_INTERVAL_MINUTES dakikada bir kontrol eder.
    
    Returns:
        True: Onay alindi
        False: Hala bekleniyor (pipeline duraklatildi)
    """
    session = state.get("current_session", {})

    # E-posta gonder (henuz gonderilmediyse)
    if not session["approval"].get("email_sent_at"):
        agent2_summary = session.get("_agent2_full_summary", {})
        success, _ = email_handler.send_excel_ready(session_id, agent2_summary)
        if success:
            state_manager.update_approval_status(state, "email_sent_at")
        else:
            logger.warning("Excel hazir e-postasi gonderilemedi. Yine de beklemeye devam.")
            save_error_log("NetworkError", "Excel hazir e-postasi gonderilemedi",
                           session_id=session_id, adim="send_excel_ready",
                           extra={"email_type": "excel_ready"})

    interval_seconds = config.APPROVAL_CHECK_INTERVAL_MINUTES * 60
    reminder_seconds = config.APPROVAL_REMINDER_AFTER_HOURS * 3600
    wait_start = time.time()
    check_count = 0

    while True:
        check_count += 1
        elapsed = time.time() - wait_start
        logger.info("Onay kontrolu #%d (%d dk beklendi)...", check_count, int(elapsed // 60))

        # 1. IMAP kontrol (reply var mi?)
        reply_found, imap_error = email_handler.check_for_approval_reply(session_id)

        if reply_found:
            logger.info("E-posta reply alindi! Excel kontrol ediliyor...")

            # 2. Excel'leri kontrol et
            excel_status = excel_checker.check_approval_status()

            if excel_status["has_any_approval"]:
                logger.info("Onay kutulari dolu (%d/%d). Agent 3'e geciliyor.",
                            excel_status["approved_rows"], excel_status["total_rows"])
                state_manager.update_approval_status(state, "approved_at")
                state_manager.update_approval_status(state, "approval_method", "email_reply+excel")
                return True
            else:
                logger.warning("Reply alindi ama Excel onay kutulari bos! Kullaniciya bildirim...")
                email_handler.send_email(
                    f"[Maestro] Excel Onay Kutulari Bos - Session {session_id}",
                    "Reply'iniz alindi ancak Excel dosyalarindaki Onay kutulari hala bos.\n"
                    "Lutfen Excel'leri doldurup tekrar reply atin."
                )

        # 3. Reply olmadan da Excel kontrolu yap (kullanici doldurup reply atmayi unutmus olabilir)
        excel_status = excel_checker.check_approval_status()
        if excel_status["has_any_approval"]:
            logger.info("Excel onay kutulari dolu bulundu (reply olmadan). Agent 3'e geciliyor.")
            state_manager.update_approval_status(state, "approved_at")
            state_manager.update_approval_status(state, "approval_method", "excel_only")
            return True

        # 4. Hatirlatma e-postasi (6 saat sonra)
        if elapsed >= reminder_seconds and not session["approval"].get("reminder_sent_at"):
            logger.info("6 saat gecti, hatirlatma e-postasi gonderiliyor...")
            email_handler.send_reminder(session_id)
            state_manager.update_approval_status(state, "reminder_sent_at")

        # 5. Bekle
        logger.info("Onay bekleniyor. Sonraki kontrol %d dk sonra.",
                     config.APPROVAL_CHECK_INTERVAL_MINUTES)
        time.sleep(interval_seconds)


# ============================================================================
# YARDIMCI FONKSIYONLAR
# ============================================================================

def _format_agent2_summary(summary):
    """Agent 2 summary'sini okunabilir stringe cevirir."""
    if not isinstance(summary, dict):
        return str(summary)

    parts = []
    parts.append(f"Hedefleme: {summary.get('toplam_hedefleme', 0)}")
    parts.append(f"Bid tavsiye: {summary.get('bid_tavsiye_sayisi', 0)}")
    parts.append(f"Negatif: {summary.get('negatif_aday_sayisi', 0)}")
    parts.append(f"Harvesting: {summary.get('harvesting_aday_sayisi', 0)}")
    return " | ".join(parts)


def _build_error_result(state, session_id, agent_name, account_label=""):
    """Hata durumu icin sonuc dict'i olusturur."""
    session = state.get("current_session", {})
    return {
        "durum": "HATA",
        "hesap": account_label,
        "session_id": session_id,
        "agent": agent_name,
        "mesaj": f"{agent_name} basarisiz oldu ({account_label}). Detaylar icin log'a bakin.",
        "hatalar": session.get("errors", []),
    }


def _build_waiting_result(state, session_id, account_label=""):
    """Onay bekleme durumu icin sonuc dict'i olusturur."""
    return {
        "durum": "ONAY_BEKLENIYOR",
        "hesap": account_label,
        "session_id": session_id,
        "mesaj": f"Pipeline onay bekliyor ({account_label}). Excel'leri doldurup e-postaya reply atin.",
    }


def _send_completion_email(state, session_id):
    """Pipeline tamamlandi e-postasi gonderir."""
    session = state.get("current_session", {})

    def _calc_duration(agent_key):
        a = session.get(agent_key, {})
        start = a.get("started_at")
        end = a.get("completed_at")
        if start and end:
            try:
                s = datetime.fromisoformat(start)
                e = datetime.fromisoformat(end)
                delta = e - s
                minutes = int(delta.total_seconds() // 60)
                seconds = int(delta.total_seconds() % 60)
                return f"{minutes}dk {seconds}sn"
            except Exception:
                pass
        return "-"

    summary = {
        "agent1_duration": _calc_duration("agent1"),
        "agent2_duration": _calc_duration("agent2"),
        "agent3_duration": _calc_duration("agent3"),
        "agent3_summary": session.get("_agent3_exec_result", {}),
    }

    email_handler.send_completed(session_id, summary)


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


def check_approval(hesap_key, marketplace):
    """Manuel onay kontrolu."""
    config.init_account(hesap_key, marketplace)
    summary, status = excel_checker.get_approval_summary()
    return summary, status


# ============================================================================
# EXECUTION QUEUE — Dashboard'dan Agent3 tetikleme
# ============================================================================

def poll_execution_queue():
    """
    Supabase execution_queue tablosundaki pending komutlari kontrol eder.
    Dashboard'dan 'Agent3'u Calistir' butonuna basildiginda buraya komut duser.
    """
    sdb = _get_sdb()
    if not sdb:
        return []

    try:
        rows = sdb._fetch_all(
            "SELECT id, hesap_key, marketplace, command FROM execution_queue WHERE status='pending' ORDER BY requested_at")
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
    """Dashboard onaylarindan Agent3'u calistirir."""
    config.init_account(hesap_key, marketplace)
    state = state_manager.load_state()
    session_id = f"queue_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{hesap_key}_{marketplace}"

    _save_log("info", f"Agent3 basliyor (dashboard queue): {hesap_key}/{marketplace}",
              "agent3", hesap_key, marketplace, session_id)

    sdb = _get_sdb()
    if sdb:
        try:
            sdb.upsert_pipeline_run(session_id, hesap_key, marketplace, "agent3_execute", "running")
            sdb.update_agent_status_detail("agent3", "running")
        except Exception:
            pass

    env = {**os.environ, "MAESTRO_SESSION_ID": session_id}
    try:
        result = subprocess.run(
            [sys.executable, config.AGENT3_SCRIPT, hesap_key, marketplace, "--execute"],
            capture_output=True, text=True, timeout=600,
            cwd=config.BASE_DIR, env=env,
        )
        success = result.returncode == 0

        final_status = "completed" if success else "failed"
        _save_log("info" if success else "error",
                  f"Agent3 {'tamamlandi' if success else 'basarisiz'} (dashboard queue): {hesap_key}/{marketplace}",
                  "agent3", hesap_key, marketplace, session_id)
        if sdb:
            try:
                sdb.upsert_pipeline_run(session_id, hesap_key, marketplace, "agent3_execute", final_status)
                sdb.update_agent_status_detail("agent3", final_status)
            except Exception:
                pass

        return success
    except Exception as e:
        logger.error("Agent3 queue calistirma hatasi: %s", e)
        _save_log("error", f"Agent3 hatasi (queue): {str(e)[:200]}",
                  "agent3", hesap_key, marketplace, session_id)
        if sdb:
            try:
                sdb.upsert_pipeline_run(session_id, hesap_key, marketplace, "agent3_execute", "failed", str(e)[:500])
                sdb.update_agent_status_detail("agent3", "failed")
            except Exception:
                pass
        return False


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
        print("  start                          : Tum hesaplar icin pipeline")
        print("  start <hesap_key> <marketplace> : Tek hesap icin pipeline")
        print("  resume <hesap_key> <marketplace> : Hata sonrasi devam")
        print("  status                         : Tum hesaplarin durumu")
        print("  status <hesap_key> <marketplace> : Tek hesap durumu")
        print("  check <hesap_key> <marketplace>  : Excel onay kontrolu")
        print("  accounts                       : Aktif hesap listesi")
        print("  log                            : Son log dosyasi")
        print("  history                        : Gecmis session ozeti")
        print("  watch [dakika]                 : Dashboard execution queue izle (varsayilan 5dk)")
        return

    command = sys.argv[1].lower().replace("-", "_")
    args = sys.argv[2:]

    if command == "start":
        if len(args) >= 2:
            result = start_pipeline(args[0], args[1])
        else:
            result = run_all_pipelines()
        print(json.dumps(result, indent=2, ensure_ascii=False))

    elif command == "force_start":
        if len(args) >= 2:
            result = start_pipeline(args[0], args[1], force=True)
        else:
            result = run_all_pipelines(force=True)
        print(json.dumps(result, indent=2, ensure_ascii=False))

    elif command == "resume":
        if len(args) < 2:
            print("Kullanim: maestro resume <hesap_key> <marketplace>")
            return
        result = resume_pipeline(args[0], args[1])
        print(json.dumps(result, indent=2, ensure_ascii=False))

    elif command == "status":
        if len(args) >= 2:
            result = get_status(args[0], args[1])
        else:
            result = get_status()
        print(json.dumps(result, indent=2, ensure_ascii=False))

    elif command == "check" or command == "check_approval":
        if len(args) < 2:
            print("Kullanim: maestro check <hesap_key> <marketplace>")
            return
        summary, _ = check_approval(args[0], args[1])
        print(summary)

    elif command == "accounts":
        pipelines = config.get_active_pipelines()
        print(f"\nAktif hesaplar ({len(pipelines)}):")
        for i, p in enumerate(pipelines, 1):
            print(f"  {i:2d}. {p['hesap_key']}/{p['marketplace']}")
        print()

    elif command == "log":
        log_path = state_manager.get_latest_log_path()
        if log_path and os.path.exists(log_path):
            with open(log_path, "r", encoding="utf-8") as f:
                print(f.read())
        else:
            print("Log dosyasi bulunamadi.")

    elif command == "history":
        if len(args) >= 2:
            result = get_history(args[0], args[1])
        else:
            result = get_history()
        for s in result:
            hesap = s.get('hesap', '')
            print(f"  {hesap:25s} | {s.get('session_id', '')} | {s.get('date', '')} | {s.get('status', '')}")

    elif command == "watch":
        interval = int(args[0]) if args else 5
        watch_queue(interval)

    else:
        print(f"Bilinmeyen komut: {command}")
        print("Gecerli komutlar: start, resume, status, check, accounts, log, history, watch")


if __name__ == "__main__":
    main()
