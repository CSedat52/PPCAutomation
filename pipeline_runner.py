"""
Pipeline Runner — Saf Python Orkestrasyon ($0 maliyet)
========================================================
Agent 1 (parallel_collector) + Agent 2 (parallel_analyzer) calistirir.
Claude Code CAGIRMAZ — tum islem saf Python subprocess.

Cron bu dosyayi cagirmali:
  python pipeline_runner.py                    → tum hesaplar
  python pipeline_runner.py vigowood_na:US     → belirli marketplace
  python pipeline_runner.py --force            → Supabase cache kontrolunu atla

Pipeline akisi:
  1. parallel_collector.py (Agent 1) — veri toplama
  2. parallel_analyzer.py (Agent 2) — analiz
  3. Eksik rapor kontrolu (Supabase) → varsa retry
  4. Durum raporu + e-posta
  5. Agent 3+4 icin watch daemon bekler (ayri process)
"""

import os
import sys
import json
import subprocess
import logging
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR))

from log_utils import save_log as _save_log

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("pipeline_runner")

PIPELINE_DATE = datetime.utcnow().strftime("%Y-%m-%d")


# ============================================================================
# E-POSTA
# ============================================================================

def _send_email(subject, body):
    """Gmail SMTP ile e-posta gonderir (maestro email_handler uzerinden)."""
    try:
        from maestro.email_handler import send_email
        ok, err = send_email(subject, body)
        if ok:
            logger.info("E-posta gonderildi: %s", subject)
        else:
            logger.warning("E-posta gonderilemedi: %s", err)
    except Exception as e:
        logger.warning("E-posta hatasi: %s", e)


# ============================================================================
# DURUM RAPORU
# ============================================================================

def _build_status_report(collect_ok, analyze_ok, session_id, targets):
    """
    Maestro'nun okuyacagi ozet rapor.
    collect_ok: bool (Agent 1 basarili mi)
    analyze_ok: bool (Agent 2 basarili mi)
    """
    report = {
        "session_id": session_id,
        "pipeline_date": PIPELINE_DATE,
        "timestamp": datetime.utcnow().isoformat(),
        "agent1": {"status": "completed" if collect_ok else "failed"},
        "agent2": {"status": "completed" if analyze_ok else "failed"},
        "eksik_raporlar": [],
        "genel_durum": "TEMIZ",
    }

    # Eksik raporlari Supabase'den kontrol et
    try:
        from supabase.db_client import SupabaseClient
        db = SupabaseClient()

        from maestro.config import get_active_pipelines
        pipelines = get_active_pipelines()

        # Sadece hedeflenen marketplace'leri kontrol et
        if targets:
            target_set = set()
            for t in targets:
                if ":" in t:
                    hk, mp = t.split(":", 1)
                    target_set.add((hk, mp))
                else:
                    for p in pipelines:
                        if p["hesap_key"] == t:
                            target_set.add((p["hesap_key"], p["marketplace"]))
            pipelines = [p for p in pipelines
                         if (p["hesap_key"], p["marketplace"]) in target_set]

        for p in pipelines:
            hk = p["hesap_key"]
            mp = p["marketplace"]

            # Smart-skip: Supabase-first kampanya sayisi kontrolu
            data_dir = BASE_DIR / "data" / f"{hk}_{mp}"
            try:
                from data_loader import count_campaigns as _count_camp
                sp_count = _count_camp(hk, mp, "SP", str(data_dir), PIPELINE_DATE)
                sb_count = _count_camp(hk, mp, "SB", str(data_dir), PIPELINE_DATE)
                sd_count = _count_camp(hk, mp, "SD", str(data_dir), PIPELINE_DATE)
            except Exception:
                # data_loader import basarisiz — guvenli tarafta kal
                sp_count = -1
                sb_count = -1
                sd_count = -1

            beklenen_mp = []
            if sp_count != 0:  # -1 (dosya yok) durumunda da bekle (guvenli taraf)
                beklenen_mp.append(("sp_targeting", "targeting_reports", "SP"))
                beklenen_mp.append(("sp_search_term", "search_term_reports", "SP"))
            else:
                logger.info("[%s/%s] SP kampanya=0 → SP raporlari smart-skip", hk, mp)

            if sb_count != 0:
                beklenen_mp.append(("sb_targeting", "targeting_reports", "SB"))
                beklenen_mp.append(("sb_search_term", "search_term_reports", "SB"))
            else:
                logger.info("[%s/%s] SB kampanya=0 → SB raporlari smart-skip", hk, mp)

            if sd_count != 0:
                beklenen_mp.append(("sd_targeting", "targeting_reports", "SD"))
            else:
                logger.info("[%s/%s] SD kampanya=0 → SD raporlari smart-skip", hk, mp)

            for rapor_adi, tablo, ad_type in beklenen_mp:
                date_col = "collection_date"
                rows = db._fetch_all(
                    f"SELECT 1 FROM {tablo} WHERE hesap_key = %s AND marketplace = %s "
                    f"AND {date_col} = %s AND ad_type = %s LIMIT 1",
                    (hk, mp, PIPELINE_DATE, ad_type)
                )
                if not rows:
                    report["eksik_raporlar"].append({
                        "hesap_key": hk,
                        "marketplace": mp,
                        "rapor": rapor_adi,
                        "ad_type": ad_type,
                    })

    except Exception as e:
        logger.warning("Eksik rapor kontrolu basarisiz: %s", e)

    if report["eksik_raporlar"]:
        report["genel_durum"] = "EKSIK"
    elif not collect_ok or not analyze_ok:
        report["genel_durum"] = "KISMI"
    else:
        report["genel_durum"] = "TEMIZ"

    return report


def _save_status_report(report):
    """Durum raporunu Supabase agent_logs'a ve lokal dosyaya yaz."""
    try:
        _save_log("info",
                  json.dumps(report, ensure_ascii=False),
                  "pipeline_runner",
                  session_id=report["session_id"])
        logger.info("Durum raporu Supabase'e yazildi")
    except Exception as e:
        logger.warning("Durum raporu yazilamadi: %s", e)

    rapor_path = BASE_DIR / "data" / f"{PIPELINE_DATE}_pipeline_report.json"
    try:
        rapor_path.parent.mkdir(parents=True, exist_ok=True)
        with open(rapor_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        logger.info("Durum raporu lokal kaydedildi: %s", rapor_path)
    except Exception:
        pass

    return rapor_path


# ============================================================================
# ANA PIPELINE
# ============================================================================

def run(targets=None, force=False):
    """
    Agent 1 + Agent 2 pipeline'ini calistirir.
    Saf Python — Claude Code CAGIRMAZ.

    Args:
        targets: None → tum aktif hesaplar, ["vigowood_na:US", ...] → belirli
        force: True ise Supabase cache kontrolunu atla
    """
    session_id = f"runner_{PIPELINE_DATE}_{datetime.utcnow().strftime('%H%M%S')}"
    logger.info("=" * 60)
    logger.info("  PIPELINE RUNNER — %s", PIPELINE_DATE)
    logger.info("  Session: %s", session_id)
    if targets:
        logger.info("  Hedefler: %s", ", ".join(targets))
    else:
        logger.info("  Hedefler: TUM AKTIF HESAPLAR")
    if force:
        logger.info("  Mod: FORCE (cache kontrolu atlanacak)")
    logger.info("=" * 60)

    _save_log("info", f"Pipeline runner basladi: {session_id}", "pipeline_runner",
              session_id=session_id)

    # ===== AGENT 1: Veri Toplama =====
    logger.info("\n--- AGENT 1: Veri Toplama (parallel_collector) ---")

    cmd_agent1 = [sys.executable, str(BASE_DIR / "parallel_collector.py")]
    if force:
        cmd_agent1.append("--force")
    if targets:
        cmd_agent1.extend(targets)

    collect_ok = False
    try:
        result = subprocess.run(
            cmd_agent1,
            capture_output=True, text=True, timeout=7200,
            cwd=str(BASE_DIR),
            env={**os.environ, "MAESTRO_SESSION_ID": session_id},
        )
        if result.returncode == 0:
            collect_ok = True
            logger.info("Agent 1 basarili (exit code 0)")
        else:
            logger.error("Agent 1 hata (exit code %d): %s", result.returncode, result.stderr[-500:])
        # Son satirlari logla
        if result.stdout:
            for line in result.stdout.strip().split("\n")[-10:]:
                logger.info("  [A1] %s", line.strip())
    except subprocess.TimeoutExpired:
        logger.error("Agent 1 timeout (2 saat)")
    except Exception as e:
        logger.error("Agent 1 calistirilamadi: %s", e)

    # ===== AGENT 2: Analiz =====
    logger.info("\n--- AGENT 2: Analiz (parallel_analyzer) ---")

    cmd_agent2 = [sys.executable, str(BASE_DIR / "parallel_analyzer.py")]
    if targets:
        cmd_agent2.extend(targets)

    analyze_ok = False
    try:
        result = subprocess.run(
            cmd_agent2,
            capture_output=True, text=True, timeout=1800,
            cwd=str(BASE_DIR),
            env={**os.environ, "MAESTRO_SESSION_ID": session_id},
        )
        if result.returncode == 0:
            analyze_ok = True
            logger.info("Agent 2 basarili (exit code 0)")
        else:
            logger.error("Agent 2 hata (exit code %d): %s", result.returncode, result.stderr[-500:])
        if result.stdout:
            for line in result.stdout.strip().split("\n")[-10:]:
                logger.info("  [A2] %s", line.strip())
    except subprocess.TimeoutExpired:
        logger.error("Agent 2 timeout (30 dakika)")
    except Exception as e:
        logger.error("Agent 2 calistirilamadi: %s", e)

    # ===== DURUM RAPORU =====
    logger.info("\n--- DURUM RAPORU ---")
    report = _build_status_report(collect_ok, analyze_ok, session_id, targets)
    rapor_path = _save_status_report(report)

    if report["genel_durum"] == "TEMIZ":
        logger.info("Pipeline temiz tamamlandi. Dashboard'dan onay bekleniyor.")
        _send_email(
            f"[PPC Pipeline] TEMIZ — {PIPELINE_DATE}",
            f"Agent 1+2 basariyla tamamlandi.\n"
            f"Session: {session_id}\n\n"
            f"Dashboard'dan Agent 3 onayi bekleniyor."
        )

    elif report["genel_durum"] == "EKSIK":
        eksik_sayisi = len(report["eksik_raporlar"])
        logger.info("Eksik rapor tespit edildi: %d rapor. Retry baslatiliyor...", eksik_sayisi)

        # Eksik raporlar icin parallel_collector'i tekrar cagir (sadece eksik marketplace'ler)
        eksik_targets = sorted(set(
            f"{e['hesap_key']}:{e['marketplace']}" for e in report["eksik_raporlar"]
        ))
        logger.info("Eksik marketplace'ler: %s", ", ".join(eksik_targets))

        retry_cmd = [sys.executable, str(BASE_DIR / "parallel_collector.py")] + eksik_targets
        try:
            result = subprocess.run(
                retry_cmd,
                capture_output=True, text=True, timeout=7200,
                cwd=str(BASE_DIR),
                env={**os.environ, "MAESTRO_SESSION_ID": session_id},
            )
            if result.returncode == 0:
                logger.info("Retry basarili")
            else:
                logger.error("Retry hatasi (exit %d): %s", result.returncode, result.stderr[-500:])
        except subprocess.TimeoutExpired:
            logger.error("Retry timeout (2 saat)")
        except Exception as e:
            logger.error("Retry basarisiz: %s", e)

        # Retry sonrasi raporu tekrar olustur
        report2 = _build_status_report(collect_ok, analyze_ok, session_id, targets)
        _save_status_report(report2)

        if report2["eksik_raporlar"]:
            logger.warning("Retry sonrasi hala %d eksik rapor var.", len(report2["eksik_raporlar"]))
            eksik_detay = "\n".join(
                f"  {e['hesap_key']}/{e['marketplace']}: {e['rapor']}"
                for e in report2["eksik_raporlar"]
            )
            _send_email(
                f"[PPC Pipeline] UYARI: {len(report2['eksik_raporlar'])} eksik rapor — {PIPELINE_DATE}",
                f"Retry sonrasi hala eksik raporlar var:\n\n{eksik_detay}\n\n"
                f"Session: {session_id}\n"
                f"Manuel kontrol gerekebilir."
            )
        else:
            logger.info("Retry sonrasi tum raporlar tamamlandi!")
            _send_email(
                f"[PPC Pipeline] TEMIZ (retry sonrasi) — {PIPELINE_DATE}",
                f"Eksik raporlar retry ile tamamlandi.\n"
                f"Session: {session_id}\n\n"
                f"Dashboard'dan Agent 3 onayi bekleniyor."
            )

    elif report["genel_durum"] == "KISMI":
        logger.warning("Pipeline kismi tamamlandi.")
        _send_email(
            f"[PPC Pipeline] KISMI — {PIPELINE_DATE}",
            f"Pipeline kismi tamamlandi.\n"
            f"Agent 1: {report['agent1']['status']}\n"
            f"Agent 2: {report['agent2']['status']}\n"
            f"Session: {session_id}\n\n"
            f"Manuel kontrol gerekebilir."
        )

    _save_log("info", f"Pipeline runner tamamlandi: {report['genel_durum']}", "pipeline_runner",
              session_id=session_id)

    logger.info("\n" + "=" * 60)
    logger.info("  PIPELINE RUNNER TAMAMLANDI — %s", report["genel_durum"])
    logger.info("  Session: %s", session_id)
    logger.info("=" * 60)


# ============================================================================
# CLI
# ============================================================================

if __name__ == "__main__":
    force_mode = "--force" in sys.argv
    if force_mode:
        sys.argv.remove("--force")
    targets = sys.argv[1:] if len(sys.argv) > 1 else None
    run(targets, force=force_mode)
