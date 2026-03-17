"""
Agent 4 — Optimizer & Learning Agent (v2 Multi-Account)
=========================================================
Kullanim:
  python agent4/optimizer.py <hesap_key> <marketplace>
  python agent4/optimizer.py vigowood_na US
  python agent4/optimizer.py vigowood_na US oneri listele
  python agent4/optimizer.py vigowood_na US durum

Versiyon: 2.0
"""

import os
import sys
import json
import logging
import traceback
from datetime import datetime
from pathlib import Path

# Proje kok dizini
BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR))

# Agent 4 modullerini import et
from agent4.db_manager      import DBManager
from agent4.kpi_collector   import KPICollector
from agent4.analyzers.segment_analyzer  import SegmentAnalyzer
from agent4.analyzers.error_analyzer    import ErrorAnalyzer
from agent4.analyzers.maestro_analyzer  import MaestroAnalyzer
from agent4.analyzers.pattern_detector  import PatternDetector
from agent4.analyzers.anomaly_detector  import AnomalyDetector
from agent4.proposal_engine import ProposalEngine
from agent4.report_generator import ReportGenerator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("agent4_optimizer")


def get_dirs(hesap_key, marketplace):
    """Hesap+marketplace icin data_dir ve config_dir doner."""
    dir_name = f"{hesap_key}_{marketplace}"
    data_dir = BASE_DIR / "data" / dir_name
    config_dir = BASE_DIR / "config" / dir_name
    return data_dir, config_dir


# ============================================================================
# HATA LOG
# ============================================================================

def save_error_log(hata_tipi, hata_mesaji, data_dir, traceback_str=None, adim=None,
                   extra=None, session_id=None):
    """
    Agent 4 hatalarini data/logs/agent4_errors.json dosyasina ekler.

    Parametreler:
        hata_tipi    : Ortak taksonomi (InternalError, DataError, FileNotFound, vb.)
        hata_mesaji  : Hata aciklamasi (max 500 char)
        data_dir     : Hesap bazli data klasoru (Path)
        traceback_str: traceback.format_exc() ciktisi (opsiyonel)
        adim         : Hatanin gerceklestigi adim
        extra        : Ek baglam dict (orn. {"modul": "kpi_collector", "detay": "..."})
        session_id   : Pipeline session ID'si (korelasyon icin)
    """
    log_dir = data_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "agent4_errors.json"

    try:
        with open(log_path, "r", encoding="utf-8") as f:
            kayitlar = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        kayitlar = []

    kayit = {
        "timestamp": datetime.utcnow().isoformat(),
        "hata_tipi": hata_tipi,
        "hata_mesaji": str(hata_mesaji)[:500],
        "adim": adim or "bilinmiyor",
    }
    if traceback_str:
        kayit["traceback"] = str(traceback_str)[:1000]
    if extra:
        kayit["extra"] = extra
    if session_id:
        kayit["session_id"] = session_id

    kayitlar.append(kayit)
    if len(kayitlar) > 200:
        kayitlar = kayitlar[-200:]

    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(kayitlar, f, indent=2, ensure_ascii=False)


# ============================================================================
# ANA FONKSIYON
# ============================================================================

def run_optimizer(hesap_key, marketplace):
    today = datetime.utcnow().strftime("%Y-%m-%d")
    data_dir, config_dir = get_dirs(hesap_key, marketplace)
    logger.info("=" * 60)
    logger.info("AGENT 4 OPTIMIZER v2 — %s/%s — %s", hesap_key, marketplace, today)
    logger.info("=" * 60)

    try:
        return _run_optimizer_impl(today, data_dir, config_dir, hesap_key, marketplace)
    except Exception as e:
        tb = traceback.format_exc()
        logger.error("BEKLENMEYEN HATA: %s", e)
        save_error_log(type(e).__name__, str(e), data_dir, tb, adim="run_optimizer",
                       extra={"hesap": f"{hesap_key}/{marketplace}"})
        return {
            "durum": "BASARISIZ",
            "tarih": today,
            "hesap": f"{hesap_key}/{marketplace}",
            "hata": str(e),
        }


def _run_optimizer_impl(today, data_dir, config_dir, hesap_key, marketplace):
    account_label = f"{hesap_key}/{marketplace}"

    # ---- 1. VERITABANI ----
    logger.info("--- [1/7] Veritabani yukleniyor ---")
    db = DBManager(data_dir)
    db.load()

    # ---- 2. KPI GUNCELLEME ----
    logger.info("--- [2/7] KPI guncelleniyor ---")
    kpi = KPICollector(data_dir, db)
    kpi_ozet = kpi.run(today)
    logger.info("KPI: %d yeni karar islendi, %d kpi_after dolduruldu",
                kpi_ozet.get("yeni_karar", 0), kpi_ozet.get("kpi_after_doldurulan", 0))

    # ---- 3. ANALIZLER ----
    logger.info("--- [3/7] Segment analizi ---")
    seg_analyzer = SegmentAnalyzer(db)
    seg_sonuc = seg_analyzer.analyze()

    logger.info("--- [4/7] Hata analizi ---")
    err_analyzer = ErrorAnalyzer(data_dir, db)
    err_sonuc = err_analyzer.analyze()

    logger.info("--- [5/7] Maestro mudahale analizi ---")
    maestro_analyzer = MaestroAnalyzer(BASE_DIR, db)
    maestro_sonuc = maestro_analyzer.analyze()

    logger.info("--- [6/7] Kalip tespiti ve anomali ---")
    pattern_detector = PatternDetector(db)
    pattern_sonuc = pattern_detector.detect()

    anomaly_detector = AnomalyDetector(db)
    anomaly_sonuc = anomaly_detector.detect()

    # ---- 4. ONERI MOTORU ----
    logger.info("--- [7/7] Oneri motoru ---")
    engine = ProposalEngine(data_dir, config_dir, db, {
        "segment": seg_sonuc,
        "hata":    err_sonuc,
        "maestro": maestro_sonuc,
        "kalip":   pattern_sonuc,
        "anomali": anomaly_sonuc,
    })
    oneriler = engine.run(today)
    logger.info("Oneri motoru: %d yeni oneri uretildi", len(oneriler))

    # ---- 5. RAPOR ----
    generator = ReportGenerator(data_dir, db)
    rapor = generator.generate(today, {
        "kpi":     kpi_ozet,
        "segment": seg_sonuc,
        "hata":    err_sonuc,
        "maestro": maestro_sonuc,
        "kalip":   pattern_sonuc,
        "anomali": anomaly_sonuc,
        "oneriler": oneriler,
    })

    # ---- 6. VERITABANI KAYDET ----
    db.save()

    # ---- 7. SUPABASE SYNC ----
    _sync_agent4_to_supabase(hesap_key, marketplace, today, db, rapor, oneriler)

    logger.info("=" * 60)
    logger.info("AGENT 4 TAMAMLANDI — %s — Rapor: %s", account_label, rapor.get("rapor_dosyasi", ""))
    logger.info("Bekleyen oneri sayisi: %d", len(oneriler))
    logger.info("=" * 60)

    return rapor


def _sync_agent4_to_supabase(hesap_key, marketplace, today, db, rapor, oneriler):
    """Agent 4 verilerini Supabase'e senkronize et."""
    try:
        import sys as _sys
        _sys.path.insert(0, str(BASE_DIR))
        from supabase.db_client import SupabaseClient
        sdb = SupabaseClient()
    except Exception as e:
        logger.warning("Supabase sync atlandi: %s", e)
        return

    try:
        # Karar gecmisi
        kararlar = db._data.get("karar_gecmisi", {}).get("kararlar", [])
        if kararlar:
            sdb.upsert_decision_history(hesap_key, marketplace, kararlar)

        # ASIN profilleri
        profiller = db._data.get("asin_profilleri", {}).get("profiller", {})
        if profiller:
            sdb.upsert_asin_profiles(hesap_key, marketplace, profiller)

        # Segment istatistikleri
        segmentler = db._data.get("segment_istatistikleri", {}).get("segmentler", {})
        if segmentler:
            sdb.upsert_segment_stats(hesap_key, marketplace, segmentler)

        # Anomaliler
        for anomali in db._data.get("anomali_gecmisi", {}).get("anomaliler", []):
            sdb.insert_anomaly(hesap_key, marketplace, anomali)

        # Kaliplar
        for kalip in db._data.get("kalip_kutuphanesi", {}).get("kaliplar", []):
            sdb.insert_pattern(hesap_key, marketplace, kalip)

        # Oneriler
        for oneri in oneriler:
            sdb.upsert_proposal(hesap_key, marketplace, oneri)

        # Durum raporu
        rapor_data = rapor.get("rapor_data", rapor)
        sdb.insert_status_report(hesap_key, marketplace, rapor_data)

        logger.info("Supabase: Agent 4 verileri yazildi (%d karar, %d oneri)",
                     len(kararlar), len(oneriler))

    except Exception as e:
        logger.error("Supabase sync hatasi (optimizer devam eder): %s", e)
        save_error_log("InternalError", f"Supabase sync: {e}", data_dir,
                       traceback.format_exc(), adim="supabase_sync",
                       extra={"hesap": f"{hesap_key}/{marketplace}"})
        try:
            sdb.insert_error_log(hesap_key, marketplace, "agent4", {
                "hata_tipi": "InternalError",
                "hata_mesaji": f"Supabase sync hatasi: {e}"[:500],
                "adim": "supabase_sync",
            })
        except Exception:
            pass


# ============================================================================
# DOGRUDAN CALISTIRMA
# ============================================================================

if __name__ == "__main__":
    import sys

    args = sys.argv[1:]

    if len(args) < 2:
        print("Kullanim: python agent4/optimizer.py <hesap_key> <marketplace> [komut]")
        print("Ornek:    python agent4/optimizer.py vigowood_na US")
        print("Ornek:    python agent4/optimizer.py vigowood_na US oneri listele")
        print("Ornek:    python agent4/optimizer.py vigowood_na US durum")
        sys.exit(1)

    hesap_key = args[0]
    marketplace = args[1]
    sub_args = args[2:]
    data_dir, config_dir = get_dirs(hesap_key, marketplace)

    # Oneri komutlari
    if len(sub_args) >= 2 and sub_args[0] == "oneri":
        from agent4.proposal_engine import cmd_oneri
        cmd_oneri(data_dir, config_dir, sub_args[1:])
        sys.exit(0)

    # Durum raporu
    if sub_args and sub_args[0] == "durum":
        from agent4.report_generator import cmd_durum
        cmd_durum(data_dir)
        sys.exit(0)

    # Normal calistirma
    result = run_optimizer(hesap_key, marketplace)
    sys.stdout.reconfigure(encoding='utf-8')
    print(json.dumps(result, indent=2, ensure_ascii=False))
