"""
Agent 4 — Optimizer & Learning Agent (v4 Multi-Account)
=========================================================
Yeni akis:
  [1/4] KPICollector (Supabase only)
  [2/4] ErrorAnalyzer (Supabase only)
  [3/4] MaestroAnalyzer (Supabase only)
  [4/4] BidParamAnalyzer (Supabase only)
  → agent4_analysis.json ciktisi (Claude Code icin)

DBManager + SegmentAnalyzer kaldirildi — decision_history dogrudan kullaniliyor.

Kullanim:
  python agent4/optimizer.py <hesap_key> <marketplace>
  python agent4/optimizer.py vigowood_na US
  python agent4/optimizer.py vigowood_na US oneri listele
  python agent4/optimizer.py vigowood_na US durum

Versiyon: 3.0
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
from agent4.kpi_collector   import KPICollector
from agent4.analyzers.error_analyzer    import ErrorAnalyzer
from agent4.analyzers.maestro_analyzer  import MaestroAnalyzer
from agent4.bid_param_analyzer import BidParamAnalyzer
from agent4.proposal_engine import ProposalEngine
from agent4.report_generator import ReportGenerator
from log_utils import save_error_log as _central_save_error_log

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

def save_error_log(hata_tipi, hata_mesaji, hesap_key="", marketplace="",
                   traceback_str=None, adim=None, extra=None, session_id=None):
    """Agent 4 hata logu — Supabase only."""
    return _central_save_error_log(
        hata_tipi, hata_mesaji,
        traceback_str=traceback_str, adim=adim, extra=extra,
        session_id=session_id, agent_name="agent4",
        hesap_key=hesap_key, marketplace=marketplace)


# ============================================================================
# ANA FONKSIYON
# ============================================================================

def run_optimizer(hesap_key, marketplace):
    today = datetime.utcnow().strftime("%Y-%m-%d")
    data_dir, config_dir = get_dirs(hesap_key, marketplace)
    logger.info("=" * 60)
    logger.info("AGENT 4 OPTIMIZER v3 — %s/%s — %s", hesap_key, marketplace, today)
    logger.info("=" * 60)

    try:
        return _run_optimizer_impl(today, data_dir, config_dir, hesap_key, marketplace)
    except Exception as e:
        tb = traceback.format_exc()
        logger.error("BEKLENMEYEN HATA: %s", e)
        save_error_log(type(e).__name__, str(e), hesap_key, marketplace,
                       tb, adim="run_optimizer",
                       extra={"hesap": f"{hesap_key}/{marketplace}"})
        return {
            "durum": "BASARISIZ",
            "tarih": today,
            "hesap": f"{hesap_key}/{marketplace}",
            "hata": str(e),
        }


def _run_optimizer_impl(today, data_dir, config_dir, hesap_key, marketplace):
    account_label = f"{hesap_key}/{marketplace}"

    # ---- 1. KPI GUNCELLEME ----
    logger.info("--- [1/4] KPI guncelleniyor ---")
    kpi = KPICollector(hesap_key, marketplace)
    kpi_ozet = kpi.run(today)
    logger.info("KPI: %d kpi_after dolduruldu",
                kpi_ozet.get("kpi_after_doldurulan", 0))

    # ---- 2. HATA ANALIZI ----
    logger.info("--- [2/4] Hata analizi ---")
    err_analyzer = ErrorAnalyzer(hesap_key, marketplace)
    err_sonuc = err_analyzer.analyze()

    # ---- 3. MAESTRO ANALIZI ----
    logger.info("--- [3/4] Maestro mudahale analizi ---")
    maestro_analyzer = MaestroAnalyzer(hesap_key, marketplace)
    maestro_sonuc = maestro_analyzer.analyze()

    # ---- 4. BID PARAMETRE ANALIZI ----
    logger.info("--- [4/4] Bid parametre analizi ---")
    bid_param_analyzer = BidParamAnalyzer(hesap_key, marketplace)
    bid_param_sonuc = bid_param_analyzer.analyze()

    # ---- RAPOR ----
    generator = ReportGenerator(hesap_key, marketplace, data_dir)
    rapor = generator.generate(today, {
        "kpi":       kpi_ozet,
        "hata":      err_sonuc,
        "maestro":   maestro_sonuc,
        "bid_param": bid_param_sonuc,
        "oneriler":  [],   # Artik Claude Code uretecek
    })

    logger.info("=" * 60)
    logger.info("AGENT 4 TAMAMLANDI — %s", account_label)
    logger.info("analysis_dosyasi: %s", rapor.get("analysis_dosyasi", ""))
    logger.info("Claude Code bu dosyayi okuyarak dinamik analiz yapacak.")
    logger.info("=" * 60)

    return rapor


# ============================================================================
# DOGRUDAN CALISTIRMA
# ============================================================================

if __name__ == "__main__":
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

    # Oneri komutlari
    if len(sub_args) >= 2 and sub_args[0] == "oneri":
        from agent4.proposal_engine import cmd_oneri
        cmd_oneri(hesap_key, marketplace, sub_args[1:])
        sys.exit(0)

    # Durum raporu
    if sub_args and sub_args[0] == "durum":
        from agent4.report_generator import cmd_durum
        cmd_durum(hesap_key, marketplace)
        sys.exit(0)

    # Normal calistirma
    result = run_optimizer(hesap_key, marketplace)
    sys.stdout.reconfigure(encoding='utf-8')
    print(json.dumps(result, indent=2, ensure_ascii=False))
