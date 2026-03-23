"""
Paralel Analizci — Tum Hesaplar
========================================
Birden fazla marketplace icin Agent 2 (analyst.py) paralel calistirir.
Tek komut, tek kompakt ozet.

Kullanim:
  python parallel_analyzer.py                                        -> tum hesaplar
  python parallel_analyzer.py vigowood_eu                            -> tek hesap
  python parallel_analyzer.py vigowood_na:US vigowood_eu:DE          -> belirli marketplace'ler

Cikti: Her marketplace icin tek satirlik ozet + toplam ozet tablo.
"""

import sys
import json
import time
import subprocess
import logging
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("parallel_analyzer")

BASE_DIR = Path(__file__).parent


def load_accounts():
    with open(BASE_DIR / "config" / "accounts.json", "r", encoding="utf-8") as f:
        return json.load(f)


def parse_targets(args):
    """CLI argumanlari parse eder. parallel_collector.py ile ayni mantik."""
    accounts = load_accounts()
    target_map = {}

    if args:
        for t in args:
            if ":" in t:
                hk, mp = t.split(":", 1)
                target_map.setdefault(hk, []).append(mp)
            else:
                hesap = accounts["hesaplar"].get(t, {})
                aktif = [mp for mp, cfg in hesap.get("marketplaces", {}).items() if cfg.get("aktif")]
                if aktif:
                    target_map[t] = aktif
    else:
        for hk, hesap in accounts["hesaplar"].items():
            aktif = [mp for mp, cfg in hesap.get("marketplaces", {}).items() if cfg.get("aktif")]
            if aktif:
                target_map[hk] = aktif

    return target_map


def run_analyst(hesap_key, marketplace):
    """Tek bir marketplace icin agent2/analyst.py calistirir. Subprocess olarak."""
    try:
        result = subprocess.run(
            [sys.executable, str(BASE_DIR / "agent2" / "analyst.py"), hesap_key, marketplace],
            capture_output=True, text=True, timeout=600, cwd=str(BASE_DIR)
        )

        # analyst.py sonunda pretty-printed JSON ozet basar
        # Son } satirini bul, oradan geriye { satirina kadar al
        stdout = result.stdout.strip()
        json_output = None
        end_idx = stdout.rfind("\n}")
        if end_idx != -1:
            # } satirindan geriye dogru { ile baslayan satiri bul
            search_area = stdout[:end_idx + 2]
            start_idx = search_area.rfind("\n{")
            if start_idx == -1 and search_area.startswith("{"):
                start_idx = -1  # dosyanin basi
            if start_idx != -1:
                json_str = search_area[start_idx + 1:]
            else:
                json_str = search_area
            try:
                json_output = json.loads(json_str)
            except json.JSONDecodeError:
                pass

        if result.returncode != 0:
            error_msg = result.stderr[-500:] if result.stderr else "Unknown error"
            return {
                "hesap_key": hesap_key,
                "marketplace": marketplace,
                "durum": "HATA",
                "hata": error_msg,
            }

        if json_output and json_output.get("durum") == "TAMAMLANDI":
            return {
                "hesap_key": hesap_key,
                "marketplace": marketplace,
                "durum": "TAMAMLANDI",
                "toplam_hedefleme": json_output.get("toplam_hedefleme", 0),
                "bid_tavsiye": json_output.get("bid_tavsiye_sayisi", 0),
                "negatif": json_output.get("negatif_aday_sayisi", 0),
                "harvesting": json_output.get("harvesting_aday_sayisi", 0),
                "para_birimi": json_output.get("para_birimi", ""),
            }
        else:
            return {
                "hesap_key": hesap_key,
                "marketplace": marketplace,
                "durum": "HATA",
                "hata": "JSON ozet parse edilemedi",
            }

    except subprocess.TimeoutExpired:
        return {
            "hesap_key": hesap_key,
            "marketplace": marketplace,
            "durum": "HATA",
            "hata": "Timeout (600s)",
        }
    except Exception as e:
        return {
            "hesap_key": hesap_key,
            "marketplace": marketplace,
            "durum": "HATA",
            "hata": str(e)[:200],
        }


def main():
    targets = sys.argv[1:] if len(sys.argv) > 1 else None
    target_map = parse_targets(targets)

    if not target_map:
        logger.info("Calistirilacak hesap yok.")
        return

    # Tum marketplace listesi
    jobs = []
    for hk, mp_list in target_map.items():
        for mp in mp_list:
            jobs.append((hk, mp))

    total = len(jobs)
    logger.info("=" * 60)
    logger.info("  PARALEL ANALIZCI (Agent 2)")
    logger.info("=" * 60)
    logger.info("  %d marketplace analiz edilecek:", total)
    for hk, mp_list in target_map.items():
        logger.info("    %s: %s", hk, ", ".join(mp_list))
    logger.info("=" * 60)

    start_time = time.time()

    # Paralel calistir — max 4 process (CPU bound degil, IO bound)
    results = []
    max_workers = min(4, total)

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(run_analyst, hk, mp): (hk, mp) for hk, mp in jobs}
        for future in as_completed(futures):
            hk, mp = futures[future]
            try:
                result = future.result()
                results.append(result)
                status = result["durum"]
                if status == "TAMAMLANDI":
                    logger.info("  [OK] %s/%s — %d hedef, %d bid",
                                hk, mp, result["toplam_hedefleme"], result["bid_tavsiye"])
                else:
                    logger.warning("  [HATA] %s/%s — %s", hk, mp, result.get("hata", "?"))
            except Exception as e:
                logger.error("  [EXCEPTION] %s/%s — %s", hk, mp, e)
                results.append({
                    "hesap_key": hk, "marketplace": mp,
                    "durum": "HATA", "hata": str(e)[:200]
                })

    elapsed = time.time() - start_time
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)

    # Kompakt ozet
    basarili = [r for r in results if r["durum"] == "TAMAMLANDI"]
    hatali = [r for r in results if r["durum"] == "HATA"]

    logger.info("")
    logger.info("=" * 60)
    logger.info("  TAMAMLANDI — %d dk %d sn", minutes, seconds)
    logger.info("=" * 60)
    for r in sorted(results, key=lambda x: f"{x['hesap_key']}/{x['marketplace']}"):
        if r["durum"] == "TAMAMLANDI":
            logger.info("  %s/%s: %d hedef, %d bid, %d neg, %d harv [OK]",
                         r["hesap_key"], r["marketplace"],
                         r["toplam_hedefleme"], r["bid_tavsiye"],
                         r["negatif"], r["harvesting"])
        else:
            logger.info("  %s/%s: %s [HATA]", r["hesap_key"], r["marketplace"], r.get("hata", "?")[:80])

    logger.info("  Toplam: %d basarili, %d hata", len(basarili), len(hatali))
    logger.info("=" * 60)

    # JSON ozet (LLM icin kompakt)
    print(json.dumps({
        "durum": "TAMAMLANDI" if not hatali else "KISMI",
        "sure": f"{minutes}dk {seconds}sn",
        "basarili": len(basarili),
        "hatali": len(hatali),
        "marketplace_ozet": [
            {
                "mp": f"{r['hesap_key']}/{r['marketplace']}",
                "hedef": r.get("toplam_hedefleme", 0),
                "bid": r.get("bid_tavsiye", 0),
                "neg": r.get("negatif", 0),
                "harv": r.get("harvesting", 0),
            }
            for r in sorted(basarili, key=lambda x: f"{x['hesap_key']}/{x['marketplace']}")
        ],
        "hatalar": [
            f"{r['hesap_key']}/{r['marketplace']}: {r.get('hata', '?')[:80]}"
            for r in hatali
        ],
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
