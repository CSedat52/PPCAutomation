"""
populate_kpi_daily.py — Campaign raporlarindan kpi_daily tablosunu doldurur
=============================================================================
Agent 1 veri topladiktan sonra calistirilir.
data/{hesap_key}_{marketplace}/ altindaki campaign report + entity dosyalarini
okur, campaign_type siniflandirmasi yapar ve kpi_daily'ye upsert eder.

Campaign tipi tespiti tamamen entity verilerinden yapilir (isim bazli degil):
  - sp_campaigns.targetingType = AUTO → SP-Auto
  - sp_keywords.matchType cogunlugu → SP-Broad / SP-Exact / SP-Phrase
  - sp_targets.expression.type → SP-ASIN / SP-Category
  - SB kampanyalar → SB-Other
  - SD kampanyalar → SD-Other

Kullanim:
    python -m supabase.populate_kpi_daily vigowood_eu UK
    python -m supabase.populate_kpi_daily vigowood_eu UK --date 2026-03-18
    python -m supabase.populate_kpi_daily --all
    # veya
    python supabase/populate_kpi_daily.py vigowood_eu UK
"""
import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime
from collections import Counter
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

_project_root = Path(__file__).parent.parent
load_dotenv(_project_root / ".env")

DB_URL = os.getenv("SUPABASE_DB_URL")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("kpi_sync")


# ============================================================================
# YARDIMCI
# ============================================================================

def _get_conn():
    if not DB_URL:
        raise RuntimeError("SUPABASE_DB_URL .env'de tanimli degil")
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = True
    return conn


def load_json(filepath: Path) -> list:
    """JSON dosyasini yukle. Yoksa veya bossa bos liste don."""
    if not filepath.exists():
        return []
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except (json.JSONDecodeError, Exception) as e:
        logger.warning("JSON okuma hatasi %s: %s", filepath.name, e)
        return []


def safe_float(val):
    try:
        return float(val) if val is not None else 0.0
    except (ValueError, TypeError):
        return 0.0


def safe_int(val):
    try:
        return int(val) if val is not None else 0
    except (ValueError, TypeError):
        return 0


# ============================================================================
# KAMPANYA TIPI TESPITI (entity verilerinden — kesin siniflandirma)
# ============================================================================

# Amazon target expression tipleri
ASIN_TARGET_TYPES = {"ASIN_SAME_AS", "ASIN_EXPANDED_FROM"}
CATEGORY_TARGET_TYPES = {"ASIN_CATEGORY_SAME_AS"}
AUTO_TARGET_TYPES = {
    "QUERY_HIGH_REL_MATCHES", "QUERY_BROAD_REL_MATCHES",
    "ASIN_ACCESSORY_RELATED", "ASIN_SUBSTITUTE_RELATED",
}


def build_campaign_type_map(data_dir: Path, date_str: str) -> dict:
    """
    Entity dosyalarindan campaignId → campaign_type haritasi olusturur.

    Mantik:
      1. sp_campaigns.targetingType = AUTO → SP-Auto
      2. MANUAL + keyword var → cogunluk matchType (BROAD/EXACT/PHRASE)
      3. MANUAL + ASIN target var → SP-ASIN
      4. MANUAL + Category target var → SP-Category
      5. Karisik veya bos → SP-Other
    """
    # 1. sp_campaigns → targetingType
    campaigns = load_json(data_dir / f"{date_str}_sp_campaigns.json")
    camp_targeting = {}
    for c in campaigns:
        cid = str(c.get("campaignId", ""))
        if cid:
            camp_targeting[cid] = c.get("targetingType", "MANUAL")

    # 2. sp_keywords → kampanya bazli matchType sayimi
    keywords = load_json(data_dir / f"{date_str}_sp_keywords.json")
    camp_kw_counts = {}
    for k in keywords:
        cid = str(k.get("campaignId", ""))
        mt = k.get("matchType", "")
        if cid and mt:
            camp_kw_counts.setdefault(cid, Counter())[mt] += 1

    # 3. sp_targets → kampanya bazli target expression tipi
    targets = load_json(data_dir / f"{date_str}_sp_targets.json")
    camp_target_types = {}
    for t in targets:
        cid = str(t.get("campaignId", ""))
        exprs = t.get("expression", []) or []
        for e in exprs:
            if not isinstance(e, dict):
                continue
            etype = e.get("type", "")
            if etype in ASIN_TARGET_TYPES:
                camp_target_types.setdefault(cid, set()).add("ASIN")
            elif etype in CATEGORY_TARGET_TYPES:
                camp_target_types.setdefault(cid, set()).add("CATEGORY")
            elif etype in AUTO_TARGET_TYPES:
                camp_target_types.setdefault(cid, set()).add("AUTO_TARGET")

    # 4. Sinifla
    result = {}
    for cid, tt in camp_targeting.items():
        if tt == "AUTO":
            result[cid] = "SP-Auto"
            continue

        kw_counts = camp_kw_counts.get(cid)
        tgt_types = camp_target_types.get(cid, set())
        has_kw = kw_counts is not None and sum(kw_counts.values()) > 0
        has_asin = "ASIN" in tgt_types
        has_cat = "CATEGORY" in tgt_types
        has_auto_tgt = "AUTO_TARGET" in tgt_types

        if has_kw and not has_asin and not has_cat:
            result[cid] = f"SP-{kw_counts.most_common(1)[0][0].capitalize()}"
        elif has_asin and not has_kw and not has_cat:
            result[cid] = "SP-ASIN"
        elif has_cat and not has_kw and not has_asin:
            result[cid] = "SP-Category"
        elif has_asin and has_cat:
            result[cid] = "SP-ASIN"
        elif has_kw:
            result[cid] = f"SP-{kw_counts.most_common(1)[0][0].capitalize()}"
        elif has_auto_tgt:
            result[cid] = "SP-Auto"
        else:
            result[cid] = "SP-Other"

    # Log
    type_counts = {}
    for ct in result.values():
        type_counts[ct] = type_counts.get(ct, 0) + 1
    logger.info("  Kampanya tipi haritasi: %d SP kampanya → %s",
                len(result), ", ".join(f"{t}:{c}" for t, c in sorted(type_counts.items())))

    return result


# ============================================================================
# PORTFOLIO ESLESTIRME
# ============================================================================

def build_portfolio_map(data_dir: Path, date_str: str) -> dict:
    """
    campaignId → (portfolio_id, portfolio_name) eslestirme haritasi.
    sp_campaigns.portfolioId + portfolios entity'sinden.
    """
    campaigns = load_json(data_dir / f"{date_str}_sp_campaigns.json")
    portfolios_list = load_json(data_dir / f"{date_str}_portfolios.json")

    # portfolioId → name
    pf_names = {}
    for p in portfolios_list:
        pid = str(p.get("portfolioId", ""))
        pname = p.get("name", "")
        if pid and pname:
            pf_names[pid] = pname

    # campaignId → (portfolio_id, portfolio_name)
    result = {}
    for c in campaigns:
        cid = str(c.get("campaignId", ""))
        pid = str(c.get("portfolioId", ""))
        if cid and pid:
            result[cid] = (pid, pf_names.get(pid))

    # SD campaigns icin de ayni islemi yap
    sd_campaigns = load_json(data_dir / f"{date_str}_sd_campaigns.json")
    for c in sd_campaigns:
        cid = str(c.get("campaignId", ""))
        pid = str(c.get("portfolioId", ""))
        if cid and pid:
            result[cid] = (pid, pf_names.get(pid))

    # SB campaigns
    sb_campaigns = load_json(data_dir / f"{date_str}_sb_campaigns.json")
    for c in sb_campaigns:
        cid = str(c.get("campaignId", ""))
        pid = str(c.get("portfolioId", ""))
        if cid and pid:
            result[cid] = (pid, pf_names.get(pid))

    has_pf = sum(1 for _, (pid, pn) in result.items() if pn)
    logger.info("  Portfolio haritasi: %d kampanya mapped, %d portfolio isimli",
                len(result), has_pf)
    return result


# ============================================================================
# ANA ISLEM: Dosyalardan oku → sinifla → aggregate → upsert
# ============================================================================

def process_and_upsert(hesap_key: str, marketplace: str, date_str: str = None):
    """
    Tek bir marketplace icin campaign raporlarini isle ve kpi_daily'ye yaz.
    """
    if not date_str:
        date_str = datetime.utcnow().strftime("%Y-%m-%d")

    data_dir = _project_root / "data" / f"{hesap_key}_{marketplace}"
    if not data_dir.exists():
        logger.error("Data klasoru yok: %s", data_dir)
        return 0

    logger.info("=" * 55)
    logger.info("KPI SYNC: %s/%s (tarih: %s)", hesap_key, marketplace, date_str)
    logger.info("=" * 55)

    # Entity'lerden haritalar olustur
    sp_type_map = build_campaign_type_map(data_dir, date_str)
    portfolio_map = build_portfolio_map(data_dir, date_str)

    # Campaign raporlarini oku ve normalize et
    rows = []

    # --- SP ---
    sp_data = load_json(data_dir / f"{date_str}_sp_campaign_report_14d.json")
    for r in sp_data:
        cid = str(r.get("campaignId", ""))
        pid, pname = portfolio_map.get(cid, (None, None))
        rows.append((
            r["date"], hesap_key, marketplace,
            sp_type_map.get(cid, "SP-Other"),
            pid, pname,
            safe_float(r.get("cost")),
            safe_float(r.get("sales14d")),
            safe_int(r.get("clicks")),
            safe_int(r.get("purchases14d")),
            safe_int(r.get("impressions")),
            safe_int(r.get("unitsSoldClicks14d")),
        ))

    # --- SB ---
    sb_data = load_json(data_dir / f"{date_str}_sb_campaign_report_14d.json")
    for r in sb_data:
        cid = str(r.get("campaignId", ""))
        pid, pname = portfolio_map.get(cid, (None, None))
        rows.append((
            r["date"], hesap_key, marketplace,
            "SB-Other",
            pid, pname,
            safe_float(r.get("cost")),
            safe_float(r.get("sales")),
            safe_int(r.get("clicks")),
            safe_int(r.get("purchases") or r.get("purchasesClicks")),
            safe_int(r.get("impressions")),
            safe_int(r.get("unitsSold")),
        ))

    # --- SD ---
    sd_data = load_json(data_dir / f"{date_str}_sd_campaign_report_14d.json")
    for r in sd_data:
        cid = str(r.get("campaignId", ""))
        pid, pname = portfolio_map.get(cid, (None, None))
        rows.append((
            r["date"], hesap_key, marketplace,
            "SD-Other",
            pid, pname,
            safe_float(r.get("cost")),
            safe_float(r.get("sales")),
            safe_int(r.get("clicks")),
            safe_int(r.get("purchases") or r.get("purchasesClicks")),
            safe_int(r.get("impressions")),
            safe_int(r.get("unitsSold")),
        ))

    logger.info("  Ham satirlar: SP=%d, SB=%d, SD=%d → Toplam=%d",
                len(sp_data), len(sb_data), len(sd_data), len(rows))

    if not rows:
        logger.warning("  Islenecek veri yok")
        return 0

    # --- AGGREGATION: gun + hesap + marketplace + campaign_type + portfolio_id ---
    agg = {}
    for (rdate, hk, mp, ctype, pid, pname,
         spend, sales, clicks, orders, impressions, units) in rows:
        key = (rdate, hk, mp, ctype, pid)
        if key not in agg:
            agg[key] = [rdate, hk, mp, ctype, pid, pname,
                        0.0, 0.0, 0, 0, 0, 0]
        a = agg[key]
        a[6] += spend
        a[7] += sales
        a[8] += clicks
        a[9] += orders
        a[10] += impressions
        a[11] += units
        if not a[5] and pname:
            a[5] = pname

    # Yuvarla
    agg_rows = []
    for a in agg.values():
        a[6] = round(a[6], 2)  # spend
        a[7] = round(a[7], 2)  # sales
        agg_rows.append(tuple(a))

    logger.info("  Aggregate sonrasi: %d benzersiz satir", len(agg_rows))

    # --- SUPABASE UPSERT ---
    conn = _get_conn()
    try:
        cur = conn.cursor()

        sql = """
            INSERT INTO kpi_daily (
                report_date, hesap_key, marketplace, campaign_type,
                portfolio_id, portfolio_name,
                spend, sales, clicks, orders, impressions, units_sold,
                updated_at
            )
            VALUES %s
            ON CONFLICT (report_date, hesap_key, marketplace, campaign_type, portfolio_id)
            DO UPDATE SET
                portfolio_name = EXCLUDED.portfolio_name,
                spend = EXCLUDED.spend,
                sales = EXCLUDED.sales,
                clicks = EXCLUDED.clicks,
                orders = EXCLUDED.orders,
                impressions = EXCLUDED.impressions,
                units_sold = EXCLUDED.units_sold,
                updated_at = NOW()
        """

        # Her tuple'a updated_at=NOW() ekle — execute_values bunu SQL'de halleder
        template = "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())"

        execute_values(cur, sql, agg_rows, template=template, page_size=500)
        count = cur.rowcount
        cur.close()

        logger.info("  Supabase upsert: %d satir", count)

        # Dogrulama
        cur2 = conn.cursor()
        cur2.execute("""
            SELECT COUNT(*), MIN(report_date), MAX(report_date)
            FROM kpi_daily
            WHERE hesap_key = %s AND marketplace = %s
        """, (hesap_key, marketplace))
        total, min_date, max_date = cur2.fetchone()
        cur2.close()

        logger.info("  kpi_daily toplam: %d satir (%s → %s)", total, min_date, max_date)

        return count

    except Exception as e:
        logger.error("  Supabase upsert hatasi: %s", e)
        return 0
    finally:
        conn.close()


# ============================================================================
# CLI
# ============================================================================

def main():
    if len(sys.argv) < 2:
        print("Kullanim:")
        print("  python supabase/populate_kpi_daily.py vigowood_eu UK")
        print("  python supabase/populate_kpi_daily.py vigowood_eu UK --date 2026-03-18")
        print("  python supabase/populate_kpi_daily.py --all")
        sys.exit(1)

    date_str = None
    if "--date" in sys.argv:
        idx = sys.argv.index("--date")
        date_str = sys.argv[idx + 1]

    if sys.argv[1] == "--all":
        accounts_path = _project_root / "config" / "accounts.json"
        if not accounts_path.exists():
            print("config/accounts.json bulunamadi")
            sys.exit(1)

        with open(accounts_path) as f:
            accounts = json.load(f)

        total = 0
        for hk, hesap in accounts.get("hesaplar", {}).items():
            for mp, cfg in hesap.get("marketplaces", {}).items():
                if cfg.get("aktif"):
                    count = process_and_upsert(hk, mp, date_str)
                    total += (count or 0)

        logger.info("\nTOPLAM: %d satir upsert edildi", total)

    else:
        hesap_key = sys.argv[1]
        marketplace = sys.argv[2] if len(sys.argv) > 2 and not sys.argv[2].startswith("-") else "US"
        process_and_upsert(hesap_key, marketplace, date_str)


if __name__ == "__main__":
    main()
