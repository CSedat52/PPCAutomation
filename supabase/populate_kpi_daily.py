"""
kpi_daily tablosunu targeting_reports verisinden doldurur.
Her hesap_key / marketplace / collection_date kombinasyonu icin
cost, sales, clicks, impressions, purchases toplar ve
acos, roas, ctr, cvr hesaplar.

Kullanim:
    python -m supabase.populate_kpi_daily
    # veya
    python supabase/populate_kpi_daily.py
"""
import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv
import psycopg2

load_dotenv(Path(__file__).parent.parent / ".env")

DB_URL = os.getenv("SUPABASE_DB_URL")
if not DB_URL:
    print("HATA: SUPABASE_DB_URL .env'de tanimli olmali")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)


def main():
    logger.info("Supabase'e baglaniliyor...")
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = True
    cur = conn.cursor()

    # targeting_reports'tan gunluk aggregate olustur ve kpi_daily'ye upsert et
    sql = """
        INSERT INTO kpi_daily (
            hesap_key, marketplace, report_date,
            spend, sales, clicks, impressions, orders,
            acos, roas, ctr, cvr, campaign_count, updated_at
        )
        SELECT
            hesap_key,
            marketplace,
            collection_date AS report_date,
            COALESCE(SUM(cost), 0) AS spend,
            COALESCE(SUM(sales), 0) AS sales,
            COALESCE(SUM(clicks), 0) AS clicks,
            COALESCE(SUM(impressions), 0) AS impressions,
            COALESCE(SUM(purchases), 0) AS orders,
            CASE WHEN SUM(sales) > 0
                 THEN ROUND((SUM(cost) / SUM(sales)) * 100, 2)
                 ELSE 0
            END AS acos,
            CASE WHEN SUM(cost) > 0
                 THEN ROUND(SUM(sales) / SUM(cost), 2)
                 ELSE 0
            END AS roas,
            CASE WHEN SUM(impressions) > 0
                 THEN ROUND((SUM(clicks)::numeric / SUM(impressions)) * 100, 4)
                 ELSE 0
            END AS ctr,
            CASE WHEN SUM(clicks) > 0
                 THEN ROUND((SUM(purchases)::numeric / SUM(clicks)) * 100, 4)
                 ELSE 0
            END AS cvr,
            COUNT(DISTINCT campaign_id) AS campaign_count,
            NOW()
        FROM targeting_reports
        GROUP BY hesap_key, marketplace, collection_date
        ON CONFLICT (hesap_key, marketplace, report_date)
        DO UPDATE SET
            spend = EXCLUDED.spend,
            sales = EXCLUDED.sales,
            clicks = EXCLUDED.clicks,
            impressions = EXCLUDED.impressions,
            orders = EXCLUDED.orders,
            acos = EXCLUDED.acos,
            roas = EXCLUDED.roas,
            ctr = EXCLUDED.ctr,
            cvr = EXCLUDED.cvr,
            campaign_count = EXCLUDED.campaign_count,
            updated_at = NOW();
    """

    logger.info("targeting_reports'tan kpi_daily'ye aggregate ediliyor...")
    cur.execute(sql)
    count = cur.rowcount
    logger.info("kpi_daily guncellendi: %d satir upsert edildi", count)

    # Dogrulama
    cur.execute("SELECT COUNT(*) FROM kpi_daily")
    total = cur.fetchone()[0]
    logger.info("kpi_daily toplam satir: %d", total)

    cur.execute("""
        SELECT hesap_key, marketplace, COUNT(*), MIN(report_date), MAX(report_date)
        FROM kpi_daily
        GROUP BY hesap_key, marketplace
        ORDER BY hesap_key, marketplace
    """)
    rows = cur.fetchall()
    for r in rows:
        logger.info("  %s / %s: %d gun (%s — %s)", r[0], r[1], r[2], r[3], r[4])

    cur.close()
    conn.close()
    logger.info("Tamamlandi.")


if __name__ == "__main__":
    main()
