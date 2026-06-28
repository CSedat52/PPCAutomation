#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
cleanup_storage.py — Supabase depolama temizligi (VPS uzerinde calistir).

Neden: Sistemde retention HIC calismiyordu; rapor tablolari raw_data JSONB ile
sinirsiz buyuyup free tier 500 MB limitini asti. Bu script eski (artik
okunmayan) satirlari budar ve VACUUM FULL ile diski geri kazanir.

GUVENLIK:
  - Varsayilan mod DRY-RUN'dir (salt-okunur, HICBIR sey silmez).
  - Gercekten silmek/vacuum icin: --apply
  - Pipeline'in okudugu GUNCEL veri korunur; sadece pencereden eski satirlar silinir.

Kullanim:
  python3 cleanup_storage.py                 # DRY-RUN: olcer + ne silinecegini gosterir
  python3 cleanup_storage.py --apply         # uygular (sil + VACUUM FULL)
  python3 cleanup_storage.py --report-days 14 --log-days 60   # pencereleri degistir

Notlar:
  - .env icindeki SUPABASE_DB_URL kullanilir (db_client ile ayni).
  - --apply sirasinda pipeline/watcher CALISMASIN (VACUUM FULL kisa sureli kilit alir).
"""
import argparse
import sys
from pathlib import Path

from dotenv import load_dotenv
import os
import psycopg2
from psycopg2 import sql

load_dotenv(Path(__file__).resolve().parent / ".env")
DB_URL = os.getenv("SUPABASE_DB_URL")

# (tablo, tarih kolonu adaylari, pencere-grubu)
#   report  -> --report-days  (varsayilan 30) : raporlar + cycle ciktilari (yeniden uretilir)
#   log     -> --log-days     (varsayilan 90) : log / exec / analiz gecmisi
#   verify  -> --verify-days  (varsayilan 7)  : verify snapshot'lari
RETENTION = [
    ("targeting_reports",     ["collection_date"],                 "report"),
    ("search_term_reports",   ["collection_date"],                 "report"),
    ("campaign_reports",      ["collection_date"],                 "report"),
    ("bid_recommendations",   ["analysis_date", "created_at"],     "report"),
    ("negative_candidates",   ["analysis_date", "created_at"],     "report"),
    ("harvesting_candidates", ["analysis_date", "created_at"],     "report"),
    ("status_reports",        ["created_at", "report_date"],       "log"),
    ("anomalies",             ["created_at", "detected_at"],       "log"),
    ("patterns",              ["created_at"],                       "log"),
    ("segment_stats",         ["created_at", "analysis_date"],     "log"),
    ("error_logs",            ["timestamp", "created_at"],         "log"),
    ("maestro_errors",        ["timestamp", "created_at"],         "log"),
    ("agent_logs",            ["created_at", "timestamp"],         "log"),
    ("execution_items",       ["created_at"],                      "log"),
    ("execution_plans",       ["created_at"],                      "log"),
    ("pipeline_runs",         ["started_at", "created_at"],        "log"),
    ("verify_snapshots",      ["verify_date", "created_at"],       "verify"),
]

# Bunlar ASLA silinmez (ogrenme/config/guncel-durum): decision_history,
# bid_param_regression(_data), kpi_daily, settings, bid_functions, accounts,
# marketplaces, ve tum entity tablolari (campaigns, keywords, targets, ...).


def fetch(cur, q, params=None):
    cur.execute(q, params or ())
    return cur.fetchall()


def table_exists(cur, t):
    cur.execute("SELECT to_regclass(%s)", (f"public.{t}",))
    return cur.fetchone()[0] is not None


def existing_columns(cur, t):
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema='public' AND table_name=%s", (t,))
    return {r[0] for r in cur.fetchall()}


def pick_date_col(cols_present, candidates):
    for c in candidates:
        if c in cols_present:
            return c
    return None


def human(cur, expr):
    cur.execute(f"SELECT pg_size_pretty(({expr}))")
    return cur.fetchone()[0]


def db_size(cur):
    return human(cur, "pg_database_size(current_database())")


def top_tables(cur, limit=15):
    return fetch(cur, """
        SELECT c.relname,
               pg_size_pretty(pg_total_relation_size(c.oid)),
               COALESCE(s.n_live_tup,0), COALESCE(s.n_dead_tup,0)
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        LEFT JOIN pg_stat_user_tables s ON s.relid=c.oid
        WHERE n.nspname='public' AND c.relkind='r'
        ORDER BY pg_total_relation_size(c.oid) DESC LIMIT %s
    """, (limit,))


def main():
    ap = argparse.ArgumentParser(description="Supabase depolama temizligi")
    ap.add_argument("--apply", action="store_true",
                    help="Gercekten sil + VACUUM (varsayilan: dry-run)")
    ap.add_argument("--report-days", type=int, default=14)
    ap.add_argument("--log-days", type=int, default=90)
    ap.add_argument("--verify-days", type=int, default=7)
    ap.add_argument("--no-vacuum", action="store_true",
                    help="--apply'da VACUUM FULL calistirma")
    args = ap.parse_args()

    if not DB_URL:
        print("HATA: SUPABASE_DB_URL .env'de yok"); sys.exit(1)

    days_for = {"report": args.report_days, "log": args.log_days,
                "verify": args.verify_days}

    mode = "APPLY (silme + vacuum)" if args.apply else "DRY-RUN (salt-okunur)"
    print("=" * 64)
    print(f"  Supabase Depolama Temizligi — MOD: {mode}")
    print(f"  Pencereler: report={args.report_days}g  log={args.log_days}g  "
          f"verify={args.verify_days}g")
    print("=" * 64)

    conn = psycopg2.connect(DB_URL, connect_timeout=20)
    conn.autocommit = True
    cur = conn.cursor()
    # NOT: Supabase pooler baglanti-seviyesi 'options'i yok sayiyor.
    # Session mode'da runtime SET kalici olur ve backend'e iletilir.
    try:
        cur.execute("SET statement_timeout = 0")               # sinirsiz (VACUUM FULL icin)
        cur.execute("SET idle_in_transaction_session_timeout = 0")
    except Exception:
        try:
            cur.execute("SET statement_timeout = '3600000'")   # 60 dk (fallback)
        except Exception as e:
            print(f"  (uyari: statement_timeout ayarlanamadi: {repr(e)[:100]})")
    if not args.apply:
        try:
            cur.execute("SET default_transaction_read_only = on")
        except Exception:
            pass

    print(f"\nTOPLAM DB BOYUTU (oncesi): {db_size(cur)}   [free limit: 500 MB]")
    print("\nEn buyuk tablolar:")
    print(f"  {'tablo':30} {'boyut':>10} {'satir':>10} {'olu':>9}")
    for nm, sz, live, dead in top_tables(cur):
        print(f"  {nm:30} {sz:>10} {live:>10} {dead:>9}")

    print("\n--- RETENTION PLANI ---")
    affected = []
    total_del = 0
    for table, cands, grp in RETENTION:
        if not table_exists(cur, table):
            continue
        cols = existing_columns(cur, table)
        dcol = pick_date_col(cols, cands)
        if not dcol:
            print(f"  [ATLA] {table}: tarih kolonu bulunamadi ({cands})")
            continue
        days = days_for[grp]
        total = fetch(cur, sql.SQL("SELECT count(*) FROM {}").format(
            sql.Identifier(table)))[0][0]
        old = fetch(cur, sql.SQL(
            "SELECT count(*) FROM {} WHERE {} < CURRENT_DATE - %s::int"
        ).format(sql.Identifier(table), sql.Identifier(dcol)), (days,))[0][0]
        keep = total - old
        flag = "" if old else "  (silinecek yok)"
        print(f"  {table:24} {dcol:16} >{days:>3}g eski: {old:>8} / {total:<8} "
              f"(kalan {keep}){flag}")
        if old > 0:
            affected.append((table, dcol, days, old))
            total_del += old

    if not args.apply:
        print(f"\nDRY-RUN: toplam {total_del} satir silinmeye ADAY.")
        print("Uygulamak icin:  python3 cleanup_storage.py --apply")
        cur.close(); conn.close()
        return

    # ---- APPLY ----
    # Buyuk DELETE'leri batch'le: her statement kisa kalir (timeout yok),
    # autocommit ile her batch hemen commit olur (uzun kilit yok).
    BATCH = 25000
    print(f"\n--- SILINIYOR (~{total_del} satir, {BATCH}'lik batch'ler) ---")
    for table, dcol, days, old in affected:
        done = 0
        while True:
            cur.execute(sql.SQL(
                "DELETE FROM {t} WHERE ctid IN ("
                "SELECT ctid FROM {t} WHERE {d} < CURRENT_DATE - %s::int LIMIT %s)"
            ).format(t=sql.Identifier(table), d=sql.Identifier(dcol)),
                (days, BATCH))
            n = cur.rowcount
            done += n
            if n:
                print(f"  {table}: {done}/{old} ...", flush=True)
            if n < BATCH:
                break
        print(f"  {table}: TOPLAM {done} satir silindi")

    if not args.no_vacuum:
        print("\n--- VACUUM FULL ANALYZE (disk geri kazanimi) ---")
        for table, _, _, _ in affected:
            print(f"  VACUUM FULL {table} ...", flush=True)
            try:
                cur.execute(sql.SQL("VACUUM (FULL, ANALYZE) {}").format(
                    sql.Identifier(table)))
            except Exception as e:
                print(f"    UYARI: {table} VACUUM hatasi: {repr(e)[:150]}")

    print(f"\nTOPLAM DB BOYUTU (sonrasi): {db_size(cur)}")
    cur.close(); conn.close()
    print("\nBITTI. (Tekrar dolmamasi icin haftalik cron onerilir — asagidaki nota bak)")


if __name__ == "__main__":
    main()
