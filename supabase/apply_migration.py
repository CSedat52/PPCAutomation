"""
Supabase migration uygulayici.
psycopg2 ile dogrudan PostgreSQL'e baglanir ve SQL migration'i calistirir.
"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import psycopg2

load_dotenv(Path(__file__).parent.parent / ".env")

DB_URL = os.getenv("SUPABASE_DB_URL")
if not DB_URL:
    print("HATA: SUPABASE_DB_URL .env'de tanimli olmali")
    sys.exit(1)


def main():
    migration_file = Path(__file__).parent / "migrations" / "001_initial_schema.sql"
    if not migration_file.exists():
        print(f"HATA: {migration_file} bulunamadi")
        sys.exit(1)

    sql_content = migration_file.read_text(encoding="utf-8")
    print(f"Migration okundu: {len(sql_content)} karakter")

    # Direct connection (session mode icin port 5432 kullan)
    # Pooler (port 6543) prepared statements desteklemez ama DDL icin sorun olmaz
    db_url = DB_URL

    print("Supabase PostgreSQL'e baglaniliyor...")
    try:
        conn = psycopg2.connect(db_url)
        conn.autocommit = True
        cur = conn.cursor()
        print("Baglanti basarili!")

        print("Migration uygulaniyor...")
        cur.execute(sql_content)
        print("BASARILI! Tum tablolar olusturuldu.")

        # Dogrulama: tablo sayisini kontrol et
        cur.execute("""
            SELECT count(*)
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_type = 'BASE TABLE'
        """)
        table_count = cur.fetchone()[0]
        print(f"\nPublic schema'da toplam {table_count} tablo mevcut.")

        # Tablo listesi
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        tables = [row[0] for row in cur.fetchall()]
        print("\nOlusturulan tablolar:")
        for t in tables:
            print(f"  - {t}")

        cur.close()
        conn.close()

    except psycopg2.Error as e:
        print(f"HATA: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
