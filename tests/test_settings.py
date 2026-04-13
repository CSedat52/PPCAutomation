"""
Settings Baglanti Testi — Dashboard <-> Supabase <-> Agent'lar
============================================================
Calistirma:
  python tests/test_settings.py
  python tests/test_settings.py vigowood_na US
"""

import sys
import json
import os
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR))

from supabase.db_client import SupabaseClient


def test_connection():
    """1. Supabase baglantisini test et."""
    print("=" * 60)
    print("  TEST 1: Supabase Baglantisi")
    print("=" * 60)
    try:
        db = SupabaseClient()
        row = db._fetch_one("SELECT 1")
        assert row is not None, "SELECT 1 basarisiz"
        print("  [OK] Supabase baglantisi basarili")
        return True
    except Exception as e:
        print(f"  [FAIL] Supabase baglantisi basarisiz: {e}")
        return False


def test_active_marketplaces(db):
    """2. Aktif marketplace'leri kontrol et."""
    print("\n" + "=" * 60)
    print("  TEST 2: Aktif Marketplace'ler")
    print("=" * 60)
    rows = db._fetch_all("SELECT hesap_key, marketplace FROM marketplaces WHERE aktif = true ORDER BY hesap_key, marketplace")
    if not rows:
        print("  [FAIL] Hic aktif marketplace bulunamadi")
        return []
    print(f"  [OK] {len(rows)} aktif marketplace:")
    result = []
    for hk, mp in rows:
        print(f"    - {hk}/{mp}")
        result.append((hk, mp))
    return result


def test_settings(db, hesap_key, marketplace):
    """3. Settings tablosunu kontrol et."""
    print(f"\n--- Settings: {hesap_key}/{marketplace} ---")
    row = db._fetch_one(
        "SELECT genel_ayarlar, esik_degerleri, asin_hedefleri, segmentasyon_kurallari, "
        "agent3_ayarlari, ozel_kurallar, negatif_keyword_kurali, yeni_keyword_kurali, harvesting_ayarlari "
        "FROM settings WHERE hesap_key = %s AND marketplace = %s",
        (hesap_key, marketplace))

    if not row:
        print(f"  [FAIL] Settings kaydi bulunamadi!")
        return False

    keys = ["genel_ayarlar", "esik_degerleri", "asin_hedefleri", "segmentasyon_kurallari",
            "agent3_ayarlari", "ozel_kurallar", "negatif_keyword_kurali",
            "yeni_keyword_kurali", "harvesting_ayarlari"]

    errors = []
    for i, key in enumerate(keys):
        val = row[i]
        if val is None:
            print(f"  [WARN] {key}: NULL (bos dict kullanilacak)")
        else:
            data = val if isinstance(val, dict) else json.loads(val)
            print(f"  [OK] {key}: {len(data)} anahtar")

    # Kritik alanlar kontrolu
    genel = row[0] if isinstance(row[0], dict) else json.loads(row[0]) if row[0] else {}
    esik = row[1] if isinstance(row[1], dict) else json.loads(row[1]) if row[1] else {}

    required_genel = ["lookback_gun", "min_bid", "max_bid"]
    for k in required_genel:
        if k not in genel:
            errors.append(f"genel_ayarlar.{k} EKSIK")
            print(f"  [FAIL] genel_ayarlar.{k} EKSIK!")
        else:
            print(f"  [OK] genel_ayarlar.{k} = {genel[k]}")

    required_esik = ["gosterim_esik", "tiklama_esik"]
    for k in required_esik:
        if k not in esik:
            errors.append(f"esik_degerleri.{k} EKSIK")
            print(f"  [FAIL] esik_degerleri.{k} EKSIK!")
        else:
            print(f"  [OK] esik_degerleri.{k} = {esik[k]}")

    # Agent 3 ayarlari
    a3 = row[4] if isinstance(row[4], dict) else json.loads(row[4]) if row[4] else {}
    a3_keys = ["dry_run", "max_bid_limiti", "min_bid_limiti", "gunluk_max_islem"]
    for k in a3_keys:
        if k in a3:
            print(f"  [OK] agent3_ayarlari.{k} = {a3[k]}")
        else:
            print(f"  [INFO] agent3_ayarlari.{k} tanimli degil (varsayilan kullanilacak)")

    return len(errors) == 0


def test_bid_functions(db, hesap_key, marketplace):
    """4. Bid functions tablosunu kontrol et."""
    print(f"\n--- Bid Functions: {hesap_key}/{marketplace} ---")
    row = db._fetch_one(
        "SELECT tanh_formulu, segment_parametreleri, genel_limitler, asin_parametreleri "
        "FROM bid_functions WHERE hesap_key = %s AND marketplace = %s",
        (hesap_key, marketplace))

    if not row:
        print(f"  [FAIL] Bid functions kaydi bulunamadi!")
        return False

    errors = []

    # tanh_formulu
    tf = row[0] if isinstance(row[0], dict) else json.loads(row[0]) if row[0] else {}
    for k in ["hassasiyet", "max_degisim"]:
        if k in tf:
            print(f"  [OK] tanh_formulu.{k} = {tf[k]}")
        else:
            errors.append(f"tanh_formulu.{k} EKSIK")
            print(f"  [FAIL] tanh_formulu.{k} EKSIK!")

    # segment_parametreleri
    sp = row[1] if isinstance(row[1], dict) else json.loads(row[1]) if row[1] else {}
    for seg in ["GORUNMEZ", "KAN_KAYBEDEN", "SUPER_STAR"]:
        if seg in sp:
            print(f"  [OK] segment_parametreleri.{seg} tanimli")
        else:
            print(f"  [WARN] segment_parametreleri.{seg} tanimli degil (varsayilan kullanilacak)")

    # genel_limitler
    gl = row[2] if isinstance(row[2], dict) else json.loads(row[2]) if row[2] else {}
    for k in ["tek_seferde_max_artis", "tek_seferde_max_dusus"]:
        if k in gl:
            print(f"  [OK] genel_limitler.{k} = {gl[k]}")
        else:
            print(f"  [WARN] genel_limitler.{k} tanimli degil (varsayilan kullanilacak)")

    # asin_parametreleri
    ap = row[3] if isinstance(row[3], dict) else json.loads(row[3]) if row[3] else {}
    asin_count = len([k for k in ap if not k.startswith("_")])
    print(f"  [OK] asin_parametreleri: {asin_count} ASIN tanimli")

    return len(errors) == 0


def test_execution_queue(db, hesap_key, marketplace):
    """5. Execution queue tablosu erisilebilir mi?"""
    print(f"\n--- Execution Queue: {hesap_key}/{marketplace} ---")
    try:
        rows = db._fetch_all(
            "SELECT id, status, command FROM execution_queue WHERE hesap_key = %s AND marketplace = %s ORDER BY requested_at DESC LIMIT 5",
            (hesap_key, marketplace))
        print(f"  [OK] execution_queue erisilebilir ({len(rows or [])} son kayit)")
        return True
    except Exception as e:
        print(f"  [FAIL] execution_queue erisilemedi: {e}")
        return False


def test_agent_status(db):
    """6. Agent status tablosu kontrol et."""
    print(f"\n--- Agent Status ---")
    try:
        rows = db._fetch_all("SELECT agent_name, status, updated_at FROM agent_status ORDER BY agent_name")
        if rows:
            for agent_name, status, updated_at in rows:
                print(f"  [OK] {agent_name}: {status} (son guncelleme: {updated_at})")
        else:
            print("  [WARN] agent_status tablosu bos")
        return True
    except Exception as e:
        print(f"  [FAIL] agent_status erisilemedi: {e}")
        return False


def test_decision_history(db, hesap_key, marketplace):
    """7. Decision history tablosu kontrol et."""
    print(f"\n--- Decision History: {hesap_key}/{marketplace} ---")
    try:
        count_row = db._fetch_one(
            "SELECT COUNT(*), COUNT(CASE WHEN decision_status='VERIFIED' THEN 1 END), COUNT(CASE WHEN decision_status='APPLIED' THEN 1 END) "
            "FROM decision_history WHERE hesap_key = %s AND marketplace = %s",
            (hesap_key, marketplace))
        total, verified, applied = count_row if count_row else (0, 0, 0)
        print(f"  [OK] Toplam: {total}, VERIFIED: {verified}, APPLIED: {applied}")
        return True
    except Exception as e:
        print(f"  [FAIL] decision_history erisilemedi: {e}")
        return False


def main():
    args = sys.argv[1:]
    specific_hk = args[0] if len(args) >= 1 else None
    specific_mp = args[1] if len(args) >= 2 else None

    # 1. Baglanti
    if not test_connection():
        print("\n[ABORT] Supabase baglantisi kurulamadi. .env dosyasini kontrol edin.")
        sys.exit(1)

    db = SupabaseClient()

    # 2. Aktif marketplace'ler
    all_mp = test_active_marketplaces(db)
    if not all_mp:
        print("\n[ABORT] Aktif marketplace bulunamadi.")
        sys.exit(1)

    # Belirli marketplace mi?
    if specific_hk and specific_mp:
        targets = [(specific_hk, specific_mp)]
    else:
        targets = all_mp

    # 3-7. Her marketplace icin testler
    print("\n" + "=" * 60)
    print("  TEST 3-7: Marketplace Bazli Testler")
    print("=" * 60)

    total_ok = 0
    total_fail = 0

    for hk, mp in targets:
        print(f"\n{'='*40}")
        print(f"  {hk}/{mp}")
        print(f"{'='*40}")

        r1 = test_settings(db, hk, mp)
        r2 = test_bid_functions(db, hk, mp)
        r3 = test_execution_queue(db, hk, mp)
        r4 = test_decision_history(db, hk, mp)

        if r1 and r2 and r3 and r4:
            total_ok += 1
        else:
            total_fail += 1

    # 6. Global testler
    test_agent_status(db)

    # Ozet
    print("\n" + "=" * 60)
    print(f"  SONUC: {total_ok} basarili, {total_fail} hatali marketplace")
    print("=" * 60)

    sys.exit(0 if total_fail == 0 else 1)


if __name__ == "__main__":
    main()
