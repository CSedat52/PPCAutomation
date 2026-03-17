"""
Mevcut config ve veri dosyalarini Supabase'e aktaran ilk senkronizasyon scripti.
Bir kez calistirilir.
"""
import json
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))
from supabase.db_client import SupabaseClient

PROJECT_ROOT = Path(__file__).parent.parent


def sync_accounts():
    """accounts.json → accounts + marketplaces tablolari."""
    accounts_file = PROJECT_ROOT / "config" / "accounts.json"
    if not accounts_file.exists():
        print("accounts.json bulunamadi, atlaniyor")
        return

    with open(accounts_file, encoding="utf-8") as f:
        config = json.load(f)

    db = SupabaseClient()
    db.sync_accounts_from_config(config)
    print("accounts + marketplaces senkronize edildi")


def sync_settings():
    """config/{hesap}_{mp}/settings.json → settings tablosu."""
    db = SupabaseClient()
    config_dir = PROJECT_ROOT / "config"

    for d in config_dir.iterdir():
        if not d.is_dir() or d.name == "__pycache__":
            continue
        parts = d.name.rsplit("_", 1)
        if len(parts) != 2:
            continue
        hesap_key = parts[0]
        mp = parts[1]

        settings_file = d / "settings.json"
        if settings_file.exists():
            with open(settings_file, encoding="utf-8") as f:
                settings = json.load(f)
            db.upsert_settings(hesap_key, mp, settings)
            print(f"  settings: {hesap_key}/{mp}")

        bid_file = d / "bid_functions.json"
        if bid_file.exists():
            with open(bid_file, encoding="utf-8") as f:
                bid_funcs = json.load(f)
            db.upsert_bid_functions(hesap_key, mp, bid_funcs)
            print(f"  bid_functions: {hesap_key}/{mp}")


def sync_latest_entity_data():
    """En son Agent 1 entity verilerini upsert et."""
    db = SupabaseClient()
    data_dir = PROJECT_ROOT / "data"

    if not data_dir.exists():
        print("data/ klasoru bulunamadi")
        return

    for d in data_dir.iterdir():
        if not d.is_dir():
            continue
        parts = d.name.rsplit("_", 1)
        if len(parts) != 2:
            continue
        hesap_key = parts[0]
        mp = parts[1]

        print(f"\n--- {hesap_key}/{mp} ---")

        # En son dosyalari bul
        entity_map = {
            "portfolios": ("portfolios", None, db.upsert_portfolios),
            "sp_campaigns": ("campaigns", "SP", None),
            "sb_campaigns": ("campaigns", "SB", None),
            "sd_campaigns": ("campaigns", "SD", None),
            "sp_ad_groups": ("ad_groups", "SP", None),
            "sb_ad_groups": ("ad_groups", "SB", None),
            "sp_keywords": ("keywords", "SP", None),
            "sb_keywords": ("keywords", "SB", None),
            "sp_targets": ("targets", "SP", None),
            "sb_targets": ("targets", "SB", None),
            "sd_targets": ("targets", "SD", None),
            "sp_product_ads": ("product_ads", None, db.upsert_product_ads),
            "sp_negative_keywords": ("negative_keywords", "SP", None),
            "sp_campaign_negative_keywords": ("negative_keywords_campaign", "SP", None),
            "sb_negative_keywords": ("negative_keywords", "SB", None),
            "sp_negative_targets": ("negative_targets", None, db.upsert_negative_targets),
        }

        for suffix, (entity_type, ad_type, custom_fn) in entity_map.items():
            # En son dosyayi bul
            files = sorted(d.glob(f"*_{suffix}.json"), key=lambda f: f.name, reverse=True)
            # verify dosyalarini atla
            files = [f for f in files if "_verify_" not in f.name]
            if not files:
                continue

            latest = files[0]
            try:
                with open(latest, encoding="utf-8") as f:
                    data = json.load(f)
                if not isinstance(data, list) or not data:
                    continue

                count = 0
                if custom_fn:
                    count = custom_fn(hesap_key, mp, data)
                elif entity_type == "campaigns":
                    count = db.upsert_campaigns(hesap_key, mp, ad_type, data)
                elif entity_type == "ad_groups":
                    count = db.upsert_ad_groups(hesap_key, mp, ad_type, data)
                elif entity_type == "keywords":
                    count = db.upsert_keywords(hesap_key, mp, ad_type, data)
                elif entity_type == "targets":
                    count = db.upsert_targets(hesap_key, mp, ad_type, data)
                elif entity_type == "negative_keywords":
                    count = db.upsert_negative_keywords(hesap_key, mp, ad_type, data, scope="AD_GROUP")
                elif entity_type == "negative_keywords_campaign":
                    count = db.upsert_negative_keywords(hesap_key, mp, ad_type, data, scope="CAMPAIGN")

                print(f"  {suffix}: {len(data)} kayit -> {count} upsert")
            except Exception as e:
                print(f"  {suffix}: HATA - {e}")


def sync_latest_reports():
    """En son targeting/search term raporlarini insert et."""
    db = SupabaseClient()
    data_dir = PROJECT_ROOT / "data"

    if not data_dir.exists():
        return

    for d in data_dir.iterdir():
        if not d.is_dir():
            continue
        parts = d.name.rsplit("_", 1)
        if len(parts) != 2:
            continue
        hesap_key = parts[0]
        mp = parts[1]

        report_map = {
            "sp_targeting_report_14d": ("SP", "14d", "targeting"),
            "sp_targeting_report_30d": ("SP", "30d", "targeting"),
            "sb_targeting_report_14d": ("SB", "14d", "targeting"),
            "sd_targeting_report_14d": ("SD", "14d", "targeting"),
            "sd_targeting_report_30d": ("SD", "30d", "targeting"),
            "sp_search_term_report_30d": ("SP", "30d", "search_term"),
            "sb_search_term_report_30d": ("SB", "30d", "search_term"),
        }

        for suffix, (ad_type, period, report_type) in report_map.items():
            files = sorted(d.glob(f"*_{suffix}.json"), key=lambda f: f.name, reverse=True)
            if not files:
                continue

            latest = files[0]
            # Dosya adindan collection_date cikar (YYYY-MM-DD_xxx.json)
            collection_date = latest.name[:10]

            try:
                with open(latest, encoding="utf-8") as f:
                    data = json.load(f)
                if not isinstance(data, list) or not data:
                    continue

                if report_type == "targeting":
                    count = db.insert_targeting_reports(
                        hesap_key, mp, ad_type, period, collection_date, data)
                else:
                    count = db.insert_search_term_reports(
                        hesap_key, mp, ad_type, collection_date, data)

                print(f"  {suffix}: {len(data)} kayit -> {count} insert ({hesap_key}/{mp})")
            except Exception as e:
                print(f"  {suffix}: HATA - {e}")


if __name__ == "__main__":
    print("=" * 60)
    print("ADIM 1: Hesap ve marketplace senkronizasyonu")
    print("=" * 60)
    sync_accounts()

    print("\n" + "=" * 60)
    print("ADIM 2: Config senkronizasyonu (settings + bid_functions)")
    print("=" * 60)
    sync_settings()

    print("\n" + "=" * 60)
    print("ADIM 3: Entity verileri (son durum)")
    print("=" * 60)
    sync_latest_entity_data()

    print("\n" + "=" * 60)
    print("ADIM 4: Rapor verileri (birikmeli)")
    print("=" * 60)
    sync_latest_reports()

    print("\n" + "=" * 60)
    print("Ilk senkronizasyon tamamlandi!")
    print("=" * 60)
