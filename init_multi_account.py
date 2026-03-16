"""
Amazon PPC Otomasyon — Klasor Yapisi Olusturucu
=================================================
accounts.json'daki tum aktif hesap+marketplace kombinasyonlari icin:
  1. data/{hesap}_{marketplace}/ klasor yapisini olusturur
  2. config/{hesap}_{marketplace}/ altina template settings.json ve bid_functions.json kopyalar
  3. Mevcut tek-hesap verileri varsa vigowood_na_US altina tasir (migrasyon)

Kullanim: python init_multi_account.py
"""

import json
import os
import shutil
from pathlib import Path
from datetime import datetime

# Proje kok dizini (bu script proje kokunde calistirilmali)
BASE_DIR = Path(".")
ACCOUNTS_FILE = BASE_DIR / "config" / "accounts.json"


def load_accounts():
    """accounts.json dosyasini yukler."""
    if not ACCOUNTS_FILE.exists():
        # Eger config/ icinde yoksa proje kokunde ara
        alt = BASE_DIR / "accounts.json"
        if alt.exists():
            return json.loads(alt.read_text(encoding="utf-8"))
        raise FileNotFoundError(f"accounts.json bulunamadi: {ACCOUNTS_FILE} veya {alt}")
    return json.loads(ACCOUNTS_FILE.read_text(encoding="utf-8"))


def get_active_combinations(accounts):
    """Aktif hesap+marketplace listesi doner."""
    combos = []
    for hesap_key, hesap in accounts.get("hesaplar", {}).items():
        for mp_code, mp_config in hesap.get("marketplaces", {}).items():
            if mp_config.get("aktif", False):
                combos.append({
                    "hesap_key": hesap_key,
                    "marketplace": mp_code,
                    "dir_name": f"{hesap_key}_{mp_code}",
                    "hesap_adi": hesap.get("hesap_adi", hesap_key),
                    "profile_id": mp_config.get("profile_id", ""),
                    "account_id": hesap.get("account_id", ""),
                })
    return combos


def create_data_dirs(combo):
    """Bir hesap+marketplace icin data klasor yapisini olusturur."""
    base = BASE_DIR / "data" / combo["dir_name"]
    dirs = [
        base,
        base / "analysis",
        base / "decisions",
        base / "logs",
        base / "agent4" / "db",
        base / "agent4" / "raporlar",
        base / "agent4" / "proposals" / "bekleyen",
        base / "agent4" / "proposals" / "arsiv",
    ]
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)
    return base


def create_config_dirs(combo, accounts):
    """Bir hesap+marketplace icin config dosyalarini olusturur."""
    config_dir = BASE_DIR / "config" / combo["dir_name"]
    config_dir.mkdir(parents=True, exist_ok=True)

    mp_config = accounts.get("marketplace_config", {}).get(combo["marketplace"], {})
    currency = mp_config.get("currency", "$")

    # --- settings.json ---
    settings_path = config_dir / "settings.json"
    if not settings_path.exists():
        settings = {
            "_aciklama": f"Amazon PPC Ayarlari — {combo['hesap_adi']} / {combo['marketplace']}",
            "_versiyon": "2.0",
            "_son_guncelleme": datetime.utcnow().strftime("%Y-%m-%d"),
            "_hesap": combo["hesap_key"],
            "_marketplace": combo["marketplace"],
            "genel_ayarlar": {
                "analiz_periyodu_gun": 3,
                "lookback_gun": 14,
                "son_gun_haric": True,
                "aktif_marketplace": combo["marketplace"],
                "min_bid": 0.15,
                "max_bid": 2.0,
                "para_birimi": currency
            },
            "esik_degerleri": {
                "gosterim_esik": 250,
                "tiklama_esik": 30,
                "_aciklama_gosterim": "Bu degerden az impression alan hedeflemeler GORUNMEZ segmentine girer",
                "_aciklama_tiklama": "Satis=0 durumunda bu degerden az tiklama alan hedeflemeler YETERSIZ VERI segmentine girer"
            },
            "asin_hedefleri": {
                "_aciklama": "Bu marketplace icin ASIN hedef ACoS degerlerini tanimlayin.",
                "_varsayilan_acos": 20,
                "_varsayilan_aciklama": "Tanimlanmamis ASIN'ler icin varsayilan hedef ACoS"
            },
            "negatif_keyword_kurali": {
                "min_tiklama": 30,
                "max_satis": 0,
                "_aciklama": "30+ tiklama ve 0 satis yapan hedeflemeler negatif keyword adayi"
            },
            "yeni_keyword_kurali": {
                "min_satis": 3,
                "max_acos": 20,
                "lookback_gun": 30,
                "_aciklama": "Harvesting kurallari"
            },
            "harvesting_ayarlari": {
                "lookback_gun": 30,
                "_aciklama": "Harvest periyodu"
            },
            "segmentasyon_kurallari": {
                "_aciklama": "Segmentasyon kurallari global settings ile ayni. Marketplace'e ozel degisiklik gerekirse burada yapin."
            },
            "ozel_kurallar": {
                "impression_takibi": {"aktif": True, "impression_dusus_esik_yuzde": 50},
                "dusuk_cvr_tuzagi": {"aktif": True, "cvr_esik": 1.0},
                "super_star_koruma": {"aktif": True, "acos_esik": 10, "max_artis_yuzde": 10}
            },
            "agent3_ayarlari": {
                "dry_run": True,
                "dry_run_aciklama": "true = sadece rapor uretir. false = gercekten uygular.",
                "max_bid_limiti": 2.0,
                "min_bid_limiti": 0.15,
                "gunluk_max_islem": 1000,
                "yeni_kampanya_butcesi": 30.0,
                "negatif_match_type": "NEGATIVE_EXACT",
                "portfolio_asin_target_kampanyalari": {}
            }
        }
        settings_path.write_text(json.dumps(settings, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"    [+] settings.json olusturuldu")
    else:
        print(f"    [=] settings.json zaten var, atlaniyor")

    # --- bid_functions.json ---
    bid_path = config_dir / "bid_functions.json"
    if not bid_path.exists():
        bid_functions = {
            "_aciklama": f"Bid hesaplama parametreleri — {combo['hesap_adi']} / {combo['marketplace']}",
            "_versiyon": "2.0",
            "_son_guncelleme": datetime.utcnow().strftime("%Y-%m-%d"),
            "_hesap": combo["hesap_key"],
            "_marketplace": combo["marketplace"],
            "tanh_formulu": {
                "_formul": "bid_degisim = -tanh(acos_fark_orani * hassasiyet) * max_degisim",
                "hassasiyet": 0.5,
                "max_degisim": 0.2
            },
            "segment_parametreleri": {
                "GORUNMEZ": {"fonksiyon": "sabit_artis", "artis_orani": 0.1},
                "YETERSIZ_VERI": {"fonksiyon": "dokunma"},
                "KAN_KAYBEDEN": {"fonksiyon": "tiklama_bazli_dusus", "min_dusus": 0.01, "max_dusus": 0.25, "referans_tiklama": 40},
                "SUPER_STAR": {"fonksiyon": "tanh_korumali", "max_artis": 0.1},
                "TUZAK": {"fonksiyon": "dokunma"},
                "KAZANAN": {"fonksiyon": "tanh"},
                "OPTIMIZE_ET": {"fonksiyon": "tanh"},
                "ZARAR": {"fonksiyon": "tanh"}
            },
            "genel_limitler": {
                "tek_seferde_max_artis": 0.3,
                "tek_seferde_max_dusus": 0.35
            },
            "asin_parametreleri": {},
            "ogrenme_gecmisi": {
                "_aciklama": "Learning Agent bu alani guncelleyecek.",
                "degisiklikler": []
            }
        }
        bid_path.write_text(json.dumps(bid_functions, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"    [+] bid_functions.json olusturuldu")
    else:
        print(f"    [=] bid_functions.json zaten var, atlaniyor")

    return config_dir


def migrate_existing_data():
    """
    Mevcut tek-hesap verilerini vigowood_na_US altina tasir.
    Sadece ilk calistirmada yapilir — zaten tasinmissa atlanir.
    """
    old_data = BASE_DIR / "data"
    new_data = old_data / "vigowood_na_US"
    old_config = BASE_DIR / "config"
    new_config = old_config / "vigowood_na_US"

    # Zaten tasinmis mi kontrol et
    if (new_data / "analysis").exists() and any(new_data.glob("*.json")):
        print("\n[=] Migrasyon: vigowood_na_US verileri zaten mevcut, atlaniyor.")
        return

    # Eski data/ icinde dogrudan JSON dosyalari var mi?
    old_jsons = [f for f in old_data.glob("*.json") if f.is_file()]
    if not old_jsons:
        print("\n[=] Migrasyon: Tasinacak eski veri bulunamadi.")
        return

    print(f"\n[*] MIGRASYON: {len(old_jsons)} dosya data/ -> data/vigowood_na_US/")

    new_data.mkdir(parents=True, exist_ok=True)
    tasinan = 0
    for f in old_jsons:
        dest = new_data / f.name
        if not dest.exists():
            shutil.copy2(f, dest)
            tasinan += 1

    # analysis/ klasorunu tasi
    old_analysis = old_data / "analysis"
    new_analysis = new_data / "analysis"
    if old_analysis.exists() and not new_analysis.exists():
        shutil.copytree(old_analysis, new_analysis)
        print(f"    analysis/ klasoru kopyalandi")

    # decisions/ klasorunu tasi
    old_decisions = old_data / "decisions"
    new_decisions = new_data / "decisions"
    if old_decisions.exists() and not new_decisions.exists():
        shutil.copytree(old_decisions, new_decisions)
        print(f"    decisions/ klasoru kopyalandi")

    # logs/ klasorunu tasi
    old_logs = old_data / "logs"
    new_logs = new_data / "logs"
    if old_logs.exists() and not new_logs.exists():
        shutil.copytree(old_logs, new_logs)
        print(f"    logs/ klasoru kopyalandi")

    # agent4/ klasorunu tasi
    old_a4 = old_data / "agent4"
    new_a4 = new_data / "agent4"
    if old_a4.exists() and not new_a4.exists():
        shutil.copytree(old_a4, new_a4)
        print(f"    agent4/ klasoru kopyalandi")

    print(f"    {tasinan} JSON dosyasi kopyalandi")

    # Mevcut config dosyalarini kopyala
    old_settings = old_config / "settings.json"
    old_bid = old_config / "bid_functions.json"
    new_config.mkdir(parents=True, exist_ok=True)

    if old_settings.exists() and not (new_config / "settings.json").exists():
        shutil.copy2(old_settings, new_config / "settings.json")
        print(f"    settings.json kopyalandi (mevcut ayarlar korundu)")

    if old_bid.exists() and not (new_config / "bid_functions.json").exists():
        shutil.copy2(old_bid, new_config / "bid_functions.json")
        print(f"    bid_functions.json kopyalandi (mevcut parametreler korundu)")

    print(f"\n[OK] Migrasyon tamamlandi. Eski dosyalar yerinde kaldi (silmek size kalmis).")


def main():
    print("=" * 60)
    print("  MULTI-ACCOUNT KLASOR YAPISI OLUSTURUCU")
    print("=" * 60)

    accounts = load_accounts()
    combos = get_active_combinations(accounts)

    print(f"\n  {len(combos)} aktif marketplace bulundu:\n")
    for c in combos:
        print(f"    {c['dir_name']:25s} — {c['hesap_adi']} / {c['marketplace']} (profile: {c['profile_id']})")

    # 1. Migrasyon (mevcut verileri vigowood_na_US altina tasi)
    migrate_existing_data()

    # 2. Tum kombinasyonlar icin klasor ve config olustur
    print(f"\n{'=' * 60}")
    print(f"  KLASOR VE CONFIG OLUSTURULUYOR")
    print(f"{'=' * 60}")

    for c in combos:
        print(f"\n  [{c['dir_name']}]")
        data_dir = create_data_dirs(c)
        print(f"    [+] data/{c['dir_name']}/ klasor yapisi olusturuldu")
        config_dir = create_config_dirs(c, accounts)

    # 3. accounts.json'u config/ altina kopyala (henuz yoksa)
    accounts_dest = BASE_DIR / "config" / "accounts.json"
    if not accounts_dest.exists():
        accounts_src = BASE_DIR / "accounts.json"
        if accounts_src.exists():
            shutil.copy2(accounts_src, accounts_dest)
            print(f"\n  [+] accounts.json -> config/accounts.json kopyalandi")

    # 4. Sonuc ozeti
    print(f"\n{'=' * 60}")
    print(f"  TAMAMLANDI!")
    print(f"{'=' * 60}")
    print(f"\n  Olusturulan klasor yapisi:\n")
    print(f"  config/")
    print(f"  +-- accounts.json")
    for c in combos:
        print(f"  +-- {c['dir_name']}/")
        print(f"  |   +-- settings.json")
        print(f"  |   +-- bid_functions.json")
    print(f"  data/")
    for c in combos:
        print(f"  +-- {c['dir_name']}/")
        print(f"  |   +-- analysis/")
        print(f"  |   +-- decisions/")
        print(f"  |   +-- logs/")
        print(f"  |   +-- agent4/")
    print(f"\n  Sonraki adim: Agent'lari account_context destegi ile guncellemek.")
    print(f"  Bu dosyayi Claude'a gonder ve Asama 2'ye gecmeyi iste.\n")


if __name__ == "__main__":
    main()
