"""
Config Doldurucu — Tum Hesaplar
=================================
asin_report.json'dan ASIN'leri okur ve her marketplace icin:
  - config/{hesap}_{mp}/settings.json → asin_hedefleri doldurulur
  - config/{hesap}_{mp}/bid_functions.json → asin_parametreleri doldurulur

Varsayilan parametreler:
  - Hedef ACoS: %20
  - Hassasiyet: 0.6
  - Max degisim: 0.15 (%15)

Kullanim: python populate_configs.py
"""

import json
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).parent
ASIN_REPORT = BASE_DIR / "asin_report.json"

# ============================================
# VARSAYILAN PARAMETRELER
# ============================================
DEFAULT_HEDEF_ACOS = 20
DEFAULT_HASSASIYET = 0.6
DEFAULT_MAX_DEGISIM = 0.15

# Bilinen urun adi eslesmesi (mevcut Vigowood US settings'ten)
URUN_ADLARI = {
    "B09PDQJLM5": "MKOS41",
    "B09RBDLLPZ": "IN-904J-FA2V",
    "B0F1TL2KN3": "DK50M",
    "B08WHS3JJ1": "LS021-00",
    "B0FF2LKM4X": "LS023",
    "B0FF2MB1R1": "LS024",
    "B0FF2JSKPV": "LS025",
    "B0BRSM52M2": "MD08C",
    "B0CRBBG1TX": "BT201",
    "B08CZ3ZN97": "DJ-S2VD-VHVT",
    "B0F6SXZ8FK": "DK20C",
    "B0F6V38XTV": "DK20M",
    "B0F1TMCCJT": "DK30C",
    "B0F1TNQ69S": "DK30M",
    "B0F1TLB8J4": "DK50C",
    "B0BVLZWDBR": "DYKOS",
    "B08THYG5MD": "FM-IHQ7-JK7A",
    "B08CZPT1H8": "HO-GQQ6-5T4I",
}


def main():
    with open(ASIN_REPORT, "r", encoding="utf-8") as f:
        report = json.load(f)

    today = datetime.utcnow().strftime("%Y-%m-%d")

    print("=" * 60)
    print("  CONFIG DOLDURUCU — Tum Hesaplar")
    print(f"  Hedef ACoS: %{DEFAULT_HEDEF_ACOS}")
    print(f"  Hassasiyet: {DEFAULT_HASSASIYET}")
    print(f"  Max degisim: {DEFAULT_MAX_DEGISIM} (%{int(DEFAULT_MAX_DEGISIM*100)})")
    print("=" * 60)

    for dir_name, data in report.items():
        asins = data.get("asinler", {})
        config_dir = BASE_DIR / "config" / dir_name

        if not config_dir.exists():
            print(f"\n[!] {dir_name}: config klasoru yok, atlaniyor")
            continue

        print(f"\n--- {dir_name} ({len(asins)} ASIN) ---")

        # ---- settings.json guncelle ----
        settings_path = config_dir / "settings.json"
        if settings_path.exists():
            with open(settings_path, "r", encoding="utf-8") as f:
                settings = json.load(f)
        else:
            print(f"  [!] settings.json yok, atlaniyor")
            continue

        asin_hedefleri = settings.get("asin_hedefleri", {})
        # Mevcut meta alanlari koru
        meta_keys = {k: v for k, v in asin_hedefleri.items() if k.startswith("_")}

        yeni_hedefler = dict(meta_keys)
        yeni_hedefler["_varsayilan_acos"] = DEFAULT_HEDEF_ACOS
        yeni_hedefler["_varsayilan_aciklama"] = "Tanimlanmamis ASIN'ler icin varsayilan hedef ACoS"

        eklenen = 0
        for asin, info in asins.items():
            mevcut = asin_hedefleri.get(asin, {})
            if isinstance(mevcut, dict) and mevcut.get("hedef_acos"):
                # Mevcut hedef varsa koru
                yeni_hedefler[asin] = mevcut
            else:
                yeni_hedefler[asin] = {
                    "hedef_acos": DEFAULT_HEDEF_ACOS,
                    "reklam_tipleri": info.get("reklam_tipleri", []),
                    "state": info.get("state", ""),
                }
                if asin in URUN_ADLARI:
                    yeni_hedefler[asin]["urun_adi"] = URUN_ADLARI[asin]
                eklenen += 1

        settings["asin_hedefleri"] = yeni_hedefler
        settings["_son_guncelleme"] = today

        with open(settings_path, "w", encoding="utf-8") as f:
            json.dump(settings, f, indent=2, ensure_ascii=False)
        print(f"  settings.json: {eklenen} yeni ASIN eklendi, {len(asins)} toplam")

        # ---- bid_functions.json guncelle ----
        bid_path = config_dir / "bid_functions.json"
        if bid_path.exists():
            with open(bid_path, "r", encoding="utf-8") as f:
                bid_funcs = json.load(f)
        else:
            print(f"  [!] bid_functions.json yok, atlaniyor")
            continue

        # Global tanh parametrelerini guncelle
        if "tanh_formulu" in bid_funcs:
            bid_funcs["tanh_formulu"]["hassasiyet"] = DEFAULT_HASSASIYET
            bid_funcs["tanh_formulu"]["max_degisim"] = DEFAULT_MAX_DEGISIM

        # ASIN parametreleri
        asin_params = bid_funcs.get("asin_parametreleri", {})
        eklenen_bid = 0
        for asin, info in asins.items():
            if asin not in asin_params:
                asin_params[asin] = {
                    "hassasiyet": DEFAULT_HASSASIYET,
                    "max_degisim": DEFAULT_MAX_DEGISIM,
                    "aktif": False,
                    "_kaynak": "auto_populate",
                    "_son_guncelleme": today,
                }
                if asin in URUN_ADLARI:
                    asin_params[asin]["urun_adi"] = URUN_ADLARI[asin]
                eklenen_bid += 1
            else:
                # Mevcut parametreleri koru ama hassasiyet/max_degisim guncelle
                if not asin_params[asin].get("aktif", False):
                    asin_params[asin]["hassasiyet"] = DEFAULT_HASSASIYET
                    asin_params[asin]["max_degisim"] = DEFAULT_MAX_DEGISIM
                    asin_params[asin]["_son_guncelleme"] = today

        bid_funcs["asin_parametreleri"] = asin_params
        bid_funcs["_son_guncelleme"] = today

        with open(bid_path, "w", encoding="utf-8") as f:
            json.dump(bid_funcs, f, indent=2, ensure_ascii=False)
        print(f"  bid_functions.json: {eklenen_bid} yeni ASIN eklendi, {len(asin_params)} toplam")

    # Ozet
    print(f"\n{'=' * 60}")
    print(f"  TAMAMLANDI!")
    print(f"  Tum config dosyalari guncellendi.")
    print(f"  Varsayilan: ACoS=%{DEFAULT_HEDEF_ACOS}, hassasiyet={DEFAULT_HASSASIYET}, max_degisim={DEFAULT_MAX_DEGISIM}")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    main()
