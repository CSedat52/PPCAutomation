"""
Para Birimi Bazli min/max Bid Duzeltme
========================================
EUR disindaki marketplace'lerde min_bid ve max_bid degerlerini
para birimine uygun sekilde gunceller.

Kullanim: python fix_currency_bids.py
"""
import json
from pathlib import Path

BASE_DIR = Path(__file__).parent

# Para birimi bazli min/max bid (EUR karsiligindan hesaplandi)
CURRENCY_BIDS = {
    "USD": {"min_bid": 0.15, "max_bid": 5.00},
    "CAD": {"min_bid": 0.15, "max_bid": 5.00},
    "GBP": {"min_bid": 0.15, "max_bid": 5.00},
    "EUR": {"min_bid": 0.15, "max_bid": 5.00},
    "SEK": {"min_bid": 2.00, "max_bid": 50.00},
    "PLN": {"min_bid": 0.70, "max_bid": 20.00},
}

# Marketplace -> para birimi
MP_CURRENCY = {
    "US": "USD", "CA": "CAD",
    "UK": "GBP",
    "DE": "EUR", "FR": "EUR", "ES": "EUR", "IT": "EUR", "NL": "EUR",
    "SE": "SEK", "PL": "PLN",
}

def main():
    config_dir = BASE_DIR / "config"
    guncellenen = 0

    for mp_dir in sorted(config_dir.iterdir()):
        if not mp_dir.is_dir() or mp_dir.name == "__pycache__":
            continue

        settings_path = mp_dir / "settings.json"
        if not settings_path.exists():
            continue

        with open(settings_path, "r", encoding="utf-8") as f:
            settings = json.load(f)

        mp = settings.get("genel_ayarlar", {}).get("aktif_marketplace", "")
        para_birimi = settings.get("genel_ayarlar", {}).get("para_birimi", "")

        if not para_birimi and mp in MP_CURRENCY:
            para_birimi = MP_CURRENCY.get(mp, "EUR")

        bids = CURRENCY_BIDS.get(para_birimi, CURRENCY_BIDS["EUR"])

        degisti = False

        # genel_ayarlar
        ga = settings.get("genel_ayarlar", {})
        if ga.get("min_bid") != bids["min_bid"] or ga.get("max_bid") != bids["max_bid"]:
            ga["min_bid"] = bids["min_bid"]
            ga["max_bid"] = bids["max_bid"]
            degisti = True

        # agent3_ayarlari
        a3 = settings.get("agent3_ayarlari", {})
        if a3.get("min_bid_limiti") != bids["min_bid"] or a3.get("max_bid_limiti") != bids["max_bid"]:
            a3["min_bid_limiti"] = bids["min_bid"]
            a3["max_bid_limiti"] = bids["max_bid"]
            degisti = True

        if degisti:
            with open(settings_path, "w", encoding="utf-8") as f:
                json.dump(settings, f, indent=2, ensure_ascii=False)
            print(f"  GUNCELLENDI: {mp_dir.name} — {para_birimi} min={bids['min_bid']} max={bids['max_bid']}")
            guncellenen += 1
        else:
            print(f"  OK: {mp_dir.name} — {para_birimi} min={bids['min_bid']} max={bids['max_bid']}")

    print(f"\nToplam {guncellenen} dosya guncellendi.")


if __name__ == "__main__":
    main()
