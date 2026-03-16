"""
ASIN Listesi Cekici — Tum Hesaplar (SP + SB + SD)
====================================================
Her marketplace icin 3 entity ceker:
  - SP product_ads (asin alani)
  - SB ad_groups (reklam verilen urunler)
  - SD ad_groups (reklam verilen urunler)

Mevcut dosya varsa API'ye gitmez, dosyadan okur.

Kullanim: python fetch_asins.py
"""

import json
import urllib.request
import urllib.parse
import time
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).parent
ACCOUNTS_FILE = BASE_DIR / "config" / "accounts.json"
if not ACCOUNTS_FILE.exists():
    ACCOUNTS_FILE = BASE_DIR / "accounts.json"

SP_PRODUCT_ADS_CT = "application/vnd.spProductAd.v3+json"
SB_AD_GROUPS_CT = "application/vnd.sbcampaignresource.v4+json"


def load_accounts():
    with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def get_access_token(refresh_token, client_id, client_secret, token_endpoint):
    data = urllib.parse.urlencode({
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
    }).encode("utf-8")
    req = urllib.request.Request(token_endpoint, data=data,
                                 headers={"Content-Type": "application/x-www-form-urlencoded"})
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())["access_token"]


def api_post(access_token, api_endpoint, path, profile_id, client_id,
             body, content_type="application/json", accept="application/json"):
    url = f"{api_endpoint}{path}"
    all_items = []

    while True:
        req = urllib.request.Request(url, data=json.dumps(body).encode("utf-8"), headers={
            "Authorization": f"Bearer {access_token}",
            "Amazon-Advertising-API-ClientId": client_id,
            "Amazon-Advertising-API-Scope": profile_id,
            "Content-Type": content_type,
            "Accept": accept,
        })
        with urllib.request.urlopen(req) as resp:
            result = json.loads(resp.read())

        if isinstance(result, list):
            all_items.extend(result)
            break
        else:
            for key in ("productAds", "adGroups", "campaigns", "targets"):
                if key in result:
                    all_items.extend(result[key])
                    break
            else:
                break
            next_token = result.get("nextToken")
            if next_token:
                body["nextToken"] = next_token
                time.sleep(0.5)
            else:
                break

    return all_items


def api_get(access_token, api_endpoint, path, profile_id, client_id,
            accept="application/json"):
    url = f"{api_endpoint}{path}"
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {access_token}",
        "Amazon-Advertising-API-ClientId": client_id,
        "Amazon-Advertising-API-Scope": profile_id,
        "Accept": accept,
    })
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def fetch_or_load(data_dir, filename, fetch_fn):
    existing = sorted(data_dir.glob(f"*_{filename}.json"), reverse=True) if data_dir.exists() else []
    if existing:
        with open(existing[0], "r", encoding="utf-8") as f:
            data = json.load(f)
        print(f"    {filename}: mevcut ({existing[0].name}, {len(data)} kayit)")
        return data
    try:
        data = fetch_fn()
        data_dir.mkdir(parents=True, exist_ok=True)
        today = datetime.utcnow().strftime("%Y-%m-%d")
        save_path = data_dir / f"{today}_{filename}.json"
        with open(save_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"    {filename}: API'den cekildi ({len(data)} kayit)")
        return data
    except Exception as e:
        print(f"    {filename}: [HATA] {str(e)[:200]}")
        return []


def extract_asins_from_sp(product_ads):
    asins = {}
    for ad in product_ads:
        asin = ad.get("asin", "")
        if asin:
            if asin not in asins:
                asins[asin] = {"reklam_tipleri": set(), "state": ""}
            asins[asin]["reklam_tipleri"].add("SP")
            asins[asin]["state"] = ad.get("state", "")
    return asins


def extract_asins_from_sb(ad_groups):
    asins = {}
    for ag in ad_groups:
        for field in ("productASINs", "asins"):
            items = ag.get(field, [])
            if isinstance(items, list):
                for asin in items:
                    if asin and isinstance(asin, str):
                        if asin not in asins:
                            asins[asin] = {"reklam_tipleri": set(), "state": ""}
                        asins[asin]["reklam_tipleri"].add("SB")
                        asins[asin]["state"] = ag.get("state", "")
        creative = ag.get("creative", {})
        if isinstance(creative, dict):
            for asin in creative.get("asins", []):
                if asin and isinstance(asin, str):
                    if asin not in asins:
                        asins[asin] = {"reklam_tipleri": set(), "state": ""}
                    asins[asin]["reklam_tipleri"].add("SB")
    return asins


def extract_asins_from_sd(ad_groups):
    asins = {}
    for ag in ad_groups:
        for field in ("promotedASINs", "productAds"):
            items = ag.get(field, [])
            if isinstance(items, list):
                for item in items:
                    asin = item if isinstance(item, str) else (item.get("asin", "") if isinstance(item, dict) else "")
                    if asin:
                        if asin not in asins:
                            asins[asin] = {"reklam_tipleri": set(), "state": ""}
                        asins[asin]["reklam_tipleri"].add("SD")
                        asins[asin]["state"] = ag.get("state", "")
    return asins


def main():
    accounts = load_accounts()
    lwa = accounts["lwa_app"]

    print("=" * 70)
    print("  ASIN LISTESI CEKICI — SP + SB + SD — Tum Hesaplar")
    print("=" * 70)

    all_results = {}

    for hesap_key, hesap in accounts["hesaplar"].items():
        for mp_code, mp_config in hesap["marketplaces"].items():
            if not mp_config.get("aktif", False):
                continue

            dir_name = f"{hesap_key}_{mp_code}"
            data_dir = BASE_DIR / "data" / dir_name
            print(f"\n--- {dir_name} ---")

            token = None
            def ensure_token():
                nonlocal token
                if token is None:
                    token = get_access_token(
                        hesap["refresh_token"], lwa["client_id"],
                        lwa["client_secret"], hesap["token_endpoint"]
                    )
                return token

            # SP Product Ads
            sp_data = fetch_or_load(data_dir, "sp_product_ads", lambda: api_post(
                ensure_token(), hesap["api_endpoint"], "/sp/productAds/list",
                mp_config["profile_id"], lwa["client_id"],
                {"stateFilter": {"include": ["ENABLED", "PAUSED"]}, "maxResults": 1000},
                content_type=SP_PRODUCT_ADS_CT, accept=SP_PRODUCT_ADS_CT
            ))

            # SB Ad Groups
            sb_data = fetch_or_load(data_dir, "sb_ad_groups", lambda: api_post(
                ensure_token(), hesap["api_endpoint"], "/sb/v4/adGroups/list",
                mp_config["profile_id"], lwa["client_id"],
                {"stateFilter": {"include": ["ENABLED", "PAUSED"]}, "maxResults": 100},
                content_type=SB_AD_GROUPS_CT, accept=SB_AD_GROUPS_CT
            ))

            # SD Ad Groups
            sd_data = fetch_or_load(data_dir, "sd_ad_groups", lambda: api_get(
                ensure_token(), hesap["api_endpoint"],
                "/sd/adGroups?stateFilter=enabled,paused&count=1000",
                mp_config["profile_id"], lwa["client_id"]
            ))

            # ASIN birlestir
            asins = extract_asins_from_sp(sp_data)
            for asin, info in extract_asins_from_sb(sb_data).items():
                if asin in asins:
                    asins[asin]["reklam_tipleri"].update(info["reklam_tipleri"])
                else:
                    asins[asin] = info
            for asin, info in extract_asins_from_sd(sd_data).items():
                if asin in asins:
                    asins[asin]["reklam_tipleri"].update(info["reklam_tipleri"])
                else:
                    asins[asin] = info

            all_results[dir_name] = asins
            print(f"  Toplam: {len(asins)} benzersiz ASIN")
            time.sleep(1)

    # Kaydet
    output = {}
    for dir_name, asins in all_results.items():
        output[dir_name] = {
            "asin_sayisi": len(asins),
            "asinler": {
                asin: {"reklam_tipleri": sorted(list(info["reklam_tipleri"])), "state": info.get("state", "")}
                for asin, info in asins.items()
            }
        }

    output_path = BASE_DIR / "asin_report.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    # Ozet
    print(f"\n{'=' * 70}")
    print(f"  OZET")
    print(f"{'=' * 70}")
    for dir_name, asins in all_results.items():
        sp = sum(1 for a in asins.values() if "SP" in a["reklam_tipleri"])
        sb = sum(1 for a in asins.values() if "SB" in a["reklam_tipleri"])
        sd = sum(1 for a in asins.values() if "SD" in a["reklam_tipleri"])
        print(f"  {dir_name:25s} : {len(asins):3d} ASIN  (SP:{sp} SB:{sb} SD:{sd})")

    print(f"\n  Detay: {output_path}")
    print(f"  Bu dosyayi Claude'a gonder!\n")


if __name__ == "__main__":
    main()
