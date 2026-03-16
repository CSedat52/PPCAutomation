"""
SB Keyword Bid Update — Header Test
=====================================
4 farkli Content-Type/Accept kombinasyonunu test eder.
Dogru calisan kombinasyonu bulur.

Kullanim: python test_sb_keyword_update.py
"""

import json
import httpx

# --- Config yukle ---
accounts = json.load(open("config/accounts.json", encoding="utf-8"))
lwa = accounts["lwa_app"]
hesap = accounts["hesaplar"]["vigowood_eu"]
mp = hesap["marketplaces"]["UK"]

# --- Token al ---
token_resp = httpx.post(hesap["token_endpoint"], data={
    "grant_type": "refresh_token",
    "refresh_token": hesap["refresh_token"],
    "client_id": lwa["client_id"],
    "client_secret": lwa["client_secret"],
})
token = token_resp.json()["access_token"]
print(f"Token alindi.\n")

# --- Bir SB keyword bul (test icin) ---
base_url = hesap["api_endpoint"]
headers_get = {
    "Authorization": f"Bearer {token}",
    "Amazon-Advertising-API-ClientId": lwa["client_id"],
    "Amazon-Advertising-API-Scope": mp["profile_id"],
    "Accept": "application/vnd.sbkeyword.v3.2+json",
}

resp = httpx.get(f"{base_url}/sb/keywords?stateFilter=enabled,paused&count=5", headers=headers_get)
keywords = resp.json()
if not keywords:
    print("SB keyword bulunamadi! Test yapilamaz.")
    exit()

# Test icin ilk keyword'u sec
test_kw = keywords[0]
kw_id = test_kw["keywordId"]
current_bid = test_kw.get("bid", 0)
campaign_id = test_kw.get("campaignId", "")
ad_group_id = test_kw.get("adGroupId", "")
state = test_kw.get("state", "enabled")

print(f"Test keyword: {test_kw.get('keywordText', '?')}")
print(f"  keywordId: {kw_id}")
print(f"  campaignId: {campaign_id}")
print(f"  adGroupId: {ad_group_id}")
print(f"  state: {state}")
print(f"  current bid: {current_bid}")
print(f"  test bid: {current_bid} (AYNI bid — degisiklik yok)")
print()

# --- 4 kombinasyon test et ---
SB_VER = "application/vnd.sbkeyword.v3.2+json"
APP_JSON = "application/json"

combinations = [
    ("json/json",     APP_JSON, APP_JSON),
    ("json/versioned", APP_JSON, SB_VER),
    ("versioned/json", SB_VER,  APP_JSON),
    ("versioned/versioned", SB_VER, SB_VER),
]

# Payload: ayni bid degerini gonder (degisiklik olmaz, guvenli test)
payload = [{"keywordId": kw_id, "bid": current_bid, "state": state}]

# Bazi API'ler campaignId/adGroupId de bekler
payload_full = [{"keywordId": kw_id, "bid": current_bid, "state": state,
                 "campaignId": campaign_id, "adGroupId": ad_group_id}]

print("=" * 60)
print("TEST 1: Minimal payload (sadece keywordId + bid + state)")
print("=" * 60)

for name, ct, accept in combinations:
    headers = {
        "Authorization": f"Bearer {token}",
        "Amazon-Advertising-API-ClientId": lwa["client_id"],
        "Amazon-Advertising-API-Scope": mp["profile_id"],
        "Content-Type": ct,
        "Accept": accept,
    }
    try:
        resp = httpx.put(f"{base_url}/sb/keywords", headers=headers, json=payload, timeout=30)
        status = resp.status_code
        body = resp.text[:300]
        result = "BASARILI" if status < 400 else "BASARISIZ"
        print(f"\n[{name}] CT={ct}")
        print(f"  Accept={accept}")
        print(f"  Status: {status} — {result}")
        print(f"  Body: {body[:200]}")
    except Exception as e:
        print(f"\n[{name}] EXCEPTION: {e}")

print()
print("=" * 60)
print("TEST 2: Full payload (keywordId + bid + state + campaignId + adGroupId)")
print("=" * 60)

for name, ct, accept in combinations:
    headers = {
        "Authorization": f"Bearer {token}",
        "Amazon-Advertising-API-ClientId": lwa["client_id"],
        "Amazon-Advertising-API-Scope": mp["profile_id"],
        "Content-Type": ct,
        "Accept": accept,
    }
    try:
        resp = httpx.put(f"{base_url}/sb/keywords", headers=headers, json=payload_full, timeout=30)
        status = resp.status_code
        body = resp.text[:300]
        result = "BASARILI" if status < 400 else "BASARISIZ"
        print(f"\n[{name}] CT={ct}")
        print(f"  Accept={accept}")
        print(f"  Status: {status} — {result}")
        print(f"  Body: {body[:200]}")
    except Exception as e:
        print(f"\n[{name}] EXCEPTION: {e}")

print()
print("=" * 60)
print("TAMAMLANDI")
print("=" * 60)
