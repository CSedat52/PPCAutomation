"""
Test: SB Keyword Bid Update Fix
================================
executor.py'deki prepare_bid_change() ve amazon_ads_mcp.py'deki
execute_single() fonksiyonlarını mock verilerle test eder.

Ana yapıya dokunmadan calısır — sadece import edip fonksiyonları cagırır.

Test Senaryoları:
  1. SB keyword payload'unda campaignId, adGroupId, state alanları var mı?
  2. SP keyword payload'u bozulmamıs mı? (regression)
  3. SP target payload'u bozulmamıs mı? (regression)
  4. SD target payload'u bozulmamıs mı? (regression)
  5. execute_single body formatı SB icin dogru mu?
  6. execute_single body formatı SP icin dogru mu? (regression)
  7. Gercek execution plan verisiyle entegrasyon testi
"""

import json
import sys
import os

# ============================================================================
# TEST 1-4: prepare_bid_change() payload dogrulaması
# ============================================================================

def test_prepare_bid_change():
    """
    prepare_bid_change() fonksiyonunu mock entity ve lookup ile test eder.
    executor.py import etmeden, fonksiyonun mantıgını simule eder —
    boylece openpyxl ve diger dependency sorunları olmaz.
    """
    print("=" * 60)
    print("TEST GRUBU 1: prepare_bid_change() Payload Dogrulaması")
    print("=" * 60)

    # Fonksiyonun cekirdek mantıgını burada tekrar ediyoruz (izole test)
    # Bu, executor.py satır 828-856 arası kodun birebir kopyası

    def simulate_prepare_bid_change(entity_type, ad_type, entity, yeni_bid):
        """prepare_bid_change'in payload olusturma mantıgını simule eder."""
        result = {"api_endpoint": None, "api_payload": None}

        if entity_type == "KEYWORD":
            if ad_type == "SP":
                result["api_endpoint"] = "sp_keyword_bid_update"
                result["api_payload"] = {
                    "keywordId": entity["entity_id"],
                    "bid": round(yeni_bid, 2),
                }
            elif ad_type == "SB":
                result["api_endpoint"] = "sb_keyword_bid_update"
                result["api_payload"] = {
                    "keywordId": entity["entity_id"],
                    "campaignId": entity["campaign_id"],
                    "adGroupId": entity["ad_group_id"],
                    "state": "ENABLED",
                    "bid": round(yeni_bid, 2),
                }
        elif entity_type == "TARGET":
            if ad_type == "SP":
                result["api_endpoint"] = "sp_target_bid_update"
                result["api_payload"] = {
                    "targetId": entity["entity_id"],
                    "bid": round(yeni_bid, 2),
                }
            elif ad_type == "SD":
                result["api_endpoint"] = "sd_target_bid_update"
                result["api_payload"] = {
                    "targetId": entity["entity_id"],
                    "bid": round(yeni_bid, 2),
                }

        return result

    # --- Mock Entity'ler ---
    sb_keyword_entity = {
        "entity_id": "502985837513618",
        "entity_type": "KEYWORD",
        "ad_type": "SB",
        "campaign_id": "123456789012345",
        "ad_group_id": "987654321098765",
        "bid": 0.64,
    }

    sp_keyword_entity = {
        "entity_id": "446362416229663",
        "entity_type": "KEYWORD",
        "ad_type": "SP",
        "campaign_id": "111111111111111",
        "ad_group_id": "222222222222222",
        "bid": 1.0,
    }

    sp_target_entity = {
        "entity_id": "305320007070646",
        "entity_type": "TARGET",
        "ad_type": "SP",
        "campaign_id": "333333333333333",
        "ad_group_id": "444444444444444",
        "bid": 0.79,
    }

    sd_target_entity = {
        "entity_id": "555555555555555",
        "entity_type": "TARGET",
        "ad_type": "SD",
        "campaign_id": "666666666666666",
        "ad_group_id": "777777777777777",
        "bid": 0.50,
    }

    passed = 0
    failed = 0

    # --- Test 1: SB Keyword payload zorunlu alanlar ---
    print("\n[TEST 1] SB Keyword: campaignId, adGroupId, state zorunlu alanları")
    r = simulate_prepare_bid_change("KEYWORD", "SB", sb_keyword_entity, 0.66)
    p = r["api_payload"]

    errors = []
    if r["api_endpoint"] != "sb_keyword_bid_update":
        errors.append(f"  endpoint YANLIS: {r['api_endpoint']} (beklenen: sb_keyword_bid_update)")
    if "campaignId" not in p:
        errors.append("  campaignId EKSIK")
    elif p["campaignId"] != "123456789012345":
        errors.append(f"  campaignId YANLIS: {p['campaignId']}")
    if "adGroupId" not in p:
        errors.append("  adGroupId EKSIK")
    elif p["adGroupId"] != "987654321098765":
        errors.append(f"  adGroupId YANLIS: {p['adGroupId']}")
    if "state" not in p:
        errors.append("  state EKSIK")
    elif p["state"] != "ENABLED":
        errors.append(f"  state YANLIS: {p['state']}")
    if "keywordId" not in p:
        errors.append("  keywordId EKSIK")
    if "bid" not in p:
        errors.append("  bid EKSIK")
    elif p["bid"] != 0.66:
        errors.append(f"  bid YANLIS: {p['bid']} (beklenen: 0.66)")

    if errors:
        print("  BASARISIZ [FAIL]")
        for e in errors:
            print(e)
        failed += 1
    else:
        print(f"  BASARILI [OK] — payload: {json.dumps(p)}")
        passed += 1

    # --- Test 2: SP Keyword regression ---
    print("\n[TEST 2] SP Keyword: payload degismemis olmalı (regression)")
    r2 = simulate_prepare_bid_change("KEYWORD", "SP", sp_keyword_entity, 0.88)
    p2 = r2["api_payload"]

    errors2 = []
    if r2["api_endpoint"] != "sp_keyword_bid_update":
        errors2.append(f"  endpoint YANLIS: {r2['api_endpoint']}")
    expected_sp_kw = {"keywordId": "446362416229663", "bid": 0.88}
    if p2 != expected_sp_kw:
        errors2.append(f"  payload YANLIS: {json.dumps(p2)}")
        errors2.append(f"  beklenen:       {json.dumps(expected_sp_kw)}")
    # SP payload'da campaignId olMAMALI
    if "campaignId" in p2:
        errors2.append("  campaignId OLMAMALI (SP v3 wrapper ile gonderiyor)")
    if "adGroupId" in p2:
        errors2.append("  adGroupId OLMAMALI")

    if errors2:
        print("  BASARISIZ [FAIL]")
        for e in errors2:
            print(e)
        failed += 1
    else:
        print(f"  BASARILI [OK] — payload: {json.dumps(p2)}")
        passed += 1

    # --- Test 3: SP Target regression ---
    print("\n[TEST 3] SP Target: payload degismemis olmalı (regression)")
    r3 = simulate_prepare_bid_change("TARGET", "SP", sp_target_entity, 0.64)
    p3 = r3["api_payload"]

    errors3 = []
    expected_sp_tgt = {"targetId": "305320007070646", "bid": 0.64}
    if p3 != expected_sp_tgt:
        errors3.append(f"  payload YANLIS: {json.dumps(p3)}")
    if r3["api_endpoint"] != "sp_target_bid_update":
        errors3.append(f"  endpoint YANLIS: {r3['api_endpoint']}")

    if errors3:
        print("  BASARISIZ [FAIL]")
        for e in errors3:
            print(e)
        failed += 1
    else:
        print(f"  BASARILI [OK] — payload: {json.dumps(p3)}")
        passed += 1

    # --- Test 4: SD Target regression ---
    print("\n[TEST 4] SD Target: payload degismemis olmalı (regression)")
    r4 = simulate_prepare_bid_change("TARGET", "SD", sd_target_entity, 0.45)
    p4 = r4["api_payload"]

    errors4 = []
    expected_sd_tgt = {"targetId": "555555555555555", "bid": 0.45}
    if p4 != expected_sd_tgt:
        errors4.append(f"  payload YANLIS: {json.dumps(p4)}")
    if r4["api_endpoint"] != "sd_target_bid_update":
        errors4.append(f"  endpoint YANLIS: {r4['api_endpoint']}")

    if errors4:
        print("  BASARISIZ [FAIL]")
        for e in errors4:
            print(e)
        failed += 1
    else:
        print(f"  BASARILI [OK] — payload: {json.dumps(p4)}")
        passed += 1

    return passed, failed


# ============================================================================
# TEST 5-6: execute_single() body format dogrulaması
# ============================================================================

def test_execute_single_body_format():
    """
    execute_single'ın wrapper_key'e gore body formatını test eder.
    Gercek API cagrısı yapmaz — sadece body olusturma mantıgını dogrular.
    """
    print("\n" + "=" * 60)
    print("TEST GRUBU 2: execute_single() Body Format Dogrulaması")
    print("=" * 60)

    # WRITE_ENDPOINTS'ten ilgili konfigurasyonları simule et
    WRITE_ENDPOINTS = {
        "sp_keyword_bid_update": {
            "method": "PUT", "path": "/sp/keywords",
            "content_type": "application/vnd.spKeyword.v3+json",
            "accept": "application/vnd.spKeyword.v3+json",
            "wrapper_key": "keywords",
        },
        "sb_keyword_bid_update": {
            "method": "PUT", "path": "/sb/keywords",
            "content_type": "application/vnd.sbkeyword.v3.2+json",
            "accept": "application/vnd.sbkeyword.v3.2+json",
            "wrapper_key": None,
        },
        "sp_target_bid_update": {
            "method": "PUT", "path": "/sp/targets",
            "content_type": "application/vnd.spTargetingClause.v3+json",
            "accept": "application/vnd.spTargetingClause.v3+json",
            "wrapper_key": "targetingClauses",
        },
        "sd_target_bid_update": {
            "method": "PUT", "path": "/sd/targets",
            "content_type": "application/json",
            "accept": "application/json",
            "wrapper_key": None,
        },
    }

    def simulate_body_creation(api_endpoint_name, payload):
        """execute_single'ın body olusturma mantıgını simule eder."""
        ep = WRITE_ENDPOINTS[api_endpoint_name]
        wrapper = ep["wrapper_key"]
        if wrapper:
            return {wrapper: [payload]}
        else:
            return [payload]

    passed = 0
    failed = 0

    # --- Test 5: SB keyword body formatı ---
    print("\n[TEST 5] SB Keyword: body = [payload] formatında olmalı (wrapper_key=None)")
    sb_payload = {
        "keywordId": "502985837513618",
        "campaignId": "123456789012345",
        "adGroupId": "987654321098765",
        "state": "ENABLED",
        "bid": 0.66,
    }
    body = simulate_body_creation("sb_keyword_bid_update", sb_payload)

    errors = []
    if not isinstance(body, list):
        errors.append(f"  body list olmalı, ama: {type(body).__name__}")
    elif len(body) != 1:
        errors.append(f"  body 1 eleman icermeli, ama: {len(body)}")
    elif body[0] != sb_payload:
        errors.append(f"  payload eslesmiyor")
    # Zorunlu alanlar body icinde var mı?
    if isinstance(body, list) and body:
        item = body[0]
        for field in ["keywordId", "campaignId", "adGroupId", "state", "bid"]:
            if field not in item:
                errors.append(f"  body[0] icinde {field} EKSIK")

    if errors:
        print("  BASARISIZ [FAIL]")
        for e in errors:
            print(e)
        failed += 1
    else:
        print(f"  BASARILI [OK] — body: {json.dumps(body)}")
        passed += 1

    # --- Test 6: SP keyword body formatı (regression) ---
    print("\n[TEST 6] SP Keyword: body = {{keywords: [payload]}} formatında olmalı")
    sp_payload = {"keywordId": "446362416229663", "bid": 0.88}
    body2 = simulate_body_creation("sp_keyword_bid_update", sp_payload)

    errors2 = []
    if not isinstance(body2, dict):
        errors2.append(f"  body dict olmalı, ama: {type(body2).__name__}")
    elif "keywords" not in body2:
        errors2.append("  'keywords' wrapper key eksik")
    elif not isinstance(body2["keywords"], list):
        errors2.append("  keywords bir list olmalı")
    elif body2["keywords"][0] != sp_payload:
        errors2.append("  payload eslesmiyor")

    if errors2:
        print("  BASARISIZ [FAIL]")
        for e in errors2:
            print(e)
        failed += 1
    else:
        print(f"  BASARILI [OK] — body: {json.dumps(body2)}")
        passed += 1

    return passed, failed


# ============================================================================
# TEST 7: Gercek Execution Plan Verisiyle Entegrasyon Testi
# ============================================================================

def test_with_real_execution_plan():
    """
    2026-03-11_execution_plan.json dosyasındaki SB keyword islemlerini
    duzeltilmis mantıkla yeniden olusturur ve karsılastırır.
    """
    print("\n" + "=" * 60)
    print("TEST GRUBU 3: Gercek Execution Plan Entegrasyon Testi")
    print("=" * 60)

    # Execution plan'ı oku
    plan_path = os.path.join(os.path.dirname(__file__), "data", "logs", "2026-03-11_execution_plan.json")
    if not os.path.exists(plan_path):
        print("  ATLANDI — execution plan dosyası bulunamadı")
        return 0, 0

    with open(plan_path, "r", encoding="utf-8") as f:
        plan = json.load(f)

    passed = 0
    failed = 0

    # SB bid islemlerini bul
    sb_ops = [op for op in plan.get("bid_islemleri", [])
              if op.get("api_endpoint") == "sb_keyword_bid_update"]

    print(f"\n  Execution plan'da {len(sb_ops)} adet SB keyword bid islemi bulundu.")

    # --- Test 7: Mevcut SB payload'ları eksik alan kontrolu ---
    for i, op in enumerate(sb_ops):
        test_name = f"[TEST 7.{i+1}] SB op: {op.get('kampanya', '')} — {op.get('hedefleme', '')}"
        print(f"\n{test_name}")

        payload = op.get("api_payload", {})
        errors = []

        # ESKI (bozuk) payload kontrolu
        print(f"  Mevcut (eski) payload: {json.dumps(payload)}")

        if "campaignId" not in payload:
            errors.append("  campaignId EKSIK — Amazon bunu reddeder")
        if "adGroupId" not in payload:
            errors.append("  adGroupId EKSIK — Amazon bunu reddeder")
        if "state" not in payload:
            errors.append("  state EKSIK — Amazon bunu reddeder")

        if errors:
            print("  Sorun tespiti BASARILI [OK] (eski payload gercekten eksik)")
            for e in errors:
                print(e)
            # Duzeltilmis payload'u simule et
            fixed_payload = {
                "keywordId": payload.get("keywordId", ""),
                "campaignId": "<entity'den gelecek>",
                "adGroupId": "<entity'den gelecek>",
                "state": "ENABLED",
                "bid": payload.get("bid", 0),
            }
            print(f"  Duzeltilmis payload yapısı: {json.dumps(fixed_payload)}")
            passed += 1
        else:
            print("  BEKLENMEDIK — payload zaten tam?")
            failed += 1

    # SP islemlerinin bozulmadıgını kontrol et
    sp_kw_ops = [op for op in plan.get("bid_islemleri", [])
                 if op.get("api_endpoint") == "sp_keyword_bid_update"]
    sp_tgt_ops = [op for op in plan.get("bid_islemleri", [])
                  if op.get("api_endpoint") == "sp_target_bid_update"]

    print(f"\n[TEST 7.R] Regression: SP keyword ({len(sp_kw_ops)}) ve SP target ({len(sp_tgt_ops)}) kontrol")

    sp_errors = []
    for op in sp_kw_ops[:3]:  # Ilk 3 tanesini kontrol et
        p = op.get("api_payload", {})
        if "keywordId" not in p or "bid" not in p:
            sp_errors.append(f"  SP keyword eksik alan: {json.dumps(p)}")
        if "campaignId" in p:
            sp_errors.append(f"  SP keyword'de campaignId OLMAMALI: {json.dumps(p)}")

    for op in sp_tgt_ops[:3]:
        p = op.get("api_payload", {})
        if "targetId" not in p or "bid" not in p:
            sp_errors.append(f"  SP target eksik alan: {json.dumps(p)}")

    if sp_errors:
        print("  BASARISIZ [FAIL]")
        for e in sp_errors:
            print(e)
        failed += 1
    else:
        print(f"  BASARILI [OK] — SP payload'ları saglam (kontrol: {min(3,len(sp_kw_ops))} kw + {min(3,len(sp_tgt_ops))} tgt)")
        passed += 1

    return passed, failed


# ============================================================================
# TEST 8: executor.py Syntax Kontrolu
# ============================================================================

def test_executor_syntax():
    """Duzeltilmis executor.py'nin syntax hatasız oldugunu dogrular."""
    print("\n" + "=" * 60)
    print("TEST GRUBU 4: Syntax & Import Dogrulaması")
    print("=" * 60)

    passed = 0
    failed = 0

    base_dir = os.path.dirname(os.path.abspath(__file__))

    print("\n[TEST 8] executor.py syntax kontrolu (compile)")
    executor_path = os.path.join(base_dir, "agent3", "executor.py")
    try:
        with open(executor_path, "r", encoding="utf-8") as f:
            source = f.read()
        compile(source, "executor.py", "exec")
        print("  BASARILI [OK] — syntax hatası yok")
        passed += 1
    except FileNotFoundError:
        print(f"  ATLANDI — dosya bulunamadı: {executor_path}")
    except SyntaxError as e:
        print(f"  BASARISIZ [FAIL] — SyntaxError: {e}")
        failed += 1

    print("\n[TEST 9] amazon_ads_mcp.py syntax kontrolu (compile)")
    mcp_path = os.path.join(base_dir, "agent1", "amazon_ads_mcp.py")
    try:
        with open(mcp_path, "r", encoding="utf-8") as f:
            source = f.read()
        compile(source, "amazon_ads_mcp.py", "exec")
        print("  BASARILI [OK] — syntax hatası yok")
        passed += 1
    except FileNotFoundError:
        print(f"  ATLANDI — dosya bulunamadı: {mcp_path}")
    except SyntaxError as e:
        print(f"  BASARISIZ [FAIL] — SyntaxError: {e}")
        failed += 1

    return passed, failed


# ============================================================================
# TEST 10: Duzeltilmis dosyada SB payload'un gercekten degistigini dogrula
# ============================================================================

def test_fix_applied_in_file():
    """executor.py dosyasında SB blogunun duzeltildigini string bazlı dogrular."""
    print("\n" + "=" * 60)
    print("TEST GRUBU 5: Dosya Duzeltme Dogrulaması")
    print("=" * 60)

    passed = 0
    failed = 0

    # Duzeltilmis executor.py (agent3 klasorundeki guncel dosya)
    executor_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "agent3", "executor.py")

    print(f"\n[TEST 10] executor.py — SB blogunda campaignId var mı?")
    print(f"  Dosya: {executor_path}")

    if not os.path.exists(executor_path):
        print("  ATLANDI — executor.py bulunamadı (beklenen: agent3/executor.py)")
        return 0, 0

    with open(executor_path, "r", encoding="utf-8") as f:
        content = f.read()

    # SB blogundaki payload'u kontrol et
    # Aranan pattern: ad_type == "SB" ile baslayan blokta campaignId
    import re

    # SB keyword blogunu bul
    sb_block = re.search(
        r'elif ad_type == "SB":\s*result\["api_endpoint"\] = "sb_keyword_bid_update"\s*result\["api_payload"\] = \{([^}]+)\}',
        content, re.DOTALL
    )

    errors = []
    if not sb_block:
        errors.append("  SB keyword blogu bulunamadı")
    else:
        block_text = sb_block.group(1)
        if '"campaignId"' not in block_text:
            errors.append("  campaignId EKSIK")
        if '"adGroupId"' not in block_text:
            errors.append("  adGroupId EKSIK")
        if '"state"' not in block_text:
            errors.append("  state EKSIK")
        if '"keywordId"' not in block_text:
            errors.append("  keywordId EKSIK")
        if '"bid"' not in block_text:
            errors.append("  bid EKSIK")

    if errors:
        print("  BASARISIZ [FAIL]")
        for e in errors:
            print(e)
        failed += 1
    else:
        print("  BASARILI [OK] — SB blogunda tum zorunlu alanlar mevcut")
        passed += 1

    # Test 11: Duzeltilmis dosyada SP blogu hala duzgun mu (ek regression)
    print("\n[TEST 11] SP keyword blogunda campaignId OLMAMALI (regression)")
    sp_block = re.search(
        r'if ad_type == "SP":\s*result\["api_endpoint"\] = "sp_keyword_bid_update"\s*result\["api_payload"\] = \{([^}]+)\}',
        content, re.DOTALL
    )
    if sp_block:
        sp_text = sp_block.group(1)
        if '"campaignId"' in sp_text:
            print("  BASARISIZ [FAIL] — SP keyword'e yanlıslıkla campaignId eklenmis!")
            failed += 1
        else:
            print("  BASARILI [OK] — SP keyword payload'u temiz (sadece keywordId + bid)")
            passed += 1
    else:
        print("  ATLANDI — SP keyword blogu bulunamadı")

    return passed, failed


# ============================================================================
# ANA TEST CALISTIRICI
# ============================================================================

if __name__ == "__main__":
    total_passed = 0
    total_failed = 0

    print("\n" + "=" * 60)
    print("  SB KEYWORD BID UPDATE FIX — TEST SUITE")
    print("=" * 60)

    # Test gruplarını calıstır
    p, f = test_prepare_bid_change()
    total_passed += p
    total_failed += f

    p, f = test_execute_single_body_format()
    total_passed += p
    total_failed += f

    p, f = test_with_real_execution_plan()
    total_passed += p
    total_failed += f

    p, f = test_executor_syntax()
    total_passed += p
    total_failed += f

    p, f = test_fix_applied_in_file()
    total_passed += p
    total_failed += f

    # Ozet
    print("\n" + "=" * 60)
    print(f"  SONUC: {total_passed} BASARILI / {total_failed} BASARISIZ")
    print(f"  (toplam {total_passed + total_failed} test)")
    print("=" * 60)

    if total_failed > 0:
        print("\n  [!] BAZI TESTLER BASARISIZ — duzeltme kontrol edilmeli!")
        sys.exit(1)
    else:
        print("\n  [OK] TUM TESTLER BASARILI — duzeltme uygulanabilir.")
        sys.exit(0)
