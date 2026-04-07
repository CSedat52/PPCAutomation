"""
Agent 4 Phase 2 — Direct Anthropic API Caller
================================================
Claude Code yerine dogrudan Anthropic API cagirarak hata analizi yapar.

Claude Code:  ~500K token/session, ~15 API call, 25K system prompt
Bu modul:     ~3-5K token/session, 1 API call, ~500 token system prompt

Kullanim:
    from maestro.agent4_api_caller import run_agent4_phase2
    success = run_agent4_phase2(hesap_key, marketplace, base_dir)
"""

import json
import logging
import os
import hashlib
from datetime import datetime

logger = logging.getLogger("maestro.agent4_api")

# Anthropic API ayarlari
API_URL = "https://api.anthropic.com/v1/messages"
MODEL = "claude-sonnet-4-6"
MAX_TOKENS = 4096

SYSTEM_PROMPT = """Sen Amazon PPC otomasyon sisteminin hata analizcisisin.
Sana verilen hata ve pipeline verilerini analiz et.
Tekrarlayan hata kaliplarini tespit et, kok neden analizi yap, iyilestirme onerileri uret.

Her oneri icin JSON formatinda dondur:
{
  "oneriler": [
    {
      "kategori": "ERROR_PREVENTION",
      "ne": "Ne degistirilecek",
      "neden": "Kanita dayali neden",
      "kanit": {"veri": "destekleyen veri"},
      "risk": "Olasi olumsuz etki",
      "kazanim": "Beklenen iyilesme",
      "degisecek_dosya": "config/... veya ilgili dosya",
      "degisecek_alan": {}
    }
  ],
  "ozet": "Kisa analiz ozeti"
}

Hata yoksa veya veri yetersizse: {"oneriler": [], "ozet": "Sorun tespit edilmedi"}
SADECE JSON dondur, baska bir sey yazma."""


def run_agent4_phase2(hesap_key: str, marketplace: str, base_dir: str) -> bool:
    """
    Agent 4 Phase 2: Hata analizi — dogrudan Anthropic API ile.

    Returns:
        bool: Basarili ise True
    """
    import httpx

    account_label = f"{hesap_key}/{marketplace}"

    # ---- 1. VERIYI OKU ----
    error_data = _load_error_data(hesap_key, marketplace, base_dir)
    if not error_data:
        logger.warning("Hata verisi bos veya okunamadi: %s — Phase 2 atlaniyor", account_label)
        return False

    # ---- 2. API KEY KONTROL ----
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        logger.error("ANTHROPIC_API_KEY bulunamadi — Phase 2 atlaniyor")
        return False

    # ---- 3. PROMPT OLUSTUR ----
    user_message = (
        f"Hesap: {hesap_key}/{marketplace}\n\n"
        f"HATA VERILERI:\n{json.dumps(error_data, ensure_ascii=False, default=str)}"
    )

    # ---- 4. API CAGRI ----
    logger.info("Anthropic API cagriliyor: %s (model: %s)", account_label, MODEL)
    try:
        with httpx.Client(timeout=60) as client:
            response = client.post(
                API_URL,
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": MODEL,
                    "max_tokens": MAX_TOKENS,
                    "system": SYSTEM_PROMPT,
                    "messages": [
                        {"role": "user", "content": user_message}
                    ],
                },
            )
            response.raise_for_status()
    except httpx.TimeoutException:
        logger.error("Anthropic API timeout (60s): %s", account_label)
        return False
    except httpx.HTTPStatusError as e:
        logger.error("Anthropic API hata %d: %s", e.response.status_code,
                      e.response.text[:300])
        return False
    except Exception as e:
        logger.error("Anthropic API baglanti hatasi: %s", e)
        return False

    # ---- 5. RESPONSE PARSE ----
    result = response.json()
    usage = result.get("usage", {})
    input_tokens = usage.get("input_tokens", 0)
    output_tokens = usage.get("output_tokens", 0)
    logger.info("API kullanim: input=%d, output=%d, toplam=%d token",
                input_tokens, output_tokens, input_tokens + output_tokens)

    # Text content'i cek
    text_content = ""
    for block in result.get("content", []):
        if block.get("type") == "text":
            text_content += block.get("text", "")

    if not text_content:
        logger.warning("API bos response dondurdu: %s", account_label)
        return False

    # ---- 6. JSON PARSE ----
    proposals = _parse_proposals(text_content)
    if proposals is None:
        logger.warning("API response JSON parse edilemedi: %s", account_label)
        return False

    oneriler = proposals.get("oneriler", [])
    ozet = proposals.get("ozet", "")

    if not oneriler:
        logger.info("Hata analizi tamamlandi — oneri yok: %s (%s)",
                     account_label, ozet)
        return True

    # ---- 7. SUPABASE'E YAZ ----
    yazilan = _write_proposals_to_supabase(hesap_key, marketplace, oneriler)
    logger.info("Phase 2 tamamlandi: %d oneri yazildi (%s) — %s",
                yazilan, ozet, account_label)

    return True


# ============================================================================
# YARDIMCI FONKSIYONLAR
# ============================================================================

def _load_error_data(hesap_key: str, marketplace: str, base_dir: str) -> dict:
    """Phase 1'in olusturdugu agent4_error_data.json dosyasini oku."""
    error_data_path = os.path.join(
        base_dir, "data", f"{hesap_key}_{marketplace}",
        "agent4", "agent4_error_data.json"
    )
    try:
        if os.path.exists(error_data_path):
            with open(error_data_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            logger.info("Hata verisi yuklendi: %s (%d byte)",
                        error_data_path, os.path.getsize(error_data_path))
            return data
    except (json.JSONDecodeError, IOError) as e:
        logger.error("Hata verisi okunamadi: %s — %s", error_data_path, e)

    return {}


def _parse_proposals(text: str) -> dict:
    """API response'undan JSON'u parse et."""
    # Markdown code block varsa cikar
    cleaned = text.strip()
    if cleaned.startswith("```"):
        lines = cleaned.split("\n")
        # Ilk ve son satiri at (```json ve ```)
        lines = [l for l in lines if not l.strip().startswith("```")]
        cleaned = "\n".join(lines).strip()

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        # JSON objesini bulmaya calis
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start >= 0 and end > start:
            try:
                return json.loads(cleaned[start:end + 1])
            except json.JSONDecodeError:
                pass

    logger.error("JSON parse hatasi. Response: %s", text[:500])
    return None


def _write_proposals_to_supabase(hesap_key: str, marketplace: str,
                                  oneriler: list) -> int:
    """Onerileri Supabase proposals tablosuna yaz."""
    try:
        import sys
        base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if base not in sys.path:
            sys.path.insert(0, base)
        from supabase.db_client import SupabaseClient
        sdb = SupabaseClient()
    except Exception as e:
        logger.error("Supabase baglantisi kurulamadi: %s", e)
        return 0

    yazilan = 0
    today = datetime.utcnow().strftime("%Y-%m-%d")

    for oneri in oneriler:
        try:
            # Proposal ID olustur
            icerik = f"{oneri.get('kategori', '')}|{oneri.get('ne', '')}|{today}"
            proposal_id = "ONR-" + hashlib.md5(icerik.encode()).hexdigest()[:8].upper()

            sdb.upsert_proposal(hesap_key, marketplace, {
                "id":               proposal_id,
                "tarih":            today,
                "kategori":         oneri.get("kategori", "ERROR_PREVENTION"),
                "durum":            "PENDING",
                "ne":               oneri.get("ne", ""),
                "neden":            oneri.get("neden", ""),
                "kanit":            oneri.get("kanit", {}),
                "risk":             oneri.get("risk", ""),
                "kazanim":          oneri.get("kazanim", ""),
                "beklenen_sonuc":   oneri.get("risk", ""),
                "gerceklesen_sonuc": oneri.get("kazanim", ""),
                "degisecek_dosya":  oneri.get("degisecek_dosya", ""),
                "degisecek_alan":   oneri.get("degisecek_alan", {}),
            })
            yazilan += 1
        except Exception as e:
            logger.warning("Oneri yazilamadi: %s — %s", oneri.get("ne", "?"), e)

    return yazilan
