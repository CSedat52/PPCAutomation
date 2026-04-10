"""
Agent 4 Phase 2 — Direct Anthropic API Caller
================================================
Claude Code subprocess yerine dogrudan Anthropic API cagrisi yapar.
agent4_error_data.json'u okur, hata kaliplarini analiz eder,
cozum onerileri uretir ve proposals_system tablosuna yazar.

Maliyet: ~$0.01-0.02 per calisma (tek API call, ~200 token system prompt)
"""

import json
import logging
import os
from pathlib import Path

logger = logging.getLogger("agent4.api_caller")

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
MODEL = "claude-sonnet-4-20250514"

SYSTEM_PROMPT = """Sen Amazon PPC otomasyon sisteminin hata analiz asistanisin.
Sana pipeline hata verileri verilecek. Gorevlerin:
1. Tekrarlayan hata kaliplarini bul, kok neden tespit et
2. Somut cozum onerileri uret (config degisikligi, retry ayari, timeout vb.)
3. Bid param optimizasyonu YAPMA — bu Python tarafindan yapiliyor

Her oneri icin su 5 soruyu yanitla:
- ne: Ne degistirilecek?
- neden: Kanita dayali neden?
- kanit: Hangi veri?
- risk: Olasi olumsuz etki?
- kazanim: Beklenen iyilesme?

SADECE JSON formatinda yanit ver, baska hicbir sey yazma.
Format:
{"oneriler": [{"kategori": "...", "ne": "...", "neden": "...", "kanit": "...", "risk": "...", "kazanim": "..."}]}

Eger oneri yoksa: {"oneriler": []}
KRITIK KURALLAR:
- Eger bir hata tipi son_7_tipler'de YOKSA, bu sorun COZULMUS demektir — oneri URETME.
- Ayni konu icin birden fazla oneri uretme — en iyi tek oneriyi sec.
- Duplike oneri uretme — farkli kelimelerle ayni seyi soylemek yasak.

Kategori tipleri: CONFIG, RETRY, TIMEOUT, BUGFIX, MONITORING, ARCHITECTURE"""


def run_agent4_phase2(hesap_key: str, marketplace: str, base_dir) -> bool:
    """Agent 4 Phase 2: Hata verilerini analiz et, oneri uret.
    
    Returns:
        True: oneriler basariyla yazildi
        False: hata veya oneri yok
    """
    base_dir = Path(base_dir)
    dir_name = f"{hesap_key}_{marketplace}"
    error_data_path = base_dir / "data" / dir_name / "agent4" / "agent4_error_data.json"

    # --- 1. Hata verisini oku ---
    if not error_data_path.exists():
        logger.warning("agent4_error_data.json bulunamadi: %s", error_data_path)
        return False

    try:
        with open(error_data_path, "r", encoding="utf-8") as f:
            error_data = json.load(f)
    except Exception as e:
        logger.error("Hata verisi okunamadi: %s", e)
        return False

    # Hata yoksa API cagrisina gerek yok
    hata_analizi = error_data.get("hata_analizi", {})
    toplam_hata = sum(
        v.get("toplam", 0) for k, v in hata_analizi.items()
        if isinstance(v, dict) and "toplam" in v
    )
    if toplam_hata == 0:
        logger.info("Hata yok — Phase 2 atlanıyor")
        return False

    # --- 2. Anthropic API cagrisi ---
    if not ANTHROPIC_API_KEY:
        logger.error("ANTHROPIC_API_KEY env variable bulunamadi")
        return False

    try:
        import httpx
    except ImportError:
        logger.error("httpx kurulu degil — pip install httpx gerekli")
        return False

    user_content = json.dumps(error_data, ensure_ascii=False)
    # Token tasarrufu: cok buyukse kes
    if len(user_content) > 15000:
        user_content = user_content[:15000] + "\n...(kesildi)"

    payload = {
        "model": MODEL,
        "max_tokens": 1000,
        "system": SYSTEM_PROMPT,
        "messages": [{"role": "user", "content": user_content}],
    }

    try:
        with httpx.Client(timeout=60) as client:
            resp = client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json=payload,
            )
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        logger.error("Anthropic API hatasi: %s", e)
        return False

    # --- 3. Yaniti parse et ---
    raw_text = ""
    for block in data.get("content", []):
        if block.get("type") == "text":
            raw_text += block.get("text", "")

    if not raw_text.strip():
        logger.warning("API bos yanit dondu")
        return False

    # JSON fences temizle
    clean = raw_text.strip()
    if clean.startswith("```"):
        clean = clean.split("\n", 1)[-1]  # ilk satiri at
    if clean.endswith("```"):
        clean = clean.rsplit("```", 1)[0]
    clean = clean.strip()

    try:
        result = json.loads(clean)
    except json.JSONDecodeError as e:
        logger.error("API yaniti JSON parse hatasi: %s — yanit: %s", e, raw_text[:500])
        return False

    oneriler = result.get("oneriler", [])
    if not oneriler:
        logger.info("API oneri uretmedi — hata kaliplari yeterince ciddi degil")
        return False

    # --- 4. proposals_system tablosuna yaz ---
    try:
        import sys
        if str(base_dir) not in sys.path:
            sys.path.insert(0, str(base_dir))
        from agent4.proposal_engine import ProposalEngine
        from supabase.db_client import SupabaseClient

        sdb = SupabaseClient()
        engine = ProposalEngine(hesap_key, marketplace, sdb, error_data)

        from datetime import datetime
        today = datetime.utcnow().strftime("%Y-%m-%d")

        formatted = []
        for o in oneriler:
            proposal = ProposalEngine.create_proposal(
                today=today,
                kategori=o.get("kategori", "BUGFIX"),
                ne=o.get("ne", ""),
                neden=o.get("neden", ""),
                kanit=o.get("kanit", ""),
                risk=o.get("risk", ""),
                kazanim=o.get("kazanim", ""),
            )
            formatted.append(proposal)

        yazilan = engine.write_proposals(formatted)
        logger.info("Agent 4 Phase 2: %d/%d oneri yazildi (%s/%s)",
                     yazilan, len(formatted), hesap_key, marketplace)

        # Token kullanimi logla
        usage = data.get("usage", {})
        logger.info("Token kullanimi: input=%d, output=%d",
                     usage.get("input_tokens", 0), usage.get("output_tokens", 0))

        return yazilan > 0

    except Exception as e:
        logger.error("Oneri yazma hatasi: %s", e)
        return False
