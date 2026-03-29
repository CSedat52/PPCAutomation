"""
Agent 4 — Proposal Engine (v3 — Supabase Only)
=================================================
Claude Code'un urettigi onerileri proposals tablosuna yazar.
Statik if/else oneri mantigi kaldirildi — artik sadece writer.
CLI (listele/onayla/reddet) Supabase'den okur.

KRITIK KURAL:
  Her oneri 5 soruyu yanitlamadan uretilemez:
    1. NE     — Ne degistirilecek?
    2. NEDEN  — Kanita dayali neden?
    3. KANIT  — Hangi veri?
    4. RISK   — Olasi olumsuz etki?
    5. KAZANIM — Beklenen iyilesme?
"""

import json
import logging
import hashlib
from datetime import datetime

logger = logging.getLogger("agent4.proposal")


class ProposalEngine:

    def __init__(self, hesap_key: str, marketplace: str, db, analiz_sonuclari: dict):
        self.hesap_key = hesap_key
        self.marketplace = marketplace
        self.db = db
        self.analiz = analiz_sonuclari

    def _get_sdb(self):
        from supabase.db_client import SupabaseClient
        return SupabaseClient()

    # ------------------------------------------------------------------ write
    def write_proposal(self, oneri: dict) -> bool:
        """Tek oneriyi proposals tablosuna yaz."""
        try:
            sdb = self._get_sdb()
            sdb.upsert_proposal(self.hesap_key, self.marketplace, {
                "id":               oneri.get("id", ""),
                "kategori":         oneri.get("kategori", ""),
                "baslik":           oneri.get("ne", ""),
                "aciklama":         oneri.get("neden", ""),
                "gerekce":          oneri.get("kanit", ""),
                "beklenen_sonuc":   oneri.get("risk", ""),
                "gerceklesen_sonuc": oneri.get("kazanim", ""),
                "status":           oneri.get("durum", "PENDING"),
            })
            logger.info("Oneri Supabase'e kaydedildi: %s", oneri.get("id", ""))
            return True
        except Exception as e:
            logger.warning("Oneri Supabase'e yazilamadi: %s", e)
            return False

    def write_proposals(self, oneriler: list) -> int:
        """Birden fazla oneriyi yaz. Basarili yazilan sayisini doner."""
        yazilan = 0
        for oneri in oneriler:
            if self.write_proposal(oneri):
                yazilan += 1
        return yazilan

    @staticmethod
    def create_proposal(today, kategori, ne, neden, kanit, risk, kazanim,
                         degisecek_dosya="", degisecek_alan=None) -> dict:
        """Standart oneri dict olusturur."""
        icerik = f"{kategori}|{ne}|{today}"
        oneri_id = "ONR-" + hashlib.md5(icerik.encode()).hexdigest()[:8].upper()

        return {
            "id":               oneri_id,
            "tarih":            today,
            "kategori":         kategori,
            "durum":            "BEKLIYOR",
            "ne":               ne,
            "neden":            neden,
            "kanit":            kanit,
            "risk":             risk,
            "kazanim":          kazanim,
            "beklenen_sonuc":   risk,
            "gerceklesen_sonuc": kazanim,
            "degisecek_dosya":  degisecek_dosya,
            "degisecek_alan":   degisecek_alan or {},
            "olusturma_zamani": datetime.utcnow().isoformat(),
        }

    def get_bekleyen_sayisi(self) -> int:
        """Bekleyen oneri sayisini Supabase'den doner."""
        try:
            sdb = self._get_sdb()
            row = sdb._fetch_one("""
                SELECT COUNT(*) FROM proposals
                WHERE hesap_key = %s AND marketplace = %s
                  AND status IN ('PENDING', 'BEKLIYOR')
            """, (self.hesap_key, self.marketplace))
            return row[0] if row else 0
        except Exception:
            return 0


# ------------------------------------------------------------------ CLI
def cmd_oneri(hesap_key, marketplace, args):
    """
    Komut satirindan oneri yonetimi (Supabase'den okur):
      python agent4/optimizer.py <hesap> <mp> oneri listele
      python agent4/optimizer.py <hesap> <mp> oneri onayla ONR-XXXXXXXX
      python agent4/optimizer.py <hesap> <mp> oneri reddet ONR-XXXXXXXX [sebep]
    """
    try:
        from supabase.db_client import SupabaseClient
        sdb = SupabaseClient()
    except Exception as e:
        print(f"Supabase baglantisi basarisiz: {e}")
        return

    if not args or args[0] == "listele":
        rows = sdb._fetch_all("""
            SELECT proposal_id, proposal_type, title, description,
                   beklenen_sonuc, gerceklesen_sonuc, status, created_at
            FROM proposals
            WHERE hesap_key = %s AND marketplace = %s
              AND status IN ('PENDING', 'BEKLIYOR')
            ORDER BY created_at DESC
        """, (hesap_key, marketplace))

        if not rows:
            print("Bekleyen oneri yok.")
            return

        print(f"\n{'='*60}")
        print(f"BEKLEYEN ONERILER ({len(rows)} adet)")
        print(f"{'='*60}")
        for r in rows:
            pid, ptype, title, desc, beklenen, gercek, status, created = r
            print(f"\n[{pid}] {ptype}")
            print(f"  NE      : {title}")
            print(f"  NEDEN   : {desc}")
            print(f"  RISK    : {beklenen}")
            print(f"  KAZANIM : {gercek}")
        print(f"\n{'='*60}")
        print(f"Onaylamak icin: python agent4/optimizer.py {hesap_key} {marketplace} oneri onayla <ID>")
        print(f"Reddetmek icin: python agent4/optimizer.py {hesap_key} {marketplace} oneri reddet <ID> [sebep]")
        return

    if args[0] == "onayla" and len(args) >= 2:
        oneri_id = args[1]
        sdb.update_proposal_status(hesap_key, marketplace, oneri_id, "APPROVED")
        print(f"Onaylandi: {oneri_id}")
        print(f"  NOT: Bu degisikligi manuel olarak uygulamaniz gerekiyor.")
        return

    if args[0] == "reddet" and len(args) >= 2:
        oneri_id = args[1]
        sebep = " ".join(args[2:]) if len(args) > 2 else "Belirtilmedi"
        sdb.update_proposal_status(hesap_key, marketplace, oneri_id, "REJECTED", sebep)
        print(f"Reddedildi: {oneri_id} — Sebep: {sebep}")
        return

    print("Kullanim: python agent4/optimizer.py <hesap> <mp> oneri [listele|onayla|reddet] ...")
