"""
Agent 4 — Maestro Analyzer
============================
Maestro'nun self-heal etkinliğini ve pipeline sağlığını ölçer.
"""

import json
import logging
from pathlib import Path
from collections import Counter

logger = logging.getLogger("agent4.maestro")


class MaestroAnalyzer:

    def __init__(self, base_dir, db):
        self.base_dir    = Path(base_dir)
        self.state_dir   = self.base_dir / "maestro" / "state"
        self.db          = db

    def analyze(self) -> dict:
        """Tum hesaplarin maestro state dosyalarini okuyup birlestir."""
        if not self.state_dir.exists():
            return {"durum": "VERI_YOK", "mesaj": "maestro/state/ klasoru bulunamadi"}

        state_files = sorted(self.state_dir.glob("*_state.json"))
        if not state_files:
            return {"durum": "VERI_YOK", "mesaj": "Hicbir state dosyasi bulunamadi"}

        sessionlar = []
        for sf in state_files:
            try:
                with open(sf, "r", encoding="utf-8") as f:
                    state = json.load(f)
                history = state.get("history", [])
                current = state.get("current_session")
                sessionlar.extend(history)
                if current:
                    sessionlar.append(current)
            except Exception:
                continue

        if not sessionlar:
            return {"durum": "VERI_YOK", "mesaj": "Session gecmisi bos"}

        # Temel istatistikler
        toplam       = len(sessionlar)
        tamamlanan   = sum(1 for s in sessionlar if s.get("status") == "completed")
        hatali       = sum(1 for s in sessionlar if s.get("status") == "error")
        bekleyen     = sum(1 for s in sessionlar if s.get("status") == "waiting_approval")

        # Agent bazlı başarı oranları
        agent_basari = {}
        for agent_adi in ("agent1", "agent2", "agent3"):
            tamamlanan_ag = sum(
                1 for s in sessionlar
                if s.get(agent_adi, {}).get("status") == "completed"
            )
            hatali_ag = sum(
                1 for s in sessionlar
                if s.get(agent_adi, {}).get("status") == "failed"
            )
            toplam_ag = tamamlanan_ag + hatali_ag
            agent_basari[agent_adi] = {
                "tamamlanan": tamamlanan_ag,
                "hatali":     hatali_ag,
                "basari_orani": round(tamamlanan_ag / toplam_ag, 3) if toplam_ag > 0 else 1.0,
            }

        # Tekrarlayan hata tipleri
        hata_sayac = Counter()
        for s in sessionlar:
            for hata in s.get("errors", []):
                agent = hata.get("agent", "bilinmiyor")
                hata_sayac[agent] += 1

        # Ardışık başarısız session kontrolü (son 3)
        son_3 = sessionlar[-3:]
        ardisik_hata = all(s.get("status") == "error" for s in son_3) and len(son_3) >= 3

        sonuc = {
            "durum":           "TAMAMLANDI",
            "toplam_session":  toplam,
            "tamamlanan":      tamamlanan,
            "hatali":          hatali,
            "bekleyen":        bekleyen,
            "basari_orani":    round(tamamlanan / toplam, 3) if toplam > 0 else 0,
            "agent_basari":    agent_basari,
            "hata_dagilimi":   dict(hata_sayac.most_common()),
            "ardisik_hata_alarmi": ardisik_hata,
        }

        if ardisik_hata:
            logger.warning("SON 3 SESSION ARDISIK HATA — kritik uyari!")
            self.db.add_anomali({
                "tip":      "MAESTRO_ARDISIK_HATA",
                "tanim":    "Son 3 session ardışık olarak hata ile sonuçlandı",
                "siddet":   "KRITIK",
                "durum":    "AKTIF",
                "oneri":    "CLAUDE.md yeni senaryo veya retry_handler.py güncellenmesi gerekebilir",
            })

        logger.info("Maestro analizi: toplam=%d tamamlanan=%d hatali=%d",
                    toplam, tamamlanan, hatali)
        return sonuc
