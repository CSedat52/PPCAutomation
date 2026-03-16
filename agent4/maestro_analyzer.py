"""
Agent 4 — Maestro Analyzer (v2 Multi-Account + Error Logs)
=============================================================
Maestro pipeline session gecmisini + maestro_errors.json loglarini analiz eder.

Veri kaynaklari:
  1. maestro/state/*_state.json  — Session gecmisi (tamamlanan, hata veren sessionlar)
  2. maestro/logs/*_maestro_errors.json — Structured hata kayitlari (yeni!)
"""

import json
import logging
from collections import Counter
from pathlib import Path

logger = logging.getLogger("agent4.maestro_analyzer")


class MaestroAnalyzer:

    def __init__(self, base_dir, db):
        self.base_dir    = Path(base_dir)
        self.state_dir   = self.base_dir / "maestro" / "state"
        self.log_dir     = self.base_dir / "maestro" / "logs"
        self.db          = db

    def analyze(self) -> dict:
        """Tum hesaplarin maestro state + error log dosyalarini okuyup analiz eder."""

        # 1. State dosyalarindan session analizi
        session_analiz = self._analiz_sessionlar()

        # 2. maestro_errors.json dosyalarindan structured hata analizi
        hata_analiz = self._analiz_maestro_errors()

        # Birlestir
        sonuc = {**session_analiz}
        sonuc["maestro_hatalar"] = hata_analiz

        return sonuc

    # --------------------------------------------------------- Session analizi
    def _analiz_sessionlar(self) -> dict:
        """maestro/state/ altindaki tum state dosyalarini okuyup birlestir."""
        if not self.state_dir.exists():
            return {"durum": "VERI_YOK", "mesaj": "maestro/state/ klasoru bulunamadi",
                    "toplam_session": 0, "basari_orani": 0, "ardisik_hata_alarmi": False,
                    "agent_basari": {}}

        state_files = sorted(self.state_dir.glob("*_state.json"))
        if not state_files:
            return {"durum": "VERI_YOK", "mesaj": "Hicbir state dosyasi bulunamadi",
                    "toplam_session": 0, "basari_orani": 0, "ardisik_hata_alarmi": False,
                    "agent_basari": {}}

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
            return {"durum": "VERI_YOK", "mesaj": "Hicbir session bulunamadi",
                    "toplam_session": 0, "basari_orani": 0, "ardisik_hata_alarmi": False,
                    "agent_basari": {}}

        tamamlanan = [s for s in sessionlar if s.get("status") == "completed"]
        hata_veren = [s for s in sessionlar if s.get("status") == "error"]

        # Agent bazinda basari oranlari
        agent_basari = {}
        for agent_key in ("agent1", "agent2", "agent3"):
            agent_completed = sum(1 for s in sessionlar
                                  if s.get(agent_key, {}).get("status") == "completed")
            agent_failed = sum(1 for s in sessionlar
                               if s.get(agent_key, {}).get("status") == "failed")
            total = agent_completed + agent_failed
            agent_basari[agent_key] = {
                "tamamlanan": agent_completed,
                "basarisiz": agent_failed,
                "basari_orani": round(agent_completed / total, 3) if total > 0 else 0,
            }

        # Ardisik hata alarmi — son 3 session art arda hata mi?
        son_3 = sessionlar[-3:] if len(sessionlar) >= 3 else sessionlar
        ardisik_hata = all(s.get("status") == "error" for s in son_3) and len(son_3) == 3

        basari_orani = round(len(tamamlanan) / len(sessionlar), 3) if sessionlar else 0

        return {
            "toplam_session":     len(sessionlar),
            "tamamlanan":         len(tamamlanan),
            "hata_veren":         len(hata_veren),
            "basari_orani":       basari_orani,
            "hata_orani":         round(1 - basari_orani, 3),
            "ardisik_hata_alarmi": ardisik_hata,
            "agent_basari":       agent_basari,
        }

    # --------------------------------------------------------- Maestro error log analizi
    def _analiz_maestro_errors(self) -> dict:
        """maestro/logs/*_maestro_errors.json dosyalarini okur ve analiz eder."""
        if not self.log_dir.exists():
            return {"toplam": 0, "mesaj": "maestro/logs/ bulunamadi"}

        error_files = sorted(self.log_dir.glob("*_maestro_errors.json"))
        if not error_files:
            return {"toplam": 0, "mesaj": "Maestro hata logu dosyasi bulunamadi"}

        tum_kayitlar = []
        for ef in error_files:
            try:
                with open(ef, "r", encoding="utf-8") as f:
                    kayitlar = json.load(f)
                tum_kayitlar.extend(kayitlar)
            except Exception:
                continue

        if not tum_kayitlar:
            return {"toplam": 0, "mesaj": "Maestro hata kaydi yok"}

        # Hata tipi dagilimi
        tip_sayac = Counter(k.get("hata_tipi", "Bilinmiyor") for k in tum_kayitlar)

        # Adim dagilimi (run_agent2, run_agent3_execute, vb.)
        adim_sayac = Counter(k.get("adim", "bilinmiyor") for k in tum_kayitlar)

        # Hangi agentlar basarisiz oluyor
        agent_sayac = Counter()
        for k in tum_kayitlar:
            extra = k.get("extra", {})
            if extra and extra.get("agent"):
                agent_sayac[extra["agent"]] += 1

        # Son 5 hata
        son_5 = tum_kayitlar[-5:]

        return {
            "toplam":          len(tum_kayitlar),
            "tip_dagilimi":    dict(tip_sayac.most_common(10)),
            "adim_dagilimi":   dict(adim_sayac.most_common(10)),
            "agent_dagilimi":  dict(agent_sayac.most_common()),
            "son_hatalar":     [
                {
                    "timestamp": k.get("timestamp"),
                    "hata_tipi": k.get("hata_tipi"),
                    "hata_mesaji": k.get("hata_mesaji", "")[:200],
                    "adim": k.get("adim"),
                    "session_id": k.get("session_id"),
                }
                for k in son_5
            ],
        }
