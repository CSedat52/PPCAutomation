"""
Agent 4 — Error Analyzer
==========================
Tüm bileşenlerin hata loglarını okur, tekrar eden kalıpları tespit eder.

Kaynaklar:
  data/logs/agent1_errors.json
  data/logs/agent2_errors.json
  data/logs/agent3_errors.json
  maestro/logs/maestro_log_*.log  (text parse)
  maestro/maestro_state.json
"""

import re
import json
import logging
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger("agent4.error")

# Tekrar eden hata eşiği — kaç kez görülürse kalıp sayılır
TEKRAR_ESIGI = 3


class ErrorAnalyzer:

    def __init__(self, data_dir, db):
        self.data_dir   = Path(data_dir)
        self.log_dir    = self.data_dir / "logs"
        self.db         = db

    def analyze(self) -> dict:
        sonuc = {
            "agent1": self._analiz_agent1(),
            "agent2": self._analiz_agent2(),
            "agent3": self._analiz_agent3(),
            "tekrar_eden_kaliplar": [],
        }

        # Tüm bileşenler üzerinde çapraz kalıp analizi
        sonuc["tekrar_eden_kaliplar"] = self._kalip_tespiti(sonuc)

        # Kritik tekrarlayan hataları DB'ye kalıp olarak ekle
        for kalip in sonuc["tekrar_eden_kaliplar"]:
            if kalip["tekrar"] >= TEKRAR_ESIGI:
                self.db.add_kalip({
                    "tip":     "HATA_DONGUSU",
                    "tanim":   kalip["tanim"],
                    "bilesen": kalip["bilesen"],
                    "oneri":   kalip.get("oneri", ""),
                })

        logger.info("Hata analizi: agent1=%d, agent2=%d, agent3=%d, kalip=%d",
                    sonuc["agent1"].get("toplam", 0),
                    sonuc["agent2"].get("toplam", 0),
                    sonuc["agent3"].get("toplam", 0),
                    len(sonuc["tekrar_eden_kaliplar"]))
        return sonuc

    # --------------------------------------------------------- Agent 1
    def _analiz_agent1(self) -> dict:
        kayitlar = self._yukle_json_log("agent1_errors.json")
        if not kayitlar:
            return {"toplam": 0, "mesaj": "Hata logu yok veya bos"}

        tip_sayac  = Counter(k.get("hata_tipi", "Bilinmiyor") for k in kayitlar)
        adim_sayac = Counter(k.get("adim", "bilinmiyor") for k in kayitlar)

        # Son 30 gündeki kayıtları filtrele
        son_30 = self._son_n_gun(kayitlar, 30)
        son_30_tip = Counter(k.get("hata_tipi", "") for k in son_30)

        return {
            "toplam":          len(kayitlar),
            "son_30_gun":      len(son_30),
            "tip_dagilimi":    dict(tip_sayac.most_common(10)),
            "adim_dagilimi":   dict(adim_sayac.most_common(5)),
            "son_30_tipler":   dict(son_30_tip.most_common(5)),
            "en_son_hata":     kayitlar[-1] if kayitlar else None,
        }

    # --------------------------------------------------------- Agent 2
    def _analiz_agent2(self) -> dict:
        kayitlar = self._yukle_json_log("agent2_errors.json")
        if not kayitlar:
            return {"toplam": 0, "mesaj": "Hata logu yok veya bos"}

        tip_sayac  = Counter(k.get("hata_tipi", "Bilinmiyor") for k in kayitlar)
        adim_sayac = Counter(k.get("adim", "bilinmiyor") for k in kayitlar)
        son_30     = self._son_n_gun(kayitlar, 30)

        return {
            "toplam":         len(kayitlar),
            "son_30_gun":     len(son_30),
            "tip_dagilimi":   dict(tip_sayac.most_common(10)),
            "adim_dagilimi":  dict(adim_sayac.most_common(5)),
            "en_son_hata":    kayitlar[-1] if kayitlar else None,
        }

    # --------------------------------------------------------- Agent 3
    def _analiz_agent3(self) -> dict:
        kayitlar = self._yukle_json_log("agent3_errors.json")
        if not kayitlar:
            return {"toplam": 0, "mesaj": "Hata logu yok veya bos"}

        tip_sayac    = Counter(k.get("hata_tipi", "Bilinmiyor") for k in kayitlar)
        adim_sayac   = Counter(k.get("adim", "bilinmiyor") for k in kayitlar)
        son_30       = self._son_n_gun(kayitlar, 30)

        # Execution hatalarında kampanya bazlı kümelenme
        exec_hatalar = [k for k in kayitlar if k.get("adim") == "execution_plan"]
        kampanya_sayac = Counter()
        for k in exec_hatalar:
            extra = k.get("extra", {})
            for det in extra.get("detaylar", []):
                kamp = det.get("kampanya", "")
                if kamp:
                    kampanya_sayac[kamp] += 1

        return {
            "toplam":           len(kayitlar),
            "son_30_gun":       len(son_30),
            "tip_dagilimi":     dict(tip_sayac.most_common(10)),
            "adim_dagilimi":    dict(adim_sayac.most_common(5)),
            "kampanya_kumesi":  dict(kampanya_sayac.most_common(5)),
            "en_son_hata":      kayitlar[-1] if kayitlar else None,
        }

    # --------------------------------------------------------- Çapraz kalıp
    def _kalip_tespiti(self, sonuc: dict) -> list:
        kaliplar = []

        # Kalıp 1: Rate limit döngüsü (Agent 1)
        a1 = sonuc.get("agent1", {})
        rate_limit_sayisi = a1.get("son_30_tipler", {}).get("RateLimit", 0)
        if rate_limit_sayisi >= TEKRAR_ESIGI:
            kaliplar.append({
                "bilesen": "Agent1",
                "tip":     "RateLimit_Dongusu",
                "tanim":   f"Son 30 günde {rate_limit_sayisi} rate limit hatası",
                "tekrar":  rate_limit_sayisi,
                "oneri":   "config.py'de SCHEDULE_TIME_HOUR_UTC değerini değiştirin",
            })

        # Kalıp 2: Agent 2 preflight tekrarı
        a2 = sonuc.get("agent2", {})
        preflight_sayisi = a2.get("adim_dagilimi", {}).get("preflight_check", 0)
        if preflight_sayisi >= TEKRAR_ESIGI:
            kaliplar.append({
                "bilesen": "Agent2",
                "tip":     "Preflight_Tekrari",
                "tanim":   f"Agent 2 preflight {preflight_sayisi} kez başarısız",
                "tekrar":  preflight_sayisi,
                "oneri":   "Agent 1 eksik veri üretiyor olabilir. Agent 1 hata logunu inceleyin.",
            })

        # Kalıp 3: Agent 3 belirli kampanya hata kümesi
        a3 = sonuc.get("agent3", {})
        for kamp, sayi in a3.get("kampanya_kumesi", {}).items():
            if sayi >= TEKRAR_ESIGI:
                kaliplar.append({
                    "bilesen": "Agent3",
                    "tip":     "Kampanya_Hata_Kumesi",
                    "tanim":   f"'{kamp}' kampanyası {sayi} kez execution hatası aldı",
                    "tekrar":  sayi,
                    "oneri":   f"'{kamp}' kampanyasının entity verilerini kontrol edin.",
                })

        return kaliplar

    # ----------------------------------------------------------------- utils
    def _yukle_json_log(self, dosya_adi: str) -> list:
        path = self.log_dir / dosya_adi
        if not path.exists():
            return []
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []

    def _son_n_gun(self, kayitlar: list, n: int) -> list:
        sinir = datetime.utcnow() - timedelta(days=n)
        sonuc = []
        for k in kayitlar:
            ts = k.get("timestamp", "")
            try:
                if datetime.fromisoformat(ts) >= sinir:
                    sonuc.append(k)
            except Exception:
                continue
        return sonuc
