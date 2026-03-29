"""
Agent 4 — Error Analyzer (v3 — Supabase Only)
================================================
error_logs tablosundan okur, tekrar eden kaliplari tespit eder.
JSON dosya bagimliligi kaldirildi.
"""

import logging
from datetime import datetime, timedelta

logger = logging.getLogger("agent4.error")

TEKRAR_ESIGI = 3


class ErrorAnalyzer:

    def __init__(self, hesap_key: str, marketplace: str, db):
        self.hesap_key = hesap_key
        self.marketplace = marketplace
        self.db = db

    def _get_sdb(self):
        from supabase.db_client import SupabaseClient
        return SupabaseClient()

    def analyze(self) -> dict:
        try:
            sdb = self._get_sdb()
        except Exception as e:
            logger.warning("Supabase baglantisi kurulamadi: %s", e)
            return self._empty_result()

        sonuc = {
            "agent1": self._analiz_agent(sdb, "agent1"),
            "agent2": self._analiz_agent(sdb, "agent2"),
            "agent3": self._analiz_agent(sdb, "agent3"),
            "tekrar_eden_kaliplar": [],
        }

        sonuc["tekrar_eden_kaliplar"] = self._kalip_tespiti(sdb, sonuc)

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

    # --------------------------------------------------------- Agent analizi
    def _analiz_agent(self, sdb, agent: str) -> dict:
        try:
            toplam_row = sdb._fetch_one("""
                SELECT COUNT(*) FROM error_logs
                WHERE hesap_key = %s AND marketplace = %s AND agent = %s
            """, (self.hesap_key, self.marketplace, agent))
            toplam = toplam_row[0] if toplam_row else 0

            if toplam == 0:
                return {"toplam": 0, "mesaj": "Hata logu yok veya bos"}

            sinir = (datetime.utcnow() - timedelta(days=30)).isoformat()

            son_30_row = sdb._fetch_one("""
                SELECT COUNT(*) FROM error_logs
                WHERE hesap_key = %s AND marketplace = %s AND agent = %s
                  AND timestamp > %s
            """, (self.hesap_key, self.marketplace, agent, sinir))
            son_30 = son_30_row[0] if son_30_row else 0

            tip_rows = sdb._fetch_all("""
                SELECT error_type, COUNT(*) as cnt FROM error_logs
                WHERE hesap_key = %s AND marketplace = %s AND agent = %s
                GROUP BY error_type ORDER BY cnt DESC LIMIT 10
            """, (self.hesap_key, self.marketplace, agent))
            tip_dagilimi = {r[0]: r[1] for r in (tip_rows or [])}

            adim_rows = sdb._fetch_all("""
                SELECT step, COUNT(*) as cnt FROM error_logs
                WHERE hesap_key = %s AND marketplace = %s AND agent = %s
                GROUP BY step ORDER BY cnt DESC LIMIT 5
            """, (self.hesap_key, self.marketplace, agent))
            adim_dagilimi = {r[0]: r[1] for r in (adim_rows or [])}

            son_30_tip_rows = sdb._fetch_all("""
                SELECT error_type, COUNT(*) as cnt FROM error_logs
                WHERE hesap_key = %s AND marketplace = %s AND agent = %s
                  AND timestamp > %s
                GROUP BY error_type ORDER BY cnt DESC LIMIT 5
            """, (self.hesap_key, self.marketplace, agent, sinir))
            son_30_tipler = {r[0]: r[1] for r in (son_30_tip_rows or [])}

            son_hata_row = sdb._fetch_one("""
                SELECT timestamp, error_type, error_message, step
                FROM error_logs
                WHERE hesap_key = %s AND marketplace = %s AND agent = %s
                ORDER BY timestamp DESC LIMIT 1
            """, (self.hesap_key, self.marketplace, agent))
            en_son_hata = None
            if son_hata_row:
                en_son_hata = {
                    "timestamp":  str(son_hata_row[0]),
                    "hata_tipi":  son_hata_row[1],
                    "hata_mesaji": son_hata_row[2],
                    "adim":       son_hata_row[3],
                }

            return {
                "toplam":        toplam,
                "son_30_gun":    son_30,
                "tip_dagilimi":  tip_dagilimi,
                "adim_dagilimi": adim_dagilimi,
                "son_30_tipler": son_30_tipler,
                "en_son_hata":   en_son_hata,
            }

        except Exception as e:
            logger.warning("Agent %s hata analizi basarisiz: %s", agent, e)
            return {"toplam": 0, "mesaj": f"Analiz hatasi: {e}"}

    # --------------------------------------------------------- Capraz kalip
    def _kalip_tespiti(self, sdb, sonuc: dict) -> list:
        kaliplar = []

        # Kalip 1: Rate limit dongusu (Agent 1)
        a1 = sonuc.get("agent1", {})
        rl_sayisi = a1.get("son_30_tipler", {}).get("RateLimit", 0)
        if rl_sayisi >= TEKRAR_ESIGI:
            kaliplar.append({
                "bilesen": "Agent1",
                "tip":     "RateLimit_Dongusu",
                "tanim":   f"Son 30 gunde {rl_sayisi} rate limit hatasi",
                "tekrar":  rl_sayisi,
                "oneri":   "config.py'de SCHEDULE_TIME_HOUR_UTC degerini degistirin",
            })

        # Kalip 2: Agent 2 preflight tekrari
        a2 = sonuc.get("agent2", {})
        pf_sayisi = a2.get("adim_dagilimi", {}).get("preflight_check", 0)
        if pf_sayisi >= TEKRAR_ESIGI:
            kaliplar.append({
                "bilesen": "Agent2",
                "tip":     "Preflight_Tekrari",
                "tanim":   f"Agent 2 preflight {pf_sayisi} kez basarisiz",
                "tekrar":  pf_sayisi,
                "oneri":   "Agent 1 eksik veri uretiyor olabilir.",
            })

        # Kalip 3: Agent 3 kampanya kumesi
        try:
            kamp_rows = sdb._fetch_all("""
                SELECT extra->>'kampanya' as kampanya, COUNT(*) as cnt
                FROM error_logs
                WHERE hesap_key = %s AND marketplace = %s AND agent = 'agent3'
                  AND step = 'execution_plan'
                  AND extra->>'kampanya' IS NOT NULL
                GROUP BY extra->>'kampanya'
                HAVING COUNT(*) >= %s
            """, (self.hesap_key, self.marketplace, TEKRAR_ESIGI))

            for row in (kamp_rows or []):
                kamp, sayi = row
                kaliplar.append({
                    "bilesen": "Agent3",
                    "tip":     "Kampanya_Hata_Kumesi",
                    "tanim":   f"'{kamp}' kampanyasi {sayi} kez execution hatasi aldi",
                    "tekrar":  sayi,
                    "oneri":   f"'{kamp}' kampanyasinin entity verilerini kontrol edin.",
                })
        except Exception:
            pass

        return kaliplar

    def _empty_result(self):
        return {
            "agent1": {"toplam": 0, "mesaj": "Supabase baglantisi yok"},
            "agent2": {"toplam": 0, "mesaj": "Supabase baglantisi yok"},
            "agent3": {"toplam": 0, "mesaj": "Supabase baglantisi yok"},
            "tekrar_eden_kaliplar": [],
        }
