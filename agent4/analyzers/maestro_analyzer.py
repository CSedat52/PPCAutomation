"""
Agent 4 — Maestro Analyzer (v3 — Supabase Only)
==================================================
pipeline_runs ve maestro_errors tablolarindan okur.
Dosya bagimliligi (maestro/state/) kaldirildi.
"""

import logging
from collections import Counter

logger = logging.getLogger("agent4.maestro")


class MaestroAnalyzer:

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
            return {"durum": "VERI_YOK", "mesaj": f"Supabase hatasi: {e}"}

        # Pipeline gecmisi
        try:
            rows = sdb._fetch_all("""
                SELECT session_id, status, current_step,
                       started_at, updated_at, error_message,
                       agent1_completed_at, agent2_completed_at,
                       agent3_completed_at, agent4_completed_at
                FROM pipeline_runs
                WHERE hesap_key = %s AND marketplace = %s
                ORDER BY started_at DESC
                LIMIT 50
            """, (self.hesap_key, self.marketplace))
        except Exception as e:
            logger.warning("pipeline_runs okunamadi: %s", e)
            return {"durum": "VERI_YOK", "mesaj": f"Sorgu hatasi: {e}"}

        if not rows:
            return {"durum": "VERI_YOK", "mesaj": "Pipeline gecmisi bos"}

        toplam     = len(rows)
        tamamlanan = sum(1 for r in rows if r[1] == "COMPLETED")
        hatali     = sum(1 for r in rows if r[1] == "FAILED")
        bekleyen   = sum(1 for r in rows if r[1] in ("RUNNING", "PARTIAL"))

        # Agent bazli basari oranlari
        # rows columns: 0=session_id, 1=status, 2=current_step,
        #   3=started_at, 4=completed_at, 5=error_message,
        #   6=agent1_completed_at, 7=agent2_completed_at,
        #   8=agent3_completed_at, 9=agent4_completed_at
        agent_basari = {}
        for col_idx, agent_adi in [(6, "agent1"), (7, "agent2"), (8, "agent3")]:
            tamamlanan_ag = sum(1 for r in rows if r[col_idx] is not None)
            hatali_ag     = sum(1 for r in rows if r[col_idx] is None and r[1] == "FAILED")
            toplam_ag     = tamamlanan_ag + hatali_ag
            agent_basari[agent_adi] = {
                "tamamlanan":  tamamlanan_ag,
                "hatali":      hatali_ag,
                "basari_orani": round(tamamlanan_ag / toplam_ag, 3) if toplam_ag > 0 else 1.0,
            }

        # Maestro hata dagilimi
        hata_sayac = Counter()
        try:
            hata_rows = sdb._fetch_all("""
                SELECT agent, COUNT(*) as cnt FROM maestro_errors
                WHERE hesap_key = %s AND marketplace = %s
                GROUP BY agent
            """, (self.hesap_key, self.marketplace))
            for r in (hata_rows or []):
                hata_sayac[r[0] or "bilinmiyor"] = r[1]
        except Exception as e:
            logger.warning("Sessiz hata: %s", e)

        # Ardisik basarisiz session kontrolu (son 3)
        # rows DESC sirayla geldi, ilk 3 = en son 3
        son_3_status = [r[1] for r in rows[:3]]
        ardisik_hata = (
            len(son_3_status) >= 3 and
            all(s == "FAILED" for s in son_3_status)
        )

        sonuc = {
            "durum":              "TAMAMLANDI",
            "toplam_session":     toplam,
            "tamamlanan":         tamamlanan,
            "hatali":             hatali,
            "bekleyen":           bekleyen,
            "basari_orani":       round(tamamlanan / toplam, 3) if toplam > 0 else 0,
            "agent_basari":       agent_basari,
            "hata_dagilimi":      dict(hata_sayac),
            "ardisik_hata_alarmi": ardisik_hata,
        }

        if ardisik_hata:
            logger.warning("SON 3 SESSION ARDISIK HATA — kritik uyari!")
            self.db.add_anomali({
                "tip":    "MAESTRO_ARDISIK_HATA",
                "tanim":  "Son 3 session ardisik olarak hata ile sonuclandi",
                "siddet": "KRITIK",
                "durum":  "AKTIF",
                "oneri":  "CLAUDE.md yeni senaryo veya retry_handler.py guncellenmesi gerekebilir",
            })

        logger.info("Maestro analizi: toplam=%d tamamlanan=%d hatali=%d",
                    toplam, tamamlanan, hatali)
        return sonuc
