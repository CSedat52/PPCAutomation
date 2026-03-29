"""
Agent 4 — Report Generator (v3)
==================================
Tum analiz sonuclarini birlestirerek:
  1. agent4_analysis.json — Claude Code icin (dinamik analiz girdisi)
  2. Supabase status_reports tablosu
  3. Konsol ozeti
"""

import json
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("agent4.report")


class ReportGenerator:

    def __init__(self, hesap_key: str, marketplace: str, data_dir, db):
        self.hesap_key   = hesap_key
        self.marketplace = marketplace
        self.data_dir    = Path(data_dir)
        self.rapor_dir   = self.data_dir / "agent4" / "raporlar"
        self.rapor_dir.mkdir(parents=True, exist_ok=True)
        self.db          = db

    def _get_sdb(self):
        from supabase.db_client import SupabaseClient
        return SupabaseClient()

    def generate(self, today: str, sonuclar: dict) -> dict:
        rapor = {
            "tarih":           today,
            "olusturma":       datetime.utcnow().isoformat(),
            "versiyon":        "3.0",
            "sistem_sagligi":  self._sistem_sagligi(sonuclar),
            "onay_bekliyor":   self._onay_bekliyor(sonuclar),
            "ozet":            self._ozet(sonuclar),
        }

        # agent4_analysis.json — Claude Code icin
        analysis_data = self._build_analysis_json(today, sonuclar)
        analysis_path = self.data_dir / "agent4" / "agent4_analysis.json"
        analysis_path.parent.mkdir(parents=True, exist_ok=True)
        with open(analysis_path, "w", encoding="utf-8") as f:
            json.dump(analysis_data, f, indent=2, ensure_ascii=False)
        rapor["analysis_dosyasi"] = str(analysis_path)
        logger.info("agent4_analysis.json kaydedildi: %s", analysis_path)

        # Supabase status_reports
        try:
            sdb = self._get_sdb()
            sdb.insert_status_report(self.hesap_key, self.marketplace, rapor)
        except Exception as e:
            logger.warning("Status raporu Supabase'e yazilamadi: %s", e)

        # Konsol ozeti
        self._yazdir_ozet(rapor)
        return rapor

    # ------------------------------------------ agent4_analysis.json builder
    def _build_analysis_json(self, today: str, sonuclar: dict) -> dict:
        """Claude Code'un okuyacagi tum analiz verilerini tek dosyaya yaz."""

        # Mevcut bid_functions ve settings
        mevcut_bf = {}
        mevcut_settings = {}
        try:
            sdb = self._get_sdb()
            bf_row = sdb._fetch_one("""
                SELECT tanh_formulu, segment_parametreleri, asin_parametreleri
                FROM bid_functions WHERE hesap_key = %s AND marketplace = %s
            """, (self.hesap_key, self.marketplace))
            if bf_row:
                mevcut_bf = {
                    "tanh_formulu": bf_row[0] if isinstance(bf_row[0], dict) else {},
                    "segment_parametreleri": bf_row[1] if isinstance(bf_row[1], dict) else {},
                    "asin_parametreleri": bf_row[2] if isinstance(bf_row[2], dict) else {},
                }

            set_row = sdb._fetch_one("""
                SELECT genel_ayarlar, esik_degerleri, segmentasyon_kurallari
                FROM settings WHERE hesap_key = %s AND marketplace = %s
            """, (self.hesap_key, self.marketplace))
            if set_row:
                mevcut_settings = {
                    "genel_ayarlar": set_row[0] if isinstance(set_row[0], dict) else {},
                    "esik_degerleri": set_row[1] if isinstance(set_row[1], dict) else {},
                    "segmentasyon_kurallari": set_row[2] if isinstance(set_row[2], dict) else {},
                }
        except Exception:
            pass

        return {
            "tarih":              today,
            "hesap_key":          self.hesap_key,
            "marketplace":        self.marketplace,
            "kpi_ozet":           sonuclar.get("kpi", {}),
            "segment_sonuclari":  sonuclar.get("segment", {}),
            "hata_analizi":       sonuclar.get("hata", {}),
            "maestro_analizi":    sonuclar.get("maestro", {}),
            "bid_param_analizi":  sonuclar.get("bid_param", []),
            "mevcut_bid_functions": mevcut_bf,
            "mevcut_settings":    mevcut_settings,
        }

    # ------------------------------------------ Sistem sagligi bolumu
    def _sistem_sagligi(self, sonuclar: dict) -> dict:
        maestro = sonuclar.get("maestro", {})
        hata    = sonuclar.get("hata", {})
        kpi     = sonuclar.get("kpi", {})
        segment = sonuclar.get("segment", {})

        skor = 100

        # Maestro basari orani
        maestro_basari = maestro.get("basari_orani", 1.0)
        maestro_hata = 1.0 - maestro_basari
        if maestro_hata > 0.30:
            skor -= 30
        elif maestro_hata > 0.15:
            skor -= 15

        if maestro.get("ardisik_hata_alarmi"):
            skor -= 25

        # Aktif anomaliler (in-memory DB'den)
        aktif_anomali = len(self.db.get_aktif_anomaliler())
        skor -= min(aktif_anomali * 5, 20)

        # Segment dusuk performans
        dusuk_performans = segment.get("dusuk_performans", [])
        skor -= min(len(dusuk_performans) * 5, 15)

        skor = max(0, skor)

        return {
            "saglik_skoru":       skor,
            "saglik_durumu":      self._skor_etiketi(skor),
            "pipeline": {
                "toplam_session":  maestro.get("toplam_session", 0),
                "basari_orani":    maestro.get("basari_orani", 0),
                "ardisik_hata":    maestro.get("ardisik_hata_alarmi", False),
                "agent_basari":    maestro.get("agent_basari", {}),
            },
            "hata_ozeti": {
                "agent1_toplam":   hata.get("agent1", {}).get("toplam", 0),
                "agent2_toplam":   hata.get("agent2", {}).get("toplam", 0),
                "agent3_toplam":   hata.get("agent3", {}).get("toplam", 0),
                "tekrar_eden_kalip": len(hata.get("tekrar_eden_kaliplar", [])),
            },
            "kpi_ozeti": {
                "islenen_karar":       kpi.get("yeni_karar", 0),
                "kpi_after_doldurulan": kpi.get("kpi_after_doldurulan", 0),
            },
            "anomaliler": {
                "aktif": aktif_anomali,
            },
            "segment_sagligi": {
                "olculebilir_karar":  segment.get("olculebilir_karar", 0),
                "dusuk_performans":   dusuk_performans,
            },
        }

    # ------------------------------------------ Onay bekliyor
    def _onay_bekliyor(self, sonuclar: dict) -> dict:
        oneriler = sonuclar.get("oneriler", [])
        bekleyen_sayisi = 0
        try:
            sdb = self._get_sdb()
            row = sdb._fetch_one("""
                SELECT COUNT(*) FROM proposals
                WHERE hesap_key = %s AND marketplace = %s
                  AND status IN ('PENDING', 'BEKLIYOR')
            """, (self.hesap_key, self.marketplace))
            bekleyen_sayisi = row[0] if row else 0
        except Exception:
            pass

        return {
            "bekleyen_oneri_sayisi": bekleyen_sayisi,
            "bu_calisma_uretilen":   len(oneriler),
            "goruntuleme_komutu":    f"python agent4/optimizer.py {self.hesap_key} {self.marketplace} oneri listele",
        }

    # ------------------------------------------ Kisa ozet
    def _ozet(self, sonuclar: dict) -> str:
        maestro  = sonuclar.get("maestro", {})
        kpi      = sonuclar.get("kpi", {})
        oneriler = sonuclar.get("oneriler", [])

        parcalar = []
        parcalar.append(f"Pipeline: {maestro.get('tamamlanan', 0)}/{maestro.get('toplam_session', 0)} session basarili")
        parcalar.append(f"KPI: {kpi.get('kpi_after_doldurulan', 0)} karar guncellendi")
        parcalar.append(f"Anomali: {len(self.db.get_aktif_anomaliler())} aktif")
        parcalar.append(f"Oneri: {len(oneriler)} yeni")

        if maestro.get("ardisik_hata_alarmi"):
            parcalar.append("[!] KRITIK: Ardisik pipeline hatalari tespit edildi")

        return " | ".join(parcalar)

    # ------------------------------------------ Konsol ciktisi
    def _yazdir_ozet(self, rapor: dict):
        sg = rapor["sistem_sagligi"]
        ob = rapor["onay_bekliyor"]

        print("\n" + "="*60)
        print(f"  AGENT 4 DURUM RAPORU -- {rapor['tarih']}")
        print("="*60)
        print(f"  Sistem Sagligi : {sg['saglik_skoru']}/100  [{sg['saglik_durumu']}]")
        print(f"  Pipeline       : {sg['pipeline']['toplam_session']} session, "
              f"basari orani %{sg['pipeline']['basari_orani']*100:.0f}")
        print(f"  Hata Loglari   : "
              f"Agent1={sg['hata_ozeti']['agent1_toplam']} "
              f"Agent2={sg['hata_ozeti']['agent2_toplam']} "
              f"Agent3={sg['hata_ozeti']['agent3_toplam']} "
              f"Kalip={sg['hata_ozeti']['tekrar_eden_kalip']}")
        print(f"  Anomali        : {sg['anomaliler']['aktif']} aktif")

        if sg["pipeline"]["ardisik_hata"]:
            print("  [!] KRITIK: Son 3 session ardisik hata!")

        if sg["segment_sagligi"]["dusuk_performans"]:
            print("  [!] Dusuk performans:")
            for dp in sg["segment_sagligi"]["dusuk_performans"]:
                print(f"     {dp['segment']}: %{dp['basari_orani']*100:.0f} basari ({dp['toplam']} karar)")

        print("-"*60)
        print(f"  Bekleyen Oneri : {ob['bekleyen_oneri_sayisi']} adet")
        if ob["bekleyen_oneri_sayisi"] > 0:
            print(f"  Goruntulemek icin:")
            print(f"    {ob['goruntuleme_komutu']}")
        print("="*60 + "\n")

    def _skor_etiketi(self, skor: int) -> str:
        if skor >= 85:
            return "IYI"
        elif skor >= 65:
            return "ORTA"
        elif skor >= 40:
            return "DIKKAT"
        else:
            return "KRITIK"


# ------------------------------------------------------------------ CLI
def cmd_durum(hesap_key, marketplace):
    """Son durum raporunu Supabase'den okuyarak konsola yazdirir."""
    try:
        from supabase.db_client import SupabaseClient
        sdb = SupabaseClient()
        row = sdb._fetch_one("""
            SELECT report_date, health_score, health_status, report_text
            FROM status_reports
            WHERE hesap_key = %s AND marketplace = %s
            ORDER BY report_date DESC LIMIT 1
        """, (hesap_key, marketplace))

        if not row:
            print("Henuz durum raporu yok. Once Agent 4'u calistirin.")
            return

        print(f"\nSon rapor: {row[0]}")
        print(f"Saglik: {row[1]}/100 [{row[2]}]")
        if row[3]:
            print(row[3])
    except Exception as e:
        print(f"Supabase hatasi: {e}")
