"""
Agent 4 — Report Generator
============================
Tum analiz sonuclarini birlestirerek durum raporu uretir.

Cikti: data/agent4/raporlar/{tarih}_durum_raporu.json

Rapor iki bolumden olusur:
  1. SISTEM SAGLIGI — Pipeline, agent basari oranlari, anomaliler
  2. ONAY BEKLIYOR  — Uretilen oneriler ve ozet bilgi
"""

import json
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("agent4.report")


class ReportGenerator:

    def __init__(self, data_dir, db):
        self.data_dir    = Path(data_dir)
        self.rapor_dir   = self.data_dir / "agent4" / "raporlar"
        self.rapor_dir.mkdir(parents=True, exist_ok=True)
        self.db          = db

    def generate(self, today: str, sonuclar: dict) -> dict:
        rapor = {
            "tarih":           today,
            "olusturma":       datetime.utcnow().isoformat(),
            "versiyon":        "1.0",
            "sistem_sagligi":  self._sistem_sagligi(sonuclar),
            "onay_bekliyor":   self._onay_bekliyor(sonuclar),
            "ozet":            self._ozet(sonuclar),
        }

        rapor_path = self.rapor_dir / f"{today}_durum_raporu.json"
        with open(rapor_path, "w", encoding="utf-8") as f:
            json.dump(rapor, f, indent=2, ensure_ascii=False)

        rapor["rapor_dosyasi"] = str(rapor_path)
        logger.info("Durum raporu kaydedildi: %s", rapor_path)

        # Konsol ozeti yazdir
        self._yazdir_ozet(rapor)
        return rapor

    # ------------------------------------------ Sistem sagligi bolumu
    def _sistem_sagligi(self, sonuclar: dict) -> dict:
        maestro = sonuclar.get("maestro", {})
        hata    = sonuclar.get("hata", {})
        kpi     = sonuclar.get("kpi", {})
        anomali = sonuclar.get("anomali", {})
        segment = sonuclar.get("segment", {})

        # Genel saglik skoru hesapla (0-100)
        skor = 100

        # Maestro hata orani
        maestro_hata = maestro.get("hata_orani", 0)
        if maestro_hata > 0.30:
            skor -= 30
        elif maestro_hata > 0.15:
            skor -= 15

        # Ardisik hata alarmi
        if maestro.get("ardisik_hata_alarmi"):
            skor -= 25

        # Aktif anomali sayisi
        aktif_anomali = anomali.get("aktif_anomali", 0)
        skor -= min(aktif_anomali * 5, 20)

        # Segment basari orani dusuk
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
                "aktif":      anomali.get("aktif_anomali", 0),
                "yeni":       anomali.get("yeni_anomali", 0),
                "detaylar":   anomali.get("yeni_detaylar", []),
            },
            "segment_sagligi": {
                "olculebilir_karar":  segment.get("olculebilir_karar", 0),
                "dusuk_performans":   dusuk_performans,
            },
        }

    # ------------------------------------------ Onay bekliyor bölümü
    def _onay_bekliyor(self, sonuclar: dict) -> dict:
        oneriler   = sonuclar.get("oneriler", [])
        proposals_dir = self.data_dir / "agent4" / "proposals" / "bekleyen"

        # Mevcut bekleyen dosyalari say
        bekleyen_dosyalar = sorted(proposals_dir.glob("ONR-*.json")) if proposals_dir.exists() else []
        bekleyen_sayisi   = len(bekleyen_dosyalar)

        oneri_ozet = []
        for d in bekleyen_dosyalar[:10]:   # Max 10 göster
            try:
                with open(d, encoding="utf-8") as f:
                    o = json.load(f)
                oneri_ozet.append({
                    "id":       o["id"],
                    "kategori": o["kategori"],
                    "ne":       o["ne"],
                    "kazanim":  o["kazanim"],
                })
            except Exception:
                continue

        return {
            "bekleyen_oneri_sayisi": bekleyen_sayisi,
            "bu_calisma_uretilen":   len(oneriler),
            "goruntuleme_komutu":    "python agent4/optimizer.py oneri listele",
            "oneriler":              oneri_ozet,
        }

    # ------------------------------------------ Kisa ozet
    def _ozet(self, sonuclar: dict) -> str:
        maestro = sonuclar.get("maestro", {})
        anomali = sonuclar.get("anomali", {})
        oneriler = sonuclar.get("oneriler", [])
        kpi     = sonuclar.get("kpi", {})

        parcalar = []
        parcalar.append(f"Pipeline: {maestro.get('tamamlanan', 0)}/{maestro.get('toplam_session', 0)} session basarili")
        parcalar.append(f"KPI: {kpi.get('kpi_after_doldurulan', 0)} karar guncellendi")
        parcalar.append(f"Anomali: {anomali.get('aktif_anomali', 0)} aktif")
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
        print(f"  Anomali        : {sg['anomaliler']['aktif']} aktif, {sg['anomaliler']['yeni']} yeni")

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
def cmd_durum(data_dir):
    """Son durum raporunu konsola yazdirir."""
    rapor_dir = Path(data_dir) / "agent4" / "raporlar"
    if not rapor_dir.exists():
        print("Henuz durum raporu yok. Once Agent 4'u calistirin.")
        return

    dosyalar = sorted(rapor_dir.glob("*_durum_raporu.json"))
    if not dosyalar:
        print("Henuz durum raporu yok.")
        return

    son_rapor = dosyalar[-1]
    with open(son_rapor, encoding="utf-8") as f:
        rapor = json.load(f)

    print(f"\nSon rapor: {son_rapor.name}")
    print(json.dumps(rapor["ozet"], indent=2, ensure_ascii=False))
