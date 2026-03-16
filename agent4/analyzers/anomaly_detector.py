"""
Agent 4 — Anomaly Detector
============================
Normal dağılımdan sapmaları tespit eder ve DB'ye anomali olarak kaydeder.
"""

import logging
from collections import defaultdict
from datetime import datetime

logger = logging.getLogger("agent4.anomaly")


class AnomalyDetector:

    ACOS_SAPMA_ESIGI   = 0.60   # %60 sapma
    SIFIR_IMP_ESIGI    = 2      # Ardışık kaç çalışmada 0 impression
    MIN_VERI           = 3      # Karşılaştırma için minimum geçmiş nokta

    def __init__(self, db):
        self.db = db

    def detect(self) -> dict:
        kararlar    = self.db.get("karar_gecmisi")["kararlar"]
        anomaliler  = []

        anomaliler += self._acos_ani_yukselis(kararlar)
        anomaliler += self._sifir_impression(kararlar)

        # DB'ye kaydet ve mevcut aktif anomalileri güncelle
        for a in anomaliler:
            self.db.add_anomali(a)

        aktif = self.db.get_aktif_anomaliler()
        logger.info("Anomali tespiti: %d yeni, %d toplam aktif", len(anomaliler), len(aktif))

        return {
            "yeni_anomali":   len(anomaliler),
            "aktif_anomali":  len(aktif),
            "yeni_detaylar":  anomaliler,
        }

    # ------------------------------------------------- ACOS ani yükseliş
    def _acos_ani_yukselis(self, kararlar: list) -> list:
        """
        Bir ASIN'in ACOS'u son 3 çalışmanın ortalamasından
        ACOS_SAPMA_ESIGI kadar saptıysa anomali işaretle.
        """
        asin_acos = defaultdict(list)

        for k in sorted(kararlar, key=lambda x: x.get("tarih", "")):
            asin = k.get("asin", "")
            acos = k.get("metrikler", {}).get("acos")
            if asin and acos is not None:
                asin_acos[asin].append(acos)

        anomaliler = []
        for asin, gecmis in asin_acos.items():
            if len(gecmis) < self.MIN_VERI + 1:
                continue

            referans_ort = sum(gecmis[:-1]) / len(gecmis[:-1])
            son_acos     = gecmis[-1]

            if referans_ort == 0:
                continue

            sapma = abs(son_acos - referans_ort) / referans_ort
            if sapma >= self.ACOS_SAPMA_ESIGI:
                anomaliler.append({
                    "tip":             "ACOS_ANI_YUKSELIS",
                    "asin":            asin,
                    "tanim":           f"{asin}: ACOS {referans_ort:.1f}% → {son_acos:.1f}% (%{sapma*100:.0f} sapma)",
                    "referans_acos":   round(referans_ort, 2),
                    "son_acos":        round(son_acos, 2),
                    "sapma_orani":     round(sapma, 3),
                    "siddet":          "YUKSEK" if sapma >= 1.0 else "ORTA",
                    "durum":           "AKTIF",
                    "oneri":           f"{asin} için güncel kampanya durumunu ve negatif keyword stratejisini kontrol edin",
                })

        return anomaliler

    # ------------------------------------------------- Sıfır impression
    def _sifir_impression(self, kararlar: list) -> list:
        """
        Aktif hedefleme N+ ardışık çalışmada 0 impression aldıysa anomali.
        """
        hedefleme_imp = defaultdict(list)

        for k in sorted(kararlar, key=lambda x: x.get("tarih", "")):
            hid = k.get("hedefleme_id", "")
            imp = k.get("metrikler", {}).get("impressions", None)
            if hid and imp is not None:
                hedefleme_imp[hid].append({"imp": imp, "seg": k.get("segment", ""),
                                            "hedefleme": k.get("hedefleme", "")})

        anomaliler = []
        for hid, gecmis in hedefleme_imp.items():
            if len(gecmis) < self.SIFIR_IMP_ESIGI:
                continue

            son_n = gecmis[-self.SIFIR_IMP_ESIGI:]
            if all(g["imp"] == 0 for g in son_n):
                # YETERSIZ_VERI veya TUZAK segmentindeyse normal sayılır
                son_seg = son_n[-1]["seg"]
                if son_seg in ("YETERSIZ_VERI", "TUZAK", "GORUNMEZ"):
                    continue
                hedefleme_adi = son_n[-1]["hedefleme"]
                anomaliler.append({
                    "tip":            "SIFIR_IMPRESSION",
                    "hedefleme_id":   hid,
                    "hedefleme":      hedefleme_adi,
                    "tanim":          f"'{hedefleme_adi}' {self.SIFIR_IMP_ESIGI}+ çalışmadır 0 impression",
                    "ardisik_sayi":   self.SIFIR_IMP_ESIGI,
                    "son_segment":    son_seg,
                    "siddet":         "ORTA",
                    "durum":          "AKTIF",
                    "oneri":          "Bid çok düşük olabilir veya hedefleme pasif edilmiş olabilir",
                })

        return anomaliler[:10]   # max 10
