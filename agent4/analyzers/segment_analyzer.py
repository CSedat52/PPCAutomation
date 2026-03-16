"""
Agent 4 — Segment Analyzer
============================
Agent 2'nin 8 segmentinin gerçekte ne kadar doğru çalıştığını ölçer.

Başarı Kriteri:
  KAZANAN    → kpi_after ACOS, kpi_before ACOS'tan düşük
  OPTIMIZE_ET → kpi_after ACOS, kpi_before'dan düşük (bid düşüşüne rağmen kazanç)
  ZARAR      → kpi_after spend, kpi_before spend'den düşük
  KAN_KAYBEDEN → kpi_after spend, kpi_before spend'den düşük
  GORUNMEZ   → kpi_after impressions > 0
  SUPER_STAR → kpi_after ACOS korunmuş (±2pp tolerans)
"""

import logging
from collections import defaultdict

logger = logging.getLogger("agent4.segment")

# Her segment için başarı kriteri fonksiyonu
BASARI_KRITERLERI = {
    "KAZANAN":     lambda b, a: a["acos"] is not None and b["acos"] is not None and a["acos"] < b["acos"],
    "OPTIMIZE_ET": lambda b, a: a["acos"] is not None and b["acos"] is not None and a["acos"] < b["acos"],
    "ZARAR":       lambda b, a: a.get("spend", 0) < b.get("spend", 0),
    "KAN_KAYBEDEN":lambda b, a: a.get("spend", 0) < b.get("spend", 0),
    "GORUNMEZ":    lambda b, a: a.get("impressions", 0) > 0,
    "SUPER_STAR":  lambda b, a: (a["acos"] is not None and b["acos"] is not None
                                  and abs(a["acos"] - b["acos"]) <= 2.0),
    "YETERSIZ_VERI": lambda b, a: True,   # Nötr — değerlendirme yok
    "TUZAK":         lambda b, a: True,   # Nötr — dokunulmaz
}


class SegmentAnalyzer:

    def __init__(self, db):
        self.db = db

    def analyze(self) -> dict:
        kararlar = self.db.get("karar_gecmisi")["kararlar"]

        # Sadece kpi_after dolu ve uygulanan kararlar
        olculebilir = [
            k for k in kararlar
            if k.get("kpi_after") and k.get("karar_durumu") == "UYGULANDI"
        ]

        if not olculebilir:
            logger.info("Segment analizi: yeterli kpi_after verisi yok.")
            return {"durum": "YETERSIZ_VERI", "olculebilir_karar": 0}

        seg_data = defaultdict(lambda: {"toplam": 0, "basarili": 0, "ornekler": []})

        for k in olculebilir:
            seg   = k.get("segment", "BILINMIYOR")
            b_met = k.get("metrikler", {})   # kpi_before
            a_met = k.get("kpi_after", {})   # kpi_after

            kriter = BASARI_KRITERLERI.get(seg)
            if kriter:
                try:
                    basarili = kriter(b_met, a_met)
                except Exception:
                    basarili = False
            else:
                basarili = False

            seg_data[seg]["toplam"]   += 1
            seg_data[seg]["basarili"] += int(basarili)

            # Her segment için max 5 örnek sakla
            if len(seg_data[seg]["ornekler"]) < 5:
                seg_data[seg]["ornekler"].append({
                    "hedefleme":    k.get("hedefleme", ""),
                    "acos_oncesi":  b_met.get("acos"),
                    "acos_sonrasi": a_met.get("acos"),
                    "basarili":     basarili,
                })

            # DB istatistiklerini güncelle
            self.db.update_segment_stat(seg, int(basarili), 1)

        # Sonuç özeti
        sonuclar = {}
        for seg, d in seg_data.items():
            oran = round(d["basarili"] / d["toplam"], 3) if d["toplam"] > 0 else 0
            sonuclar[seg] = {
                "toplam":       d["toplam"],
                "basarili":     d["basarili"],
                "basari_orani": oran,
                "ornekler":     d["ornekler"],
            }

        # Dikkat gerektiren segmentler (başarı oranı < %40, en az 10 karar)
        dusuk_performans = [
            {"segment": seg, "basari_orani": v["basari_orani"], "toplam": v["toplam"]}
            for seg, v in sonuclar.items()
            if v["basari_orani"] < 0.40 and v["toplam"] >= 10
            and seg not in ("YETERSIZ_VERI", "TUZAK")
        ]

        logger.info("Segment analizi: %d segment, %d ölçülebilir karar",
                    len(sonuclar), len(olculebilir))

        return {
            "durum":             "TAMAMLANDI",
            "olculebilir_karar": len(olculebilir),
            "segment_sonuclari": sonuclar,
            "dusuk_performans":  dusuk_performans,
        }
