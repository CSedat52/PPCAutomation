"""
Agent 4 — Segment Analyzer (v3 — Supabase Only)
==================================================
decision_history tablosundan direkt okur.
Agent 2'nin 8 segmentinin gercekte ne kadar dogru calistigini olcer.

Basari Kriteri:
  KAZANAN     -> kpi_after ACOS, kpi_before ACOS'tan dusuk
  OPTIMIZE_ET -> kpi_after ACOS, kpi_before'dan dusuk
  ZARAR       -> kpi_after spend, kpi_before spend'den dusuk
  KAN_KAYBEDEN -> kpi_after spend, kpi_before spend'den dusuk
  GORUNMEZ    -> kpi_after impressions > 0
  SUPER_STAR  -> kpi_after ACOS korunmus (+/-2pp tolerans)
"""

import logging
from collections import defaultdict

logger = logging.getLogger("agent4.segment")

BASARI_KRITERLERI = {
    "KAZANAN":      lambda b, a: a.get("acos") is not None and b.get("acos") is not None and a["acos"] < b["acos"],
    "OPTIMIZE_ET":  lambda b, a: a.get("acos") is not None and b.get("acos") is not None and a["acos"] < b["acos"],
    "ZARAR":        lambda b, a: a.get("spend", 0) < b.get("spend", 0),
    "KAN_KAYBEDEN": lambda b, a: a.get("spend", 0) < b.get("spend", 0),
    "GORUNMEZ":     lambda b, a: a.get("impressions", 0) > 0,
    "SUPER_STAR":   lambda b, a: (a.get("acos") is not None and b.get("acos") is not None
                                   and abs(a["acos"] - b["acos"]) <= 2.0),
    "YETERSIZ_VERI": lambda b, a: True,
    "TUZAK":         lambda b, a: True,
}


class SegmentAnalyzer:

    def __init__(self, hesap_key: str, marketplace: str, db):
        self.hesap_key = hesap_key
        self.marketplace = marketplace
        self.db = db

    def _get_sdb(self):
        from supabase.db_client import SupabaseClient
        return SupabaseClient()

    def analyze(self) -> dict:
        """decision_history tablosundan APPLIED + kpi_after IS NOT NULL olanlari analiz et."""
        try:
            sdb = self._get_sdb()
            rows = sdb._fetch_all("""
                SELECT segment, previous_bid, new_bid, metrics, kpi_after
                FROM decision_history
                WHERE hesap_key = %s AND marketplace = %s
                  AND decision_status = 'APPLIED' AND kpi_after IS NOT NULL
            """, (self.hesap_key, self.marketplace))
        except Exception as e:
            logger.warning("decision_history okunamadi, fallback: %s", e)
            return self._analyze_from_db()

        if not rows:
            logger.info("Segment analizi: yeterli kpi_after verisi yok (Supabase).")
            return {"durum": "YETERSIZ_VERI", "olculebilir_karar": 0}

        seg_data = defaultdict(lambda: {"toplam": 0, "basarili": 0, "ornekler": []})

        for segment, prev_bid, new_bid, metrics, kpi_after in rows:
            seg = segment or "BILINMIYOR"
            b_met = metrics if isinstance(metrics, dict) else {}
            a_met = kpi_after if isinstance(kpi_after, dict) else {}

            kriter = BASARI_KRITERLERI.get(seg)
            try:
                basarili = kriter(b_met, a_met) if kriter else False
            except Exception:
                basarili = False

            seg_data[seg]["toplam"] += 1
            seg_data[seg]["basarili"] += int(basarili)

            if len(seg_data[seg]["ornekler"]) < 5:
                seg_data[seg]["ornekler"].append({
                    "acos_oncesi":  b_met.get("acos"),
                    "acos_sonrasi": a_met.get("acos"),
                    "basarili":     basarili,
                })

            self.db.update_segment_stat(seg, int(basarili), 1)

        sonuclar = {}
        toplam_olculebilir = 0
        for seg, d in seg_data.items():
            oran = round(d["basarili"] / d["toplam"], 3) if d["toplam"] > 0 else 0
            sonuclar[seg] = {
                "toplam":       d["toplam"],
                "basarili":     d["basarili"],
                "basari_orani": oran,
                "ornekler":     d["ornekler"],
            }
            toplam_olculebilir += d["toplam"]

        dusuk_performans = [
            {"segment": seg, "basari_orani": v["basari_orani"], "toplam": v["toplam"]}
            for seg, v in sonuclar.items()
            if v["basari_orani"] < 0.40 and v["toplam"] >= 10
            and seg not in ("YETERSIZ_VERI", "TUZAK")
        ]

        logger.info("Segment analizi: %d segment, %d olculebilir karar",
                     len(sonuclar), toplam_olculebilir)

        return {
            "durum":             "TAMAMLANDI",
            "olculebilir_karar": toplam_olculebilir,
            "segment_sonuclari": sonuclar,
            "dusuk_performans":  dusuk_performans,
        }

    def _analyze_from_db(self) -> dict:
        """Fallback: in-memory DB'den analiz et."""
        kararlar = self.db.get("karar_gecmisi")["kararlar"]
        olculebilir = [
            k for k in kararlar
            if k.get("kpi_after") and k.get("karar_durumu") == "UYGULANDI"
        ]
        if not olculebilir:
            return {"durum": "YETERSIZ_VERI", "olculebilir_karar": 0}

        seg_data = defaultdict(lambda: {"toplam": 0, "basarili": 0, "ornekler": []})
        for k in olculebilir:
            seg   = k.get("segment", "BILINMIYOR")
            b_met = k.get("metrikler", {})
            a_met = k.get("kpi_after", {})
            kriter = BASARI_KRITERLERI.get(seg)
            try:
                basarili = kriter(b_met, a_met) if kriter else False
            except Exception:
                basarili = False
            seg_data[seg]["toplam"]   += 1
            seg_data[seg]["basarili"] += int(basarili)
            if len(seg_data[seg]["ornekler"]) < 5:
                seg_data[seg]["ornekler"].append({
                    "acos_oncesi":  b_met.get("acos"),
                    "acos_sonrasi": a_met.get("acos"),
                    "basarili":     basarili,
                })
            self.db.update_segment_stat(seg, int(basarili), 1)

        sonuclar = {}
        for seg, d in seg_data.items():
            oran = round(d["basarili"] / d["toplam"], 3) if d["toplam"] > 0 else 0
            sonuclar[seg] = {
                "toplam":       d["toplam"],
                "basarili":     d["basarili"],
                "basari_orani": oran,
                "ornekler":     d["ornekler"],
            }

        dusuk = [
            {"segment": seg, "basari_orani": v["basari_orani"], "toplam": v["toplam"]}
            for seg, v in sonuclar.items()
            if v["basari_orani"] < 0.40 and v["toplam"] >= 10
            and seg not in ("YETERSIZ_VERI", "TUZAK")
        ]

        return {
            "durum":             "TAMAMLANDI",
            "olculebilir_karar": len(olculebilir),
            "segment_sonuclari": sonuclar,
            "dusuk_performans":  dusuk,
        }
