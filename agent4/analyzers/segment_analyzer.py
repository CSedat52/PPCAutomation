"""
Agent 4 — Segment Analyzer (v3 — Supabase Only)
==================================================
bid_recommendations tablosundan direkt okur.
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
        """bid_recommendations tablosundan APPROVED/MODIFIED olanlari analiz et."""
        try:
            sdb = self._get_sdb()
            rows = sdb._fetch_all("""
                SELECT segment, current_bid, recommended_bid, decision_bid,
                       impressions, clicks, cost, sales, orders, acos, cvr, cpc,
                       decision, decided_at
                FROM bid_recommendations
                WHERE hesap_key = %s AND marketplace = %s
                  AND decision IN ('APPROVED', 'MODIFIED') AND decided_at IS NOT NULL
            """, (self.hesap_key, self.marketplace))
        except Exception as e:
            logger.warning("bid_recommendations okunamadi, fallback: %s", e)
            return self._analyze_from_db()

        if not rows:
            logger.info("Segment analizi: yeterli veri yok (Supabase).")
            return {"durum": "YETERSIZ_VERI", "olculebilir_karar": 0}

        seg_data = defaultdict(lambda: {"toplam": 0, "basarili": 0, "ornekler": []})

        for (segment, current_bid, recommended_bid, decision_bid,
             impressions, clicks, cost, sales, orders, acos, cvr, cpc,
             decision, decided_at) in rows:
            seg = segment or "BILINMIYOR"
            b_met = {
                "impressions": int(impressions or 0),
                "clicks": int(clicks or 0),
                "spend": float(cost or 0),
                "sales": float(sales or 0),
                "orders": int(orders or 0),
                "acos": float(acos) if acos is not None else None,
                "cvr": float(cvr) if cvr is not None else None,
                "cpc": float(cpc) if cpc is not None else None,
            }
            # kpi_after yok bid_recommendations'ta — in-memory DB'den eslestir
            a_met = {}
            kararlar = self.db.get("karar_gecmisi", {}).get("kararlar", [])
            hedefleme_id = ""  # eslestirme icin
            for k in kararlar:
                if (k.get("segment") == segment and
                        k.get("onceki_bid") == float(current_bid or 0) and
                        k.get("kpi_after")):
                    a_met = k["kpi_after"]
                    break

            if not a_met:
                continue  # kpi_after yoksa olculemez

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
