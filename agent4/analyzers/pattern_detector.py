"""
Agent 4 — Pattern Detector
============================
Kümülatif karar geçmişinde tekrar eden davranış kalıplarını arar.

Aradığı kalıplar:
  - Sezonsal: Haftanın belirli günleri farklı ACOS sonuçları
  - Portfolio: Belirli portföylerde segment kümelenmesi
  - Bid döngüsü: Aynı hedefleme tekrar tekrar aynı segmente giriyor
  - Bütçe drenajı: Kampanya bütçe tükenmesi kalıbı
"""

import logging
from collections import defaultdict, Counter
from datetime import datetime

logger = logging.getLogger("agent4.pattern")

MIN_VERI    = 6    # Kalıp için minimum gözlem sayısı
MIN_FARK_PP = 3.0  # Sezonsal kalıp için minimum ACOS farkı (pp)


class PatternDetector:

    def __init__(self, db):
        self.db = db

    def detect(self) -> dict:
        kararlar = self.db.get("karar_gecmisi")["kararlar"]
        olculebilir = [
            k for k in kararlar
            if k.get("kpi_after") and k.get("karar_durumu") == "UYGULANDI"
        ]

        kaliplar = []
        kaliplar += self._sezonsal_kalip(olculebilir)
        kaliplar += self._bid_dongusu(kararlar)
        kaliplar += self._portfolio_kumelenme(kararlar)

        # Tespit edilenleri DB'ye kaydet
        for k in kaliplar:
            self.db.add_kalip(k)

        logger.info("Kalıp tespiti: %d ölçülebilir karar, %d kalıp bulundu",
                    len(olculebilir), len(kaliplar))

        return {
            "olculebilir_karar": len(olculebilir),
            "kalip_sayisi":      len(kaliplar),
            "kaliplar":          kaliplar,
        }

    # ------------------------------------------------- Sezonsal kalıp
    def _sezonsal_kalip(self, olculebilir: list) -> list:
        """Haftanın günlerine göre ACOS değişim farklarını karşılaştırır."""
        if len(olculebilir) < MIN_VERI:
            return []

        gun_degisimler = defaultdict(list)   # gun_adi -> [acos_degisim, ...]

        for k in olculebilir:
            try:
                tarih_dt  = datetime.strptime(k["tarih"], "%Y-%m-%d")
                gun_adi   = tarih_dt.strftime("%A")  # Monday, Tuesday...
                acos_once = k["metrikler"].get("acos")
                acos_sonra = k["kpi_after"].get("acos")
                if acos_once is not None and acos_sonra is not None:
                    degisim = acos_sonra - acos_once   # negatif = iyi
                    gun_degisimler[gun_adi].append(degisim)
            except Exception:
                continue

        if len(gun_degisimler) < 3:
            return []

        gun_ortalamalari = {
            gun: sum(vals) / len(vals)
            for gun, vals in gun_degisimler.items()
            if len(vals) >= 3
        }

        if len(gun_ortalamalari) < 2:
            return []

        en_iyi  = min(gun_ortalamalari, key=gun_ortalamalari.get)
        en_kotu = max(gun_ortalamalari, key=gun_ortalamalari.get)
        fark    = gun_ortalamalari[en_kotu] - gun_ortalamalari[en_iyi]

        kaliplar = []
        if fark >= MIN_FARK_PP:
            kaliplar.append({
                "tip":    "SEZONSAL",
                "tanim":  f"{en_iyi} günleri ACOS {abs(gun_ortalamalari[en_iyi]):.1f}pp iyileşiyor, {en_kotu} günleri {fark:.1f}pp fark var",
                "veri":   gun_ortalamalari,
                "oneri":  f"Agent 2 için hafta içi/sonu bid agresifliği parametresi eklenebilir",
            })
        return kaliplar

    # ------------------------------------------------- Bid döngüsü
    def _bid_dongusu(self, kararlar: list) -> list:
        """Aynı hedefleme tekrar tekrar aynı segmente giriyor mu?"""
        hedefleme_segmentler = defaultdict(list)

        for k in sorted(kararlar, key=lambda x: x.get("tarih", "")):
            hid = k.get("hedefleme_id", "")
            seg = k.get("segment", "")
            if hid and seg not in ("YETERSIZ_VERI", "TUZAK"):
                hedefleme_segmentler[hid].append(seg)

        kaliplar = []
        for hid, segmentler in hedefleme_segmentler.items():
            if len(segmentler) < 4:
                continue
            # Son 4 segmentin 3'ü ZARAR veya KAN_KAYBEDEN ise döngü var
            son_4     = segmentler[-4:]
            kotu_sayisi = sum(1 for s in son_4 if s in ("ZARAR", "KAN_KAYBEDEN", "OPTIMIZE_ET"))
            if kotu_sayisi >= 3:
                # Hedefleme adını bul
                hedefleme_adi = next(
                    (k.get("hedefleme", "") for k in kararlar if k.get("hedefleme_id") == hid),
                    hid
                )
                kaliplar.append({
                    "tip":         "BID_DONGUSU",
                    "tanim":       f"'{hedefleme_adi}' son 4 çalışmada {kotu_sayisi}x kötü segment",
                    "hedefleme_id": hid,
                    "segment_gecmisi": son_4,
                    "oneri":       f"Bu hedefleme için max_degisim düşürülebilir veya bid tavanı gözden geçirilebilir",
                })
        return kaliplar[:5]   # max 5 döngü kalıbı

    # ------------------------------------------------- Portföy kümelenmesi
    def _portfolio_kumelenme(self, kararlar: list) -> list:
        """Belirli portföylerde belirli segmentler aşırı temsil ediliyor mu?"""
        if not kararlar:
            return []

        portfolio_segmentler = defaultdict(list)
        for k in kararlar:
            pid = k.get("portfolio_id", "")
            seg = k.get("segment", "")
            if pid and seg:
                portfolio_segmentler[pid].append(seg)

        kaliplar = []
        for pid, segmentler in portfolio_segmentler.items():
            if len(segmentler) < MIN_VERI:
                continue
            sayac = Counter(segmentler)
            toplam = len(segmentler)
            # KAN_KAYBEDEN oranı > %25 ise dikkat çek
            kan_kayb_oran = sayac.get("KAN_KAYBEDEN", 0) / toplam
            if kan_kayb_oran > 0.25:
                kaliplar.append({
                    "tip":          "PORTFOLIO_KUMELENMESI",
                    "tanim":        f"Portfolio {pid}: KAN_KAYBEDEN oranı %{kan_kayb_oran*100:.0f} ({sayac['KAN_KAYBEDEN']}/{toplam})",
                    "portfolio_id": pid,
                    "segment_dagilimi": dict(sayac.most_common()),
                    "oneri":        f"Portfolio {pid}'deki match type veya hedefleme stratejisi gözden geçirilebilir",
                })
        return kaliplar[:3]
