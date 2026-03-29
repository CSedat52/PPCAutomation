"""
Agent 4 — Bid Parameter Analyzer (v3 — NEW)
=============================================
ASIN bazinda bid parametresi (hassasiyet + max_degisim) etki analizi.
decision_history tablosundan ASIN bazli performans verisi toplar,
basit Python onerisi uretir. Claude Code bu onerileri degerlendirip
nihai karari verir.

Bid formulu hatirlatma:
  bid_degisim = -tanh(acos_fark_orani x hassasiyet) x max_degisim

  hassasiyet UP   = kucuk ACoS sapmalarina agresif tepki
  hassasiyet DOWN = ACoS sapmasina toleransli, yumusak tepki
  max_degisim UP  = daha buyuk tek seferlik bid degisimi
  max_degisim DOWN = daha kucuk, kademeli degisim
"""

import json
import logging
from collections import defaultdict

logger = logging.getLogger("agent4.bid_param")

MIN_OLCULEBILIR_KARAR = 6   # Oneri uretmek icin minimum olculebilir karar


class BidParamAnalyzer:

    def __init__(self, hesap_key: str, marketplace: str, config_dir=None):
        self.hesap_key = hesap_key
        self.marketplace = marketplace
        self.config_dir = config_dir

    def _get_sdb(self):
        from supabase.db_client import SupabaseClient
        return SupabaseClient()

    def analyze(self) -> list:
        """Her ASIN icin bid param etki analizi ve basit oneri uretir."""
        try:
            sdb = self._get_sdb()
        except Exception as e:
            logger.warning("Supabase baglantisi kurulamadi: %s", e)
            return []

        # ASIN bazli karar verisi topla
        try:
            rows = sdb._fetch_all("""
                SELECT asin, segment, previous_bid, new_bid, change_pct,
                       metrics, kpi_after, decision_status, ad_type
                FROM decision_history
                WHERE hesap_key = %s AND marketplace = %s
                  AND asin IS NOT NULL AND asin != ''
                ORDER BY decision_date ASC
            """, (self.hesap_key, self.marketplace))
        except Exception as e:
            logger.warning("decision_history okunamadi: %s", e)
            return []

        if not rows:
            logger.info("ASIN bazli karar verisi yok.")
            return []

        # ASIN bazli gruplama
        asin_data = defaultdict(lambda: {
            "kararlar": [],
            "segment_dagilimi": defaultdict(int),
        })

        for asin, segment, prev_bid, new_bid, change_pct, metrics, kpi_after, status, ad_type in rows:
            entry = {
                "segment": segment,
                "onceki_bid": float(prev_bid or 0),
                "yeni_bid": float(new_bid or 0),
                "degisim": float(change_pct or 0),
                "metrikler": metrics if isinstance(metrics, dict) else {},
                "kpi_after": kpi_after if isinstance(kpi_after, dict) else None,
                "durum": status,
                "reklam_tipi": ad_type,
            }
            asin_data[asin]["kararlar"].append(entry)
            asin_data[asin]["segment_dagilimi"][segment or "BILINMIYOR"] += 1

        # Mevcut bid parametrelerini al
        mevcut_params = self._get_mevcut_params(sdb)

        # Her ASIN icin analiz
        sonuclar = []
        for asin, data in asin_data.items():
            analiz = self._analiz_asin(asin, data, mevcut_params)
            if analiz:
                sonuclar.append(analiz)

        logger.info("Bid param analizi: %d ASIN analiz edildi, %d oneri uretildi",
                    len(asin_data),
                    sum(1 for s in sonuclar if s.get("python_onerisi")))
        return sonuclar

    def _analiz_asin(self, asin: str, data: dict, mevcut_params: dict) -> dict:
        """Tek ASIN icin performans analizi ve basit oneri."""
        kararlar = data["kararlar"]
        toplam = len(kararlar)
        uygulanan = [k for k in kararlar if k["durum"] in ("APPLIED", "UYGULANDI")]
        kpi_after_olan = [k for k in uygulanan if k["kpi_after"]]

        # Mevcut parametreler
        asin_params = mevcut_params.get(asin, {})
        hassasiyet = asin_params.get("hassasiyet", mevcut_params.get("_global", {}).get("hassasiyet", 0.5))
        max_degisim = asin_params.get("max_degisim", mevcut_params.get("_global", {}).get("max_degisim", 0.20))
        aktif = asin_params.get("aktif", False)
        kaynak = "ASIN" if asin in mevcut_params else "GLOBAL"

        # Segment bazli performans
        segment_perf = defaultdict(lambda: {"toplam": 0, "basarili": 0, "degisimler": []})

        for k in kpi_after_olan:
            seg = k["segment"] or "BILINMIYOR"
            b_met = k["metrikler"]
            a_met = k["kpi_after"]
            segment_perf[seg]["toplam"] += 1

            acos_once = b_met.get("acos")
            acos_sonra = a_met.get("acos")

            # Basari kriteri: segment'e gore
            basarili = False
            if seg in ("KAZANAN", "OPTIMIZE_ET"):
                basarili = (acos_sonra is not None and acos_once is not None
                           and acos_sonra < acos_once)
            elif seg in ("ZARAR", "KAN_KAYBEDEN"):
                basarili = a_met.get("spend", 0) < b_met.get("spend", 0)
            elif seg == "GORUNMEZ":
                basarili = a_met.get("impressions", 0) > 0
            elif seg == "SUPER_STAR":
                basarili = (acos_sonra is not None and acos_once is not None
                           and abs(acos_sonra - acos_once) <= 2.0)
            else:
                basarili = True  # YETERSIZ_VERI, TUZAK

            if basarili:
                segment_perf[seg]["basarili"] += 1

            if acos_once is not None and acos_sonra is not None:
                segment_perf[seg]["degisimler"].append(acos_sonra - acos_once)

        # Genel performans metrikleri
        acos_before_list = [k["metrikler"].get("acos") for k in kpi_after_olan
                            if k["metrikler"].get("acos") is not None]
        acos_after_list = [k["kpi_after"].get("acos") for k in kpi_after_olan
                           if k["kpi_after"].get("acos") is not None]

        ort_acos_once = sum(acos_before_list) / len(acos_before_list) if acos_before_list else None
        ort_acos_sonra = sum(acos_after_list) / len(acos_after_list) if acos_after_list else None

        toplam_basarili = sum(sp["basarili"] for sp in segment_perf.values())
        toplam_olculen = sum(sp["toplam"] for sp in segment_perf.values())
        basari_orani = toplam_basarili / toplam_olculen if toplam_olculen > 0 else None

        # Bid degisim ortalamalari (segment bazli)
        bid_degisim_ortalamalari = {}
        for seg, sp in segment_perf.items():
            if sp["degisimler"]:
                ort_deg = sum(sp["degisimler"]) / len(sp["degisimler"])
                seg_basari = sp["basarili"] / sp["toplam"] if sp["toplam"] > 0 else 0
                bid_degisim_ortalamalari[seg] = {
                    "ort_degisim": round(ort_deg, 2),
                    "basari": round(seg_basari, 2),
                }

        # Python onerisi uret
        python_onerisi = None
        if toplam_olculen >= MIN_OLCULEBILIR_KARAR:
            python_onerisi = self._uret_oneri(
                hassasiyet, max_degisim, basari_orani,
                segment_perf, bid_degisim_ortalamalari,
                ort_acos_once, ort_acos_sonra
            )

        acos_iyilesme = None
        if ort_acos_once and ort_acos_sonra and ort_acos_once > 0:
            acos_iyilesme = round((ort_acos_once - ort_acos_sonra) / ort_acos_once, 3)

        return {
            "asin": asin,
            "mevcut_parametreler": {
                "hassasiyet": hassasiyet,
                "max_degisim": max_degisim,
                "aktif": aktif,
                "kaynak": kaynak,
            },
            "karar_istatistikleri": {
                "toplam_karar": toplam,
                "uygulanan": len(uygulanan),
                "kpi_after_olan": len(kpi_after_olan),
                "segment_dagilimi": dict(data["segment_dagilimi"]),
            },
            "performans_metrikleri": {
                "ortalama_acos_once": round(ort_acos_once, 1) if ort_acos_once else None,
                "ortalama_acos_sonra": round(ort_acos_sonra, 1) if ort_acos_sonra else None,
                "acos_iyilesme_orani": acos_iyilesme,
                "basari_orani": round(basari_orani, 2) if basari_orani is not None else None,
                "bid_degisim_ortalamalari": bid_degisim_ortalamalari,
            },
            "python_onerisi": python_onerisi,
        }

    def _uret_oneri(self, hassasiyet, max_degisim, basari_orani,
                     segment_perf, bid_degisim_ort,
                     ort_acos_once, ort_acos_sonra):
        """Basit kural tabanli oneri uret."""
        oneriler = {}

        # Kural 1: Segment bazli basari orani < %50 → hassasiyeti dusur
        kotu_segmentler = [
            seg for seg, sp in segment_perf.items()
            if sp["toplam"] >= 3 and (sp["basarili"] / sp["toplam"]) < 0.50
            and seg in ("ZARAR", "KAN_KAYBEDEN", "OPTIMIZE_ET")
        ]
        if kotu_segmentler:
            yeni_hass = round(hassasiyet * 0.80, 2)  # %20 dusur
            oneriler["hassasiyet"] = {
                "mevcut": hassasiyet,
                "onerilen": yeni_hass,
                "sebep": f"{', '.join(kotu_segmentler)} segmentlerinde basari orani <%50 — daha yumusak tepki gerekli",
            }

        # Kural 2: ACOS kotulesiyor → max_degisim dusur
        if ort_acos_once and ort_acos_sonra and ort_acos_sonra > ort_acos_once:
            yeni_max = round(max_degisim * 0.80, 2)  # %20 dusur
            oneriler["max_degisim"] = {
                "mevcut": max_degisim,
                "onerilen": yeni_max,
                "sebep": f"ACOS {ort_acos_once:.1f}% -> {ort_acos_sonra:.1f}% (kotulesme) — kademeli yaklasim daha iyi",
            }

        # Kural 3: ACOS iyilesiyor ama cok yavas → hassasiyeti artir
        if (basari_orani and basari_orani > 0.60 and
                ort_acos_once and ort_acos_sonra):
            iyilesme = ort_acos_once - ort_acos_sonra
            if 0 < iyilesme < 1.0:  # Pozitif ama cok kucuk
                yeni_hass = round(hassasiyet * 1.15, 2)  # %15 artir
                if "hassasiyet" not in oneriler:
                    oneriler["hassasiyet"] = {
                        "mevcut": hassasiyet,
                        "onerilen": min(yeni_hass, 1.5),  # max 1.5
                        "sebep": f"Basari orani iyi (%{basari_orani*100:.0f}) ama ACOS iyilesmesi yavas ({iyilesme:.1f}pp) — daha agresif tepki faydali olabilir",
                    }

        return oneriler if oneriler else None

    def _get_mevcut_params(self, sdb) -> dict:
        """Mevcut ASIN ve global bid parametrelerini Supabase'den al."""
        params = {}

        # ASIN bazli parametreler
        try:
            rows = sdb._fetch_all("""
                SELECT asin, aktif, hassasiyet, max_degisim
                FROM asin_bid_params
                WHERE hesap_key = %s AND marketplace = %s
            """, (self.hesap_key, self.marketplace))
            for asin, aktif, hass, max_d in (rows or []):
                params[asin] = {
                    "hassasiyet": float(hass or 0.5),
                    "max_degisim": float(max_d or 0.20),
                    "aktif": bool(aktif),
                }
        except Exception as e:
            logger.warning("Sessiz hata: %s", e)

        # Global tanh parametreleri
        try:
            row = sdb._fetch_one("""
                SELECT tanh_formulu FROM bid_functions
                WHERE hesap_key = %s AND marketplace = %s
            """, (self.hesap_key, self.marketplace))
            if row and row[0]:
                tf = row[0] if isinstance(row[0], dict) else json.loads(row[0])
                params["_global"] = {
                    "hassasiyet": tf.get("hassasiyet", 0.5),
                    "max_degisim": tf.get("max_degisim", 0.20),
                }
        except Exception as e:
            logger.warning("Sessiz hata: %s", e)

        if "_global" not in params:
            params["_global"] = {"hassasiyet": 0.5, "max_degisim": 0.20}

        return params
