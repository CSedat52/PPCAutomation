"""
Agent 4 — Bid Parameter Analyzer (v5 — Gap Closure Regression)
================================================================
(ASIN x targeting_type) bazinda optimal hassasiyet ve max_degisim bulur.

Mantik:
  Her karar icin "ideal bid degisimi" hesaplanir:
    - Gercek bid degisimi X ACoS degisimi Y uretmis
    - Hedefe ulasmak icin gereken ACoS degisimi = gap_before
    - ideal_bid_degisim = gap_before * actual_bid_change / actual_acos_change

  Sonra scipy curve_fit ile:
    ideal_bid_degisim = -tanh(acos_fark_orani * hassasiyet) * max_degisim
  formulune fit edilir.

Gereksinimler: numpy, scipy
"""

import json
import logging
import warnings
from collections import defaultdict
from datetime import datetime

import numpy as np
from scipy.optimize import curve_fit, OptimizeWarning

logger = logging.getLogger("agent4.bid_param")

MIN_VERI_NOKTASI = 20
MIN_R_SQUARED = 0.30
HASSASIYET_MIN = 0.1
HASSASIYET_MAX = 2.0
MAX_DEGISIM_MIN = 0.05
MAX_DEGISIM_MAX = 0.40


def tanh_model(x, hassasiyet, max_degisim):
    return -np.tanh(x * hassasiyet) * max_degisim


class BidParamAnalyzer:

    def __init__(self, hesap_key: str, marketplace: str, config_dir=None):
        self.hesap_key = hesap_key
        self.marketplace = marketplace

    def _get_sdb(self):
        from supabase.db_client import SupabaseClient
        return SupabaseClient()

    def analyze(self) -> list:
        try:
            sdb = self._get_sdb()
        except Exception as e:
            logger.warning("Supabase baglantisi kurulamadi: %s", e)
            return []

        rows = self._fetch_verified_decisions(sdb)
        if not rows:
            logger.info("Regresyon icin veri yok.")
            return []

        gruplar = self._grupla(rows)
        mevcut_params = self._get_mevcut_params(sdb)
        today = datetime.utcnow().strftime("%Y-%m-%d")
        sonuclar = []

        for (asin, targeting_type), veri_noktalari in gruplar.items():
            params = self._get_params_for_group(asin, targeting_type, mevcut_params)
            analiz = self._analiz_grup(asin, targeting_type, veri_noktalari, params, today)
            if analiz:
                self._kaydet_supabase(sdb, analiz, veri_noktalari, today)
                sonuclar.append(analiz)

        logger.info("Bid param analizi: %d grup, %d fit basarili",
                    len(gruplar), sum(1 for s in sonuclar if s.get("fit_basarili")))
        return sonuclar

    def _fetch_verified_decisions(self, sdb) -> list:
        try:
            return sdb._fetch_all("""
                SELECT asin, targeting_type, targeting_id, decision_date,
                       change_pct, acos_before, acos_after, hedef_acos,
                       gap_closure, previous_bid, new_bid, segment
                FROM decision_history
                WHERE hesap_key = %s AND marketplace = %s
                  AND decision_status = 'VERIFIED'
                  AND acos_before IS NOT NULL AND acos_after IS NOT NULL
                  AND asin IS NOT NULL AND asin != ''
                  AND change_pct IS NOT NULL AND change_pct != 0
                ORDER BY decision_date ASC
            """, (self.hesap_key, self.marketplace)) or []
        except Exception as e:
            logger.warning("decision_history okunamadi: %s", e)
            return []

    def _grupla(self, rows: list) -> dict:
        gruplar = defaultdict(list)
        for row in rows:
            (asin, targeting_type, targeting_id, decision_date,
             change_pct, acos_before, acos_after, hedef_acos,
             gap_closure, previous_bid, new_bid, segment) = row

            if not asin or not targeting_type:
                continue

            acos_b = float(acos_before)
            acos_a = float(acos_after)
            hedef = float(hedef_acos) if hedef_acos else None
            bid_degisim = float(change_pct) / 100.0
            acos_degisim = acos_a - acos_b

            if hedef is None or hedef <= 0:
                continue
            if abs(bid_degisim) < 0.001:
                continue
            if abs(acos_degisim) < 0.01:
                continue

            acos_fark_orani = (acos_b - hedef) / hedef

            sensitivity = acos_degisim / bid_degisim
            gap_before = acos_b - hedef
            ideal_bid_degisim = -gap_before / sensitivity if abs(sensitivity) > 0.001 else None

            if ideal_bid_degisim is None:
                continue

            gruplar[(asin, targeting_type)].append({
                "targeting_id": str(targeting_id),
                "decision_date": str(decision_date),
                "acos_fark_orani": round(acos_fark_orani, 4),
                "bid_degisim_pct": round(bid_degisim * 100, 2),
                "ideal_bid_degisim_pct": round(ideal_bid_degisim * 100, 2),
                "acos_before": round(acos_b, 2),
                "acos_after": round(acos_a, 2),
                "hedef_acos": round(hedef, 2),
                "gap_closure": round(float(gap_closure), 4) if gap_closure is not None else None,
            })

        return dict(gruplar)

    def _analiz_grup(self, asin, targeting_type, veri_noktalari, mevcut, today):
        n = len(veri_noktalari)
        h_mevcut = mevcut.get("hassasiyet", 0.5)
        m_mevcut = mevcut.get("max_degisim", 0.20)

        ort_gc = [v["gap_closure"] for v in veri_noktalari if v["gap_closure"] is not None]
        ort_gap_closure = sum(ort_gc) / len(ort_gc) if ort_gc else None

        sonuc = {
            "asin": asin, "targeting_type": targeting_type,
            "veri_noktasi": n,
            "hassasiyet_mevcut": h_mevcut, "max_degisim_mevcut": m_mevcut,
            "ort_gap_closure": round(ort_gap_closure, 4) if ort_gap_closure else None,
            "ort_acos_before": round(sum(v["acos_before"] for v in veri_noktalari) / n, 2),
            "ort_acos_after": round(sum(v["acos_after"] for v in veri_noktalari) / n, 2),
            "hedef_acos": veri_noktalari[0]["hedef_acos"] if veri_noktalari else None,
            "analysis_date": today,
        }

        if n < MIN_VERI_NOKTASI:
            sonuc["durum"] = "YETERSIZ_VERI"
            sonuc["fit_basarili"] = False
            sonuc["mesaj"] = f"{n} veri noktasi, minimum {MIN_VERI_NOKTASI} gerekli"
            return sonuc

        x = np.array([v["acos_fark_orani"] for v in veri_noktalari])
        y = np.array([v["ideal_bid_degisim_pct"] / 100.0 for v in veri_noktalari])

        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", OptimizeWarning)
                popt, pcov = curve_fit(
                    tanh_model, x, y,
                    p0=[0.5, 0.20],
                    bounds=([HASSASIYET_MIN, MAX_DEGISIM_MIN],
                            [HASSASIYET_MAX, MAX_DEGISIM_MAX]),
                    maxfev=5000,
                )

            h_opt, m_opt = popt
            y_pred = tanh_model(x, h_opt, m_opt)
            ss_res = np.sum((y - y_pred) ** 2)
            ss_tot = np.sum((y - np.mean(y)) ** 2)
            r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0.0

            sonuc.update({
                "hassasiyet_optimal": round(float(h_opt), 4),
                "max_degisim_optimal": round(float(m_opt), 4),
                "r_squared": round(float(r_squared), 4),
                "fit_basarili": r_squared >= MIN_R_SQUARED,
                "durum": "BASARILI" if r_squared >= MIN_R_SQUARED else "DUSUK_R_SQUARED",
                "mesaj": f"R²={r_squared:.4f}" + ("" if r_squared >= MIN_R_SQUARED else f" < {MIN_R_SQUARED}"),
            })

            logger.info("%s/%s: h=%.4f→%.4f, m=%.4f→%.4f, R²=%.4f, gc=%.2f",
                        asin, targeting_type, h_mevcut, h_opt, m_mevcut, m_opt,
                        r_squared, ort_gap_closure or 0)

        except (RuntimeError, ValueError, TypeError) as e:
            logger.warning("Curve fit basarisiz (%s/%s): %s", asin, targeting_type, e)
            sonuc["durum"] = "FIT_BASARISIZ"
            sonuc["fit_basarili"] = False
            sonuc["mesaj"] = str(e)[:200]

        return sonuc

    def _kaydet_supabase(self, sdb, analiz, veri_noktalari, today):
        try:
            from psycopg2.extras import Json
            sdb._execute("""
                INSERT INTO bid_param_regressions
                    (hesap_key, marketplace, asin, targeting_type, analysis_date,
                     hassasiyet_mevcut, max_degisim_mevcut,
                     hassasiyet_optimal, max_degisim_optimal,
                     r_squared, veri_noktasi, ort_gap_closure,
                     ort_acos_before, ort_acos_after, hedef_acos,
                     fit_basarili, fit_mesaj)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (hesap_key, marketplace, asin, targeting_type, analysis_date)
                DO UPDATE SET
                    hassasiyet_mevcut=EXCLUDED.hassasiyet_mevcut,
                    max_degisim_mevcut=EXCLUDED.max_degisim_mevcut,
                    hassasiyet_optimal=EXCLUDED.hassasiyet_optimal,
                    max_degisim_optimal=EXCLUDED.max_degisim_optimal,
                    r_squared=EXCLUDED.r_squared,
                    veri_noktasi=EXCLUDED.veri_noktasi,
                    ort_gap_closure=EXCLUDED.ort_gap_closure,
                    ort_acos_before=EXCLUDED.ort_acos_before,
                    ort_acos_after=EXCLUDED.ort_acos_after,
                    hedef_acos=EXCLUDED.hedef_acos,
                    fit_basarili=EXCLUDED.fit_basarili,
                    fit_mesaj=EXCLUDED.fit_mesaj
            """, (self.hesap_key, self.marketplace,
                  analiz["asin"], analiz["targeting_type"], today,
                  analiz.get("hassasiyet_mevcut"), analiz.get("max_degisim_mevcut"),
                  analiz.get("hassasiyet_optimal"), analiz.get("max_degisim_optimal"),
                  analiz.get("r_squared"), analiz.get("veri_noktasi"),
                  analiz.get("ort_gap_closure"),
                  analiz.get("ort_acos_before"), analiz.get("ort_acos_after"),
                  analiz.get("hedef_acos"),
                  analiz.get("fit_basarili", False),
                  analiz.get("mesaj")))

            reg_row = sdb._fetch_one("""
                SELECT id FROM bid_param_regressions
                WHERE hesap_key=%s AND marketplace=%s AND asin=%s
                  AND targeting_type=%s AND analysis_date=%s
            """, (self.hesap_key, self.marketplace,
                  analiz["asin"], analiz["targeting_type"], today))

            if reg_row:
                reg_id = reg_row[0]
                sdb._execute(
                    "DELETE FROM regression_data_points WHERE regression_id=%s",
                    (reg_id,))
                for v in veri_noktalari:
                    sdb._execute("""
                        INSERT INTO regression_data_points
                            (regression_id, hesap_key, marketplace, asin,
                             decision_date, targeting_id,
                             acos_before, acos_after, hedef_acos,
                             acos_fark_orani, bid_degisim_pct,
                             ideal_bid_degisim_pct, gap_closure)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (reg_id, self.hesap_key, self.marketplace, analiz["asin"],
                          v.get("decision_date"), v.get("targeting_id"),
                          v.get("acos_before"), v.get("acos_after"), v.get("hedef_acos"),
                          v.get("acos_fark_orani"), v.get("bid_degisim_pct"),
                          v.get("ideal_bid_degisim_pct"), v.get("gap_closure")))

            logger.info("Supabase: %s/%s regresyon kaydedildi", analiz["asin"], analiz["targeting_type"])
        except Exception as e:
            logger.warning("Regresyon Supabase kaydi basarisiz: %s", e)

    def _get_mevcut_params(self, sdb) -> dict:
        params = {"_global": {"hassasiyet": 0.5, "max_degisim": 0.20}}
        try:
            row = sdb._fetch_one("""
                SELECT tanh_formulu, asin_parametreleri FROM bid_functions
                WHERE hesap_key = %s AND marketplace = %s
            """, (self.hesap_key, self.marketplace))
            if row:
                tf = row[0] if isinstance(row[0], dict) else {}
                params["_global"] = {
                    "hassasiyet": tf.get("hassasiyet", 0.5),
                    "max_degisim": tf.get("max_degisim", 0.20),
                }
                asin_p = row[1] if isinstance(row[1], dict) else {}
                for asin, val in asin_p.items():
                    if asin.startswith("_"):
                        continue
                    if isinstance(val, dict) and val.get("aktif"):
                        params[asin] = val
        except Exception as e:
            logger.warning("bid_functions okunamadi: %s", e)
        return params

    def _get_params_for_group(self, asin, targeting_type, mevcut_params):
        g = mevcut_params.get("_global", {"hassasiyet": 0.5, "max_degisim": 0.20})
        if asin in mevcut_params:
            p = mevcut_params[asin]
            return {"hassasiyet": p.get("hassasiyet", g["hassasiyet"]),
                    "max_degisim": p.get("max_degisim", g["max_degisim"])}
        return g
