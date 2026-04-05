"""
Agent 4 — Bid Parameter Analyzer (v4 — Tanh Regresyon)
========================================================
(ASIN x hedefleme tipi) bazinda tanh curve fitting.
Optimal hassasiyet ve max_degisim parametrelerini veriden turetir.

Hedefleme tipleri:
  KEYWORD        — SP keyword + SB keyword
  PRODUCT_TARGET — SP ASIN/category target + SD target

Bid formulu hatirlatma:
  bid_degisim = -tanh(acos_fark_orani x hassasiyet) x max_degisim

Gereksinimler: scipy (pip install scipy)
"""

import json
import logging
import warnings
from collections import defaultdict
from datetime import datetime

import numpy as np
from scipy.optimize import curve_fit, OptimizeWarning

logger = logging.getLogger("agent4.bid_param")

# Sabitler
MIN_OLCULEBILIR_KARAR = 20   # Her grup icin ayri ayri 20 veri noktasi
MIN_R_SQUARED = 0.50          # Fit kalitesi esigi
HASSASIYET_MIN = 0.1
HASSASIYET_MAX = 2.0
MAX_DEGISIM_MIN = 0.05
MAX_DEGISIM_MAX = 0.40
TARGETING_TYPES = ["KEYWORD", "PRODUCT_TARGET"]


def tanh_model(bid_degisim, alpha, beta):
    """Tanh regresyon modeli: acos_degisim = -tanh(bid_degisim * alpha) * beta"""
    return -np.tanh(bid_degisim * alpha) * beta


class BidParamAnalyzer:

    def __init__(self, hesap_key: str, marketplace: str, config_dir=None):
        self.hesap_key = hesap_key
        self.marketplace = marketplace
        self.config_dir = config_dir

    def _get_sdb(self):
        from supabase.db_client import SupabaseClient
        return SupabaseClient()

    def analyze(self) -> list:
        """Her (ASIN x targeting_type) icin tanh regresyon analizi."""
        try:
            sdb = self._get_sdb()
        except Exception as e:
            logger.warning("Supabase baglantisi kurulamadi: %s", e)
            return []

        # decision_history'den kpi_after dolu kararlari cek
        rows = self._fetch_decisions(sdb)
        if not rows:
            logger.info("Analiz icin karar verisi yok.")
            return []

        # (ASIN, targeting_type) bazinda grupla
        gruplar = self._grupla(rows)

        # Mevcut bid parametrelerini al
        mevcut_params = self._get_mevcut_params(sdb)

        # ASIN hedeflerini al
        asin_hedefleri = self._get_asin_hedefleri(sdb)

        today = datetime.utcnow().strftime("%Y-%m-%d")
        sonuclar = []

        for (asin, targeting_type), veri_noktalari in gruplar.items():
            hedef_acos = asin_hedefleri.get(asin, {}).get("hedef_acos")
            params = self._get_params_for_group(asin, targeting_type, mevcut_params)
            analiz = self._analiz_grup(
                asin, targeting_type, veri_noktalari,
                params, hedef_acos, today
            )
            if analiz:
                # Supabase'e kaydet
                self._kaydet_supabase(sdb, analiz, veri_noktalari, today)
                sonuclar.append(analiz)

        logger.info("Bid param analizi: %d grup analiz edildi, %d fit basarili",
                    len(gruplar),
                    sum(1 for s in sonuclar if s.get("fit_basarili")))
        return sonuclar

    def _fetch_decisions(self, sdb) -> list:
        """decision_history'den kpi_after dolu kararlari cek."""
        try:
            rows = sdb._fetch_all("""
                SELECT asin, targeting_type, targeting_id, decision_date,
                       change_pct, metrics, kpi_after, hedef_acos,
                       previous_bid, new_bid, segment
                FROM decision_history
                WHERE hesap_key = %s AND marketplace = %s
                  AND kpi_after IS NOT NULL
                  AND asin IS NOT NULL AND asin != ''
                  AND decision_status IN ('APPLIED', 'VERIFIED')
                ORDER BY decision_date ASC
            """, (self.hesap_key, self.marketplace))
            return rows or []
        except Exception as e:
            logger.warning("decision_history okunamadi: %s", e)
            return []

    def _grupla(self, rows: list) -> dict:
        """Kararlari (ASIN, targeting_type) bazinda grupla."""
        gruplar = defaultdict(list)
        for row in rows:
            (asin, targeting_type, targeting_id, decision_date,
             change_pct, metrics, kpi_after, hedef_acos,
             previous_bid, new_bid, segment) = row

            if not asin or not targeting_type:
                continue

            # metrics ve kpi_after JSONB
            if isinstance(metrics, str):
                metrics = json.loads(metrics)
            if isinstance(kpi_after, str):
                kpi_after = json.loads(kpi_after)

            acos_once = metrics.get("acos") if metrics else None
            acos_sonra = kpi_after.get("acos") if kpi_after else None

            if acos_once is None or acos_sonra is None:
                continue

            bid_degisim = float(change_pct or 0) / 100.0  # yuzde → oran

            gruplar[(asin, targeting_type)].append({
                "targeting_id": targeting_id,
                "decision_date": str(decision_date),
                "bid_degisim": bid_degisim,
                "acos_once": float(acos_once),
                "acos_sonra": float(acos_sonra),
                "acos_degisim": float(acos_sonra) - float(acos_once),
                "spend_before": float((metrics or {}).get("spend", 0)),
                "spend_after": float((kpi_after or {}).get("spend", 0)),
                "hedef_acos": float(hedef_acos) if hedef_acos else None,
                "asin": asin,
                "targeting_type": targeting_type,
            })

        return dict(gruplar)

    def _analiz_grup(self, asin: str, targeting_type: str,
                     veri_noktalari: list, mevcut: dict,
                     hedef_acos: float, today: str) -> dict:
        """Tek (ASIN x targeting_type) grubu icin tanh curve fitting."""
        n = len(veri_noktalari)
        hassasiyet_mevcut = mevcut.get("hassasiyet", 0.5)
        max_degisim_mevcut = mevcut.get("max_degisim", 0.20)
        parametre_kaynagi = mevcut.get("kaynak", "GLOBAL")

        # Ortalama ACoS metrikleri
        acos_once_list = [v["acos_once"] for v in veri_noktalari]
        acos_sonra_list = [v["acos_sonra"] for v in veri_noktalari]
        ort_acos_once = sum(acos_once_list) / len(acos_once_list) if acos_once_list else None
        ort_acos_sonra = sum(acos_sonra_list) / len(acos_sonra_list) if acos_sonra_list else None

        # Gap closure hesapla (bilgi amacli)
        ort_gap_closure = self._hesapla_gap_closure(veri_noktalari, hedef_acos)

        sonuc = {
            "asin": asin,
            "targeting_type": targeting_type,
            "hedef_acos": hedef_acos,
            "veri_noktasi": n,
            "hassasiyet_mevcut": hassasiyet_mevcut,
            "max_degisim_mevcut": max_degisim_mevcut,
            "parametre_kaynagi": parametre_kaynagi,
            "ort_acos_once": round(ort_acos_once, 2) if ort_acos_once else None,
            "ort_acos_sonra": round(ort_acos_sonra, 2) if ort_acos_sonra else None,
            "ort_gap_closure": round(ort_gap_closure, 4) if ort_gap_closure is not None else None,
            "analysis_date": today,
        }

        if n < MIN_OLCULEBILIR_KARAR:
            sonuc["durum"] = "YETERSIZ_VERI"
            sonuc["fit_basarili"] = False
            sonuc["mesaj"] = f"{n} veri noktasi var, minimum {MIN_OLCULEBILIR_KARAR} gerekli"
            logger.info("%s/%s: YETERSIZ_VERI (%d/%d)",
                        asin, targeting_type, n, MIN_OLCULEBILIR_KARAR)
            return sonuc

        # Tanh curve fitting
        x = np.array([v["bid_degisim"] for v in veri_noktalari])
        y = np.array([v["acos_degisim"] for v in veri_noktalari])

        fit_sonuc = self._fit_tanh(x, y)
        if not fit_sonuc:
            sonuc["durum"] = "FIT_BASARISIZ"
            sonuc["fit_basarili"] = False
            sonuc["mesaj"] = "Tanh curve fitting basarisiz"
            return sonuc

        alpha_fit, beta_fit_pp, r_squared, alpha_std, beta_std = fit_sonuc

        sonuc.update({
            "alpha_fit": round(alpha_fit, 4),
            "beta_fit_pp": round(beta_fit_pp, 2),
            "r_squared": round(r_squared, 4),
            "alpha_std_err": round(alpha_std, 4),
            "beta_std_err": round(beta_std, 2),
            "fit_basarili": True,
        })

        if r_squared < MIN_R_SQUARED:
            sonuc["durum"] = "DUSUK_R_SQUARED"
            sonuc["mesaj"] = f"R²={r_squared:.4f} < {MIN_R_SQUARED} — fit guvenilir degil"
            sonuc["python_onerisi"] = None
            logger.info("%s/%s: DUSUK_R_SQUARED (%.4f)",
                        asin, targeting_type, r_squared)
            return sonuc

        # Oneri uret
        python_onerisi = self._uret_oneri(
            alpha_fit, beta_fit_pp, alpha_std, beta_std,
            hassasiyet_mevcut, max_degisim_mevcut
        )

        sonuc["durum"] = "BASARILI"
        sonuc["python_onerisi"] = python_onerisi

        logger.info("%s/%s: alpha_fit=%.4f, beta_fit_pp=%.2f, R²=%.4f, oneri=%s",
                    asin, targeting_type, alpha_fit, beta_fit_pp, r_squared,
                    "VAR" if python_onerisi else "YOK")
        return sonuc

    def _fit_tanh(self, x: np.ndarray, y: np.ndarray):
        """scipy curve_fit ile tanh modeli fit et."""
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", OptimizeWarning)
                # Baslangic tahmini: alpha=0.5, beta=5.0 (pp)
                popt, pcov = curve_fit(
                    tanh_model, x, y,
                    p0=[0.5, 5.0],
                    bounds=([HASSASIYET_MIN, 0.1], [HASSASIYET_MAX, 50.0]),
                    maxfev=5000,
                )

            alpha_fit, beta_fit_pp = popt
            # Standart hata
            perr = np.sqrt(np.diag(pcov))
            alpha_std = perr[0]
            beta_std = perr[1]

            # R² hesapla
            y_pred = tanh_model(x, alpha_fit, beta_fit_pp)
            ss_res = np.sum((y - y_pred) ** 2)
            ss_tot = np.sum((y - np.mean(y)) ** 2)
            r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0.0

            # Parametre sinirlari icerisinde tut
            alpha_fit = max(HASSASIYET_MIN, min(HASSASIYET_MAX, alpha_fit))

            return alpha_fit, beta_fit_pp, r_squared, alpha_std, beta_std

        except (RuntimeError, ValueError, TypeError) as e:
            logger.warning("Tanh fit basarisiz: %s", e)
            return None

    def _uret_oneri(self, alpha_fit, beta_fit_pp, alpha_std, beta_std,
                     hassasiyet_mevcut, max_degisim_mevcut):
        """Regresyondan parametre onerisi uret."""
        oneriler = {}

        # Hassasiyet onerisi: fark 2 sigma disindaysa
        alpha_fark = abs(alpha_fit - hassasiyet_mevcut)
        if alpha_std > 0 and alpha_fark > 2 * alpha_std:
            yeni_hass = max(HASSASIYET_MIN, min(HASSASIYET_MAX, round(alpha_fit, 2)))
            if yeni_hass != hassasiyet_mevcut:
                yon = "artir" if alpha_fit > hassasiyet_mevcut else "dusur"
                oneriler["hassasiyet"] = {
                    "mevcut": hassasiyet_mevcut,
                    "onerilen": yeni_hass,
                    "alpha_fit": round(alpha_fit, 4),
                    "alpha_std": round(alpha_std, 4),
                    "sigma_uzakligi": round(alpha_fark / alpha_std, 2),
                    "sebep": (
                        f"Regresyon alpha={alpha_fit:.4f} vs mevcut={hassasiyet_mevcut:.2f} "
                        f"({alpha_fark/alpha_std:.1f} sigma uzakta) — hassasiyeti {yon}"
                    ),
                }

        # Beta analizi: Claude Code max_degisim ile yorumlayacak
        oneriler["beta_analiz"] = {
            "beta_fit_pp": round(beta_fit_pp, 2),
            "beta_std": round(beta_std, 2),
            "max_degisim_mevcut": max_degisim_mevcut,
            "aciklama": (
                f"Gercek max ACoS etkisi: {beta_fit_pp:.2f}pp. "
                f"Claude Code bu degeri max_degisim ({max_degisim_mevcut:.2f}) ile birlestirerek "
                f"max_degisim onerisi uretecek."
            ),
        }

        return oneriler if oneriler else None

    def _hesapla_gap_closure(self, veri_noktalari: list, hedef_acos: float):
        """Gap closure hesapla — ACoS hedefe ne kadar yaklasti."""
        if not hedef_acos:
            return None

        closures = []
        for v in veri_noktalari:
            gap_once = v["acos_once"] - hedef_acos
            gap_sonra = v["acos_sonra"] - hedef_acos
            if abs(gap_once) > 0.01:
                closure = 1 - (gap_sonra / gap_once)
                closures.append(closure)

        return sum(closures) / len(closures) if closures else None

    def _get_mevcut_params(self, sdb) -> dict:
        """Mevcut ASIN ve global bid parametrelerini Supabase'den al."""
        params = {}

        # ASIN bazli parametreler (yeni format: ASIN x targeting_type destekli)
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
            logger.warning("asin_bid_params okunamadi: %s", e)

        # bid_functions tablosundan ASIN x targeting_type parametreleri (yeni yapi)
        try:
            row = sdb._fetch_one("""
                SELECT asin_parametreleri, tanh_formulu FROM bid_functions
                WHERE hesap_key = %s AND marketplace = %s
            """, (self.hesap_key, self.marketplace))
            if row:
                asin_params = row[0] if isinstance(row[0], dict) else {}
                for asin, val in asin_params.items():
                    if asin.startswith("_"):
                        continue
                    if isinstance(val, dict):
                        # Yeni format: {"KEYWORD": {...}, "PRODUCT_TARGET": {...}}
                        if "KEYWORD" in val or "PRODUCT_TARGET" in val:
                            params[f"{asin}_KEYWORD"] = val.get("KEYWORD", {})
                            params[f"{asin}_PRODUCT_TARGET"] = val.get("PRODUCT_TARGET", {})
                        # Eski format: {"hassasiyet": ..., "max_degisim": ...}
                        elif "hassasiyet" in val:
                            if asin not in params:
                                params[asin] = val

                # Global tanh parametreleri
                tf = row[1] if isinstance(row[1], dict) else {}
                params["_global"] = {
                    "hassasiyet": tf.get("hassasiyet", 0.5),
                    "max_degisim": tf.get("max_degisim", 0.20),
                }
        except Exception as e:
            logger.warning("bid_functions okunamadi: %s", e)

        if "_global" not in params:
            params["_global"] = {"hassasiyet": 0.5, "max_degisim": 0.20}

        return params

    def _get_params_for_group(self, asin: str, targeting_type: str,
                               mevcut_params: dict) -> dict:
        """Belirli bir (ASIN, targeting_type) icin mevcut parametreleri dondur."""
        global_p = mevcut_params.get("_global", {"hassasiyet": 0.5, "max_degisim": 0.20})

        # 1. ASIN x targeting_type ozel (yeni yapi)
        key = f"{asin}_{targeting_type}"
        if key in mevcut_params:
            p = mevcut_params[key]
            if p.get("aktif", False):
                return {
                    "hassasiyet": p.get("hassasiyet", global_p["hassasiyet"]),
                    "max_degisim": p.get("max_degisim", global_p["max_degisim"]),
                    "kaynak": f"ASIN_TIP ({targeting_type})",
                }

        # 2. ASIN genel (eski yapi)
        if asin in mevcut_params:
            p = mevcut_params[asin]
            if p.get("aktif", False):
                return {
                    "hassasiyet": p.get("hassasiyet", global_p["hassasiyet"]),
                    "max_degisim": p.get("max_degisim", global_p["max_degisim"]),
                    "kaynak": "ASIN_OZEL",
                }

        # 3. Global
        return {
            "hassasiyet": global_p["hassasiyet"],
            "max_degisim": global_p["max_degisim"],
            "kaynak": "GLOBAL",
        }

    def _get_asin_hedefleri(self, sdb) -> dict:
        """Settings tablosundan ASIN hedeflerini yukle."""
        try:
            row = sdb._fetch_one("""
                SELECT asin_hedefleri FROM settings
                WHERE hesap_key = %s AND marketplace = %s
            """, (self.hesap_key, self.marketplace))
            if row and row[0]:
                return row[0] if isinstance(row[0], dict) else {}
        except Exception as e:
            logger.warning("asin_hedefleri yuklenemedi: %s", e)
        return {}

    def _kaydet_supabase(self, sdb, analiz: dict, veri_noktalari: list, today: str):
        """Sonuclari Supabase'e yaz."""
        try:
            reg_data = {
                "asin": analiz["asin"],
                "targeting_type": analiz["targeting_type"],
                "hedef_acos": analiz.get("hedef_acos"),
                "alpha_fit": analiz.get("alpha_fit"),
                "beta_fit_pp": analiz.get("beta_fit_pp"),
                "r_squared": analiz.get("r_squared"),
                "alpha_std_err": analiz.get("alpha_std_err"),
                "beta_std_err": analiz.get("beta_std_err"),
                "fit_basarili": analiz.get("fit_basarili", False),
                "veri_noktasi": analiz.get("veri_noktasi", 0),
                "hassasiyet_mevcut": analiz.get("hassasiyet_mevcut"),
                "max_degisim_mevcut": analiz.get("max_degisim_mevcut"),
                "parametre_kaynagi": analiz.get("parametre_kaynagi"),
                "ort_gap_closure": analiz.get("ort_gap_closure"),
                "ort_acos_once": analiz.get("ort_acos_once"),
                "ort_acos_sonra": analiz.get("ort_acos_sonra"),
                "analysis_date": today,
            }
            reg_id = sdb.upsert_bid_param_regression(
                self.hesap_key, self.marketplace, reg_data
            )

            if reg_id and veri_noktalari:
                sdb.insert_bid_param_regression_data(
                    reg_id, self.hesap_key, self.marketplace, veri_noktalari
                )

            logger.info("Supabase: %s/%s regresyon kaydedildi (id=%s)",
                        analiz["asin"], analiz["targeting_type"], reg_id)
        except Exception as e:
            logger.warning("Regresyon Supabase kaydi basarisiz: %s", e)
