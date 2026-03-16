"""
Agent 4 — KPI Collector
=========================
decisions.json + rollback.json dosyalarını eşleştirir.
Yeni kararları DB'ye ekler, 3 gün önce uygulanan kararların
kpi_after değerlerini güncel Agent 1 verileriyle doldurur.

KPI Penceresi:
  Gün 0: Karar verilir (decisions.json) + uygulanır (rollback.json)
  Gün 3: Bir sonraki Agent 1 çalışmasında yeni metrikler gelir
         Agent 4 bu metrikleri kpi_after olarak işler
"""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger("agent4.kpi")


class KPICollector:

    def __init__(self, data_dir, db):
        self.data_dir      = Path(data_dir)
        self.decisions_dir = self.data_dir / "decisions"
        self.log_dir       = self.data_dir / "logs"
        self.db            = db

    # ------------------------------------------------------------------ run
    def run(self, today: str) -> dict:
        ozet = {
            "tarih": today,
            "yeni_karar": 0,
            "guncellenen_karar": 0,
            "kpi_after_doldurulan": 0,
            "rollback_eslestirilen": 0,
        }

        # 1. Bugunun decisions.json → DB'ye ekle
        self._isle_decisions(today, ozet)

        # 2. Rollback.json ile karar_durumu'nu guncelle
        self._isle_rollback(today, ozet)

        # 3. 3 gun onceki kararlar icin kpi_after doldur
        self._doldur_kpi_after(today, ozet)

        # 4. ASIN profillerini guncelle
        self._guncelle_asin_profilleri()

        logger.info("KPI Collector ozeti: %s", ozet)
        return ozet

    # ----------------------------------------------- decisions.json isleme
    def _isle_decisions(self, today: str, ozet: dict):
        decisions_path = self.decisions_dir / f"{today}_decisions.json"
        if not decisions_path.exists():
            logger.warning("decisions.json bulunamadi: %s", decisions_path)
            return

        with open(decisions_path, "r", encoding="utf-8") as f:
            decisions = json.load(f)

        mevcut_idler = {
            f"{k['tarih']}_{k['hedefleme_id']}"
            for k in self.db.get("karar_gecmisi")["kararlar"]
        }

        for d in decisions:
            uid = f"{d['tarih']}_{d['hedefleme_id']}"
            karar = {
                "tarih":          d["tarih"],
                "hedefleme_id":   d["hedefleme_id"],
                "reklam_tipi":    d.get("reklam_tipi", ""),
                "hedefleme":      d.get("hedefleme", ""),
                "kampanya":       d.get("kampanya", ""),
                "portfolio_id":   d.get("portfolio_id", ""),
                "asin":           d.get("asin", ""),
                "segment":        d.get("segment", ""),
                "onceki_bid":     d.get("onceki_bid", 0),
                "yeni_bid":       d.get("yeni_bid", 0),
                "degisim_yuzde":  d.get("degisim_yuzde", 0),
                "sebep":          d.get("sebep", ""),
                "metrikler":      d.get("metrikler", {}),
                "karar_durumu":   d.get("karar_durumu", "ONAY_BEKLIYOR"),
                "kpi_after":      None,   # 3 gun sonra doldurulacak
                "kpi_after_tarih": None,
            }

            if uid not in mevcut_idler:
                self.db.add_karar(karar)
                ozet["yeni_karar"] += 1
            else:
                ozet["guncellenen_karar"] += 1

        logger.info("%d yeni karar, %d mevcut guncellendi",
                    ozet["yeni_karar"], ozet["guncellenen_karar"])

    # ----------------------------------------------- rollback.json eslestirme
    def _isle_rollback(self, today: str, ozet: dict):
        rollback_path = self.log_dir / f"{today}_rollback.json"
        if not rollback_path.exists():
            logger.info("rollback.json bulunamadi (dry-run veya bos onay): %s", rollback_path)
            return

        with open(rollback_path, "r", encoding="utf-8") as f:
            rollback = json.load(f)

        islemler = rollback.get("islemler", [])
        # Uygulanan bid degisikliklerini hedefleme ile eslestir
        uygulanan = {
            op["hedefleme"]: op
            for op in islemler
            if op.get("tip") == "BID_DEGISIKLIGI"
        }

        kararlar = self.db.get("karar_gecmisi")["kararlar"]
        for k in kararlar:
            if k["tarih"] == today and k["karar_durumu"] == "ONAY_BEKLIYOR":
                hedefleme = k.get("hedefleme", "")
                if hedefleme in uygulanan:
                    k["karar_durumu"] = "UYGULANDI"
                    k["uygulama_entity_id"] = uygulanan[hedefleme].get("entity_id", "")
                    ozet["rollback_eslestirilen"] += 1

        logger.info("%d karar UYGULANDI olarak isaretlendi", ozet["rollback_eslestirilen"])

    # ----------------------------------------------- kpi_after doldurma
    def _doldur_kpi_after(self, today: str, ozet: dict):
        """
        3 gun once uygulanan ve kpi_after=None olan kararlar icin
        bugunun Agent 1 raporlarindan (targeting_14d) metrikleri ceker.
        """
        bos_kararlar = self.db.get_kpi_after_bos()
        if not bos_kararlar:
            logger.info("Doldurlacak kpi_after yok.")
            return

        # Bugunun SP ve SB targeting raporlarini yukle
        sp_rapor  = self._yukle_rapor(today, "sp_targeting_14d")
        sb_rapor  = self._yukle_rapor(today, "sb_targeting_14d")
        sd_rapor  = self._yukle_rapor(today, "sd_targeting_14d")

        # Hedefleme_id ile hizli arama icin index olustur
        rapor_index = {}
        for satir in (sp_rapor + sb_rapor + sd_rapor):
            tid = self._get_targeting_id(satir)
            if tid:
                rapor_index[tid] = satir

        today_dt = datetime.strptime(today, "%Y-%m-%d")

        for k in bos_kararlar:
            karar_tarihi = datetime.strptime(k["tarih"], "%Y-%m-%d")
            gun_farki    = (today_dt - karar_tarihi).days

            # En az 3 gun gecmis olmali
            if gun_farki < 3:
                continue

            satir = rapor_index.get(k["hedefleme_id"])
            if not satir:
                continue

            reklam_tipi = k.get("reklam_tipi", "SP")
            k["kpi_after"] = {
                "impressions": satir.get("impressions", 0),
                "clicks":      satir.get("clicks", 0),
                "spend":       satir.get("spend", 0.0),
                "sales":       self._get_sales(satir, reklam_tipi),
                "orders":      self._get_orders(satir, reklam_tipi),
                "acos":        self._hesapla_acos(satir, reklam_tipi),
                "cvr":         self._hesapla_cvr(satir),
                "cpc":         self._hesapla_cpc(satir),
            }
            k["kpi_after_tarih"]  = today
            k["kpi_after_gun_farki"] = gun_farki
            ozet["kpi_after_doldurulan"] += 1

        logger.info("%d karar icin kpi_after dolduruldu", ozet["kpi_after_doldurulan"])

    # ----------------------------------------------- asin profil guncelleme
    def _guncelle_asin_profilleri(self):
        kararlar = self.db.get("karar_gecmisi")["kararlar"]
        asin_data = {}

        for k in kararlar:
            asin = k.get("asin", "")
            if not asin:
                continue
            if asin not in asin_data:
                asin_data[asin] = {
                    "toplam_karar": 0,
                    "uygulanan_karar": 0,
                    "kpi_after_olan": 0,
                    "acos_before_toplam": 0.0,
                    "acos_after_toplam":  0.0,
                    "segment_sayilari": {},
                }
            d = asin_data[asin]
            d["toplam_karar"] += 1

            if k.get("karar_durumu") == "UYGULANDI":
                d["uygulanan_karar"] += 1

            seg = k.get("segment", "")
            d["segment_sayilari"][seg] = d["segment_sayilari"].get(seg, 0) + 1

            m = k.get("metrikler", {})
            if m.get("acos") is not None:
                d["acos_before_toplam"] += m["acos"]

            if k.get("kpi_after"):
                d["kpi_after_olan"] += 1
                after_acos = k["kpi_after"].get("acos")
                if after_acos is not None:
                    d["acos_after_toplam"] += after_acos

        for asin, d in asin_data.items():
            avg_before = (d["acos_before_toplam"] / d["toplam_karar"]
                          if d["toplam_karar"] > 0 else None)
            avg_after  = (d["acos_after_toplam"] / d["kpi_after_olan"]
                          if d["kpi_after_olan"] > 0 else None)

            self.db.update_asin_profil(asin, {
                "toplam_karar":            d["toplam_karar"],
                "uygulanan_karar":         d["uygulanan_karar"],
                "kpi_after_olan":          d["kpi_after_olan"],
                "ortalama_acos_oncesi":    round(avg_before, 2) if avg_before else None,
                "ortalama_acos_sonrasi":   round(avg_after, 2)  if avg_after  else None,
                "segment_sayilari":        d["segment_sayilari"],
            })

    # ----------------------------------------------------------------- utils
    def _yukle_rapor(self, today: str, rapor_adi: str) -> list:
        path = self.data_dir / f"{today}_{rapor_adi}.json"
        if not path.exists():
            # Gece yarisi gecisi icin bir onceki gunu de kontrol et
            onceki = (datetime.strptime(today, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
            path = self.data_dir / f"{onceki}_{rapor_adi}.json"
            if not path.exists():
                return []
            logger.info("Rapor bugunun tarihiyle bulunamadi, onceki gun kullaniliyor: %s", path.name)
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []

    def _get_targeting_id(self, satir: dict) -> str:
        """SP: keywordId veya targetId, SB: keywordId, SD: targetId"""
        return (
            satir.get("keywordId") or
            satir.get("targetId")  or
            satir.get("keyword_id") or
            ""
        )

    def _get_sales(self, satir: dict, reklam_tipi: str) -> float:
        if reklam_tipi == "SP":
            return float(satir.get("sales14d", satir.get("sales", 0)) or 0)
        return float(satir.get("sales", 0) or 0)

    def _get_orders(self, satir: dict, reklam_tipi: str) -> int:
        if reklam_tipi == "SP":
            return int(satir.get("purchases14d", satir.get("purchases", 0)) or 0)
        return int(satir.get("purchases", 0) or 0)

    def _hesapla_acos(self, satir: dict, reklam_tipi: str) -> float:
        spend = float(satir.get("spend", 0) or 0)
        sales = self._get_sales(satir, reklam_tipi)
        if sales > 0:
            return round((spend / sales) * 100, 2)
        return None

    def _hesapla_cvr(self, satir: dict) -> float:
        clicks = int(satir.get("clicks", 0) or 0)
        orders = int(satir.get("purchases14d", satir.get("purchases", 0)) or 0)
        if clicks > 0:
            return round((orders / clicks) * 100, 2)
        return 0.0

    def _hesapla_cpc(self, satir: dict) -> float:
        clicks = int(satir.get("clicks", 0) or 0)
        spend  = float(satir.get("spend", 0) or 0)
        if clicks > 0:
            return round(spend / clicks, 3)
        return 0.0
