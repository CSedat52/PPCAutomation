"""
Agent 4 — KPI Collector (v3 — Supabase Only)
===============================================
bid_recommendations + execution_items + targeting_reports
tablolarindan okur. JSON dosya bagimliligi kaldirildi.

KPI Penceresi:
  Gun 0: Karar verilir (bid_recommendations APPROVED) + uygulanir (execution_items)
  Gun N: Agent 1 yeni targeting_reports toplar
         KPI Collector bu metrikleri kpi_after olarak isler
  Sure siniri YOK — ilk uygun rapor bulundugunda doldurulur.
"""

import logging
from datetime import datetime

logger = logging.getLogger("agent4.kpi")


class KPICollector:

    def __init__(self, hesap_key: str, marketplace: str, db):
        self.hesap_key = hesap_key
        self.marketplace = marketplace
        self.db = db

    def _get_sdb(self):
        from supabase.db_client import SupabaseClient
        return SupabaseClient()

    # ------------------------------------------------------------------ run
    def run(self, today: str) -> dict:
        ozet = {
            "tarih": today,
            "yeni_karar": 0,
            "guncellenen_karar": 0,
            "kpi_after_doldurulan": 0,
            "rollback_eslestirilen": 0,
        }

        # 1. Onaylanmis bid_recommendations -> DB'ye ekle
        self._isle_decisions(today, ozet)

        # 2. execution_items ile karar_durumu guncelle
        self._isle_rollback(today, ozet)

        # 3. kpi_after doldur (sure siniri YOK)
        self._doldur_kpi_after(ozet)

        # 4. ASIN profillerini guncelle
        self._guncelle_asin_profilleri()

        logger.info("KPI Collector ozeti: %s", ozet)
        return ozet

    # ----------------------------------------------- bid_recommendations isleme
    def _isle_decisions(self, today: str, ozet: dict):
        """Onaylanmis bid_recommendations'i DB'ye ekler."""
        try:
            sdb = self._get_sdb()
            rows = sdb._fetch_all("""
                SELECT analysis_date, ad_type, campaign_id, campaign_name,
                       keyword_id, target_id, keyword_text, targeting,
                       match_type, segment, current_bid, recommended_bid,
                       decision_bid, bid_change_pct,
                       impressions, clicks, cost, sales, orders, acos, cvr, cpc,
                       portfolio, decision
                FROM bid_recommendations
                WHERE hesap_key = %s AND marketplace = %s
                  AND analysis_date = %s
                  AND decision IN ('APPROVED', 'MODIFIED')
            """, (self.hesap_key, self.marketplace, today))
        except Exception as e:
            logger.warning("bid_recommendations okunamadi: %s", e)
            return

        if not rows:
            logger.info("Bugun icin onaylanmis karar yok: %s", today)
            return

        mevcut_idler = {
            f"{k['tarih']}_{k['hedefleme_id']}"
            for k in self.db.get("karar_gecmisi")["kararlar"]
        }

        for row in rows:
            (analysis_date, ad_type, campaign_id, campaign_name,
             keyword_id, target_id, keyword_text, targeting,
             match_type, segment, current_bid, recommended_bid,
             decision_bid, bid_change_pct,
             impressions, clicks, cost, sales, orders, acos, cvr, cpc,
             portfolio, decision) = row

            hedefleme_id = str(keyword_id or target_id or "")
            tarih = str(analysis_date)
            yeni_bid = float(decision_bid or recommended_bid or 0)

            uid = f"{tarih}_{hedefleme_id}"

            karar = {
                "tarih":          tarih,
                "hedefleme_id":   hedefleme_id,
                "reklam_tipi":    ad_type or "",
                "hedefleme":      keyword_text or targeting or "",
                "kampanya":       campaign_name or "",
                "portfolio_id":   portfolio or "",
                "asin":           "",
                "segment":        segment or "",
                "onceki_bid":     float(current_bid or 0),
                "yeni_bid":       yeni_bid,
                "degisim_yuzde":  float(bid_change_pct or 0),
                "sebep":          "",
                "metrikler": {
                    "impressions": int(impressions or 0),
                    "clicks":      int(clicks or 0),
                    "spend":       float(cost or 0),
                    "sales":       float(sales or 0),
                    "orders":      int(orders or 0),
                    "acos":        float(acos) if acos is not None else None,
                    "cvr":         float(cvr) if cvr is not None else None,
                    "cpc":         float(cpc) if cpc is not None else None,
                },
                "karar_durumu":   "ONAY_BEKLIYOR",
                "kpi_after":      None,
                "kpi_after_tarih": None,
            }

            if uid not in mevcut_idler:
                self.db.add_karar(karar)
                ozet["yeni_karar"] += 1
            else:
                ozet["guncellenen_karar"] += 1

        logger.info("%d yeni karar, %d mevcut guncellendi",
                    ozet["yeni_karar"], ozet["guncellenen_karar"])

    # ----------------------------------------------- execution_items eslestirme
    def _isle_rollback(self, today: str, ozet: dict):
        """execution_items'dan uygulanan islemleri eslestir."""
        try:
            sdb = self._get_sdb()
            rows = sdb._fetch_all("""
                SELECT keyword_id, target_id, targeting, new_bid
                FROM execution_items
                WHERE hesap_key = %s AND marketplace = %s
                  AND item_type = 'BID_CHANGE' AND status = 'SUCCESS'
                  AND created_at::date = %s
            """, (self.hesap_key, self.marketplace, today))
        except Exception as e:
            logger.warning("execution_items okunamadi: %s", e)
            return

        if not rows:
            logger.info("Bugun icin uygulanan islem yok: %s", today)
            return

        uygulanan = set()
        for kw_id, tgt_id, targeting, new_bid in rows:
            eid = str(kw_id or tgt_id or "")
            uygulanan.add(eid)

        kararlar = self.db.get("karar_gecmisi")["kararlar"]
        for k in kararlar:
            if k["tarih"] == today and k["karar_durumu"] == "ONAY_BEKLIYOR":
                hid = k.get("hedefleme_id", "")
                if hid in uygulanan:
                    k["karar_durumu"] = "UYGULANDI"
                    ozet["rollback_eslestirilen"] += 1

        logger.info("%d karar UYGULANDI olarak isaretlendi", ozet["rollback_eslestirilen"])

    # ----------------------------------------------- kpi_after doldurma
    def _doldur_kpi_after(self, ozet: dict):
        """
        UYGULANDI + kpi_after=None olan kararlar icin
        targeting_reports tablosundan metrikleri ceker.
        Sure siniri YOK — collection_date > karar_tarihi olan ilk rapor kullanilir.
        """
        bos_kararlar = self.db.get_kpi_after_bos()
        if not bos_kararlar:
            logger.info("Doldurulacak kpi_after yok.")
            return

        try:
            sdb = self._get_sdb()
        except Exception as e:
            logger.warning("Supabase baglantisi kurulamadi: %s", e)
            return

        for k in bos_kararlar:
            hedefleme_id = k.get("hedefleme_id", "")
            karar_tarihi = k.get("tarih", "")

            if not hedefleme_id or not karar_tarihi:
                continue

            try:
                row = sdb._fetch_one("""
                    SELECT impressions, clicks, cost, sales, purchases,
                           acos, collection_date
                    FROM targeting_reports
                    WHERE hesap_key = %s AND marketplace = %s
                      AND (keyword_id = %s OR target_id = %s)
                      AND collection_date > %s
                    ORDER BY collection_date ASC
                    LIMIT 1
                """, (self.hesap_key, self.marketplace,
                      hedefleme_id, hedefleme_id, karar_tarihi))
            except Exception:
                continue

            if not row:
                continue

            impressions, clicks, cost, sales, purchases, acos, collection_date = row
            spend = float(cost or 0)
            sales_val = float(sales or 0)
            clicks_val = int(clicks or 0)
            orders_val = int(purchases or 0)

            calculated_acos = None
            if acos is not None:
                calculated_acos = float(acos)
            elif sales_val > 0:
                calculated_acos = round((spend / sales_val) * 100, 2)

            k["kpi_after"] = {
                "impressions": int(impressions or 0),
                "clicks":      clicks_val,
                "spend":       spend,
                "sales":       sales_val,
                "orders":      orders_val,
                "acos":        calculated_acos,
                "cvr":         round((orders_val / clicks_val) * 100, 2) if clicks_val > 0 else 0.0,
                "cpc":         round(spend / clicks_val, 3) if clicks_val > 0 else 0.0,
            }
            k["kpi_after_tarih"] = str(collection_date)
            k["kpi_after_gun_farki"] = (
                datetime.strptime(str(collection_date), "%Y-%m-%d") -
                datetime.strptime(karar_tarihi, "%Y-%m-%d")
            ).days

            # decision_history tablosunu da dogrudan guncelle
            try:
                sdb.update_decision_kpi(
                    self.hesap_key, self.marketplace,
                    hedefleme_id, karar_tarihi,
                    k["kpi_after"]
                )
            except Exception:
                pass  # In-memory guncelleme yeterli, sync de yazacak

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
