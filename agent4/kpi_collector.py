"""
Agent 4 — KPI Collector (v4 — Decision History Direct)
========================================================
decision_history tablosundaki APPLIED kayitlarin kpi_after'ini
targeting_reports tablosundan doldurur.

db_manager bagimliligi kaldirildi — dogrudan Supabase.
"""

import logging
from datetime import datetime

logger = logging.getLogger("agent4.kpi")


class KPICollector:

    def __init__(self, hesap_key: str, marketplace: str):
        self.hesap_key = hesap_key
        self.marketplace = marketplace

    def _get_sdb(self):
        from supabase.db_client import SupabaseClient
        return SupabaseClient()

    def run(self, today: str) -> dict:
        """decision_history'deki kpi_after bos kayitlari doldur."""
        ozet = {
            "tarih": today,
            "kpi_after_doldurulan": 0,
            "islenen_kayit": 0,
            "veri_bulunamayan": 0,
        }

        try:
            sdb = self._get_sdb()
        except Exception as e:
            logger.warning("Supabase baglantisi kurulamadi: %s", e)
            return ozet

        # 1. kpi_after bos olan APPLIED kayitlari cek (max 100)
        try:
            rows = sdb._fetch_all("""
                SELECT id, targeting_id, decision_date, targeting_type, hedef_acos, acos_before
                FROM decision_history
                WHERE hesap_key = %s AND marketplace = %s
                  AND decision_status = 'APPLIED'
                  AND kpi_after IS NULL
                ORDER BY decision_date ASC
                LIMIT 5000
            """, (self.hesap_key, self.marketplace))
        except Exception as e:
            logger.warning("decision_history okunamadi: %s", e)
            return ozet

        if not rows:
            logger.info("Doldurulacak kpi_after yok.")
            return ozet

        logger.info("kpi_after bos kayit: %d (max 5000)", len(rows))

        # 2. Tum targeting_id'leri topla ve batch sorgu yap
        id_map = {}
        en_eski_tarih = None
        tum_targeting_ids = set()

        for row in rows:
            row_id, targeting_id, decision_date, targeting_type, hedef_acos, acos_before = row
            targeting_id = str(targeting_id or "")
            if not targeting_id:
                continue
            tum_targeting_ids.add(targeting_id)
            if targeting_id not in id_map:
                id_map[targeting_id] = []
            id_map[targeting_id].append({
                "row_id": row_id,
                "decision_date": str(decision_date),
                "hedef_acos": float(hedef_acos) if hedef_acos else None,
                "acos_before": float(acos_before) if acos_before else None,
            })
            d = str(decision_date)
            if en_eski_tarih is None or d < en_eski_tarih:
                en_eski_tarih = d

        if not tum_targeting_ids:
            return ozet

        # 3. targeting_reports'tan batch sorgu
        id_listesi = list(tum_targeting_ids)
        placeholders = ",".join(["%s"] * len(id_listesi))
        try:
            report_rows = sdb._fetch_all(f"""
                SELECT keyword_id, target_id,
                       impressions, clicks, cost, sales, purchases,
                       acos, collection_date
                FROM targeting_reports
                WHERE hesap_key = %s AND marketplace = %s
                  AND (keyword_id IN ({placeholders}) OR target_id IN ({placeholders}))
                  AND collection_date > %s
                ORDER BY collection_date ASC
            """, (self.hesap_key, self.marketplace,
                  *id_listesi, *id_listesi, en_eski_tarih))
        except Exception as e:
            logger.warning("targeting_reports batch sorgu hatasi: %s", e)
            return ozet

        if not report_rows:
            logger.info("Hicbir kayit icin targeting_reports verisi bulunamadi.")
            return ozet

        # 4. targeting_id -> [(collection_date, metrics), ...] map
        rapor_map = {}
        for rrow in report_rows:
            kw_id, tgt_id, imp, clicks, cost, sales, purchases, r_acos, coll_date = rrow
            for rid in [str(kw_id or ""), str(tgt_id or "")]:
                if rid and rid in tum_targeting_ids:
                    if rid not in rapor_map:
                        rapor_map[rid] = []
                    rapor_map[rid].append({
                        "collection_date": str(coll_date),
                        "impressions": int(imp or 0),
                        "clicks": int(clicks or 0),
                        "spend": float(cost or 0),
                        "sales": float(sales or 0),
                        "orders": int(purchases or 0),
                        "acos": float(r_acos) if r_acos is not None else None,
                    })

        # 5. Her decision_history kaydini esle ve UPDATE
        from psycopg2.extras import Json

        for targeting_id, entries in id_map.items():
            raporlar = rapor_map.get(targeting_id)
            if not raporlar:
                ozet["veri_bulunamayan"] += len(entries)
                continue

            for entry in entries:
                secilen = None
                for rapor in raporlar:
                    if rapor["collection_date"] > entry["decision_date"]:
                        secilen = rapor
                        break

                if not secilen:
                    ozet["veri_bulunamayan"] += 1
                    continue

                ozet["islenen_kayit"] += 1
                acos_after = secilen.get("acos")

                gap_closure = None
                hedef = entry["hedef_acos"]
                acos_before = entry["acos_before"]
                if hedef and acos_before and acos_after is not None:
                    gap_before = acos_before - hedef
                    gap_after = acos_after - hedef
                    if abs(gap_before) > 0.01:
                        gap_closure = round(1 - (gap_after / gap_before), 4)

                kpi_after = {
                    "impressions": secilen["impressions"],
                    "clicks": secilen["clicks"],
                    "spend": secilen["spend"],
                    "sales": secilen["sales"],
                    "orders": secilen["orders"],
                    "acos": acos_after,
                }

                try:
                    sdb._execute("""
                        UPDATE decision_history
                        SET kpi_after = %s,
                            acos_after = %s,
                            gap_closure = %s,
                            kpi_collected_at = NOW(),
                            decision_status = 'VERIFIED'
                        WHERE id = %s
                    """, (Json(kpi_after),
                          acos_after,
                          gap_closure,
                          entry["row_id"]))
                    ozet["kpi_after_doldurulan"] += 1
                except Exception as e:
                    logger.warning("decision_history update hatasi (%s): %s",
                                   targeting_id, e)

        logger.info("KPI Collector: %d doldurulan, %d veri bulunamayan, %d islenen",
                    ozet["kpi_after_doldurulan"],
                    ozet["veri_bulunamayan"],
                    ozet["islenen_kayit"])
        return ozet
