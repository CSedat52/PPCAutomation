"""
Agent 4 — DB Manager (v3 — Supabase Only)
===========================================
Kumülatif veritabanı: Supabase agent4_learning_db tablosu.
JSON dosya fallback kaldırıldı.
"""

import json
import logging
from datetime import datetime

logger = logging.getLogger("agent4.db")


class DBManager:

    DB_KEYS = [
        "karar_gecmisi",
        "segment_istatistikleri",
        "asin_profilleri",
        "anomali_gecmisi",
        "kalip_kutuphanesi",
    ]

    def __init__(self, hesap_key: str, marketplace: str):
        self.hesap_key = hesap_key
        self.marketplace = marketplace
        self._data = {}

    def _get_sdb(self):
        from supabase.db_client import SupabaseClient
        return SupabaseClient()

    # ------------------------------------------------------------------ load
    def load(self):
        """Supabase agent4_learning_db tablosundan yükle."""
        try:
            sdb = self._get_sdb()
            rows = sdb._fetch_all(
                "SELECT db_key, db_data FROM agent4_learning_db "
                "WHERE hesap_key = %s AND marketplace = %s",
                (self.hesap_key, self.marketplace)
            )
            for db_key, db_data in (rows or []):
                if db_key in self.DB_KEYS:
                    self._data[db_key] = db_data if isinstance(db_data, dict) else json.loads(db_data)
                    logger.info("DB Supabase'den yuklendi: %s (%d kayit)",
                                db_key, self._record_count(db_key))
        except Exception as e:
            logger.error("Agent4 DB Supabase'den okunamadi: %s", e)
            raise

        # Eksik anahtarlar icin bos baslat
        for key in self.DB_KEYS:
            if key not in self._data:
                self._data[key] = self._empty(key)
        return self

    # ------------------------------------------------------------------ save
    def save(self):
        """Supabase agent4_learning_db tablosuna yaz."""
        try:
            from psycopg2.extras import Json
            sdb = self._get_sdb()
            conn = sdb._conn()
            cur = conn.cursor()
            for key in self.DB_KEYS:
                cur.execute("""
                    INSERT INTO agent4_learning_db (hesap_key, marketplace, db_key, db_data, updated_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT (hesap_key, marketplace, db_key)
                    DO UPDATE SET db_data = EXCLUDED.db_data, updated_at = NOW()
                """, (self.hesap_key, self.marketplace, key,
                      Json(self._data.get(key, {}))))
            conn.commit()
            cur.close()
            conn.close()
            logger.info("DB Supabase'e kaydedildi (%d tablo)", len(self.DB_KEYS))
        except Exception as e:
            logger.error("Agent4 DB Supabase'e yazilamadi: %s", e)
            raise

    # ----------------------------------------------------------- accessors
    def get(self, key):
        return self._data.get(key, self._empty(key))

    def set(self, key, value):
        self._data[key] = value

    # --------------------------------------------------- karar_gecmisi helpers
    def add_karar(self, karar: dict):
        """Yeni karar ekler. Ayni hedefleme_id+tarih varsa gunceller."""
        kg = self._data["karar_gecmisi"]
        uid = f"{karar['tarih']}_{karar['hedefleme_id']}"
        for i, k in enumerate(kg["kararlar"]):
            if f"{k['tarih']}_{k['hedefleme_id']}" == uid:
                kg["kararlar"][i] = karar
                return
        kg["kararlar"].append(karar)

    def get_kararlar(self, hedefleme_id=None, asin=None, son_n=None):
        """Filtrelenmis karar listesi doner."""
        kararlar = self._data["karar_gecmisi"]["kararlar"]
        if hedefleme_id:
            kararlar = [k for k in kararlar if k.get("hedefleme_id") == hedefleme_id]
        if asin:
            kararlar = [k for k in kararlar if k.get("asin") == asin]
        if son_n:
            kararlar = kararlar[-son_n:]
        return kararlar

    def get_kpi_after_bos(self):
        """kpi_after henuz doldurulmamis kararlari doner."""
        return [
            k for k in self._data["karar_gecmisi"]["kararlar"]
            if k.get("kpi_after") is None and k.get("karar_durumu") == "UYGULANDI"
        ]

    # --------------------------------------------------- segment helpers
    def update_segment_stat(self, segment, basarili, toplam):
        ss = self._data["segment_istatistikleri"]["segmentler"]
        if segment not in ss:
            ss[segment] = {"toplam": 0, "basarili": 0, "basarisiz": 0, "basari_orani": 0.0}
        ss[segment]["toplam"]    += toplam
        ss[segment]["basarili"]  += basarili
        ss[segment]["basarisiz"] += (toplam - basarili)
        if ss[segment]["toplam"] > 0:
            ss[segment]["basari_orani"] = round(
                ss[segment]["basarili"] / ss[segment]["toplam"], 3)
        self._data["segment_istatistikleri"]["son_guncelleme"] = \
            datetime.utcnow().isoformat()

    # --------------------------------------------------- anomali helpers
    def add_anomali(self, anomali: dict):
        ag = self._data["anomali_gecmisi"]
        anomali["id"] = f"A-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        ag["anomaliler"].append(anomali)
        ag["son_guncelleme"] = datetime.utcnow().isoformat()

    def get_aktif_anomaliler(self):
        return [
            a for a in self._data["anomali_gecmisi"]["anomaliler"]
            if a.get("durum") == "AKTIF"
        ]

    # --------------------------------------------------- kalip helpers
    def add_kalip(self, kalip: dict):
        kk = self._data["kalip_kutuphanesi"]
        for i, k in enumerate(kk["kaliplar"]):
            if k.get("tip") == kalip.get("tip") and k.get("tanim") == kalip.get("tanim"):
                kk["kaliplar"][i]["tekrar_sayisi"] = k.get("tekrar_sayisi", 1) + 1
                kk["kaliplar"][i]["son_gorulme"] = datetime.utcnow().isoformat()
                return
        kalip["id"] = f"K-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        kalip["tekrar_sayisi"] = 1
        kalip["ilk_gorulme"] = datetime.utcnow().isoformat()
        kalip["son_gorulme"]  = datetime.utcnow().isoformat()
        kk["kaliplar"].append(kalip)
        kk["son_guncelleme"] = datetime.utcnow().isoformat()

    # --------------------------------------------------- asin profil helpers
    def get_asin_profil(self, asin):
        return self._data["asin_profilleri"]["profiller"].get(asin, {})

    def update_asin_profil(self, asin, guncelleme: dict):
        profiller = self._data["asin_profilleri"]["profiller"]
        if asin not in profiller:
            profiller[asin] = {
                "asin": asin,
                "toplam_karar": 0,
                "segment_gecmisi": [],
                "ortalama_acos_oncesi": None,
                "ortalama_acos_sonrasi": None,
                "son_guncelleme": None,
            }
        profiller[asin].update(guncelleme)
        profiller[asin]["son_guncelleme"] = datetime.utcnow().isoformat()
        self._data["asin_profilleri"]["son_guncelleme"] = datetime.utcnow().isoformat()

    # ----------------------------------------------------------------- utils
    def _record_count(self, key):
        d = self._data.get(key, {})
        for liste_adi in ("kararlar", "segmentler", "profiller", "anomaliler", "kaliplar"):
            if liste_adi in d:
                v = d[liste_adi]
                return len(v) if isinstance(v, (list, dict)) else 0
        return 0

    def _empty(self, key):
        templates = {
            "karar_gecmisi": {
                "_aciklama": "Tum bid kararlari ve kpi_after sonuclari",
                "son_guncelleme": None,
                "kararlar": [],
            },
            "segment_istatistikleri": {
                "_aciklama": "Segment bazinda basari/basarisizlik oranlari",
                "son_guncelleme": None,
                "segmentler": {},
            },
            "asin_profilleri": {
                "_aciklama": "ASIN bazinda ogrenilmis davranis profilleri",
                "son_guncelleme": None,
                "profiller": {},
            },
            "anomali_gecmisi": {
                "_aciklama": "Tespit edilen tum anomaliler",
                "son_guncelleme": None,
                "anomaliler": [],
            },
            "kalip_kutuphanesi": {
                "_aciklama": "Tekrar eden kaliplar ve cikarilan kurallar",
                "son_guncelleme": None,
                "kaliplar": [],
            },
        }
        return templates.get(key, {})
