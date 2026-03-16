"""
Agent 4 — DB Manager
======================
Kumülatif veritabanı dosyalarını okur ve yazar.

Dosyalar (data/agent4/db/):
  karar_gecmisi.json       — Tüm bid kararları + kpi_after sonuçları
  segment_istatistikleri.json — Segment başarı/başarısızlık oranları
  asin_profilleri.json     — ASIN bazında öğrenilmiş profiller
  anomali_gecmisi.json     — Tespit edilen anomaliler
  kalip_kutuphanesi.json   — Tekrar eden kalıplar
"""

import json
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("agent4.db")


class DBManager:

    DB_FILES = {
        "karar_gecmisi":        "karar_gecmisi.json",
        "segment_istatistikleri": "segment_istatistikleri.json",
        "asin_profilleri":      "asin_profilleri.json",
        "anomali_gecmisi":      "anomali_gecmisi.json",
        "kalip_kutuphanesi":    "kalip_kutuphanesi.json",
    }

    def __init__(self, data_dir):
        self.data_dir = Path(data_dir)
        self.db_dir   = self.data_dir / "agent4" / "db"
        self.db_dir.mkdir(parents=True, exist_ok=True)
        self._data = {}

    # ------------------------------------------------------------------ load
    def load(self):
        for key, filename in self.DB_FILES.items():
            path = self.db_dir / filename
            if path.exists():
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        self._data[key] = json.load(f)
                    logger.info("DB yuklendi: %s (%d kayit)",
                                filename, self._record_count(key))
                except (json.JSONDecodeError, IOError) as e:
                    logger.warning("DB okunamadi %s: %s — bos baslaniyor", filename, e)
                    self._data[key] = self._empty(key)
            else:
                self._data[key] = self._empty(key)
        return self

    # ------------------------------------------------------------------ save
    def save(self):
        for key, filename in self.DB_FILES.items():
            path = self.db_dir / filename
            with open(path, "w", encoding="utf-8") as f:
                json.dump(self._data[key], f, indent=2, ensure_ascii=False)
        logger.info("DB kaydedildi (%d tablo)", len(self.DB_FILES))

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
        # Guncelleme: varsa degistir, yoksa ekle
        for i, k in enumerate(kg["kararlar"]):
            if f"{k['tarih']}_{k['hedefleme_id']}" == uid:
                kg["kararlar"][i] = karar
                return
        kg["kararlar"].append(karar)

    def get_kararlar(self, hedefleme_id=None, asin=None, son_n=None):
        """Filtrelenmiş karar listesi döner."""
        kararlar = self._data["karar_gecmisi"]["kararlar"]
        if hedefleme_id:
            kararlar = [k for k in kararlar if k.get("hedefleme_id") == hedefleme_id]
        if asin:
            kararlar = [k for k in kararlar if k.get("asin") == asin]
        if son_n:
            kararlar = kararlar[-son_n:]
        return kararlar

    def get_kpi_after_bos(self):
        """kpi_after henüz doldurulmamış kararları döner."""
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
        # Ayni tip+tanim varsa guncelle
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
        # Her tablonun ana listesi farkli isimde
        for liste_adi in ("kararlar", "segmentler", "profiller", "anomaliler", "kaliplar"):
            if liste_adi in d:
                v = d[liste_adi]
                return len(v) if isinstance(v, (list, dict)) else 0
        return 0

    def _empty(self, key):
        templates = {
            "karar_gecmisi": {
                "_aciklama": "Tüm bid kararları ve kpi_after sonuçları",
                "son_guncelleme": None,
                "kararlar": [],
            },
            "segment_istatistikleri": {
                "_aciklama": "Segment bazında başarı/başarısızlık oranları",
                "son_guncelleme": None,
                "segmentler": {},
            },
            "asin_profilleri": {
                "_aciklama": "ASIN bazında öğrenilmiş davranış profilleri",
                "son_guncelleme": None,
                "profiller": {},
            },
            "anomali_gecmisi": {
                "_aciklama": "Tespit edilen tüm anomaliler",
                "son_guncelleme": None,
                "anomaliler": [],
            },
            "kalip_kutuphanesi": {
                "_aciklama": "Tekrar eden kalıplar ve çıkarılan kurallar",
                "son_guncelleme": None,
                "kaliplar": [],
            },
        }
        return templates.get(key, {})
