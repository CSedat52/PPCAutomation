"""
Agent 4 — Proposal Engine
===========================
Analiz sonuçlarından öneri paketleri üretir.

KRITIK KURAL:
  Her öneri 5 soruyu yanıtlamadan üretilemez:
    1. NE     — Ne değiştirilecek?
    2. NEDEN  — Kanıta dayalı neden?
    3. KANIT  — Hangi veri?
    4. RISK   — Olası olumsuz etki?
    5. KAZANIM — Beklenen iyileşme?

Öneri kategorileri:
  PARAMETRE_DEGISIMI  — bid_functions.json → asin_parametreleri
  GLOBAL_FORMUL       — bid_functions.json → tanh_formulu
  ESIK_DEGERI         — settings.json → esik_degerleri
  KOD_DEGISIKLIGI     — CLAUDE.md / retry_handler.py / agent dosyaları
  AGENT_KURAL         — settings.json → ozel_kurallar
"""

import os
import json
import logging
import hashlib
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("agent4.proposal")

# Öneri üretmek için minimum kanıt eşikleri
MIN_OLCULEBILIR_KARAR = 15   # Parametre önerisi için
MIN_HATA_TEKRAR       = 3    # Hata döngüsü önerisi için
MIN_SEGMENT_KARAR     = 10   # Segment eşik önerisi için


class ProposalEngine:

    def __init__(self, data_dir, config_dir, db, analiz_sonuclari: dict):
        self.data_dir = Path(data_dir)
        self.config_dir = Path(config_dir)
        self.proposals_dir = self.data_dir / "agent4" / "proposals" / "bekleyen"
        self.proposals_dir.mkdir(parents=True, exist_ok=True)
        self.db      = db
        self.analiz  = analiz_sonuclari

    # ------------------------------------------------------------------ run
    def run(self, today: str) -> list:
        """Tüm analizlerden öneri üretir, dosyalara yazar."""
        oneriler = []

        oneriler += self._parametre_onerileri(today)
        oneriler += self._esik_onerileri(today)
        oneriler += self._hata_dongus_onerileri(today)
        oneriler += self._maestro_oneri(today)

        # Dosyalara yaz
        for oneri in oneriler:
            self._kaydet(oneri)

        logger.info("Öneri motoru: %d öneri üretildi", len(oneriler))
        return oneriler

    # ----------------------------------------- Parametre önerileri
    def _parametre_onerileri(self, today: str) -> list:
        """
        Segment başarı oranı düşük segmentlerdeki ASIN'ler için
        hassasiyet veya max_degisim parametresi önerisi.
        """
        seg_sonuc = self.analiz.get("segment", {})
        if seg_sonuc.get("durum") != "TAMAMLANDI":
            return []

        dusuk = seg_sonuc.get("dusuk_performans", [])
        if not dusuk:
            return []

        oneriler = []
        kararlar = self.db.get("karar_gecmisi")["kararlar"]

        for item in dusuk:
            seg          = item["segment"]
            basari_orani = item["basari_orani"]
            toplam       = item["toplam"]

            if toplam < MIN_SEGMENT_KARAR:
                continue

            # Bu segmentteki ASIN'leri bul
            seg_kararlar = [
                k for k in kararlar
                if k.get("segment") == seg and k.get("kpi_after")
            ]

            # ASIN bazında ACOS değişim ortalaması
            asin_sonuclar = {}
            for k in seg_kararlar:
                asin = k.get("asin", "")
                if not asin:
                    continue
                acos_once  = k["metrikler"].get("acos")
                acos_sonra = k["kpi_after"].get("acos")
                if acos_once is None or acos_sonra is None:
                    continue
                if asin not in asin_sonuclar:
                    asin_sonuclar[asin] = []
                asin_sonuclar[asin].append(acos_sonra - acos_once)

            # Tutarsız ASIN'ler için öneri üret
            for asin, degisimler in asin_sonuclar.items():
                if len(degisimler) < 4:
                    continue
                ort_degisim = sum(degisimler) / len(degisimler)

                # ZARAR/OPTIMIZE_ET segmentinde ACOS düşmüyorsa — max_degisim too high
                if seg in ("ZARAR", "OPTIMIZE_ET") and ort_degisim > 0:
                    mevcut = self._get_asin_parametre(asin, "max_degisim", 0.20)
                    yeni   = round(mevcut * 0.75, 2)   # %25 düşür
                    oneri  = self._olustur_oneri(
                        today=today,
                        kategori="PARAMETRE_DEGISIMI",
                        ne=f"{asin} ({seg}) için max_degisim: {mevcut} → {yeni}",
                        neden=f"{seg} segmentinde {len(degisimler)} çalışmada ACOS ortalama {ort_degisim:+.1f}pp değişti (beklenen: negatif)",
                        kanit={
                            "asin":             asin,
                            "segment":          seg,
                            "olculen_karar":    len(degisimler),
                            "ortalama_degisim":  round(ort_degisim, 2),
                            "segment_basari_orani": basari_orani,
                        },
                        risk="Bid düşüşü daha yavaş olacak — kısa vadede spend biraz daha yüksek kalabilir",
                        kazanim=f"Kademeli düşüş sayesinde impression kaybı önlenerek {seg} döngüsü kırılabilir",
                        degisecek_dosya="config/bid_functions.json → asin_parametreleri",
                        degisecek_alan={"asin": asin, "parametre": "max_degisim",
                                        "mevcut": mevcut, "yeni": yeni},
                    )
                    oneriler.append(oneri)

        return oneriler[:3]   # max 3 parametre önerisi per çalışma

    # ----------------------------------------- Eşik değeri önerileri
    def _esik_onerileri(self, today: str) -> list:
        """
        YETERSIZ_VERI'de kalan hedeflemelerin başarı oranı çok düşükse
        tıklama eşiğini düşür.
        """
        seg_sonuc = self.analiz.get("segment", {})
        if seg_sonuc.get("durum") != "TAMAMLANDI":
            return []

        seg_sonuclari = seg_sonuc.get("segment_sonuclari", {})
        yv = seg_sonuclari.get("YETERSIZ_VERI", {})

        if yv.get("toplam", 0) < MIN_OLCULEBILIR_KARAR:
            return []

        # Eğer toplam kararların %30'dan fazlası YETERSIZ_VERI'deyse
        toplam_karar = seg_sonuc.get("olculebilir_karar", 1)
        yv_oran      = yv.get("toplam", 0) / toplam_karar if toplam_karar > 0 else 0

        if yv_oran < 0.30:
            return []

        mevcut_esik = self._get_settings_deger("esik_degerleri.tiklama_esik", 30)
        yeni_esik   = max(mevcut_esik - 5, 15)   # En fazla 5 düşür, 15'in altına inme

        if yeni_esik == mevcut_esik:
            return []

        oneri = self._olustur_oneri(
            today=today,
            kategori="ESIK_DEGERI",
            ne=f"tiklama_esik: {mevcut_esik} → {yeni_esik}",
            neden=f"Ölçülebilir kararların %{yv_oran*100:.0f}'i YETERSIZ_VERI segmentinde kalıyor ({yv['toplam']}/{toplam_karar})",
            kanit={
                "yetersiz_veri_orani": round(yv_oran, 3),
                "yetersiz_veri_karar": yv["toplam"],
                "toplam_karar":        toplam_karar,
            },
            risk="Daha düşük eşik, istatistiksel güveni az kararlar üretebilir",
            kazanim=f"Daha fazla hedefleme analiz edilip segment alabilecek — karar kalitesi artabilir",
            degisecek_dosya="config/settings.json → esik_degerleri",
            degisecek_alan={"parametre": "tiklama_esik",
                            "mevcut": mevcut_esik, "yeni": yeni_esik},
        )
        return [oneri]

    # ----------------------------------------- Hata döngüsü önerileri
    def _hata_dongus_onerileri(self, today: str) -> list:
        """
        Tekrar eden hata kalıpları için CLAUDE.md / config önerisi.
        """
        hata_sonuc = self.analiz.get("hata", {})
        kaliplar   = hata_sonuc.get("tekrar_eden_kaliplar", [])

        oneriler = []
        for kalip in kaliplar:
            if kalip.get("tekrar", 0) < MIN_HATA_TEKRAR:
                continue

            bilesen = kalip.get("bilesen", "")
            tip     = kalip.get("tip", "")
            tekrar  = kalip.get("tekrar", 0)

            if tip == "RateLimit_Dongusu":
                oneri = self._olustur_oneri(
                    today=today,
                    kategori="KOD_DEGISIKLIGI",
                    ne="config.py'de SCHEDULE_TIME_HOUR_UTC değiştirilmesi veya CLAUDE.md'ye rate limit senaryosu eklenmesi",
                    neden=f"Agent 1 son 30 günde {tekrar} kez rate limit hatası aldı",
                    kanit={"hata_tipi": "RateLimit", "tekrar": tekrar, "bilesen": bilesen},
                    risk="Zamanlama değişikliği diğer görevlerle çakışabilir",
                    kazanim="Rate limit döngüsü kırılarak Agent 1 başarı oranı artar",
                    degisecek_dosya="maestro/CLAUDE.md veya config/config.py",
                    degisecek_alan={"senaryo": "rate_limit_zamanlama"},
                )
                oneriler.append(oneri)

            elif tip == "Kampanya_Hata_Kumesi":
                kamp = kalip.get("tanim", "")
                oneri = self._olustur_oneri(
                    today=today,
                    kategori="KOD_DEGISIKLIGI",
                    ne=f"CLAUDE.md'ye kampanya execution hata senaryosu eklenmesi",
                    neden=f"{kamp} ({tekrar} kez execution hatası)",
                    kanit={"hata_tipi": "ExecutionError", "tekrar": tekrar, "bilesen": bilesen},
                    risk="Senaryo yanlış tanımlanırsa Maestro yanlış self-heal adımı atabilir",
                    kazanim="Tekrarlayan execution hataları otomatik çözülebilir hale gelir",
                    degisecek_dosya="maestro/CLAUDE.md",
                    degisecek_alan={"senaryo": "execution_hata"},
                )
                oneriler.append(oneri)

        return oneriler[:2]

    # ----------------------------------------- Maestro önerisi
    def _maestro_oneri(self, today: str) -> list:
        maestro_sonuc = self.analiz.get("maestro", {})
        if not maestro_sonuc.get("ardisik_hata_alarmi"):
            return []

        oneri = self._olustur_oneri(
            today=today,
            kategori="KOD_DEGISIKLIGI",
            ne="CLAUDE.md veya retry_handler.py güncellenmesi — ardışık hata döngüsünü kırmak için",
            neden="Son 3 pipeline session ardışık olarak hata ile sonuçlandı",
            kanit={
                "toplam_session":  maestro_sonuc.get("toplam_session"),
                "hatali_session":  maestro_sonuc.get("hatali"),
                "ardisik_hata":    True,
            },
            risk="Yanlış senaryo eklenirse Maestro farklı hataları bu senaryo ile eşleştirebilir",
            kazanim="Ardışık pipeline hataları önlenerek sistem sürekliliği korunur",
            degisecek_dosya="maestro/CLAUDE.md ve/veya maestro/retry_handler.py",
            degisecek_alan={"durum": "ardisik_hata_analizi"},
        )
        return [oneri]

    # ----------------------------------------- Öneri oluşturucu
    def _olustur_oneri(self, today, kategori, ne, neden,
                        kanit, risk, kazanim, degisecek_dosya, degisecek_alan) -> dict:
        icerik = f"{kategori}|{ne}|{today}"
        oneri_id = "ONR-" + hashlib.md5(icerik.encode()).hexdigest()[:8].upper()

        return {
            "id":               oneri_id,
            "tarih":            today,
            "kategori":         kategori,
            "durum":            "BEKLIYOR",
            "ne":               ne,
            "neden":            neden,
            "kanit":            kanit,
            "beklenen_sonuc":   risk,           # Supabase kolon adi
            "gerceklesen_sonuc": kazanim,        # Supabase kolon adi
            "risk":             risk,            # Geriye uyumluluk (JSON dosyalari)
            "kazanim":          kazanim,         # Geriye uyumluluk (JSON dosyalari)
            "degisecek_dosya":  degisecek_dosya,
            "degisecek_alan":   degisecek_alan,
            "olusturma_zamani": datetime.utcnow().isoformat(),
        }

    def _kaydet(self, oneri: dict):
        # Supabase'e yaz
        hk = os.environ.get("HESAP_KEY", "")
        mp = os.environ.get("MARKETPLACE", "")
        if hk and mp:
            try:
                from supabase.db_client import SupabaseClient
                db = SupabaseClient()
                db.upsert_proposal(hk, mp, {
                    "id": oneri["id"],
                    "kategori": oneri.get("kategori", ""),
                    "baslik": oneri.get("ne", ""),
                    "aciklama": oneri.get("neden", ""),
                    "gerekce": oneri.get("kanit", ""),
                    "beklenen_sonuc": oneri.get("beklenen_sonuc", ""),
                    "gerceklesen_sonuc": oneri.get("gerceklesen_sonuc", ""),
                    "status": oneri.get("durum", "PENDING"),
                })
                logger.info("Öneri Supabase'e kaydedildi: %s", oneri["id"])
            except Exception as e:
                logger.warning("Öneri Supabase'e yazilamadi: %s", e)

        # JSON dosyasina da yaz (fallback)
        path = self.proposals_dir / f"{oneri['id']}.json"
        if path.exists():
            return
        with open(path, "w", encoding="utf-8") as f:
            json.dump(oneri, f, indent=2, ensure_ascii=False)

    # ----------------------------------------------------------------- utils
    def _get_asin_parametre(self, asin: str, parametre: str, varsayilan):
        """ASIN parametresini Supabase asin_bid_params'dan yukler. Fallback: bid_functions.json."""
        hk = os.environ.get("HESAP_KEY", "")
        mp = os.environ.get("MARKETPLACE", "")
        if hk and mp:
            try:
                from supabase.db_client import SupabaseClient
                db = SupabaseClient()
                conn = db._conn()
                cur = conn.cursor()
                cur.execute("SELECT aktif, hassasiyet, max_degisim FROM asin_bid_params WHERE hesap_key = %s AND marketplace = %s AND asin = %s", (hk, mp, asin))
                row = cur.fetchone()
                cur.close()
                conn.close()
                if row and row[0]:  # aktif=True
                    params = {"hassasiyet": float(row[1] or 0.5), "max_degisim": float(row[2] or 0.2)}
                    if parametre in params:
                        return params[parametre]
                # Global tanh parametrelerini Supabase'den al
                cur2 = db._conn().cursor()
                cur2.execute("SELECT tanh_formulu FROM bid_functions WHERE hesap_key = %s AND marketplace = %s", (hk, mp))
                row2 = cur2.fetchone()
                cur2.connection.close()
                if row2 and row2[0]:
                    tf = row2[0] if isinstance(row2[0], dict) else json.loads(row2[0])
                    return tf.get(parametre, varsayilan)
            except Exception:
                pass

        bid_path = self.config_dir / "bid_functions.json"
        try:
            with open(bid_path, "r", encoding="utf-8") as f:
                bf = json.load(f)
            asin_p = bf.get("asin_parametreleri", {}).get(asin, {})
            if asin_p.get("aktif") and parametre in asin_p:
                return asin_p[parametre]
            return bf.get("tanh_formulu", {}).get(parametre, varsayilan)
        except Exception:
            return varsayilan

    def _get_settings_deger(self, yol: str, varsayilan):
        """Settings degerini Supabase'den yukler. Fallback: settings.json."""
        hk = os.environ.get("HESAP_KEY", "")
        mp = os.environ.get("MARKETPLACE", "")
        if hk and mp:
            try:
                from supabase.db_client import SupabaseClient
                db = SupabaseClient()
                conn = db._conn()
                cur = conn.cursor()
                cur.execute("SELECT genel_ayarlar, esik_degerleri, segmentasyon_kurallari, agent3_ayarlari FROM settings WHERE hesap_key = %s AND marketplace = %s", (hk, mp))
                row = cur.fetchone()
                cur.close()
                conn.close()
                if row:
                    s = {}
                    for i, key in enumerate(["genel_ayarlar", "esik_degerleri", "segmentasyon_kurallari", "agent3_ayarlari"]):
                        if row[i]:
                            s[key] = row[i] if isinstance(row[i], dict) else json.loads(row[i])
                    parcalar = yol.split(".")
                    obj = s
                    for p in parcalar:
                        obj = obj[p]
                    return obj
            except Exception:
                pass

        settings_path = self.config_dir / "settings.json"
        try:
            with open(settings_path, "r", encoding="utf-8") as f:
                s = json.load(f)
            parcalar = yol.split(".")
            obj = s
            for p in parcalar:
                obj = obj[p]
            return obj
        except Exception:
            return varsayilan


# ------------------------------------------------------------------ CLI
def cmd_oneri(data_dir, config_dir, args):
    """
    Komut satırından öneri yönetimi:
      python agent4/optimizer.py <hesap> <mp> oneri listele
      python agent4/optimizer.py <hesap> <mp> oneri onayla ONR-XXXXXXXX
      python agent4/optimizer.py <hesap> <mp> oneri reddet ONR-XXXXXXXX [sebep]
    """
    proposals_dir = Path(data_dir) / "agent4" / "proposals" / "bekleyen"
    arsiv_dir     = Path(data_dir) / "agent4" / "proposals" / "arsiv"
    arsiv_dir.mkdir(parents=True, exist_ok=True)

    if not args or args[0] == "listele":
        dosyalar = sorted(proposals_dir.glob("ONR-*.json"))
        if not dosyalar:
            print("Bekleyen öneri yok.")
            return
        print(f"\n{'='*60}")
        print(f"BEKLEYEN ÖNERİLER ({len(dosyalar)} adet)")
        print(f"{'='*60}")
        for d in dosyalar:
            with open(d) as f:
                o = json.load(f)
            print(f"\n[{o['id']}] {o['kategori']}")
            print(f"  NE      : {o['ne']}")
            print(f"  NEDEN   : {o['neden']}")
            print(f"  KANIT   : {json.dumps(o['kanit'], ensure_ascii=False)}")
            print(f"  RISK    : {o['risk']}")
            print(f"  KAZANIM : {o['kazanim']}")
            print(f"  DOSYA   : {o['degisecek_dosya']}")
        print(f"\n{'='*60}")
        print("Onaylamak için: python agent4/optimizer.py oneri onayla <ID>")
        print("Reddetmek için: python agent4/optimizer.py oneri reddet <ID> [sebep]")
        return

    if args[0] == "onayla" and len(args) >= 2:
        oneri_id = args[1]
        path = proposals_dir / f"{oneri_id}.json"
        if not path.exists():
            print(f"Öneri bulunamadı: {oneri_id}")
            return
        with open(path) as f:
            o = json.load(f)
        o["durum"]          = "ONAYLANDI"
        o["onay_zamani"]    = datetime.utcnow().isoformat()
        arsiv_path = arsiv_dir / path.name
        with open(arsiv_path, "w", encoding="utf-8") as f:
            json.dump(o, f, indent=2, ensure_ascii=False)
        path.unlink()
        print(f"✓ Onaylandı: {oneri_id}")
        print(f"  Değiştirilecek dosya: {o['degisecek_dosya']}")
        print(f"  Alan: {json.dumps(o['degisecek_alan'], ensure_ascii=False)}")
        print(f"  NOT: Bu değişikliği manuel olarak uygulamanız gerekiyor.")
        return

    if args[0] == "reddet" and len(args) >= 2:
        oneri_id = args[1]
        sebep    = " ".join(args[2:]) if len(args) > 2 else "Belirtilmedi"
        path = proposals_dir / f"{oneri_id}.json"
        if not path.exists():
            print(f"Öneri bulunamadı: {oneri_id}")
            return
        with open(path) as f:
            o = json.load(f)
        o["durum"]        = "REDDEDILDI"
        o["red_zamani"]   = datetime.utcnow().isoformat()
        o["red_sebebi"]   = sebep
        arsiv_path = arsiv_dir / path.name
        with open(arsiv_path, "w", encoding="utf-8") as f:
            json.dump(o, f, indent=2, ensure_ascii=False)
        path.unlink()
        print(f"✗ Reddedildi: {oneri_id} — Sebep: {sebep}")
        return

    print("Kullanım: python agent4/optimizer.py oneri [listele|onayla|reddet] ...")
