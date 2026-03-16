"""
Agent 4 — Error Analyzer (v2 Multi-Account + Normalize)
=========================================================
Agent 1, 2, 3 hata loglarini (agentN_errors.json) okur ve analiz eder.

Hata tipi normalizasyonu: Farkli agentlarin farkli formatlarda yazdigi
hata tiplerini ortak taksonomiye cevirir.

Ortak Taksonomi:
  RateLimit, AuthError, ApiError, ServerError, NetworkError,
  FileNotFound, DataError, Preflight, ExecutionError,
  VerificationError, ReportFailed, AgentFailure, InternalError
"""

import json
import logging
from collections import Counter
from pathlib import Path

logger = logging.getLogger("agent4.error_analyzer")


# ============================================================================
# HATA TIPI NORMALIZASYON TABLOSU
# ============================================================================

# Farkli agentlarin kullandigi hata_tipi degerleri -> ortak taksonomi
_NORMALIZE_MAP = {
    # Agent 1 eski formatlari
    "ApiError_400":      "ApiError",
    "Timeout":           "NetworkError",
    "API_ERROR":         "ApiError",
    # retry_handler formatlari (kucuk harf + underscore)
    "rate_limit":        "RateLimit",
    "auth_error":        "AuthError",
    "data_error":        "DataError",
    "network":           "NetworkError",
    "server_error":      "ServerError",
    "file_not_found":    "FileNotFound",
    # Agent 3 eski formatlari
    "EntityNotFound":    "DataError",
    "VerificationFailed": "VerificationError",
    # Python exception sinif isimleri
    "FileNotFoundError": "FileNotFound",
    "PermissionError":   "FileNotFound",
    "JSONDecodeError":   "DataError",
    "json.JSONDecodeError": "DataError",
    "KeyError":          "DataError",
    "ValueError":        "DataError",
    "TypeError":         "DataError",
    "IndexError":        "DataError",
    "ConnectionError":   "NetworkError",
    "TimeoutError":      "NetworkError",
    "RuntimeError":      "InternalError",
    "OSError":           "NetworkError",
    "IOError":           "NetworkError",
}

# Bilinen gecerli tipler (normalizasyon sonrasi)
VALID_TYPES = {
    "RateLimit", "AuthError", "ApiError", "ServerError", "NetworkError",
    "FileNotFound", "DataError", "Preflight", "ExecutionError",
    "VerificationError", "ReportFailed", "AgentFailure", "InternalError",
}


def normalize_error_type(hata_tipi):
    """
    Herhangi bir hata tipini ortak taksonomiye cevirir.
    Zaten gecerli bir tipse aynen doner.
    """
    if hata_tipi in VALID_TYPES:
        return hata_tipi
    return _NORMALIZE_MAP.get(hata_tipi, hata_tipi)


class ErrorAnalyzer:

    def __init__(self, data_dir, db):
        self.data_dir = Path(data_dir)
        self.log_dir  = self.data_dir / "logs"
        self.db       = db

    def analyze(self) -> dict:
        sonuc = {
            "agent1": self._analiz_agent("agent1_errors.json", "Agent 1"),
            "agent2": self._analiz_agent("agent2_errors.json", "Agent 2"),
            "agent3": self._analiz_agent("agent3_errors.json", "Agent 3"),
            "tekrar_eden_kaliplar": [],
        }

        # Capraz kalip tespiti
        sonuc["tekrar_eden_kaliplar"] = self._kalip_tespiti(sonuc)

        # DB'ye tekrar eden kaliplari kaydet
        for kalip in sonuc["tekrar_eden_kaliplar"]:
            self.db.add_kalip({
                "tip":   "HATA_KALIBI",
                "tanim": kalip["tanim"],
                "kaynak": kalip["bilesen"],
                "oneri": kalip.get("oneri", ""),
            })

        return sonuc

    # --------------------------------------------------------- Agent analizi
    def _analiz_agent(self, dosya_adi: str, agent_label: str) -> dict:
        """Tek bir agent'in hata logunu okur, normalize eder, analiz eder."""
        log_path = self.log_dir / dosya_adi
        if not log_path.exists():
            return {"toplam": 0, "mesaj": f"{dosya_adi} bulunamadi"}

        try:
            with open(log_path, "r", encoding="utf-8") as f:
                kayitlar = json.load(f)
        except (json.JSONDecodeError, IOError):
            return {"toplam": 0, "mesaj": f"{dosya_adi} okunamadi"}

        if not kayitlar:
            return {"toplam": 0, "mesaj": "Hata kaydi yok"}

        # Hata tiplerini normalize et ve say
        tip_sayac = Counter()
        adim_sayac = Counter()
        session_sayac = Counter()
        son_10 = kayitlar[-10:]

        for k in kayitlar:
            raw_tip = k.get("hata_tipi", "Bilinmiyor")
            normalized = normalize_error_type(raw_tip)
            tip_sayac[normalized] += 1
            adim_sayac[k.get("adim", "bilinmiyor")] += 1

            sid = k.get("session_id")
            if sid:
                session_sayac[sid] += 1

        # En sik hata tipleri
        en_sik_tipler = tip_sayac.most_common(5)
        en_sik_adimlar = adim_sayac.most_common(5)

        # Son 7 gundeki trend (varsa)
        from datetime import datetime, timedelta
        simdi = datetime.utcnow()
        yedi_gun_once = (simdi - timedelta(days=7)).isoformat()
        son_7_gun = [k for k in kayitlar if k.get("timestamp", "") >= yedi_gun_once]
        son_7_gun_tip = Counter(
            normalize_error_type(k.get("hata_tipi", "Bilinmiyor"))
            for k in son_7_gun
        )

        return {
            "toplam":           len(kayitlar),
            "son_7_gun":        len(son_7_gun),
            "tip_dagilimi":     dict(en_sik_tipler),
            "adim_dagilimi":    dict(en_sik_adimlar),
            "session_dagilimi": dict(session_sayac.most_common(5)),
            "son_7_gun_tipler": dict(son_7_gun_tip.most_common(5)),
            "son_hatalar":      [
                {
                    "timestamp": k.get("timestamp"),
                    "hata_tipi": normalize_error_type(k.get("hata_tipi", "")),
                    "hata_mesaji": k.get("hata_mesaji", "")[:200],
                    "adim": k.get("adim"),
                    "session_id": k.get("session_id"),
                }
                for k in son_10
            ],
        }

    # --------------------------------------------------------- Capraz kalip tespiti
    def _kalip_tespiti(self, sonuc: dict) -> list:
        """Tum agentlar arasinda tekrar eden hata kaliplarini tespit eder."""
        kaliplar = []

        # Kalip 1: Ayni hata tipi birden fazla agent'ta tekrarliyorsa
        tip_toplam = Counter()
        for agent_key in ("agent1", "agent2", "agent3"):
            agent_data = sonuc.get(agent_key, {})
            for tip, sayi in agent_data.get("tip_dagilimi", {}).items():
                tip_toplam[tip] += sayi

        for tip, toplam in tip_toplam.most_common():
            if toplam >= 5:
                # Hangi agentlarda goruluyor?
                gorulen = []
                for agent_key in ("agent1", "agent2", "agent3"):
                    agent_data = sonuc.get(agent_key, {})
                    if tip in agent_data.get("tip_dagilimi", {}):
                        gorulen.append(agent_key)

                if len(gorulen) >= 2:
                    kaliplar.append({
                        "bilesen": ", ".join(gorulen),
                        "tip":     "Capraz_Agent_Hata",
                        "tanim":   f"{tip} hatasi {len(gorulen)} agent'ta toplam {toplam} kez tekrarladi",
                        "tekrar":  toplam,
                        "oneri":   self._oneri_uret(tip),
                    })

        # Kalip 2: Agent bazinda yuksek hata yogunlugu (son 7 gunde 10+)
        for agent_key in ("agent1", "agent2", "agent3"):
            agent_data = sonuc.get(agent_key, {})
            son_7 = agent_data.get("son_7_gun", 0)
            if son_7 >= 10:
                en_sik = agent_data.get("son_7_gun_tipler", {})
                en_sik_tip = list(en_sik.keys())[0] if en_sik else "Bilinmiyor"
                kaliplar.append({
                    "bilesen": agent_key,
                    "tip":     "Yuksek_Hata_Yogunlugu",
                    "tanim":   f"{agent_key}: Son 7 gunde {son_7} hata (en sik: {en_sik_tip})",
                    "tekrar":  son_7,
                    "oneri":   f"{agent_key} son hatalari incelenmeli. En sik tip: {en_sik_tip}",
                })

        # Kalip 3: Ayni session'da 3+ hata (pipeline calismasinda sorun)
        for agent_key in ("agent1", "agent2", "agent3"):
            agent_data = sonuc.get(agent_key, {})
            for sid, count in agent_data.get("session_dagilimi", {}).items():
                if count >= 3 and sid:
                    kaliplar.append({
                        "bilesen": agent_key,
                        "tip":     "Session_Coklu_Hata",
                        "tanim":   f"{agent_key}: Session {sid}'de {count} hata kaydi",
                        "tekrar":  count,
                        "oneri":   f"Session {sid} detayli incelenmeli — pipeline bu calismada sorunlu",
                    })

        return kaliplar

    def _oneri_uret(self, hata_tipi: str) -> str:
        """Hata tipine gore iyilestirme onerisi uretir."""
        oneriler = {
            "RateLimit":   "Amazon API rate limit ayarlari gozden gecirilmeli. Bekleme sureleri arttirilabilir.",
            "AuthError":   "Token yenileme mekanizmasi kontrol edilmeli. Token suresi yeterli mi?",
            "ApiError":    "API payload formatlari incelenmeli. Amazon API dokumantasyonu ile karsilastirilmali.",
            "ServerError": "Amazon tarafinda gecici sorun. Retry mekanizmasi yeterli mi kontrol edilmeli.",
            "NetworkError": "Network baglantisi ve timeout ayarlari gozden gecirilmeli.",
            "FileNotFound": "Pipeline calisma sirasi kontrol edilmeli. Onceki adim dosyalari uretiyor mu?",
            "DataError":   "Veri format uyumlulugu kontrol edilmeli. JSON yapilarinda degisiklik olmus olabilir.",
            "Preflight":   "On kontrol kosullari gozden gecirilmeli. Gerekli dosyalar mevcut mu?",
            "ExecutionError": "Amazon API islem hatalari incelenmeli. Payload ve entity ID'ler dogru mu?",
            "VerificationError": "Uygulanan islemler Amazon'da gecerli mi? Bid/target guncellemeleri dogrulanmali.",
            "ReportFailed": "Rapor indirme timeout'lari arttirilmali. Buyuk raporlar icin bekleme suresi yeterli mi?",
        }
        return oneriler.get(hata_tipi, "Hata detaylari incelenmeli.")
