"""
Maestro Agent — E-posta Handler
=================================
Gmail SMTP ile e-posta gonderir, IMAP ile reply kontrol eder.
"""

import re
import email
import imaplib
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

from . import config

logger = logging.getLogger("maestro.email")


# ============================================================================
# SMTP — E-POSTA GONDERIM
# ============================================================================

def send_email(subject, body, to_email=None):
    """
    Gmail SMTP ile e-posta gonderir.
    Returns: (success: bool, error: str or None)
    """
    if not config.GMAIL_ADDRESS or not config.GMAIL_APP_PASSWORD:
        logger.error("Gmail ayarlari eksik. MAESTRO_GMAIL_ADDRESS ve MAESTRO_GMAIL_APP_PASSWORD env var'lari ayarlanmali.")
        return False, "Gmail ayarlari eksik"

    to_email = to_email or config.NOTIFY_EMAIL

    msg = MIMEMultipart()
    msg["From"] = config.GMAIL_ADDRESS
    msg["To"] = to_email
    msg["Subject"] = subject

    msg.attach(MIMEText(body, "plain", "utf-8"))

    try:
        with smtplib.SMTP(config.SMTP_HOST, config.SMTP_PORT) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(config.GMAIL_ADDRESS, config.GMAIL_APP_PASSWORD)
            server.sendmail(config.GMAIL_ADDRESS, to_email, msg.as_string())

        logger.info("E-posta gonderildi: %s -> %s", subject, to_email)
        return True, None

    except smtplib.SMTPAuthenticationError as e:
        error = f"Gmail kimlik dogrulama hatasi: {e}"
        logger.error(error)
        return False, error
    except Exception as e:
        error = f"E-posta gonderim hatasi: {e}"
        logger.error(error)
        return False, error


# ============================================================================
# E-POSTA SABLONLARI
# ============================================================================

def send_excel_ready(session_id, summary):
    """Agent 2 tamamlandi, Excel'ler hazir bildirimi."""
    account_label = (config.CURRENT_ACCOUNT or {}).get("label", "")
    subject = config.EMAIL_SUBJECTS["excel_ready"].format(
        session_id=session_id, account_label=account_label)

    bid_count = summary.get("bid_tavsiye_sayisi", 0)
    neg_count = summary.get("negatif_aday_sayisi", 0)
    harvest_count = summary.get("harvesting_aday_sayisi", 0)
    segments = summary.get("segment_dagilimi", {})

    body = f"""Merhaba,

PPC Analiz tamamlandi. Tavsiye Excel raporlari hazir.

HESAP: {account_label}
SESSION: {session_id}
TARIH: {summary.get('tarih', '-')}

OZET:
  Bid Tavsiyeleri: {bid_count} hedefleme
  Negatif Adaylar: {neg_count} aday
  Harvesting Adaylari: {harvest_count} aday

SEGMENT DAGILIMI:
"""
    for seg, count in segments.items():
        body += f"  {seg}: {count}\n"

    body += f"""
DOSYALAR:
  Bid: {summary.get('dosyalar', {}).get('bid_tavsiyeleri', '-')}
  Negatif: {summary.get('dosyalar', {}).get('negatif_adaylar', '-')}
  Harvesting: {summary.get('dosyalar', {}).get('harvesting_adaylar', '-')}

---
YAPMANIZ GEREKEN:
1. Excel dosyalarindaki Onay sutunlarini doldurun (Y veya ozel bid degeri)
2. Bu e-postaya "onay" yazarak reply atin

Maestro 1 saatte bir kontrol edecek.
"""
    return send_email(subject, body)


def send_error(session_id, agent_name, error_details, suggestion=None):
    """Cozulemeyen hata bildirimi."""
    account_label = (config.CURRENT_ACCOUNT or {}).get("label", "")
    subject = config.EMAIL_SUBJECTS["error"].format(
        session_id=session_id, agent_name=agent_name, account_label=account_label)
    body = f"""UYARI: Pipeline hatasi olustu.

SESSION: {session_id}
AGENT: {agent_name}
ZAMAN: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC

HATA DETAYI:
{error_details}
"""
    if suggestion:
        body += f"""
COZUM ONERISI:
{suggestion}
"""
    body += """
---
Sorunu cozup pipeline'i devam ettirmek icin:
Claude Code'da "maestro resume" komutunu kullanin.
"""
    return send_email(subject, body)


def send_completed(session_id, summary):
    """Pipeline basariyla tamamlandi ozet raporu."""
    account_label = (config.CURRENT_ACCOUNT or {}).get("label", "")
    subject = config.EMAIL_SUBJECTS["completed"].format(
        session_id=session_id, account_label=account_label)

    body = f"""PPC Pipeline basariyla tamamlandi.

SESSION: {session_id}
TARIH: {datetime.utcnow().strftime('%Y-%m-%d')}

"""
    # Agent 3 sonuclari varsa ekle
    a3 = summary.get("agent3_summary", {})
    if a3:
        ozet = a3.get("ozet", a3.get("ozet_mesaj", ""))
        body += f"UYGULAMA SONUCU:\n{ozet}\n\n"

        bid_info = a3.get("bid_islemleri_ozet", "")
        neg_info = a3.get("negatif_islemleri_ozet", "")
        harvest_info = a3.get("harvesting_islemleri_ozet", "")

        if bid_info:
            body += f"  Bid Degisiklikleri: {bid_info}\n"
        if neg_info:
            body += f"  Negatif Eklemeler: {neg_info}\n"
        if harvest_info:
            body += f"  Harvesting: {harvest_info}\n"

    body += f"""
ZAMAN CIZELGESI:
  Agent 1: {summary.get('agent1_duration', '-')}
  Agent 2: {summary.get('agent2_duration', '-')}
  Agent 3: {summary.get('agent3_duration', '-')}

Sonraki calisma: {config.SCHEDULE_INTERVAL_DAYS} gun sonra.
"""
    return send_email(subject, body)


def send_reminder(session_id):
    """Onay hatirlatma e-postasi."""
    account_label = (config.CURRENT_ACCOUNT or {}).get("label", "")
    subject = config.EMAIL_SUBJECTS["reminder"].format(
        session_id=session_id, account_label=account_label)
    body = f"""HATIRLATMA: Onay bekleniyor.

SESSION: {session_id}

Tavsiye Excel dosyalarindaki Onay sutunlari henuz doldurulmamis.
Lutfen Excel'leri kontrol edip Onay kutularini doldurun,
sonra bu e-postaya "onay" yazarak reply atin.

Maestro 1 saatte bir kontrol etmeye devam ediyor.
"""
    return send_email(subject, body)


# ============================================================================
# IMAP — REPLY KONTROL
# ============================================================================

def check_for_approval_reply(session_id):
    """
    Gmail IMAP ile gelen kutusunu kontrol eder.
    Ilgili session'a ait reply'da onay keyword'u arar.
    
    Returns: (found: bool, error: str or None)
    """
    if not config.GMAIL_ADDRESS or not config.GMAIL_APP_PASSWORD:
        logger.warning("Gmail ayarlari eksik, IMAP kontrolu atlanıyor.")
        return False, "Gmail ayarlari eksik"

    try:
        mail = imaplib.IMAP4_SSL(config.IMAP_HOST, config.IMAP_PORT)
        mail.login(config.GMAIL_ADDRESS, config.GMAIL_APP_PASSWORD)
        mail.select("INBOX")

        # Session ID'yi iceren e-postalari ara
        search_query = f'(SUBJECT "{session_id}")'
        status, data = mail.search(None, search_query)

        if status != "OK" or not data[0]:
            mail.logout()
            return False, None

        email_ids = data[0].split()
        logger.info("IMAP: %d e-posta bulundu (session: %s)", len(email_ids), session_id)

        for eid in reversed(email_ids):  # En yeniden eskiye
            status, msg_data = mail.fetch(eid, "(RFC822)")
            if status != "OK":
                continue

            msg = email.message_from_bytes(msg_data[0][1])

            # Sadece bize gelen reply'lari kontrol et (From != bizim adresimiz)
            from_addr = msg.get("From", "")
            if config.GMAIL_ADDRESS.lower() in from_addr.lower():
                continue  # Kendi gonderdiklerimizi atla

            # Body'yi al
            body_text = _extract_body(msg)
            if not body_text:
                continue

            # Keyword ara
            body_lower = body_text.lower().strip()
            for keyword in config.APPROVAL_KEYWORDS:
                if keyword.lower() in body_lower:
                    logger.info("IMAP: Onay reply bulundu! Keyword: '%s'", keyword)
                    mail.logout()
                    return True, None

        mail.logout()
        return False, None

    except imaplib.IMAP4.error as e:
        error = f"IMAP hatasi: {e}"
        logger.error(error)
        return False, error
    except Exception as e:
        error = f"IMAP beklenmeyen hata: {e}"
        logger.error(error)
        return False, error


def _extract_body(msg):
    """E-posta body'sini duz metin olarak cikarir."""
    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            if ctype == "text/plain":
                try:
                    return part.get_payload(decode=True).decode("utf-8", errors="replace")
                except Exception:
                    continue
    else:
        try:
            return msg.get_payload(decode=True).decode("utf-8", errors="replace")
        except Exception:
            return None
    return None
