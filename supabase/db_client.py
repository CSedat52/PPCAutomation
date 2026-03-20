"""
Supabase DB Client — Amazon PPC Automation
Tum agentlarin kullandigi paylasilan veritabani katmani.

Kullanim:
    from supabase.db_client import SupabaseClient
    db = SupabaseClient()
    db.upsert_campaigns("vigowood_na", "US", "SP", campaigns_list)
"""
import os
import json
import logging
from datetime import datetime, date
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values, Json

logger = logging.getLogger("supabase_client")

# .env yukle (proje kokunden)
_project_root = Path(__file__).parent.parent
load_dotenv(_project_root / ".env")

DB_URL = os.getenv("SUPABASE_DB_URL")


def _get_conn():
    """Yeni DB baglantisi olustur."""
    if not DB_URL:
        raise RuntimeError("SUPABASE_DB_URL .env'de tanimli degil")
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = True
    return conn


class SupabaseClient:
    """Amazon PPC Supabase client. Her islem kendi baglantisini acar/kapatir."""

    def __init__(self):
        self._test_connection()

    def _test_connection(self):
        try:
            conn = _get_conn()
            conn.close()
            logger.info("Supabase baglantisi basarili")
        except Exception as e:
            logger.warning("Supabase baglantisi basarisiz: %s", e)

    def _conn(self):
        return _get_conn()

    # ==========================================
    # YARDIMCI METODLAR
    # ==========================================

    def _upsert_batch(self, table: str, columns: list, values: list,
                      conflict_cols: list, update_cols: list = None):
        """Generic batch upsert. Entity tablolari icin."""
        if not values:
            return 0
        if update_cols is None:
            update_cols = [c for c in columns if c not in conflict_cols]

        conflict = ", ".join(conflict_cols)
        cols = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        updates = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])

        template = f"({placeholders})"
        sql = f"""
            INSERT INTO {table} ({cols})
            VALUES %s
            ON CONFLICT ({conflict})
            DO UPDATE SET {updates}
        """
        conn = self._conn()
        try:
            cur = conn.cursor()
            execute_values(cur, sql, values, template=template, page_size=500)
            count = cur.rowcount
            cur.close()
            return count
        finally:
            conn.close()

    def _insert_batch(self, table: str, columns: list, values: list):
        """Generic batch insert. Rapor tablolari icin (ON CONFLICT DO NOTHING)."""
        if not values:
            return 0
        cols = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))

        template = f"({placeholders})"
        sql = f"""
            INSERT INTO {table} ({cols})
            VALUES %s
            ON CONFLICT DO NOTHING
        """
        conn = self._conn()
        try:
            cur = conn.cursor()
            execute_values(cur, sql, values, template=template, page_size=500)
            count = cur.rowcount
            cur.close()
            return count
        finally:
            conn.close()

    def _execute(self, sql: str, params=None):
        """Tek SQL calistir."""
        conn = self._conn()
        try:
            cur = conn.cursor()
            cur.execute(sql, params)
            count = cur.rowcount
            cur.close()
            return count
        finally:
            conn.close()

    def _fetch_one(self, sql: str, params=None):
        conn = self._conn()
        try:
            cur = conn.cursor()
            cur.execute(sql, params)
            row = cur.fetchone()
            cur.close()
            return row
        finally:
            conn.close()

    def _fetch_all(self, sql: str, params=None):
        conn = self._conn()
        try:
            cur = conn.cursor()
            cur.execute(sql, params)
            rows = cur.fetchall()
            cur.close()
            return rows
        finally:
            conn.close()

    @staticmethod
    def _safe_str(val):
        """ID'leri string'e cevir (int/float olabilir)."""
        if val is None:
            return None
        return str(int(val)) if isinstance(val, float) else str(val)

    @staticmethod
    def _safe_numeric(val):
        """Numeric deger, None ise None."""
        if val is None:
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _safe_int(val):
        if val is None:
            return None
        try:
            return int(val)
        except (ValueError, TypeError):
            return None

    # ==========================================
    # AGENT 1 — ENTITY UPSERT (son durum)
    # ==========================================

    def upsert_portfolios(self, hesap_key: str, mp: str, data: list) -> int:
        cols = ["hesap_key", "marketplace", "portfolio_id", "name", "state",
                "in_budget", "budget", "collected_at"]
        conflict = ["hesap_key", "marketplace", "portfolio_id"]
        rows = []
        now = datetime.utcnow()
        for d in data:
            rows.append((
                hesap_key, mp,
                self._safe_str(d.get("portfolioId")),
                d.get("name"),
                d.get("state"),
                d.get("inBudget"),
                Json(d.get("budget")) if d.get("budget") else None,
                now
            ))
        return self._upsert_batch("portfolios", cols, rows, conflict)

    def upsert_campaigns(self, hesap_key: str, mp: str, ad_type: str, data: list) -> int:
        cols = ["hesap_key", "marketplace", "ad_type", "campaign_id", "name", "state",
                "portfolio_id", "start_date", "targeting_type", "budget", "budget_type",
                "cost_type", "dynamic_bidding", "bid_optimization",
                "bid_optimization_strategy", "goal", "tactic", "delivery_profile",
                "raw_data", "collected_at"]
        conflict = ["hesap_key", "marketplace", "ad_type", "campaign_id"]
        rows = []
        now = datetime.utcnow()
        for d in data:
            # Budget: SP icinde nested, SB/SD'de flat
            budget_val = None
            budget_type = None
            if isinstance(d.get("budget"), dict):
                budget_val = self._safe_numeric(d["budget"].get("budget"))
                budget_type = d["budget"].get("budgetType")
            else:
                budget_val = self._safe_numeric(d.get("budget"))
                budget_type = d.get("budgetType")

            rows.append((
                hesap_key, mp, ad_type,
                self._safe_str(d.get("campaignId")),
                d.get("name"),
                d.get("state"),
                self._safe_str(d.get("portfolioId")),
                d.get("startDate"),
                d.get("targetingType"),
                budget_val,
                budget_type,
                d.get("costType"),
                Json(d.get("dynamicBidding")) if d.get("dynamicBidding") else None,
                d.get("bidOptimization"),
                d.get("bidOptimizationStrategy"),
                d.get("goal"),
                d.get("tactic"),
                d.get("deliveryProfile"),
                Json(d),
                now
            ))
        return self._upsert_batch("campaigns", cols, rows, conflict)

    def upsert_ad_groups(self, hesap_key: str, mp: str, ad_type: str, data: list) -> int:
        cols = ["hesap_key", "marketplace", "ad_type", "ad_group_id", "campaign_id",
                "name", "state", "default_bid", "raw_data", "collected_at"]
        conflict = ["hesap_key", "marketplace", "ad_type", "ad_group_id"]
        rows = []
        now = datetime.utcnow()
        for d in data:
            rows.append((
                hesap_key, mp, ad_type,
                self._safe_str(d.get("adGroupId")),
                self._safe_str(d.get("campaignId")),
                d.get("name"),
                d.get("state"),
                self._safe_numeric(d.get("defaultBid")),
                Json(d),
                now
            ))
        return self._upsert_batch("ad_groups", cols, rows, conflict)

    def upsert_keywords(self, hesap_key: str, mp: str, ad_type: str, data: list) -> int:
        cols = ["hesap_key", "marketplace", "ad_type", "keyword_id", "campaign_id",
                "ad_group_id", "keyword_text", "match_type", "state", "bid",
                "raw_data", "collected_at"]
        conflict = ["hesap_key", "marketplace", "ad_type", "keyword_id"]
        rows = []
        now = datetime.utcnow()
        for d in data:
            rows.append((
                hesap_key, mp, ad_type,
                self._safe_str(d.get("keywordId")),
                self._safe_str(d.get("campaignId")),
                self._safe_str(d.get("adGroupId")),
                d.get("keywordText"),
                d.get("matchType"),
                d.get("state"),
                self._safe_numeric(d.get("bid")),
                Json(d),
                now
            ))
        return self._upsert_batch("keywords", cols, rows, conflict)

    def upsert_targets(self, hesap_key: str, mp: str, ad_type: str, data: list) -> int:
        cols = ["hesap_key", "marketplace", "ad_type", "target_id", "campaign_id",
                "ad_group_id", "bid", "state", "expression_type", "expression",
                "resolved_expression", "raw_data", "collected_at"]
        conflict = ["hesap_key", "marketplace", "ad_type", "target_id"]
        rows = []
        now = datetime.utcnow()
        for d in data:
            rows.append((
                hesap_key, mp, ad_type,
                self._safe_str(d.get("targetId")),
                self._safe_str(d.get("campaignId")),
                self._safe_str(d.get("adGroupId")),
                self._safe_numeric(d.get("bid")),
                d.get("state"),
                d.get("expressionType"),
                Json(d.get("expression")) if d.get("expression") else None,
                Json(d.get("resolvedExpression")) if d.get("resolvedExpression") else None,
                Json(d),
                now
            ))
        return self._upsert_batch("targets", cols, rows, conflict)

    def upsert_product_ads(self, hesap_key: str, mp: str, data: list) -> int:
        cols = ["hesap_key", "marketplace", "ad_id", "campaign_id", "ad_group_id",
                "asin", "sku", "state", "raw_data", "collected_at"]
        conflict = ["hesap_key", "marketplace", "ad_id"]
        rows = []
        now = datetime.utcnow()
        for d in data:
            rows.append((
                hesap_key, mp,
                self._safe_str(d.get("adId")),
                self._safe_str(d.get("campaignId")),
                self._safe_str(d.get("adGroupId")),
                d.get("asin"),
                d.get("sku"),
                d.get("state"),
                Json(d),
                now
            ))
        return self._upsert_batch("product_ads", cols, rows, conflict)

    def upsert_negative_keywords(self, hesap_key: str, mp: str, ad_type: str,
                                  data: list, scope: str = "AD_GROUP") -> int:
        cols = ["hesap_key", "marketplace", "ad_type", "keyword_id", "campaign_id",
                "ad_group_id", "keyword_text", "match_type", "state", "scope",
                "raw_data", "collected_at"]
        conflict = ["hesap_key", "marketplace", "ad_type", "keyword_id", "scope"]
        rows = []
        now = datetime.utcnow()
        for d in data:
            rows.append((
                hesap_key, mp, ad_type,
                self._safe_str(d.get("keywordId")),
                self._safe_str(d.get("campaignId")),
                self._safe_str(d.get("adGroupId")),
                d.get("keywordText"),
                d.get("matchType"),
                d.get("state"),
                scope,
                Json(d),
                now
            ))
        return self._upsert_batch("negative_keywords", cols, rows, conflict)

    def upsert_negative_targets(self, hesap_key: str, mp: str, data: list) -> int:
        cols = ["hesap_key", "marketplace", "target_id", "campaign_id", "ad_group_id",
                "expression", "state", "raw_data", "collected_at"]
        conflict = ["hesap_key", "marketplace", "target_id"]
        rows = []
        now = datetime.utcnow()
        for d in data:
            rows.append((
                hesap_key, mp,
                self._safe_str(d.get("targetId")),
                self._safe_str(d.get("campaignId")),
                self._safe_str(d.get("adGroupId")),
                Json(d.get("expression")) if d.get("expression") else None,
                d.get("state"),
                Json(d),
                now
            ))
        return self._upsert_batch("negative_targets", cols, rows, conflict)

    # ==========================================
    # AGENT 1 — RAPOR INSERT (birikmeli)
    # ==========================================

    def insert_targeting_reports(self, hesap_key: str, mp: str, ad_type: str,
                                 report_period: str, collection_date: str,
                                 data: list) -> int:
        """Targeting raporu ekle. Ayni tarih+entity icin duplicate onlenir."""
        cols = ["hesap_key", "marketplace", "ad_type", "report_period",
                "collection_date", "start_date", "end_date",
                "campaign_id", "campaign_name", "ad_group_id", "ad_group_name",
                "keyword_id", "target_id", "keyword_text", "targeting",
                "match_type", "keyword_bid", "ad_keyword_status",
                "impressions", "clicks", "cost", "cost_per_click",
                "sales", "purchases", "units_sold", "acos", "roas", "ctr",
                "new_to_brand_purchases", "new_to_brand_sales",
                "add_to_cart", "add_to_cart_clicks", "raw_data"]
        rows = []
        for d in data:
            # Sales/purchases alan adi: SP=sales14d/purchases14d, SB/SD=sales/purchases
            sales = self._safe_numeric(d.get("sales14d") or d.get("sales") or d.get("salesClicks"))
            purchases = self._safe_int(d.get("purchases14d") or d.get("purchases") or d.get("purchasesClicks"))
            units = self._safe_int(d.get("unitsSoldClicks14d") or d.get("unitsSold") or d.get("unitsSoldClicks"))
            acos_val = self._safe_numeric(d.get("acosClicks14d") or d.get("acos"))
            roas_val = self._safe_numeric(d.get("roasClicks14d") or d.get("roas"))

            rows.append((
                hesap_key, mp, ad_type, report_period,
                collection_date,
                d.get("startDate"),
                d.get("endDate"),
                self._safe_str(d.get("campaignId")),
                d.get("campaignName"),
                self._safe_str(d.get("adGroupId")),
                d.get("adGroupName"),
                self._safe_str(d.get("keywordId")),
                self._safe_str(d.get("targetId")),
                d.get("keyword") or d.get("keywordText"),
                d.get("targeting") or d.get("targetingExpression"),
                d.get("matchType") or d.get("keywordType") or d.get("targetingType"),
                self._safe_numeric(d.get("keywordBid") or d.get("bid")),
                d.get("adKeywordStatus"),
                self._safe_int(d.get("impressions")),
                self._safe_int(d.get("clicks")),
                self._safe_numeric(d.get("cost")),
                self._safe_numeric(d.get("costPerClick")),
                sales,
                purchases,
                units,
                acos_val,
                roas_val,
                self._safe_numeric(d.get("clickThroughRate") or d.get("viewClickThroughRate")),
                self._safe_int(d.get("newToBrandPurchases")),
                self._safe_numeric(d.get("newToBrandSales")),
                self._safe_int(d.get("addToCart")),
                self._safe_int(d.get("addToCartClicks")),
                Json(d)
            ))
        return self._insert_batch("targeting_reports", cols, rows)

    def insert_search_term_reports(self, hesap_key: str, mp: str, ad_type: str,
                                    collection_date: str, data: list) -> int:
        cols = ["hesap_key", "marketplace", "ad_type", "collection_date",
                "campaign_id", "campaign_name", "ad_group_id", "ad_group_name",
                "keyword_id", "keyword_text", "search_term", "targeting",
                "match_type",
                "impressions", "clicks", "cost", "cost_per_click",
                "sales", "purchases", "units_sold", "acos", "roas", "ctr",
                "raw_data"]
        rows = []
        for d in data:
            sales = self._safe_numeric(d.get("sales14d") or d.get("sales") or d.get("salesClicks"))
            purchases = self._safe_int(d.get("purchases14d") or d.get("purchases") or d.get("purchasesClicks"))
            units = self._safe_int(d.get("unitsSoldClicks14d") or d.get("unitsSold") or d.get("unitsSoldClicks"))

            rows.append((
                hesap_key, mp, ad_type, collection_date,
                self._safe_str(d.get("campaignId")),
                d.get("campaignName"),
                self._safe_str(d.get("adGroupId")),
                d.get("adGroupName"),
                self._safe_str(d.get("keywordId")),
                d.get("keywordText") or d.get("keyword"),
                d.get("searchTerm"),
                d.get("targeting"),
                d.get("matchType"),
                self._safe_int(d.get("impressions")),
                self._safe_int(d.get("clicks")),
                self._safe_numeric(d.get("cost")),
                self._safe_numeric(d.get("costPerClick")),
                sales,
                purchases,
                units,
                self._safe_numeric(d.get("acosClicks14d") or d.get("acos")),
                self._safe_numeric(d.get("roasClicks14d") or d.get("roas")),
                self._safe_numeric(d.get("clickThroughRate")),
                Json(d)
            ))
        return self._insert_batch("search_term_reports", cols, rows)

    def insert_campaign_reports(self, hesap_key: str, mp: str, ad_type: str,
                                 report_period: str, collection_date: str,
                                 data: list) -> int:
        """Campaign raporu ekle. timeUnit=DAILY ise her satirdaki 'date' alani report_date olur."""
        cols = ["hesap_key", "marketplace", "ad_type", "report_period",
                "collection_date", "report_date", "start_date", "end_date",
                "campaign_id", "campaign_name", "campaign_status",
                "impressions", "clicks", "cost", "sales", "purchases",
                "units_sold", "acos", "roas", "raw_data"]
        rows = []
        for d in data:
            sales = self._safe_numeric(d.get("sales14d") or d.get("sales") or d.get("salesClicks"))
            purchases = self._safe_int(d.get("purchases14d") or d.get("purchases") or d.get("purchasesClicks"))

            rows.append((
                hesap_key, mp, ad_type, report_period, collection_date,
                d.get("date"),              # DAILY raporlarda gunluk tarih, SUMMARY'de None
                d.get("startDate"), d.get("endDate"),
                self._safe_str(d.get("campaignId")),
                d.get("campaignName"),
                d.get("campaignStatus"),
                self._safe_int(d.get("impressions")),
                self._safe_int(d.get("clicks")),
                self._safe_numeric(d.get("cost")),
                sales,
                purchases,
                self._safe_int(d.get("unitsSoldClicks14d") or d.get("unitsSold")),
                self._safe_numeric(d.get("acosClicks14d") or d.get("acos")),
                self._safe_numeric(d.get("roasClicks14d") or d.get("roas")),
                Json(d)
            ))
        return self._insert_batch("campaign_reports", cols, rows)

    # ==========================================
    # AGENT 2 — ANALIZ CIKTILARI
    # ==========================================

    def insert_bid_recommendations(self, hesap_key: str, mp: str, analysis_date: str,
                                    data: list) -> int:
        cols = ["hesap_key", "marketplace", "analysis_date", "ad_type",
                "campaign_id", "campaign_name", "ad_group_id", "ad_group_name",
                "keyword_id", "target_id", "keyword_text", "targeting",
                "match_type", "segment",
                "current_bid", "recommended_bid", "bid_change_pct",
                "impressions", "clicks", "cost", "sales", "orders",
                "acos", "cvr", "cpc",
                "portfolio", "reason",
                "decision"]
        rows = []
        for d in data:
            rows.append((
                hesap_key, mp, analysis_date,
                d.get("reklam_tipi", "SP"),
                self._safe_str(d.get("campaign_id")),
                d.get("kampanya"),
                self._safe_str(d.get("ad_group_id")),
                d.get("ad_group_name"),
                self._safe_str(d.get("keyword_id")),
                self._safe_str(d.get("target_id")),
                d.get("hedefleme"),
                d.get("targeting"),
                d.get("match_type"),
                d.get("segment"),
                self._safe_numeric(d.get("bid") or d.get("onceki_bid")),
                self._safe_numeric(d.get("tavsiye_bid") or d.get("yeni_bid")),
                self._safe_numeric(d.get("degisim_yuzde")),
                self._safe_int(d.get("impressions")),
                self._safe_int(d.get("clicks")),
                self._safe_numeric(d.get("spend") or d.get("cost")),
                self._safe_numeric(d.get("sales")),
                self._safe_int(d.get("orders")),
                self._safe_numeric(d.get("acos")),
                self._safe_numeric(d.get("cvr")),
                self._safe_numeric(d.get("cpc")),
                d.get("portfolio"),
                d.get("reason") or d.get("sebep"),
                d.get("karar_durumu", "PENDING")
            ))
        return self._insert_batch("bid_recommendations", cols, rows)

    def insert_negative_candidates(self, hesap_key: str, mp: str, analysis_date: str,
                                    data: list) -> int:
        cols = ["hesap_key", "marketplace", "analysis_date", "ad_type",
                "campaign_name", "portfolio", "search_term", "match_type",
                "source", "reason",
                "impressions", "clicks", "cost", "sales",
                "cvr", "cpc",
                "decision"]
        rows = []
        for d in data:
            rows.append((
                hesap_key, mp, analysis_date,
                d.get("reklam_tipi", "SP"),
                d.get("kampanya"),
                d.get("portfolio"),
                d.get("hedefleme") or d.get("search_term"),
                d.get("match_type"),
                d.get("tip", "KEYWORD"),
                d.get("sebep") or d.get("reason"),
                self._safe_int(d.get("impressions")),
                self._safe_int(d.get("clicks")),
                self._safe_numeric(d.get("spend") or d.get("cost")),
                self._safe_numeric(d.get("sales")),
                self._safe_numeric(d.get("cvr")),
                self._safe_numeric(d.get("cpc")),
                d.get("karar_durumu", "PENDING")
            ))
        return self._insert_batch("negative_candidates", cols, rows)

    def insert_harvesting_candidates(self, hesap_key: str, mp: str, analysis_date: str,
                                      data: list) -> int:
        cols = ["hesap_key", "marketplace", "analysis_date", "ad_type",
                "source_campaign_id", "source_campaign_name", "source_ad_group_id",
                "search_term", "targeting", "harvest_type",
                "suggested_match_type", "suggested_bid",
                "portfolio", "cvr", "recommendation",
                "impressions", "clicks", "cost", "sales", "orders",
                "acos", "decision"]
        rows = []
        for d in data:
            rows.append((
                hesap_key, mp, analysis_date,
                d.get("reklam_tipi", "SP"),
                self._safe_str(d.get("campaign_id")),
                d.get("kaynak_kampanya"),
                self._safe_str(d.get("ad_group_id")),
                d.get("search_term"),
                d.get("hedefleme"),
                d.get("tip", "KEYWORD"),
                d.get("match_type"),
                self._safe_numeric(d.get("suggested_bid")),
                d.get("portfolio"),
                self._safe_numeric(d.get("cvr")),
                d.get("recommendation"),
                self._safe_int(d.get("impressions")),
                self._safe_int(d.get("clicks")),
                self._safe_numeric(d.get("spend") or d.get("cost")),
                self._safe_numeric(d.get("sales")),
                self._safe_int(d.get("orders")),
                self._safe_numeric(d.get("acos")),
                d.get("karar_durumu", "PENDING")
            ))
        return self._insert_batch("harvesting_candidates", cols, rows)

    # ==========================================
    # AGENT 3 — EXECUTION
    # ==========================================

    def insert_execution_plan(self, hesap_key: str, mp: str, plan_date: str,
                               mode: str, summary: dict,
                               session_id: str = None) -> str:
        """Execution plan olustur, UUID dondur."""
        sql = """
            INSERT INTO execution_plans
                (hesap_key, marketplace, plan_date, session_id, mode, status,
                 bid_total, bid_success, bid_error,
                 negative_total, negative_success, negative_error,
                 harvesting_total, harvesting_success, harvesting_error,
                 warnings)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """
        bid = summary.get("bid_degisiklikleri", {})
        neg = summary.get("negatif_eklemeler", {})
        harv = summary.get("harvesting", {})

        conn = self._conn()
        try:
            cur = conn.cursor()
            cur.execute(sql, (
                hesap_key, mp, plan_date, session_id, mode, "PENDING",
                bid.get("toplam", 0), bid.get("basarili", 0), bid.get("hata", 0),
                neg.get("toplam", 0), neg.get("basarili", 0), neg.get("hata", 0),
                harv.get("toplam", 0), harv.get("basarili", 0), harv.get("hata", 0),
                Json(summary.get("uyarilar", []))
            ))
            plan_id = cur.fetchone()[0]
            cur.close()
            return str(plan_id)
        finally:
            conn.close()

    def insert_execution_items(self, plan_id: str, hesap_key: str, mp: str,
                                items: list) -> int:
        cols = ["plan_id", "hesap_key", "marketplace", "item_type",
                "campaign_id", "campaign_name", "ad_group_id",
                "keyword_id", "target_id", "targeting",
                "old_bid", "new_bid", "bid_change_pct",
                "negative_type", "match_type",
                "harvest_type", "source_campaign",
                "api_endpoint", "api_payload", "status", "error_message"]
        rows = []
        for d in items:
            rows.append((
                plan_id, hesap_key, mp,
                d.get("item_type", "BID_CHANGE"),
                self._safe_str(d.get("campaign_id")),
                d.get("kampanya"),
                self._safe_str(d.get("ad_group_id")),
                self._safe_str(d.get("keyword_id")),
                self._safe_str(d.get("target_id")),
                d.get("hedefleme"),
                self._safe_numeric(d.get("eski_bid")),
                self._safe_numeric(d.get("yeni_bid")),
                self._safe_numeric(d.get("degisim_pct")),
                d.get("negative_type"),
                d.get("match_type"),
                d.get("harvest_type"),
                d.get("kaynak_kampanya"),
                d.get("api_endpoint"),
                Json(d.get("api_payload")) if d.get("api_payload") else None,
                d.get("status", "PENDING"),
                d.get("error_message")
            ))
        return self._insert_batch("execution_items", cols, rows)

    def update_execution_plan_status(self, plan_id: str, status: str):
        self._execute(
            "UPDATE execution_plans SET status = %s, completed_at = NOW() WHERE id = %s",
            (status, plan_id)
        )

    def insert_verification_result(self, plan_id: str, hesap_key: str, mp: str,
                                    verify_date: str, result: dict) -> int:
        sql = """
            INSERT INTO verification_results
                (plan_id, hesap_key, marketplace, verify_date,
                 total_checked, matched, mismatched, not_found, details)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        return self._execute(sql, (
            plan_id, hesap_key, mp, verify_date,
            result.get("toplam", 0),
            result.get("eslesme", 0),
            result.get("uyumsuz", 0),
            result.get("bulunamadi", 0),
            Json(result.get("detaylar", []))
        ))

    # ==========================================
    # AGENT 4 — OPTIMIZER
    # ==========================================

    def upsert_decision_history(self, hesap_key: str, mp: str, decisions: list) -> int:
        """Karar gecmisini kaydet/guncelle."""
        cols = ["hesap_key", "marketplace", "decision_date", "targeting_id",
                "ad_type", "targeting", "campaign_name", "portfolio_id", "asin",
                "segment", "previous_bid", "new_bid", "change_pct",
                "metrics", "decision_status"]
        rows = []
        for d in decisions:
            rows.append((
                hesap_key, mp,
                d.get("tarih"),
                d.get("hedefleme_id"),
                d.get("reklam_tipi"),
                d.get("hedefleme"),
                d.get("kampanya"),
                d.get("portfolio_id"),
                d.get("asin"),
                d.get("segment"),
                self._safe_numeric(d.get("onceki_bid")),
                self._safe_numeric(d.get("yeni_bid")),
                self._safe_numeric(d.get("degisim_yuzde")),
                Json(d.get("metrikler")) if d.get("metrikler") else None,
                d.get("karar_durumu", "PENDING")
            ))
        return self._insert_batch("decision_history", cols, rows)

    def update_decision_kpi(self, hesap_key: str, mp: str,
                             targeting_id: str, decision_date: str,
                             kpi_after: dict):
        """Karar sonrasi KPI guncelle."""
        self._execute("""
            UPDATE decision_history
            SET kpi_after = %s, kpi_collected_at = NOW()
            WHERE hesap_key = %s AND marketplace = %s
              AND targeting_id = %s AND decision_date = %s
        """, (Json(kpi_after), hesap_key, mp, targeting_id, decision_date))

    def upsert_asin_profiles(self, hesap_key: str, mp: str, profiles: dict):
        """ASIN profillerini guncelle."""
        for asin, profile in profiles.items():
            self._execute("""
                INSERT INTO asin_profiles (hesap_key, marketplace, asin, product_name, profile_data)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (hesap_key, marketplace, asin)
                DO UPDATE SET profile_data = EXCLUDED.profile_data, updated_at = NOW()
            """, (hesap_key, mp, asin, profile.get("urun_adi"), Json(profile)))

    def upsert_segment_stats(self, hesap_key: str, mp: str, segments: dict):
        for segment, stats in segments.items():
            self._execute("""
                INSERT INTO segment_stats (hesap_key, marketplace, segment, stats_data)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (hesap_key, marketplace, segment)
                DO UPDATE SET stats_data = EXCLUDED.stats_data, updated_at = NOW()
            """, (hesap_key, mp, segment, Json(stats)))

    def insert_anomaly(self, hesap_key: str, mp: str, anomaly: dict):
        self._execute("""
            INSERT INTO anomalies (hesap_key, marketplace, anomaly_type, severity,
                                   description, anomaly_data)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (hesap_key, mp, anomaly.get("tip"), anomaly.get("siddet"),
              anomaly.get("aciklama"), Json(anomaly)))

    def insert_pattern(self, hesap_key: str, mp: str, pattern: dict):
        self._execute("""
            INSERT INTO patterns (hesap_key, marketplace, pattern_type,
                                  description, pattern_data)
            VALUES (%s, %s, %s, %s, %s)
        """, (hesap_key, mp, pattern.get("tip"), pattern.get("aciklama"), Json(pattern)))

    def upsert_proposal(self, hesap_key: str, mp: str, proposal: dict):
        # beklenen_sonuc/gerceklesen_sonuc (yeni) veya risk/kazanim (eski) fallback
        beklenen = proposal.get("beklenen_sonuc") or proposal.get("risk", "")
        gerceklesen = proposal.get("gerceklesen_sonuc") or proposal.get("kazanim", "")
        self._execute("""
            INSERT INTO proposals (hesap_key, marketplace, proposal_id, proposal_type,
                                   title, description, current_value, proposed_value,
                                   rationale, impact_estimate,
                                   beklenen_sonuc, gerceklesen_sonuc,
                                   status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (hesap_key, marketplace, proposal_id)
            DO UPDATE SET status = EXCLUDED.status,
                          proposed_value = EXCLUDED.proposed_value,
                          rationale = EXCLUDED.rationale,
                          beklenen_sonuc = EXCLUDED.beklenen_sonuc,
                          gerceklesen_sonuc = EXCLUDED.gerceklesen_sonuc
        """, (
            hesap_key, mp,
            proposal.get("id"),
            proposal.get("kategori"),
            proposal.get("baslik"),
            proposal.get("aciklama"),
            Json(proposal.get("mevcut_deger")) if proposal.get("mevcut_deger") else None,
            Json(proposal.get("onerilen_deger")) if proposal.get("onerilen_deger") else None,
            proposal.get("gerekce"),
            Json(proposal.get("etki_tahmini")) if proposal.get("etki_tahmini") else None,
            beklenen,
            gerceklesen,
            proposal.get("durum", "PENDING")
        ))

    def update_proposal_status(self, hesap_key: str, mp: str,
                                proposal_id: str, status: str,
                                reason: str = None):
        self._execute("""
            UPDATE proposals
            SET status = %s, decided_at = NOW(), rejection_reason = %s
            WHERE hesap_key = %s AND marketplace = %s AND proposal_id = %s
        """, (status, reason, hesap_key, mp, proposal_id))

    def insert_status_report(self, hesap_key: str, mp: str, report: dict):
        self._execute("""
            INSERT INTO status_reports
                (hesap_key, marketplace, report_date, health_score, health_status,
                 pipeline_summary, error_summary, kpi_summary,
                 anomaly_summary, segment_health, pending_proposals, report_text)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (hesap_key, marketplace, report_date)
            DO UPDATE SET health_score = EXCLUDED.health_score,
                          health_status = EXCLUDED.health_status,
                          pipeline_summary = EXCLUDED.pipeline_summary,
                          error_summary = EXCLUDED.error_summary,
                          kpi_summary = EXCLUDED.kpi_summary,
                          anomaly_summary = EXCLUDED.anomaly_summary,
                          segment_health = EXCLUDED.segment_health,
                          pending_proposals = EXCLUDED.pending_proposals,
                          report_text = EXCLUDED.report_text,
                          created_at = NOW()
        """, (
            hesap_key, mp,
            report.get("tarih"),
            report.get("sistem_sagligi", {}).get("saglik_skoru"),
            report.get("sistem_sagligi", {}).get("saglik_durumu"),
            Json(report.get("sistem_sagligi", {}).get("pipeline")),
            Json(report.get("sistem_sagligi", {}).get("hata_ozeti")),
            Json(report.get("sistem_sagligi", {}).get("kpi_ozeti")),
            Json(report.get("sistem_sagligi", {}).get("anomaliler")),
            Json(report.get("sistem_sagligi", {}).get("segment_sagligi")),
            report.get("onay_bekliyor", {}).get("bekleyen_oneri_sayisi", 0),
            report.get("ozet")
        ))

    # ==========================================
    # HATA LOGLARI
    # ==========================================

    def insert_error_log(self, hesap_key: str, mp: str, agent: str,
                          error: dict):
        self._execute("""
            INSERT INTO error_logs
                (hesap_key, marketplace, agent, session_id, timestamp,
                 error_type, error_message, step, extra, traceback)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            hesap_key, mp, agent,
            error.get("session_id"),
            error.get("timestamp", datetime.utcnow().isoformat()),
            error.get("hata_tipi"),
            error.get("hata_mesaji"),
            error.get("adim"),
            Json(error.get("extra")) if error.get("extra") else None,
            error.get("traceback")
        ))

    def insert_maestro_error(self, hesap_key: str, mp: str, error: dict):
        self._execute("""
            INSERT INTO maestro_errors
                (hesap_key, marketplace, session_id, timestamp,
                 error_type, error_message, step, agent, extra, traceback)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            hesap_key, mp,
            error.get("session_id"),
            error.get("timestamp", datetime.utcnow().isoformat()),
            error.get("hata_tipi"),
            error.get("hata_mesaji"),
            error.get("adim"),
            error.get("agent"),
            Json(error.get("extra")) if error.get("extra") else None,
            error.get("traceback")
        ))

    # ==========================================
    # AGENT LOGS (yeni — Faz 3)
    # ==========================================

    def insert_agent_log(self, hesap_key: str, mp: str, agent_name: str,
                         log_dict: dict):
        """agent_logs tablosuna structured log yazar."""
        self._execute("""
            INSERT INTO agent_logs
                (agent_id, level, message, error_type,
                 hesap_key, marketplace, session_id, traceback)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            agent_name,
            log_dict.get("level", "info"),
            log_dict.get("message"),
            log_dict.get("error_type"),
            hesap_key, mp,
            log_dict.get("session_id"),
            log_dict.get("traceback"),
        ))

    # ==========================================
    # PIPELINE RUNS (yeni — Faz 3)
    # ==========================================

    def upsert_pipeline_run(self, session_id: str, hesap_key: str, mp: str,
                            step: str, status: str, error_msg: str = None):
        """pipeline_runs tablosuna INSERT ON CONFLICT ile yazar.
        step: 'starting', 'agent1', 'agent2', 'agent3_execute', 'agent3_verify', 'agent4', 'completed'
        status: 'running', 'completed', 'failed'
        """
        # Step'e gore completed_at kolonu
        step_col_map = {
            "agent1": "agent1_completed_at",
            "agent2": "agent2_completed_at",
            "agent3_execute": "agent3_completed_at",
            "agent3_verify": "agent3_completed_at",
            "agent4": "agent4_completed_at",
        }

        conn = self._conn()
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO pipeline_runs (session_id, hesap_key, marketplace,
                                           current_step, status, error_message, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (session_id, hesap_key, marketplace)
                DO UPDATE SET current_step = EXCLUDED.current_step,
                              status = EXCLUDED.status,
                              error_message = EXCLUDED.error_message,
                              updated_at = NOW()
            """, (session_id, hesap_key, mp, step, status, error_msg))

            # Step tamamlandiysa ilgili timestamp kolonu guncelle
            completed_col = step_col_map.get(step)
            if completed_col and status == "completed":
                cur.execute(f"""
                    UPDATE pipeline_runs SET {completed_col} = NOW()
                    WHERE session_id = %s AND hesap_key = %s AND marketplace = %s
                """, (session_id, hesap_key, mp))

            cur.close()
        finally:
            conn.close()

    # ==========================================
    # AGENT STATUS (yeni — Faz 3)
    # ==========================================

    def update_agent_status_detail(self, agent_name: str, status: str,
                                   health_detail: dict = None):
        """agent_status tablosunu gunceller."""
        health_score = None
        if health_detail:
            errors = health_detail.get("errors_7d", 0)
            health_score = max(0, 100 - errors * 5)

        self._execute("""
            UPDATE agent_status
            SET status = %s, last_run_at = NOW(),
                health_detail = %s, health_score = %s
            WHERE agent_name = %s
        """, (
            status,
            Json(health_detail) if health_detail else None,
            health_score,
            agent_name,
        ))

    # ==========================================
    # CONFIG TABLOLARI
    # ==========================================

    def upsert_settings(self, hesap_key: str, mp: str, settings: dict):
        self._execute("""
            INSERT INTO settings (hesap_key, marketplace, genel_ayarlar, esik_degerleri,
                                  asin_hedefleri, segmentasyon_kurallari, agent3_ayarlari)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (hesap_key, marketplace)
            DO UPDATE SET genel_ayarlar = EXCLUDED.genel_ayarlar,
                          esik_degerleri = EXCLUDED.esik_degerleri,
                          asin_hedefleri = EXCLUDED.asin_hedefleri,
                          segmentasyon_kurallari = EXCLUDED.segmentasyon_kurallari,
                          agent3_ayarlari = EXCLUDED.agent3_ayarlari
        """, (
            hesap_key, mp,
            Json(settings.get("genel_ayarlar", {})),
            Json(settings.get("esik_degerleri", {})),
            Json(settings.get("asin_hedefleri", {})),
            Json(settings.get("segmentasyon_kurallari", {})),
            Json(settings.get("agent3_ayarlari", {}))
        ))

    def upsert_bid_functions(self, hesap_key: str, mp: str, bid_funcs: dict):
        self._execute("""
            INSERT INTO bid_functions (hesap_key, marketplace, tanh_formulu,
                                       segment_parametreleri, genel_limitler,
                                       asin_parametreleri, ogrenme_gecmisi)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (hesap_key, marketplace)
            DO UPDATE SET tanh_formulu = EXCLUDED.tanh_formulu,
                          segment_parametreleri = EXCLUDED.segment_parametreleri,
                          genel_limitler = EXCLUDED.genel_limitler,
                          asin_parametreleri = EXCLUDED.asin_parametreleri,
                          ogrenme_gecmisi = EXCLUDED.ogrenme_gecmisi
        """, (
            hesap_key, mp,
            Json(bid_funcs.get("tanh_formulu", {})),
            Json(bid_funcs.get("segment_parametreleri", {})),
            Json(bid_funcs.get("genel_limitler", {})),
            Json(bid_funcs.get("asin_parametreleri", {})),
            Json(bid_funcs.get("ogrenme_gecmisi", {"degisiklikler": []}))
        ))

    # ==========================================
    # PIPELINE
    # ==========================================

    def start_pipeline_run(self, session_id: str, hesap_key: str, mp: str,
                            pipeline_date: str) -> str:
        conn = self._conn()
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO pipeline_runs (session_id, hesap_key, marketplace, pipeline_date)
                VALUES (%s, %s, %s, %s)
                RETURNING id
            """, (session_id, hesap_key, mp, pipeline_date))
            run_id = cur.fetchone()[0]
            cur.close()
            return str(run_id)
        finally:
            conn.close()

    def update_pipeline_run(self, run_id: str, **kwargs):
        sets = []
        vals = []
        for k, v in kwargs.items():
            if k == "summary":
                sets.append(f"{k} = %s")
                vals.append(Json(v))
            else:
                sets.append(f"{k} = %s")
                vals.append(v)
        if "completed_at" not in kwargs and kwargs.get("status") in ("COMPLETED", "FAILED"):
            sets.append("completed_at = NOW()")
        vals.append(run_id)
        self._execute(
            f"UPDATE pipeline_runs SET {', '.join(sets)} WHERE id = %s",
            tuple(vals)
        )

    # ==========================================
    # KPI DAILY — 13 KAMPANYA TIPI DESTEKLI
    # ==========================================

    def upsert_kpi_daily(self, hesap_key, mp, date_str=None):
        from collections import Counter
        if not date_str:
            date_str = datetime.utcnow().strftime("%Y-%m-%d")
        data_dir = _project_root / "data" / f"{hesap_key}_{mp}"
        if not data_dir.exists():
            logger.error("KPI: data klasoru yok: %s", data_dir)
            return 0

        def _load(fn):
            fp = data_dir / fn
            if not fp.exists():
                return []
            try:
                with open(fp, "r", encoding="utf-8") as f:
                    d = json.load(f)
                return d if isinstance(d, list) else []
            except Exception:
                return []

        logger.info("KPI SYNC: %s/%s (%s)", hesap_key, mp, date_str)
        sp_map = self._classify_sp(date_str, _load)
        sb_map = self._classify_sb(date_str, _load)
        sd_map = self._classify_sd(date_str, _load)

        # Portfolio eslestirme
        pf_names = {str(p.get("portfolioId", "")): p["name"]
                    for p in _load(f"{date_str}_portfolios.json") if p.get("name")}
        pf_map = {}
        for pre in ("sp", "sb", "sd"):
            for c in _load(f"{date_str}_{pre}_campaigns.json"):
                cid = str(c.get("campaignId", ""))
                pid = str(c.get("portfolioId", ""))
                if cid and pid:
                    pf_map[cid] = (pid, pf_names.get(pid))

        rows = []
        for r in _load(f"{date_str}_sp_campaign_report_14d.json"):
            cid = str(r.get("campaignId", ""))
            pid, pn = pf_map.get(cid, (None, None))
            rows.append((
                r.get("date"), hesap_key, mp,
                sp_map.get(cid, "SP-Other"), pid, pn,
                self._safe_numeric(r.get("cost")),
                self._safe_numeric(r.get("sales14d")),
                self._safe_int(r.get("clicks")),
                self._safe_int(r.get("purchases14d")),
                self._safe_int(r.get("impressions")),
                self._safe_int(r.get("unitsSoldClicks14d")),
            ))

        for r in _load(f"{date_str}_sb_campaign_report_14d.json"):
            cid = str(r.get("campaignId", ""))
            pid, pn = pf_map.get(cid, (None, None))
            rows.append((
                r.get("date"), hesap_key, mp,
                sb_map.get(cid, "SB-Other"), pid, pn,
                self._safe_numeric(r.get("cost")),
                self._safe_numeric(r.get("sales")),
                self._safe_int(r.get("clicks")),
                self._safe_int(r.get("purchases") or r.get("purchasesClicks")),
                self._safe_int(r.get("impressions")),
                self._safe_int(r.get("unitsSold")),
            ))

        for r in _load(f"{date_str}_sd_campaign_report_14d.json"):
            cid = str(r.get("campaignId", ""))
            pid, pn = pf_map.get(cid, (None, None))
            rows.append((
                r.get("date"), hesap_key, mp,
                sd_map.get(cid, "SD-Other"), pid, pn,
                self._safe_numeric(r.get("cost")),
                self._safe_numeric(r.get("sales")),
                self._safe_int(r.get("clicks")),
                self._safe_int(r.get("purchases") or r.get("purchasesClicks")),
                self._safe_int(r.get("impressions")),
                self._safe_int(r.get("unitsSold")),
            ))

        if not rows:
            logger.warning("KPI: veri yok")
            return 0

        agg = {}
        for (rd, hk, m, ct, pid, pn, sp, sa, cl, od, im, un) in rows:
            key = (rd, hk, m, ct, pid)
            if key not in agg:
                agg[key] = [rd, hk, m, ct, pid, pn, 0.0, 0.0, 0, 0, 0, 0]
            a = agg[key]
            a[6] += (sp or 0)
            a[7] += (sa or 0)
            a[8] += (cl or 0)
            a[9] += (od or 0)
            a[10] += (im or 0)
            a[11] += (un or 0)
            if not a[5] and pn:
                a[5] = pn

        agg_rows = []
        for a in agg.values():
            a[6] = round(a[6], 2)
            a[7] = round(a[7], 2)
            agg_rows.append(tuple(a))

        logger.info("KPI: %d ham -> %d aggregate", len(rows), len(agg_rows))

        conn = self._conn()
        try:
            cur = conn.cursor()
            sql = """
                INSERT INTO kpi_daily (
                    report_date, hesap_key, marketplace, campaign_type,
                    portfolio_id, portfolio_name,
                    spend, sales, clicks, orders, impressions, units_sold, updated_at
                ) VALUES %s
                ON CONFLICT (report_date, hesap_key, marketplace, campaign_type, portfolio_id)
                DO UPDATE SET
                    portfolio_name = EXCLUDED.portfolio_name,
                    spend = EXCLUDED.spend, sales = EXCLUDED.sales,
                    clicks = EXCLUDED.clicks, orders = EXCLUDED.orders,
                    impressions = EXCLUDED.impressions, units_sold = EXCLUDED.units_sold,
                    updated_at = NOW()
            """
            execute_values(cur, sql, agg_rows,
                           template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())",
                           page_size=500)
            count = cur.rowcount
            cur.close()
            logger.info("KPI: %d satir upsert (%s/%s)", count, hesap_key, mp)
            return count
        except Exception as e:
            logger.error("KPI upsert hatasi: %s", e)
            return 0
        finally:
            conn.close()

    def _classify_sp(self, date_str, _load):
        from collections import Counter
        SP_ASIN = {"ASIN_SAME_AS", "ASIN_EXPANDED_FROM"}
        SP_CAT = {"ASIN_CATEGORY_SAME_AS"}
        SP_AUTO = {"QUERY_HIGH_REL_MATCHES", "QUERY_BROAD_REL_MATCHES",
                   "ASIN_ACCESSORY_RELATED", "ASIN_SUBSTITUTE_RELATED"}
        camp_tt = {str(c["campaignId"]): c.get("targetingType", "MANUAL")
                   for c in _load(f"{date_str}_sp_campaigns.json") if c.get("campaignId")}
        camp_kw = {}
        for k in _load(f"{date_str}_sp_keywords.json"):
            cid = str(k.get("campaignId", ""))
            mt = k.get("matchType", "")
            if cid and mt:
                camp_kw.setdefault(cid, Counter())[mt] += 1
        camp_tgt = {}
        for t in _load(f"{date_str}_sp_targets.json"):
            cid = str(t.get("campaignId", ""))
            for e in (t.get("expression") or []):
                if not isinstance(e, dict):
                    continue
                et = e.get("type", "")
                if et in SP_ASIN:
                    camp_tgt.setdefault(cid, set()).add("ASIN")
                elif et in SP_CAT:
                    camp_tgt.setdefault(cid, set()).add("CATEGORY")
                elif et in SP_AUTO:
                    camp_tgt.setdefault(cid, set()).add("AUTO")
        result = {}
        for cid, tt in camp_tt.items():
            if tt == "AUTO":
                result[cid] = "SP-Auto"
                continue
            kw = camp_kw.get(cid)
            tg = camp_tgt.get(cid, set())
            has_kw = kw and sum(kw.values()) > 0
            if has_kw and "ASIN" not in tg and "CATEGORY" not in tg:
                result[cid] = f"SP-{kw.most_common(1)[0][0].capitalize()}"
            elif "ASIN" in tg and not has_kw and "CATEGORY" not in tg:
                result[cid] = "SP-ASIN"
            elif "CATEGORY" in tg and not has_kw and "ASIN" not in tg:
                result[cid] = "SP-Category"
            elif "ASIN" in tg and "CATEGORY" in tg:
                result[cid] = "SP-ASIN"
            elif has_kw:
                result[cid] = f"SP-{kw.most_common(1)[0][0].capitalize()}"
            elif "AUTO" in tg:
                result[cid] = "SP-Auto"
            else:
                result[cid] = "SP-Other"
        return result

    def _classify_sb(self, date_str, _load):
        kw_cids = {str(k["campaignId"]) for k in _load(f"{date_str}_sb_keywords.json")
                   if k.get("campaignId")}
        camp_tgt = {}
        for t in _load(f"{date_str}_sb_targets.json"):
            cid = str(t.get("campaignId", ""))
            for e in (t.get("expressions") or t.get("expression") or []):
                if not isinstance(e, dict):
                    continue
                et = e.get("type", "").lower()
                if "asinsameas" in et:
                    camp_tgt.setdefault(cid, set()).add("ASIN")
                elif "asincategorysameas" in et:
                    camp_tgt.setdefault(cid, set()).add("CATEGORY")
        all_cids = {str(c["campaignId"]) for c in _load(f"{date_str}_sb_campaigns.json")
                    if c.get("campaignId")}
        all_cids.update(kw_cids)
        all_cids.update(camp_tgt.keys())
        result = {}
        for cid in all_cids:
            tg = camp_tgt.get(cid, set())
            if cid in kw_cids:
                result[cid] = "SB-Keyword"
            elif "ASIN" in tg:
                result[cid] = "SB-ASIN"
            elif "CATEGORY" in tg:
                result[cid] = "SB-Category"
            else:
                result[cid] = "SB-Other"
        return result

    def _classify_sd(self, date_str, _load):
        all_cids = {str(c["campaignId"]) for c in _load(f"{date_str}_sd_campaigns.json")
                    if c.get("campaignId")}
        camp_types = {}
        for t in _load(f"{date_str}_sd_targets.json"):
            cid = str(t.get("campaignId", ""))
            all_cids.add(cid)
            exprs = t.get("expression") or []
            if not exprs or not isinstance(exprs[0], dict):
                continue
            main = exprs[0].get("type", "")
            vals = exprs[0].get("value", [])
            subs = set()
            if isinstance(vals, list):
                for v in vals:
                    if isinstance(v, dict):
                        subs.add(v.get("type", ""))
            if main == "audience":
                camp_types.setdefault(cid, set()).add("AUDIENCE")
            elif main == "similarProduct":
                camp_types.setdefault(cid, set()).add("PRODUCT")
            elif main == "asinCategorySameAs":
                camp_types.setdefault(cid, set()).add("CONTEXTUAL")
            elif main in ("purchases", "views"):
                if "asinCategorySameAs" in subs:
                    camp_types.setdefault(cid, set()).add("CONTEXTUAL")
                elif "exactProduct" in subs or "relatedProduct" in subs:
                    camp_types.setdefault(cid, set()).add("RETARGETING")
                elif "similarProduct" in subs:
                    camp_types.setdefault(cid, set()).add("PRODUCT")
                else:
                    camp_types.setdefault(cid, set()).add("RETARGETING")
        result = {}
        for cid in all_cids:
            tg = camp_types.get(cid, set())
            if "AUDIENCE" in tg:
                result[cid] = "SD-Audience"
            elif "RETARGETING" in tg:
                result[cid] = "SD-Retargeting"
            elif "CONTEXTUAL" in tg:
                result[cid] = "SD-Contextual"
            elif "PRODUCT" in tg:
                result[cid] = "SD-Product"
            else:
                result[cid] = "SD-Other"
        return result

    def upsert_kpi_daily_all(self, date_str=None):
        accounts_path = _project_root / "config" / "accounts.json"
        if not accounts_path.exists():
            logger.error("accounts.json bulunamadi")
            return 0
        with open(accounts_path) as f:
            accounts = json.load(f)
        total = 0
        for hk, h in accounts.get("hesaplar", {}).items():
            for mp, cfg in h.get("marketplaces", {}).items():
                if cfg.get("aktif"):
                    total += (self.upsert_kpi_daily(hk, mp, date_str) or 0)
        logger.info("KPI TOPLAM: %d satir", total)
        return total

    # ==========================================
    # HESAP YONETIMI
    # ==========================================

    def upsert_account(self, hesap_key: str, hesap_adi: str, **kwargs):
        self._execute("""
            INSERT INTO accounts (hesap_key, hesap_adi, seller_name, account_id, api_endpoint)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (hesap_key)
            DO UPDATE SET hesap_adi = EXCLUDED.hesap_adi,
                          seller_name = EXCLUDED.seller_name,
                          account_id = EXCLUDED.account_id,
                          api_endpoint = EXCLUDED.api_endpoint
        """, (
            hesap_key, hesap_adi,
            kwargs.get("seller_name"),
            kwargs.get("account_id"),
            kwargs.get("api_endpoint")
        ))

    def upsert_marketplace(self, hesap_key: str, marketplace: str, **kwargs):
        self._execute("""
            INSERT INTO marketplaces (hesap_key, marketplace, profile_id,
                                      marketplace_id, currency, timezone, aktif)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (hesap_key, marketplace)
            DO UPDATE SET profile_id = EXCLUDED.profile_id,
                          marketplace_id = EXCLUDED.marketplace_id,
                          currency = EXCLUDED.currency,
                          timezone = EXCLUDED.timezone,
                          aktif = EXCLUDED.aktif
        """, (
            hesap_key, marketplace,
            kwargs.get("profile_id"),
            kwargs.get("marketplace_id"),
            kwargs.get("currency", "$"),
            kwargs.get("timezone"),
            kwargs.get("aktif", True)
        ))

    def sync_accounts_from_config(self, accounts_config: dict):
        """accounts.json'dan hesap ve marketplace bilgilerini senkronize et."""
        hesaplar = accounts_config.get("hesaplar", {})
        mp_config = accounts_config.get("marketplace_config", {})

        for hesap_key, hesap in hesaplar.items():
            self.upsert_account(
                hesap_key=hesap_key,
                hesap_adi=hesap.get("hesap_adi", hesap_key),
                seller_name=hesap.get("seller_name"),
                account_id=hesap.get("account_id"),
                api_endpoint=hesap.get("api_endpoint")
            )

            for mp_code, mp_info in hesap.get("marketplaces", {}).items():
                mp_extra = mp_config.get(mp_code, {})
                self.upsert_marketplace(
                    hesap_key=hesap_key,
                    marketplace=mp_code,
                    profile_id=mp_info.get("profile_id"),
                    marketplace_id=mp_info.get("marketplace_id"),
                    currency=mp_extra.get("currency", "$"),
                    timezone=mp_extra.get("timezone"),
                    aktif=mp_info.get("aktif", True)
                )

        logger.info("accounts.json senkronize edildi")
