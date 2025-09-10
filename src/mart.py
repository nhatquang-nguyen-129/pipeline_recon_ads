"""
===========================================================================
RECONCILIED MATERIALIZATION MODULE
---------------------------------------------------------------------------
This function aggregates daily advertising costs across all supported
platforms (Facebook, TikTok, Google) into a unified summary table.

It dynamically checks the existence of individual platform-level spend 
tables in BigQuery, constructs UNION queries for available sources, and 
writes the result to a centralized `cost_all_daily` table, partitioned 
and clustered for efficient querying.

✔️ Dynamically supports multi-platform integration  
✔️ Aggregates and groups spend data by date, campaign info, and personnel  
✔️ Outputs a clean mart table for downstream dashboards or analysis  

⚠️ This module assumes platform-specific mart tables already exist.  
It does not trigger or depend on raw data ingestion or staging logic.
===========================================================================
"""
# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))  

# Add utilities for logging and error tracking
import logging

# Add Python 'time' library for tracking execution time and implementing delays
import time

# Add Google API Core libraries for integration
from google.api_core.exceptions import NotFound

# Add Google Authentication libraries for integration
from google.auth.exceptions import DefaultCredentialsError

# Add Google Cloud libraries for integration
from google.cloud import bigquery

# Get environment variable for Company
COMPANY = os.getenv("COMPANY") 

# Get environment variable for Google Cloud Project ID
PROJECT = os.getenv("PROJECT")

# Get environment variable for Platform
PLATFORM = os.getenv("PLATFORM")

# Get environmetn variable for Department
DEPARTMENT = os.getenv("DEPARTMENT")

# Get environment variable for Account
ACCOUNT = os.getenv("ACCOUNT")

# Get nvironment variable for Layer
LAYER = os.getenv("LAYER")

# Get environment variable for Mode
MODE = os.getenv("MODE")

# 1.1. BUILD UNIFIED COST AGGREGATION TABLE FROM ADVERTISING PLATFORM(s)

# 1.1. Build unified daily cost table from multiple advertising platforms
def mart_spend_all():
    print(f"🚀 [MART] Starting unified daily advertising spend aggregation for {COMPANY} company with {DEPARTMENT} department and {ACCOUNT} account...")
    logging.info(f"🚀 [MART] Starting unified daily advertising spend aggregation for {COMPANY} company with {DEPARTMENT} department and {ACCOUNT} account...")

    # 1.1.1. Initialize BigQuery client
    try:
        bigquery_client = bigquery.Client(project=PROJECT)
    except DefaultCredentialsError as e:
        raise RuntimeError("❌ [MART] Failed to initialize Google BigQuery client due to credentials error.") from e

    # 1.1.2. Define advertising networks
    networks = ["facebook", 
                "tiktok", 
                "google", 
                "shopee", 
                "zalo"]

    # 1.1.3. Prepare valid source tables lists
    valid_source_tables_all = []
    valid_source_tables_specific = []

    # 1.1.4. Scan source tables for each network
    for network in networks:
        try:
            print(f"🔍 [MART] Scanning for {network} materialized dataset for advertising spend unification...")
            logging.info(f"🔍 [MART] Scanning for {network} materialized dataset for advertising spend unification...")
            mart_dataset = f"{COMPANY}_dataset_{network}_api_mart"
            print(f"✅ [MART] Successfully retrieved {network} materialized dataset for advertising spend unification.")
            logging.info(f"✅ [MART] Successfully retrieved {network} materialized dataset for advertising spend unification.")
        except KeyError:
            print(f"⚠️ [MART] Materialized dataset for {network} not found then unification is skipped.")
            logging.warning(f"⚠️ [MART] Materialized dataset for {network} not found then unification is skipped.")
            continue
        source_table_all = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{network}_all_all_campaign_spend"
        try:
            bigquery_client.get_table(source_table_all)
            valid_source_tables_all.append(source_table_all)
            print(f"✅ [MART] Successfully retrieved {source_table_all} materialized table for all_all spend unification.")
            logging.info(f"✅ [MART] Successfully retrieved {source_table_all} materialized table for all_all spend unification.")
        except NotFound:
            print(f"⚠️ [MART] Source table {source_table_all} not found then all_all unification is skipped.")
            logging.warning(f"⚠️ [MART] Source table {source_table_all} not found then all_all unification is skipped.")
        source_table_specific = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{network}_{DEPARTMENT}_{ACCOUNT}_campaign_spend"
        try:
            bigquery_client.get_table(source_table_specific)
            valid_source_tables_specific.append(source_table_specific)
            print(f"✅ [MART] Successfully retrieved {source_table_specific} materialized table for pair-key spend unification.")
            logging.info(f"✅ [MART] Successfully retrieved {source_table_specific} materialized table for pair-key spend unification.")
        except NotFound:
            print(f"⚠️ [MART] Source table {source_table_specific} not found then pair-key unification is skipped.")
            logging.warning(f"⚠️ [MART] Source table {source_table_specific} not found then pair-key unification is skipped.")

    # 1.1.5. Validate at least some sources exist
    if not valid_source_tables_all and not valid_source_tables_specific:
        print("❌ [MART] Failed to unify advertising spend due to no valid source tables found.")
        logging.error("❌ [MART] Failed to unify advertising spend due to no valid source tables found.")
        return

    # 1.1.4. Always build all_all unified advertising spend table
    if valid_source_tables_all:
        output_table_all = f"{PROJECT}.{COMPANY}_dataset_{PLATFORM}_api_mart.{COMPANY}_table_{PLATFORM}_all_all_campaign_spend"
        union_sql_all = "\nUNION ALL\n".join([
            f"""
            SELECT
                nen_tang,
                ma_ngan_sach_cap_1,
                chuong_trinh,
                noi_dung,
                hinh_thuc,
                thang,
                trang_thai,
                ngay,
                nhan_su,
                chi_tieu
            FROM `{tbl}`
            """ for tbl in valid_source_tables_all
        ])
        query_all = f"""
            CREATE OR REPLACE TABLE `{output_table_all}`
            PARTITION BY ngay
            CLUSTER BY ma_ngan_sach_cap_1, chuong_trinh, nhan_su, thang
            AS
            {union_sql_all}
        """
        print(f"🔄 [MART] Querying all campaign spend table(s) to build materialized table {output_table_all}...")
        logging.info(f"🔄 [MART] Querying all campaign spend table(s) to build materialized table {output_table_all}...")
        bigquery_client.query(query_all).result()
        print(f"✅ [MART] Successfully created materialized table {output_table_all}.")
        logging.info(f"✅ [MART] Successfully created materialized table {output_table_all}.")

    # 1.1.6. If DEPARTMENT/ACCOUNT != all then build specific table
    if not (DEPARTMENT == "all" and ACCOUNT == "all") and valid_source_tables_specific:
        output_table_specific = f"{PROJECT}.{COMPANY}_dataset_{PLATFORM}_api_mart.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_spend"

        # Nếu là marketing + supplier thì build thêm supplier_name
        if DEPARTMENT == "marketing" and ACCOUNT == "supplier":
            union_sql_specific = "\nUNION ALL\n".join([
                f"""
                SELECT
                    nen_tang,
                    ma_ngan_sach_cap_1,
                    chuong_trinh,
                    noi_dung,
                    hinh_thuc,
                    thang,
                    trang_thai,
                    ngay,
                    nhan_su,
                    chi_tieu,
                    supplier_name
                FROM `{tbl}`
                """ for tbl in valid_source_tables_specific
            ])
        else:
            union_sql_specific = "\nUNION ALL\n".join([
                f"""
                SELECT
                    nen_tang,
                    ma_ngan_sach_cap_1,
                    chuong_trinh,
                    noi_dung,
                    hinh_thuc,
                    thang,
                    trang_thai,
                    ngay,
                    nhan_su,
                    chi_tieu
                FROM `{tbl}`
                """ for tbl in valid_source_tables_specific
            ])

        query_specific = f"""
            CREATE OR REPLACE TABLE `{output_table_specific}`
            PARTITION BY ngay
            CLUSTER BY ma_ngan_sach_cap_1, chuong_trinh, nhan_su, thang
            AS
            {union_sql_specific}
        """

        print(f"🔄 [MART] Querying all campaign spend table(s) to build materialized table {output_table_specific}...")
        logging.info(f"🔄 [MART] Querying all campaign spend table(s) to build materialized table {output_table_specific}...")
        bigquery_client.query(query_specific).result()
        print(f"✅ [MART] Successfully created materialized table {output_table_specific}.")
        logging.info(f"✅ [MART] Successfully created materialized table {output_table_specific}.")

# 1.2. Build unified monthly aggregate cost table from multiple advertising platforms
def mart_aggregate_all():
    print(f"🚀 [MART] Starting monthly advertising spend aggregation for {COMPANY} company with {DEPARTMENT} department and {ACCOUNT} account...")
    logging.info(f"🚀 [MART] Starting monthly advertising spend aggregation for {COMPANY} company with {DEPARTMENT} department and {ACCOUNT} account...")

    # 1.2.1. Initialize BigQuery client
    try:
        bigquery_client = bigquery.Client(project=PROJECT)
    except DefaultCredentialsError as e:
        raise RuntimeError("❌ [MART] Failed to initialize Google BigQuery client due to credentials error.") from e

    # 1.2.2. Define source tables (daily spend unified)
    source_table_all = f"{PROJECT}.{COMPANY}_dataset_{PLATFORM}_api_mart.{COMPANY}_table_{PLATFORM}_all_all_campaign_spend"
    source_table_specific = f"{PROJECT}.{COMPANY}_dataset_{PLATFORM}_api_mart.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_spend"

    # 1.2.3. Build monthly all_all aggregation
    try:
        bigquery_client.get_table(source_table_all)
        output_table_all = f"{PROJECT}.{COMPANY}_dataset_{PLATFORM}_api_mart.{COMPANY}_table_{PLATFORM}_all_all_aggregation_spend"
        query_all = f"""
            CREATE OR REPLACE TABLE `{output_table_all}`
            CLUSTER BY ma_ngan_sach_cap_1, chuong_trinh, nhan_su, thang AS
            SELECT
                nen_tang,
                ma_ngan_sach_cap_1,
                chuong_trinh,
                noi_dung,
                hinh_thuc,
                thang,
                nhan_su,
                CASE
                    WHEN COUNTIF(LOWER(trang_thai) = 'active') > 0 THEN 'active'
                    ELSE 'inactive'
                END AS trang_thai,
                SUM(chi_tieu) AS chi_tieu
            FROM `{source_table_all}`
            GROUP BY nen_tang, ma_ngan_sach_cap_1, chuong_trinh, noi_dung, hinh_thuc, thang, nhan_su
        """
        print(f"🔄 [MART] Aggregating daily spend into monthly table {output_table_all}...")
        logging.info(f"🔄 [MART] Aggregating daily spend into monthly table {output_table_all}...")
        bigquery_client.query(query_all).result()
        print(f"✅ [MART] Successfully created monthly aggregation table {output_table_all}.")
        logging.info(f"✅ [MART] Successfully created monthly aggregation table {output_table_all}.")
    except NotFound:
        print(f"⚠️ [MART] Daily spend table {source_table_all} not found, skipping all_all aggregation.")
        logging.warning(f"⚠️ [MART] Daily spend table {source_table_all} not found, skipping all_all aggregation.")

    # 1.2.4. Build monthly specific aggregation (if not all/all)
    if not (DEPARTMENT == "all" and ACCOUNT == "all"):
        try:
            bigquery_client.get_table(source_table_specific)
            output_table_specific = f"{PROJECT}.{COMPANY}_dataset_{PLATFORM}_api_mart.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_aggregation_spend"

            # Nếu là marketing + supplier thì build thêm supplier_name
            if DEPARTMENT == "marketing" and ACCOUNT == "supplier":
                query_specific = f"""
                    CREATE OR REPLACE TABLE `{output_table_specific}`
                    CLUSTER BY ma_ngan_sach_cap_1, chuong_trinh, nhan_su, thang AS
                    SELECT
                        nen_tang,
                        ma_ngan_sach_cap_1,
                        chuong_trinh,
                        noi_dung,
                        hinh_thuc,
                        thang,
                        nhan_su,
                        supplier_name,
                        CASE
                            WHEN COUNTIF(LOWER(trang_thai) = 'active') > 0 THEN 'active'
                            ELSE 'inactive'
                        END AS trang_thai,
                        SUM(chi_tieu) AS chi_tieu
                    FROM `{source_table_specific}`
                    GROUP BY nen_tang, ma_ngan_sach_cap_1, chuong_trinh, noi_dung, hinh_thuc, thang, nhan_su, supplier_name
                """
            else:
                query_specific = f"""
                    CREATE OR REPLACE TABLE `{output_table_specific}`
                    CLUSTER BY ma_ngan_sach_cap_1, chuong_trinh, nhan_su, thang AS
                    SELECT
                        nen_tang,
                        ma_ngan_sach_cap_1,
                        chuong_trinh,
                        noi_dung,
                        hinh_thuc,
                        thang,
                        nhan_su,
                        CASE
                            WHEN COUNTIF(LOWER(trang_thai) = 'active') > 0 THEN 'active'
                            ELSE 'inactive'
                        END AS trang_thai,
                        SUM(chi_tieu) AS chi_tieu
                    FROM `{source_table_specific}`
                    GROUP BY nen_tang, ma_ngan_sach_cap_1, chuong_trinh, noi_dung, hinh_thuc, thang, nhan_su
                """

            print(f"🔄 [MART] Aggregating daily spend into monthly table {output_table_specific}...")
            logging.info(f"🔄 [MART] Aggregating daily spend into monthly table {output_table_specific}...")
            bigquery_client.query(query_specific).result()
            print(f"✅ [MART] Successfully created monthly aggregation table {output_table_specific}.")
            logging.info(f"✅ [MART] Successfully created monthly aggregation table {output_table_specific}.")
        except NotFound:
            print(f"⚠️ [MART] Daily spend table {source_table_specific} not found, skipping specific aggregation.")
            logging.warning(f"⚠️ [MART] Daily spend table {source_table_specific} not found, skipping specific aggregation.")

# 2. BUILD MATERIALIZED TABLE FOR BUDGET ALLOCATION AND ADVERTISING SPEND RECONCILIATION

# 2.1 Build materialized table for monthly budget allocation and advertising spend reconciliation
def mart_recon_all():
    print(f"🚀 [MART] Starting monthly budget allocation and advertising reconciliation for {COMPANY} company with {DEPARTMENT} department and {ACCOUNT} account...")
    logging.info(f"🚀 [MART] Starting monthly budget allocation and advertising reconciliation for {COMPANY} company with {DEPARTMENT} department and {ACCOUNT} account...")

    # 2.1.1. Prepare BigQuery client
    try:
        bigquery_client = bigquery.Client(project=PROJECT)
    except DefaultCredentialsError as e:
        raise RuntimeError(" ❌ [MART] Failed to initialize Google BigQuery client due to credentials error.") from e

    # 2.1.2. Define source aggregation spend table
    input_table_spend_all = f"{PROJECT}.{COMPANY}_dataset_{PLATFORM}_api_mart.{COMPANY}_table_{PLATFORM}_all_all_aggregation_spend"
    print(f"🔍 [MART] Using aggregated spend table {input_table_spend_all} for advertising reconciliation...")
    logging.info(f"🔍 [MART] Using aggregated spend table {input_table_spend_all} for advertising reconciliation...")

    # 2.1.3. Define budget and recon output tables
    input_dataset_budget = f"{COMPANY}_dataset_budget_api_mart"
    input_table_budget_all = f"{PROJECT}.{input_dataset_budget}.{COMPANY}_table_budget_all_all_allocation_monthly"
    output_dataset_recon = f"{COMPANY}_dataset_{PLATFORM}_api_mart"
    output_table_recon_all = f"{PROJECT}.{output_dataset_recon}.{COMPANY}_table_{PLATFORM}_all_all_reconciliation_spend"
    query_recon_all = f"""
        CREATE OR REPLACE TABLE `{output_table_recon_all}`
        CLUSTER BY ma_ngan_sach_cap_1, chuong_trinh, nhan_su, thang AS
        WITH budget AS (
            SELECT * FROM `{input_table_budget_all}`
        ),
        cost AS (
            SELECT * FROM `{input_table_spend_all}`
        )
        SELECT
            COALESCE(b.ma_ngan_sach_cap_1, c.ma_ngan_sach_cap_1) AS ma_ngan_sach_cap_1,
            COALESCE(b.chuong_trinh, c.chuong_trinh) AS chuong_trinh,
            COALESCE(b.noi_dung, c.noi_dung) AS noi_dung,
            COALESCE(b.nen_tang, c.nen_tang, "other") AS nen_tang,
            COALESCE(b.hinh_thuc, c.hinh_thuc, "other") AS hinh_thuc,
            COALESCE(b.thang, c.thang) AS thang,
            COALESCE(c.nhan_su, "other") AS nhan_su,
            b.ngan_sach_ban_dau,
            b.ngan_sach_dieu_chinh,
            b.ngan_sach_bo_sung,
            b.ngan_sach_thuc_chi,
            b.ngan_sach_he_thong,
            b.ngan_sach_nha_cung_cap,
            b.ngan_sach_kinh_doanh,
            b.ngan_sach_tien_san,
            b.ngan_sach_tuyen_dung,
            b.ngan_sach_khac,
            b.thoi_gian_bat_dau,
            b.thoi_gian_ket_thuc,
            c.chi_tieu AS so_tien_thuc_tieu,
            c.trang_thai AS trang_thai,
            CASE
                -- Spend without Budget
                WHEN COALESCE(b.ngan_sach_thuc_chi, 0) = 0 
                    AND COALESCE(c.chi_tieu, 0) > 0
                    AND LOWER(COALESCE(c.trang_thai, '')) = 'active'
                THEN "🔴 Spend without Budget (On)"
                WHEN COALESCE(b.ngan_sach_thuc_chi, 0) = 0 
                    AND COALESCE(c.chi_tieu, 0) > 0
                    AND LOWER(COALESCE(c.trang_thai, '')) != 'active'
                THEN "⚪ Spend without Budget (Off)"

                -- No Budget
                WHEN COALESCE(b.ngan_sach_thuc_chi, 0) = 0
                THEN "🚫 No Budget"

                -- Not Yet Started
                WHEN b.ngan_sach_thuc_chi > 0
                    AND CURRENT_DATE() < b.thoi_gian_bat_dau
                THEN "🕓 Not Yet Started"

                -- Not Set (mới start, chưa spend, chưa có trạng thái)
                WHEN b.ngan_sach_thuc_chi > 0
                    AND CURRENT_DATE() >= b.thoi_gian_bat_dau
                    AND (c.chi_tieu IS NULL OR c.chi_tieu = 0)
                    AND (c.trang_thai IS NULL OR TRIM(c.trang_thai) = '')
                    AND DATE_DIFF(CURRENT_DATE(), b.thoi_gian_bat_dau, DAY) <= 3
                THEN "⚪ Not Set"

                -- Delayed (đã start >3 ngày, chưa spend, còn trong khoảng chạy)
                WHEN b.ngan_sach_thuc_chi > 0
                    AND CURRENT_DATE() >= b.thoi_gian_bat_dau
                    AND (c.chi_tieu IS NULL OR c.chi_tieu = 0)
                    AND DATE_DIFF(CURRENT_DATE(), b.thoi_gian_bat_dau, DAY) > 3
                    AND (CURRENT_DATE() <= b.thoi_gian_ket_thuc OR b.thoi_gian_ket_thuc IS NULL)
                THEN "⚠️ Delayed"

                -- Ended without Spend
                WHEN b.ngan_sach_thuc_chi > 0
                    AND b.thoi_gian_ket_thuc IS NOT NULL
                    AND CURRENT_DATE() > b.thoi_gian_ket_thuc
                    AND (c.chi_tieu IS NULL OR c.chi_tieu = 0)
                THEN "🔒 Ended without Spend"

                -- Over Budget
                WHEN b.ngan_sach_thuc_chi > 0
                    AND COALESCE(c.chi_tieu, 0) >= b.ngan_sach_thuc_chi * 1.01
                    AND LOWER(COALESCE(c.trang_thai, '')) = 'active'
                THEN "🔴 Over Budget (Still Running)"
                WHEN b.ngan_sach_thuc_chi > 0
                    AND COALESCE(c.chi_tieu, 0) >= b.ngan_sach_thuc_chi * 1.01
                    AND LOWER(COALESCE(c.trang_thai, '')) != 'active'
                THEN "⚪ Over Budget (Stopped)"

                -- Completed
                WHEN b.ngan_sach_thuc_chi > 0
                    AND SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), b.ngan_sach_thuc_chi) > 0.99
                    AND COALESCE(c.chi_tieu, 0) < b.ngan_sach_thuc_chi * 1.01
                THEN "🔵 Completed"

                -- Near Completion
                WHEN b.ngan_sach_thuc_chi > 0
                    AND SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), b.ngan_sach_thuc_chi) BETWEEN 0.95 AND 0.99
                THEN "🟢 Near Completion"

                -- Low Spend
                WHEN b.ngan_sach_thuc_chi > 0
                    AND SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), b.ngan_sach_thuc_chi) < 0.95
                    AND DATE_DIFF(b.thoi_gian_ket_thuc, b.thoi_gian_bat_dau, DAY) > 0
                    AND SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), b.ngan_sach_thuc_chi) <
                        SAFE_DIVIDE(DATE_DIFF(CURRENT_DATE(), b.thoi_gian_bat_dau, DAY),
                                    DATE_DIFF(b.thoi_gian_ket_thuc, b.thoi_gian_bat_dau, DAY)) - 0.3
                THEN "📉 Low Spend"

                -- High Spend
                WHEN b.ngan_sach_thuc_chi > 0
                    AND SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), b.ngan_sach_thuc_chi) < 0.95
                    AND DATE_DIFF(b.thoi_gian_ket_thuc, b.thoi_gian_bat_dau, DAY) > 0
                    AND SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), b.ngan_sach_thuc_chi) >
                        SAFE_DIVIDE(DATE_DIFF(CURRENT_DATE(), b.thoi_gian_bat_dau, DAY),
                                    DATE_DIFF(b.thoi_gian_ket_thuc, b.thoi_gian_bat_dau, DAY)) + 0.3
                THEN "📈 High Spend"

                -- In Progress (chưa completed, chưa over budget, vẫn active)
                WHEN b.ngan_sach_thuc_chi > 0
                    AND COALESCE(c.chi_tieu, 0) > 0
                    AND LOWER(COALESCE(c.trang_thai, '')) = 'active'
                THEN "🟢 In Progress"

                -- Default
                ELSE "❓ Not Recognized"
            END AS trang_thai_chien_dich,
            SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), COALESCE(b.ngan_sach_thuc_chi, 0)) AS spending_ratio
        FROM budget b
        FULL OUTER JOIN cost c
          ON b.ma_ngan_sach_cap_1 = c.ma_ngan_sach_cap_1
         AND b.chuong_trinh = c.chuong_trinh
         AND b.noi_dung = c.noi_dung
         AND b.nen_tang = c.nen_tang
         AND b.hinh_thuc = c.hinh_thuc
         AND b.thang = c.thang
    """
    try:
        print(f"🔁 [MART] Executing aggregation query to create {output_table_recon_all} reconciled advertising spend table...")
        logging.info(f"🔁 [MART] Executing aggregation query to create {output_table_recon_all} reconciled advertising spend table...")
        bigquery_client.query(query_recon_all).result()
        logging.info(f"✅ [MART] Successfully created materialized table {output_table_recon_all} for reconciled advertising spend.")
        print(f"✅ [MART] Successfully created materialized table {output_table_recon_all} for reconciled advertising spend.")
    except Exception as e:
        logging.error(f"❌ [MART] Failed to excute aggregation query to build reconciled advertising spend table due to {e}.")
        print(f"❌ [MART] Failed to excute aggregation query to build reconciled advertising spend table due to {e}.")

    # 2.1.4. Build reconciliation table for supplier/marketing
    if DEPARTMENT == "supplier" or DEPARTMENT == "marketing":
        input_table_spend_supplier = f"{PROJECT}.{COMPANY}_dataset_{PLATFORM}_api_mart.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_aggregation_spend"
        input_table_budget_supplier = f"{PROJECT}.{input_dataset_budget}.{COMPANY}_table_budget_{DEPARTMENT}_{ACCOUNT}_allocation_monthly"
        output_table_recon_supplier = f"{PROJECT}.{output_dataset_recon}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_reconciliation_spend"

        query_recon_supplier = f"""
            CREATE OR REPLACE TABLE `{output_table_recon_supplier}`
            CLUSTER BY ma_ngan_sach_cap_1, chuong_trinh, thang AS
            WITH budget AS (
                SELECT * FROM `{input_table_budget_supplier}`
            ),
            cost AS (
                SELECT * FROM `{input_table_spend_supplier}`
            )
            SELECT
                COALESCE(b.ma_ngan_sach_cap_1, c.ma_ngan_sach_cap_1) AS ma_ngan_sach_cap_1,
                COALESCE(b.chuong_trinh, c.chuong_trinh) AS chuong_trinh,
                COALESCE(b.noi_dung, c.noi_dung) AS noi_dung,
                COALESCE(b.nen_tang, c.nen_tang, "other") AS nen_tang,
                COALESCE(b.hinh_thuc, c.hinh_thuc, "other") AS hinh_thuc,
                COALESCE(b.thang, c.thang) AS thang,
                b.thoi_gian_bat_dau,
                b.thoi_gian_ket_thuc,
                b.ngan_sach_thuc_chi,
                c.chi_tieu AS so_tien_thuc_tieu,
                c.supplier_name,                
                CASE
                    -- Spend without Budget
                    WHEN (c.chi_tieu IS NOT NULL AND c.chi_tieu > 0)
                        AND COALESCE(b.ngan_sach_thuc_chi, 0) = 0
                        AND LOWER(COALESCE(c.trang_thai, '')) = 'active'
                        THEN "🔴 Spend without Budget (On)"
                    WHEN (c.chi_tieu IS NOT NULL AND c.chi_tieu > 0)
                        AND COALESCE(b.ngan_sach_thuc_chi, 0) = 0
                        AND LOWER(COALESCE(c.trang_thai, '')) != 'active'
                        THEN "⚪ Spend without Budget (Off)"

                    -- No Budget
                    WHEN COALESCE(b.ngan_sach_thuc_chi, 0) = 0
                        THEN "🚫 No Budget"

                    -- Not Yet Started
                    WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                        AND CURRENT_DATE() < b.thoi_gian_bat_dau
                        THEN "🕓 Not Yet Started"

                    -- Not Set
                    WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                        AND CURRENT_DATE() >= b.thoi_gian_bat_dau
                        AND (c.chi_tieu IS NULL OR c.chi_tieu = 0)
                        AND (c.trang_thai IS NULL OR TRIM(c.trang_thai) = '')
                        AND DATE_DIFF(CURRENT_DATE(), b.thoi_gian_bat_dau, DAY) <= 3
                        THEN "⚪ Not Set"

                    -- Delayed
                    WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                        AND CURRENT_DATE() >= b.thoi_gian_bat_dau
                        AND (c.chi_tieu IS NULL OR c.chi_tieu = 0)
                        AND DATE_DIFF(CURRENT_DATE(), b.thoi_gian_bat_dau, DAY) > 3
                        THEN "⚠️ Delayed"

                    -- Ended without Spend
                    WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                        AND CURRENT_DATE() > b.thoi_gian_ket_thuc
                        AND (c.chi_tieu IS NULL OR c.chi_tieu = 0)
                        THEN "🔒 Ended without Spend"

                    -- Low Spend
                    WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                        AND SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), b.ngan_sach_thuc_chi) < 0.95
                        AND DATE_DIFF(b.thoi_gian_ket_thuc, b.thoi_gian_bat_dau, DAY) > 0
                        AND SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), b.ngan_sach_thuc_chi) <
                            SAFE_DIVIDE(DATE_DIFF(CURRENT_DATE(), b.thoi_gian_bat_dau, DAY),
                                        DATE_DIFF(b.thoi_gian_ket_thuc, b.thoi_gian_bat_dau, DAY)) - 0.3
                        THEN "📉 Low Spend"

                    -- High Spend
                    WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                        AND SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), b.ngan_sach_thuc_chi) < 0.95
                        AND DATE_DIFF(b.thoi_gian_ket_thuc, b.thoi_gian_bat_dau, DAY) > 0
                        AND SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), b.ngan_sach_thuc_chi) >
                            SAFE_DIVIDE(DATE_DIFF(CURRENT_DATE(), b.thoi_gian_bat_dau, DAY),
                                        DATE_DIFF(b.thoi_gian_ket_thuc, b.thoi_gian_bat_dau, DAY)) + 0.3
                        THEN "📈 High Spend"

                    -- Near Completion
                    WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                        AND SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), b.ngan_sach_thuc_chi) BETWEEN 0.95 AND 0.99
                        THEN "🟢 Near Completion"

                    -- Completed
                    WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                        AND SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), b.ngan_sach_thuc_chi) > 0.99
                        AND COALESCE(c.chi_tieu, 0) < b.ngan_sach_thuc_chi * 1.01
                        THEN "🔵 Completed"

                    -- Over Budget
                    WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                        AND COALESCE(c.chi_tieu, 0) >= b.ngan_sach_thuc_chi * 1.01
                        AND LOWER(COALESCE(c.trang_thai, '')) = 'active'
                        THEN "🔴 Over Budget (Still Running)"
                    WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                        AND COALESCE(c.chi_tieu, 0) >= b.ngan_sach_thuc_chi * 1.01
                        AND LOWER(COALESCE(c.trang_thai, '')) != 'active'
                        THEN "⚪ Over Budget (Stopped)"

                    -- In Progress
                    WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                        AND (c.chi_tieu IS NOT NULL AND c.chi_tieu > 0)
                        AND LOWER(COALESCE(c.trang_thai, '')) = 'active'
                        THEN "🟢 In Progress"

                    -- Default
                    ELSE "❓ Not Recognized"
                END AS trang_thai_chien_dich,
                SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), COALESCE(b.ngan_sach_thuc_chi, 0)) AS spending_ratio
            FROM budget b
            FULL OUTER JOIN cost c
            ON b.ma_ngan_sach_cap_1 = c.ma_ngan_sach_cap_1
            AND b.chuong_trinh = c.chuong_trinh
            AND b.noi_dung = c.noi_dung
            AND b.nen_tang = c.nen_tang
            AND b.hinh_thuc = c.hinh_thuc
            AND b.thang = c.thang
        """
        try:
            print(f"🔁 [MART] Executing supplier reconciliation query to create {output_table_recon_supplier}...")
            logging.info(f"🔁 [MART] Executing supplier reconciliation query to create {output_table_recon_supplier}...")
            bigquery_client.query(query_recon_supplier).result()
            print(f"✅ [MART] Successfully created supplier reconciliation table {output_table_recon_supplier}.")
            logging.info(f"✅ [MART] Successfully created supplier reconciliation table {output_table_recon_supplier}.")
        except Exception as e:
            print(f"❌ [MART] Failed to build supplier reconciliation table {output_table_recon_supplier} due to {e}.")
            logging.error(f"❌ [MART] Failed to build supplier reconciliation table {output_table_recon_supplier} due to {e}.")

    # 2.1.5. Build reconciliation table for other specific cases
    if not (DEPARTMENT == "all" and ACCOUNT == "all") and DEPARTMENT not in ["supplier", "marketing"]:
        input_table_spend_specific = f"{PROJECT}.{COMPANY}_dataset_{PLATFORM}_api_mart.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_aggregation_spend"
        input_table_budget_specific = f"{PROJECT}.{input_dataset_budget}.{COMPANY}_table_budget_{DEPARTMENT}_{ACCOUNT}_allocation_monthly"
        output_table_recon_specific = f"{PROJECT}.{output_dataset_recon}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_reconciliation_spend"
        query_recon_specific = f"""
            CREATE OR REPLACE TABLE `{output_table_recon_specific}`
            CLUSTER BY ma_ngan_sach_cap_1, chuong_trinh, thang AS
            WITH budget AS (
                SELECT * FROM `{input_table_budget_specific}`
            ),
            cost AS (
                SELECT * FROM `{input_table_spend_specific}`
            )
            SELECT
                COALESCE(b.ma_ngan_sach_cap_1, c.ma_ngan_sach_cap_1) AS ma_ngan_sach_cap_1,
                COALESCE(b.chuong_trinh, c.chuong_trinh) AS chuong_trinh,
                COALESCE(b.noi_dung, c.noi_dung) AS noi_dung,
                COALESCE(b.nen_tang, c.nen_tang, "other") AS nen_tang,
                COALESCE(b.hinh_thuc, c.hinh_thuc, "other") AS hinh_thuc,
                COALESCE(b.thang, c.thang) AS thang,
                b.ngan_sach_ban_dau,
                b.ngan_sach_dieu_chinh,
                b.ngan_sach_bo_sung,
                b.ngan_sach_thuc_chi,
                b.thoi_gian_bat_dau,
                b.thoi_gian_ket_thuc,
                c.chi_tieu AS so_tien_thuc_tieu,
                c.trang_thai AS trang_thai,

                CASE
                    -- Spend without Budget
                    WHEN (c.chi_tieu IS NOT NULL AND c.chi_tieu > 0)
                        AND COALESCE(b.ngan_sach_thuc_chi, 0) = 0
                        AND LOWER(COALESCE(c.trang_thai, '')) = 'active'
                        THEN "🔴 Spend without Budget (On)"
                    WHEN (c.chi_tieu IS NOT NULL AND c.chi_tieu > 0)
                        AND COALESCE(b.ngan_sach_thuc_chi, 0) = 0
                        AND LOWER(COALESCE(c.trang_thai, '')) != 'active'
                        THEN "⚪ Spend without Budget (Off)"

                    -- No Budget
                    WHEN COALESCE(b.ngan_sach_thuc_chi, 0) = 0
                        THEN "🚫 No Budget"

                    -- Not Yet Started
                    WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                        AND CURRENT_DATE() < b.thoi_gian_bat_dau
                        THEN "🕓 Not Yet Started"

                    -- Not Set
                    WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                        AND CURRENT_DATE() >= b.thoi_gian_bat_dau
                        AND (c.chi_tieu IS NULL OR c.chi_tieu = 0)
                        AND (c.trang_thai IS NULL OR TRIM(c.trang_thai) = '')
                        AND DATE_DIFF(CURRENT_DATE(), b.thoi_gian_bat_dau, DAY) <= 3
                        THEN "⚪ Not Set"

                    -- Delayed
                    WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                        AND CURRENT_DATE() >= b.thoi_gian_bat_dau
                        AND (c.chi_tieu IS NULL OR c.chi_tieu = 0)
                        AND DATE_DIFF(CURRENT_DATE(), b.thoi_gian_bat_dau, DAY) > 3
                        THEN "⚠️ Delayed"

                    -- Ended without Spend
                    WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                        AND CURRENT_DATE() > b.thoi_gian_ket_thuc
                        AND (c.chi_tieu IS NULL OR c.chi_tieu = 0)
                        THEN "🔒 Ended without Spend"

                    -- Low Spend
                    WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                        AND SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), b.ngan_sach_thuc_chi) < 0.95
                        AND DATE_DIFF(b.thoi_gian_ket_thuc, b.thoi_gian_bat_dau, DAY) > 0
                        AND SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), b.ngan_sach_thuc_chi) <
                            SAFE_DIVIDE(DATE_DIFF(CURRENT_DATE(), b.thoi_gian_bat_dau, DAY),
                                        DATE_DIFF(b.thoi_gian_ket_thuc, b.thoi_gian_bat_dau, DAY)) - 0.3
                        THEN "📉 Low Spend"

                    -- High Spend
                    WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                        AND SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), b.ngan_sach_thuc_chi) < 0.95
                        AND DATE_DIFF(b.thoi_gian_ket_thuc, b.thoi_gian_bat_dau, DAY) > 0
                        AND SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), b.ngan_sach_thuc_chi) >
                            SAFE_DIVIDE(DATE_DIFF(CURRENT_DATE(), b.thoi_gian_bat_dau, DAY),
                                        DATE_DIFF(b.thoi_gian_ket_thuc, b.thoi_gian_bat_dau, DAY)) + 0.3
                        THEN "📈 High Spend"

                    -- Near Completion
                    WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                        AND SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), b.ngan_sach_thuc_chi) BETWEEN 0.95 AND 0.99
                        THEN "🟢 Near Completion"

                    -- Completed
                    WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                        AND SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), b.ngan_sach_thuc_chi) > 0.99
                        AND COALESCE(c.chi_tieu, 0) < b.ngan_sach_thuc_chi * 1.01
                        THEN "🔵 Completed"

                    -- Over Budget
                    WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                        AND COALESCE(c.chi_tieu, 0) >= b.ngan_sach_thuc_chi * 1.01
                        AND LOWER(COALESCE(c.trang_thai, '')) = 'active'
                        THEN "🔴 Over Budget (Still Running)"
                    WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                        AND COALESCE(c.chi_tieu, 0) >= b.ngan_sach_thuc_chi * 1.01
                        AND LOWER(COALESCE(c.trang_thai, '')) != 'active'
                        THEN "⚪ Over Budget (Stopped)"

                    -- In Progress
                    WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                        AND (c.chi_tieu IS NOT NULL AND c.chi_tieu > 0)
                        AND LOWER(COALESCE(c.trang_thai, '')) = 'active'
                        THEN "🟢 In Progress"

                    -- Default
                    ELSE "❓ Not Recognized"
                END AS trang_thai_chien_dich,
                SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), COALESCE(b.ngan_sach_thuc_chi, 0)) AS spending_ratio
            FROM budget b
            FULL OUTER JOIN cost c
            ON b.ma_ngan_sach_cap_1 = c.ma_ngan_sach_cap_1
            AND b.chuong_trinh = c.chuong_trinh
            AND b.noi_dung = c.noi_dung
            AND b.nen_tang = c.nen_tang
            AND b.hinh_thuc = c.hinh_thuc
            AND b.thang = c.thang
        """
        try:
            print(f"🔁 [MART] Executing specific reconciliation query to create {output_table_recon_specific}...")
            logging.info(f"🔁 [MART] Executing specific reconciliation query to create {output_table_recon_specific}...")
            bigquery_client.query(query_recon_specific).result()
            print(f"✅ [MART] Successfully created specific reconciliation table {output_table_recon_specific}.")
            logging.info(f"✅ [MART] Successfully created specific reconciliation table {output_table_recon_specific}.")
        except Exception as e:
            print(f"❌ [MART] Failed to build specific reconciliation table {output_table_recon_specific} due to {e}.")
            logging.error(f"❌ [MART] Failed to build specific reconciliation table {output_table_recon_specific} due to {e}.")
