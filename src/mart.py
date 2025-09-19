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
    print(f"🚀 [MART] Starting unified daily advertising performance aggregation for {COMPANY} company with {DEPARTMENT} department and {ACCOUNT} account...")
    logging.info(f"🚀 [MART] Starting unified daily advertising performance aggregation for {COMPANY} company with {DEPARTMENT} department and {ACCOUNT} account...")

    # 1.1.1. Initialize BigQuery client
    try:
        bigquery_client = bigquery.Client(project=PROJECT)
    except DefaultCredentialsError as e:
        raise RuntimeError("❌ [MART] Failed to initialize Google BigQuery client due to credentials error.") from e

    # 1.1.2. Define advertising networks
    networks = ["facebook", "tiktok", "google", "shopee", "zalo"]

    # 1.1.3. Prepare valid source tables lists
    valid_source_tables_all = []
    valid_source_tables_specific = []

    # 1.1.4. Scan source tables for each network
    for network in networks:
        try:
            print(f"🔍 [MART] Scanning for {network} materialized dataset for advertising performance unification...")
            logging.info(f"🔍 [MART] Scanning for {network} materialized dataset for advertising performance unification...")
            mart_dataset = f"{COMPANY}_dataset_{network}_api_mart"
            print(f"✅ [MART] Successfully retrieved {network} materialized dataset for advertising performance unification.")
            logging.info(f"✅ [MART] Successfully retrieved {network} materialized dataset for advertising performance unification.")
        except KeyError:
            print(f"⚠️ [MART] Materialized dataset for {network} not found then unification is skipped.")
            logging.warning(f"⚠️ [MART] Materialized dataset for {network} not found then unification is skipped.")
            continue

        # all_all source
        source_table_all = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{network}_all_all_campaign_performance"
        try:
            bigquery_client.get_table(source_table_all)
            valid_source_tables_all.append(source_table_all)
            print(f"✅ [MART] Successfully retrieved {source_table_all} materialized table for all_all performance unification.")
            logging.info(f"✅ [MART] Successfully retrieved {source_table_all} materialized table for all_all performance unification.")
        except NotFound:
            print(f"⚠️ [MART] Source table {source_table_all} not found then all_all unification is skipped.")
            logging.warning(f"⚠️ [MART] Source table {source_table_all} not found then all_all unification is skipped.")

        # department/account source
        source_table_specific = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{network}_{DEPARTMENT}_{ACCOUNT}_campaign_performance"
        try:
            bigquery_client.get_table(source_table_specific)
            valid_source_tables_specific.append(source_table_specific)
            print(f"✅ [MART] Successfully retrieved {source_table_specific} materialized table for pair-key performance unification.")
            logging.info(f"✅ [MART] Successfully retrieved {source_table_specific} materialized table for pair-key performance unification.")
        except NotFound:
            print(f"⚠️ [MART] Source table {source_table_specific} not found then pair-key unification is skipped.")
            logging.warning(f"⚠️ [MART] Source table {source_table_specific} not found then pair-key unification is skipped.")

    # 1.1.5. Validate at least some sources exist
    if not valid_source_tables_all and not valid_source_tables_specific:
        print("❌ [MART] Failed to unify advertising performance due to no valid source tables found.")
        logging.error("❌ [MART] Failed to unify advertising performance due to no valid source tables found.")
        return

    # 1.1.6. Build unified advertising performance table
    if valid_source_tables_specific:
        output_table = f"{PROJECT}.{COMPANY}_dataset_{PLATFORM}_api_mart.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_spend"

        if DEPARTMENT == "marketing" and ACCOUNT == "supplier":         
            union_sql = "\nUNION ALL\n".join([
                f"""
                SELECT
                    nen_tang,
                    ma_ngan_sach_cap_1,
                    chuong_trinh,
                    noi_dung,
                    hinh_thuc,
                    campaign_name,
                    FORMAT_DATE('%Y-%m', ngay) AS thang,
                    trang_thai,
                    ngay,
                    nhan_su,
                    spend AS chi_tieu,
                    supplier_name
                FROM `{tbl}`
                WHERE spend > 0
                """ for tbl in valid_source_tables_specific
            ])
        else:
            union_sql = "\nUNION ALL\n".join([
                f"""
                SELECT
                    nen_tang,
                    ma_ngan_sach_cap_1,
                    chuong_trinh,
                    noi_dung,
                    hinh_thuc,
                    campaign_name,
                    FORMAT_DATE('%Y-%m', ngay) AS thang,
                    trang_thai,
                    ngay,
                    nhan_su,
                    spend AS chi_tieu
                FROM `{tbl}`
                WHERE spend > 0
                """ for tbl in valid_source_tables_specific
            ])

        query = f"""
            CREATE OR REPLACE TABLE `{output_table}`
            PARTITION BY ngay
            CLUSTER BY ma_ngan_sach_cap_1, chuong_trinh, nhan_su, thang
            AS
            {union_sql}
        """
        print(f"🔄 [MART] Querying all campaign performance table(s) to build materialized table {output_table}...")
        logging.info(f"🔄 [MART] Querying all campaign performance table(s) to build materialized table {output_table}...")
        bigquery_client.query(query).result()
        print(f"✅ [MART] Successfully created materialized table {output_table}.")
        logging.info(f"✅ [MART] Successfully created materialized table {output_table}.")

# 1.2. Build unified monthly aggregate cost table from multiple advertising platforms
def mart_aggregate_all():
    print(f"🚀 [MART] Starting monthly advertising spend aggregation for {COMPANY} company with {DEPARTMENT} department and {ACCOUNT} account...")
    logging.info(f"🚀 [MART] Starting monthly advertising spend aggregation for {COMPANY} company with {DEPARTMENT} department and {ACCOUNT} account...")

    # 1.2.1. Initialize BigQuery client
    try:
        bigquery_client = bigquery.Client(project=PROJECT)
    except DefaultCredentialsError as e:
        raise RuntimeError("❌ [MART] Failed to initialize Google BigQuery client due to credentials error.") from e

    # 1.2.2. Define source & output tables
    source_table = f"{PROJECT}.{COMPANY}_dataset_{PLATFORM}_api_mart.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_spend"
    output_table = f"{PROJECT}.{COMPANY}_dataset_{PLATFORM}_api_mart.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_aggregation_spend"

    # 1.2.3. Build query (supplier có thêm supplier_name)
    if DEPARTMENT == "marketing" and ACCOUNT == "supplier":
        query = f"""
            CREATE OR REPLACE TABLE `{output_table}`
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
                    WHEN COUNTIF(LOWER(trang_thai) = '🟢') > 0 THEN 'active'
                    ELSE 'inactive'
                END AS trang_thai,
                SUM(chi_tieu) AS chi_tieu
            FROM `{source_table}`
            GROUP BY nen_tang, ma_ngan_sach_cap_1, chuong_trinh, noi_dung, hinh_thuc, thang, nhan_su, supplier_name
        """
    else:
        query = f"""
            CREATE OR REPLACE TABLE `{output_table}`
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
                    WHEN COUNTIF(LOWER(trang_thai) = '🟢') > 0 THEN 'active'
                    ELSE 'inactive'
                END AS trang_thai,
                SUM(chi_tieu) AS chi_tieu
            FROM `{source_table}`
            GROUP BY nen_tang, ma_ngan_sach_cap_1, chuong_trinh, noi_dung, hinh_thuc, thang, nhan_su
        """

    # 1.2.4. Execute query
    try:
        bigquery_client.get_table(source_table)  # check source tồn tại
        print(f"🔄 [MART] Aggregating daily spend into monthly table {output_table}...")
        logging.info(f"🔄 [MART] Aggregating daily spend into monthly table {output_table}...")
        bigquery_client.query(query).result()
        print(f"✅ [MART] Successfully created monthly aggregation table {output_table}.")
        logging.info(f"✅ [MART] Successfully created monthly aggregation table {output_table}.")
    except NotFound:
        print(f"⚠️ [MART] Daily spend table {source_table} not found, skipping aggregation.")
        logging.warning(f"⚠️ [MART] Daily spend table {source_table} not found, skipping aggregation.")

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

    # 2.1.2. Define source tables
    input_dataset_budget = f"{COMPANY}_dataset_budget_api_mart"
    input_table_spend = f"{PROJECT}.{COMPANY}_dataset_{PLATFORM}_api_mart.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_aggregation_spend"
    input_table_budget = f"{PROJECT}.{input_dataset_budget}.{COMPANY}_table_budget_{DEPARTMENT}_{ACCOUNT}_allocation_monthly"
    output_dataset_recon = f"{COMPANY}_dataset_{PLATFORM}_api_mart"
    output_table_recon = f"{PROJECT}.{output_dataset_recon}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_reconciliation_spend"

    # 2.1.3. Build query (check supplier trước)
    if DEPARTMENT == "marketing" and ACCOUNT == "supplier":
        query_recon = f"""
            CREATE OR REPLACE TABLE `{output_table_recon}`
            CLUSTER BY ma_ngan_sach_cap_1, chuong_trinh, supplier_name, thang AS
            WITH budget AS (
                SELECT * FROM `{input_table_budget}`
            ),
            cost AS (
                SELECT * FROM `{input_table_spend}`
            )
            SELECT
                COALESCE(b.ma_ngan_sach_cap_1, c.ma_ngan_sach_cap_1) AS ma_ngan_sach_cap_1,
                COALESCE(b.chuong_trinh, c.chuong_trinh) AS chuong_trinh,
                COALESCE(b.noi_dung, c.noi_dung) AS noi_dung,
                COALESCE(b.nen_tang, c.nen_tang, "other") AS nen_tang,
                COALESCE(b.hinh_thuc, c.hinh_thuc, "other") AS hinh_thuc,
                COALESCE(b.supplier_name, c.supplier_name) AS supplier_name,
                COALESCE(b.thang, c.thang) AS thang,
                b.thoi_gian_bat_dau,
                b.thoi_gian_ket_thuc,
                b.ngan_sach_thuc_chi,
                c.chi_tieu AS so_tien_thuc_tieu,
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

                    -- Delayed (quá 3 ngày mà vẫn chưa có chi tiêu/trạng thái, và chưa kết thúc)
                    WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                        AND CURRENT_DATE() >= b.thoi_gian_bat_dau
                        AND CURRENT_DATE() <= b.thoi_gian_ket_thuc
                        AND (c.chi_tieu IS NULL OR c.chi_tieu = 0)
                        AND (c.trang_thai IS NULL OR TRIM(c.trang_thai) = '')
                        AND DATE_DIFF(CURRENT_DATE(), b.thoi_gian_bat_dau, DAY) > 3
                        THEN "⚠️ Delayed"                 

                    -- Ended without Spend
                    WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                        AND CURRENT_DATE() > b.thoi_gian_ket_thuc
                        AND (c.chi_tieu IS NULL OR c.chi_tieu = 0)
                        THEN "🔒 Ended without Spend"

                    -- Low Spend (active only)
                    WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                        AND LOWER(COALESCE(c.trang_thai, '')) = 'active'
                        AND SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), b.ngan_sach_thuc_chi) < 0.95
                        AND DATE_DIFF(b.thoi_gian_ket_thuc, b.thoi_gian_bat_dau, DAY) > 0
                        AND SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), b.ngan_sach_thuc_chi) <
                            SAFE_DIVIDE(DATE_DIFF(CURRENT_DATE(), b.thoi_gian_bat_dau, DAY),
                                        DATE_DIFF(b.thoi_gian_ket_thuc, b.thoi_gian_bat_dau, DAY)) - 0.3
                        THEN "📉 Low Spend"

                    -- High Spend (active only)
                    WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                        AND LOWER(COALESCE(c.trang_thai, '')) = 'active'
                        AND SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), b.ngan_sach_thuc_chi) < 0.95
                        AND DATE_DIFF(b.thoi_gian_ket_thuc, b.thoi_gian_bat_dau, DAY) > 0
                        AND SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), b.ngan_sach_thuc_chi) >
                            SAFE_DIVIDE(DATE_DIFF(CURRENT_DATE(), b.thoi_gian_bat_dau, DAY),
                                        DATE_DIFF(b.thoi_gian_ket_thuc, b.thoi_gian_bat_dau, DAY)) + 0.3
                        THEN "📈 High Spend"

                    -- Near Completion (active only)
                    WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                        AND LOWER(COALESCE(c.trang_thai, '')) = 'active'
                        AND SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), b.ngan_sach_thuc_chi) BETWEEN 0.95 AND 0.99
                        THEN "🟢 Near Completion"

                    -- Off (Paused/Stopped, not ended, not over budget)
                    WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                        AND (c.chi_tieu IS NOT NULL AND c.chi_tieu > 0)
                        AND LOWER(COALESCE(c.trang_thai, '')) != 'active'
                        AND COALESCE(c.chi_tieu, 0) < b.ngan_sach_thuc_chi * 0.99
                        THEN "⚪ Off (Early Stopped)"

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

                    -- In Progress (active only)
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
             AND b.supplier_name = c.supplier_name
        """
    else:
        query_recon = f"""
            CREATE OR REPLACE TABLE `{output_table_recon}`
            CLUSTER BY ma_ngan_sach_cap_1, chuong_trinh, nhan_su, thang AS
            WITH budget AS (
                SELECT * FROM `{input_table_budget}`
            ),
            cost AS (
                SELECT * FROM `{input_table_spend}`
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

                    -- Delayed (quá 3 ngày mà vẫn chưa có chi tiêu/trạng thái, và chưa kết thúc)
                    WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                        AND CURRENT_DATE() >= b.thoi_gian_bat_dau
                        AND CURRENT_DATE() <= b.thoi_gian_ket_thuc
                        AND (c.chi_tieu IS NULL OR c.chi_tieu = 0)
                        AND (c.trang_thai IS NULL OR TRIM(c.trang_thai) = '')
                        AND DATE_DIFF(CURRENT_DATE(), b.thoi_gian_bat_dau, DAY) > 3
                        THEN "⚠️ Delayed"                 

                    -- Ended without Spend
                    WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                        AND CURRENT_DATE() > b.thoi_gian_ket_thuc
                        AND (c.chi_tieu IS NULL OR c.chi_tieu = 0)
                        THEN "🔒 Ended without Spend"

                    -- Low Spend (active only)
                    WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                        AND LOWER(COALESCE(c.trang_thai, '')) = 'active'
                        AND SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), b.ngan_sach_thuc_chi) < 0.95
                        AND DATE_DIFF(b.thoi_gian_ket_thuc, b.thoi_gian_bat_dau, DAY) > 0
                        AND SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), b.ngan_sach_thuc_chi) <
                            SAFE_DIVIDE(DATE_DIFF(CURRENT_DATE(), b.thoi_gian_bat_dau, DAY),
                                        DATE_DIFF(b.thoi_gian_ket_thuc, b.thoi_gian_bat_dau, DAY)) - 0.3
                        THEN "📉 Low Spend"

                    -- High Spend (active only)
                    WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                        AND LOWER(COALESCE(c.trang_thai, '')) = 'active'
                        AND SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), b.ngan_sach_thuc_chi) < 0.95
                        AND DATE_DIFF(b.thoi_gian_ket_thuc, b.thoi_gian_bat_dau, DAY) > 0
                        AND SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), b.ngan_sach_thuc_chi) >
                            SAFE_DIVIDE(DATE_DIFF(CURRENT_DATE(), b.thoi_gian_bat_dau, DAY),
                                        DATE_DIFF(b.thoi_gian_ket_thuc, b.thoi_gian_bat_dau, DAY)) + 0.3
                        THEN "📈 High Spend"

                    -- Near Completion (active only)
                    WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                        AND LOWER(COALESCE(c.trang_thai, '')) = 'active'
                        AND SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), b.ngan_sach_thuc_chi) BETWEEN 0.95 AND 0.99
                        THEN "🟢 Near Completion"

                    -- Off (Paused/Stopped, not ended, not over budget)
                    WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                        AND (c.chi_tieu IS NOT NULL AND c.chi_tieu > 0)
                        AND LOWER(COALESCE(c.trang_thai, '')) != 'active'
                        AND COALESCE(c.chi_tieu, 0) < b.ngan_sach_thuc_chi * 0.99
                        THEN "⚪ Off (Early Stopped)"

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

                    -- In Progress (active only)
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

    # 2.1.4. Execute query
    try:
        print(f"🔁 [MART] Executing reconciliation query to create {output_table_recon}...")
        logging.info(f"🔁 [MART] Executing reconciliation query to create {output_table_recon}...")
        bigquery_client.query(query_recon).result()
        print(f"✅ [MART] Successfully created reconciliation table {output_table_recon}.")
        logging.info(f"✅ [MART] Successfully created reconciliation table {output_table_recon}.")
    except Exception as e:
        print(f"❌ [MART] Failed to build reconciliation table {output_table_recon} due to {e}.")
        logging.error(f"❌ [MART] Failed to build reconciliation table {output_table_recon} due to {e}.")