"""
===========================================================================
RECON MATERIALIZATION MODULE
---------------------------------------------------------------------------
This function aggregates daily advertising costs across all supported
platforms (Facebook, TikTok, Google) into a unified summary table.

It dynamically checks the existence of individual platform-level spend 
tables in BigQuery, constructs UNION queries for available sources, and 
writes the result to a centralized `cost_all_daily` table, partitioned 
and clustered for efficient querying.

✔️ Dynamically supports multi-platform integration  
✔️ Skips platforms with missing tables (non-blocking)  
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

# Add internal Ads module for data handling
from config.config import (
    MAPPING_ADS_DATASET, 
    MAPPING_ADS_NETWORK
)

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

# 1.1. Build unified daily cost aggregation table from multiple advertising platforms
def mart_spend_all():
    print(f"🚀 [MART] Starting unified daily advertising spend aggregation for {COMPANY} company...")
    logging.info(f"🚀 [MART] Starting unified daily advertising spend aggregation for {COMPANY} company...")

    # 1.1.1. Prepare full table_id for upper layer(s)
    try:
        bigquery_client = bigquery.Client(project=PROJECT)
    except DefaultCredentialsError as e:
        raise RuntimeError(" ❌ [MART] Failed to initialize Google BigQuery client due to credentials error.") from e
    valid_source_tables = []
    for network in MAPPING_ADS_NETWORK:
        try:
            print(f"🔍 [MART] Scanning for {network} materialized dataset for advertising spend unification...")
            logging.warning(f"🔍 [MART] Scanning for {network} materialized dataset for advertising spend unification...")
            dataset_mart = MAPPING_ADS_DATASET[COMPANY]["mart"][network]
            print(f"✅ [MART] Successfully retrieved {network} materialized dataset for advertising spend unification.")
            logging.warning(f"✅ [MART] Successfully retrieved {network} materialized dataset for advertising spend unification.")
        except KeyError:
            print(f"⚠️ [MART] Materialized dataset for {network} not found then unification is skipped.")
            logging.warning(f"⚠️ [MART] Materialized dataset for {network} not found then unification is skipped.")
            continue
        source_table = f"{PROJECT}.{dataset_mart}.{COMPANY}_table_{network}_spend_all"
        try:
            print(f"🔍 [MART] Scanning for {network} materialized advertising spend table...")
            logging.warning(f"🔍 [MART] Scanning for {network} materialized advertising spend table...")
            bigquery_client.get_table(source_table)
            valid_source_tables.append(source_table)
            print(f"✅ [MART] Successfully retrieved {source_table} materialized table for advertising spend unification.")
            logging.warning(f"✅ [MART] Successfully retrieved {source_table} materialized table for advertising spend unification.")
        except NotFound:
            print(f"⚠️ [MART] Source table {source_table} not found then unification is skipped.")
            logging.warning(f"⚠️ [MART] Source table {source_table} not found then unification is skipped.")
    if not valid_source_tables:
        print("❌ [MART] Failed to unify advertising spend due to no valid source tables found.")
        logging.error("❌ [MART] Failed to unify advertising spend due to no valid source tables found.")
        return
    output_table = f"{PROJECT}.{COMPANY}_dataset_{PLATFORM}_api_mart.{COMPANY}_table_{PLATFORM}_spend_all"
    print(f"🔍 [MART] Preparing to build {output_table} unified advertising spend table...")
    logging.info(f"🔍 [MART] Preparing to build {output_table} unified advertising spend table...")

    # 1.1.2. Update existing table or create new table if it not exist
    try:
        print(f"🔍 [MART] Checking {output_table} unified advertising spend table existence...")
        logging.info(f"🔍 [MART] Checking {output_table} unified advertising spend table existence...")
        bigquery_client.get_table(output_table)
        print(f"✅ [MART] Successfully retrieved {output_table} unified advertising table existence.")
        logging.info(f"✅ [MART] Successfully retrieved {output_table} unified advertising table existence.")
    except NotFound:
        print(f"⚠️ [MART] Unified advertising spend table {output_table} not found then table creation will be proceeding...")
        logging.info(f"⚠️ [MART] Unified advertising spend table {output_table} not found then table creation will be proceeding...")
        schema = [
            bigquery.SchemaField("nen_tang", "STRING"),
            bigquery.SchemaField("ma_ngan_sach_cap_1", "STRING"),
            bigquery.SchemaField("chuong_trinh", "STRING"),
            bigquery.SchemaField("noi_dung", "STRING"),
            bigquery.SchemaField("hinh_thuc", "STRING"),
            bigquery.SchemaField("thang", "STRING"),
            bigquery.SchemaField("trang_thai","STRING"),
            bigquery.SchemaField("ngay", "DATE"),
            bigquery.SchemaField("nhan_su", "STRING"),
            bigquery.SchemaField("so_tien_thuc_tieu", "FLOAT"),
        ]
        table = bigquery.Table(output_table, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(field="ngay")
        table.clustering_fields = ["ma_ngan_sach_cap_1", "chuong_trinh", "noi_dung", "nhan_su"]
        print(f"🔍 [MART] Creating {table} unified advertising spend table...")
        logging.info(f"🔍 [MART] Creating {table} unified advertising spend table...")
        bigquery_client.create_table(table)
        print(f"✅ [MART] Successfully created {table} unified advertising spend table.")
        logging.info(f"✅ [MART] Successfully created {table} unified advertising spend table.")

    # 1.1.3. Query all input table(s) to build materialized table for unified advertising spend 
    union_sql = "\nUNION ALL\n".join([f"SELECT * FROM `{tbl}`" for tbl in valid_source_tables])
    query = f"""
        CREATE OR REPLACE TABLE `{output_table}`
        PARTITION BY ngay
        CLUSTER BY ma_ngan_sach_cap_1, chuong_trinh, noi_dung, nhan_su
        AS
        {union_sql}
    """
    try:
        print(f"🔁 [MART] Executing aggregation query to create {output_table} unified advertising spend table...")
        logging.info(f"🔁 [MART] Executing aggregation query to create {output_table} unified advertising spend table...")
        bigquery_client.query(query).result()
        logging.info(f"✅ [MART] Successfully created materialized table {output_table} for unified advertising spend.")
        print(f"✅ [MART] Successfully created materialized table {output_table} for unified advertising spend.")
    except Exception as e:
        logging.error(f"❌ [MART] Failed to excute aggregation query to build unified advertising spend table due to {e}.")
        print(f"❌ [MART] Failed to excute aggregation query to build unified advertising spend table due to {e}.")

# 2. BUILD MATERIALIZED TABLE FOR BUDGET ALLOCATION AND ADVERTISING SPEND RECONCILIATION

# 2.1 Build materialized table for monthly budget allocation and advertising spend reconciliation
def mart_recon_all():
    print("🚀 [MART] Starting monthly budget allocation and advertising reconciliation...")
    logging.info("🚀 [MART] Starting monthly budget allocation and advertising reconciliation...")

    # 2.1.1. Prepare full table_id for upper layer(s)
    try:
        bigquery_client = bigquery.Client(project=PROJECT)
    except DefaultCredentialsError as e:
        raise RuntimeError(" ❌ [MART] Failed to initialize Google BigQuery client due to credentials error.") from e
    valid_source_tables = []
    for network in MAPPING_ADS_NETWORK:
        try:
            input_dataset_spend = MAPPING_ADS_DATASET[COMPANY]["mart"][network]
            print(f"🔍 [MART] Scanning for {network} materialized dataset {input_dataset_spend} for advertising spend reconciliation...")
            logging.warning(f"🔍 [MART] Scanning for {network} materialized dataset {input_dataset_spend} for advertising spend reconciliation...")
        except KeyError:
            print(f"⚠️ [MART] Materialized dataset for {network} not found then reconciliation is skipped.")
            logging.warning(f"⚠️ [MART] Materialized dataset for {network} not found then reconciliation is skipped.")
            continue
        input_table = f"{PROJECT}.{input_dataset_spend}.{COMPANY}_table_{network}_spend_all"
        try:
            print(f"🔍 [MART] Scanning for {network} materialized advertising spend table for advertising spend reconciliation...")
            logging.warning(f"🔍 [MART] Scanning for {network} materialized advertising spend table for advertising spend reconciliation...")
            bigquery_client.get_table(input_table)
            valid_source_tables.append(input_table)
            print(f"✅ [MART] Successfully retrieved {input_table} materialized table for advertising spend unification.")
            logging.warning(f"✅ [MART] Successfully retrieved {input_table} materialized table for advertising spend unification.")
        except NotFound:
            print(f"⚠️ [MART] Source table {input_table} not found then reconciliation is skipped.")
            logging.warning(f"⚠️ [MART] Source table {input_table} not found then reconciliation is skipped.")
    if not valid_source_tables:
        print("❌ [MART] Failed to reconcile advertising spend due to no valid input tables found.")
        logging.error("❌ [MART] Failed to reconcile advertising spend due to no valid input tables found.")
        return

    # 2.1.2. Update existing monthly advertising spend table or create new table if it not exist
    output_table_spend = f"{PROJECT}.{COMPANY}_dataset_{PLATFORM}_api_mart.{COMPANY}_table_{PLATFORM}_spend_monthly"
    print(f"🔍 [MART] Preparing to build {output_table_spend} monthly advertising spend table...")
    logging.info(f"🔍 [MART] Preparing to build {output_table_spend} monthly advertising spend table...")
    try:
        print(f"🔍 [MART] Checking {output_table_spend} monthly advertising spend table existence...")
        logging.info(f"🔍 [MART] Checking {output_table_spend} monthly advertising spend table existence...")
        bigquery_client.get_table(output_table_spend)
        print(f"✅ [MART] Successfully retrieved {output_table_spend} monthly advertising spend table existence.")
        logging.info(f"✅ [MART] Successfully retrieved {output_table_spend} monthly advertising spend table existence.")
    except NotFound:
        schema = [
            bigquery.SchemaField("nen_tang", "STRING"),
            bigquery.SchemaField("ma_ngan_sach_cap_1", "STRING"),
            bigquery.SchemaField("chuong_trinh", "STRING"),
            bigquery.SchemaField("noi_dung", "STRING"),
            bigquery.SchemaField("hinh_thuc", "STRING"),
            bigquery.SchemaField("thang", "STRING"),
            bigquery.SchemaField("nhan_su", "STRING"),
            bigquery.SchemaField("trang_thai", "STRING"),
            bigquery.SchemaField("so_tien_thuc_tieu", "FLOAT"),
        ]
        table = bigquery.Table(output_table_spend, schema=schema)
        table.clustering_fields = ["ma_ngan_sach_cap_1", "chuong_trinh", "nhan_su", "thang"]
        print(f"🔍 [MART] Creating {table} monthly advertising spend table...")
        logging.info(f"🔍 [MART] Creating {table} monthly advertising spend table...")
        bigquery_client.create_table(table)
        print(f"✅ [MART] Successfully created {table} monthly advertising spend table.")
        logging.info(f"✅ [MART] Successfully created {table} monthly advertising spend table.")

    # 2.1.3. Query all input table(s) to build materialized table for monthly advertising spend
    union_sql = " UNION ALL ".join([
        f"""
        SELECT 
            nen_tang, 
            ma_ngan_sach_cap_1, 
            chuong_trinh, 
            noi_dung, 
            hinh_thuc, 
            thang, 
            nhan_su, 
            chi_tieu AS so_tien_thuc_tieu,
            LOWER(trang_thai) AS trang_thai
        FROM `{tbl}`
        WHERE chi_tieu > 0
        """ for tbl in valid_source_tables
    ])

    query_monthly = f"""
        TRUNCATE TABLE `{output_table_spend}`;
        INSERT INTO `{output_table_spend}` (
            nen_tang,
            ma_ngan_sach_cap_1,
            chuong_trinh,
            noi_dung,
            hinh_thuc,
            thang,
            nhan_su,
            trang_thai,
            so_tien_thuc_tieu
        )
        SELECT 
            nen_tang, 
            ma_ngan_sach_cap_1, 
            chuong_trinh, 
            noi_dung, 
            hinh_thuc, 
            thang, 
            nhan_su, 
            CASE 
                WHEN MAX(CASE WHEN trang_thai = 'active' THEN 1 ELSE 0 END) = 1 
                    THEN 'active'
                ELSE 'paused'
            END AS trang_thai,
            SUM(so_tien_thuc_tieu) AS so_tien_thuc_tieu
        FROM ({union_sql})
        GROUP BY nen_tang, ma_ngan_sach_cap_1, chuong_trinh, noi_dung, hinh_thuc, thang, nhan_su;
    """
    try:
        print(f"🔁 [MART] Executing aggregation query to create {output_table_spend} monthly advertising spend table...")
        logging.info(f"🔁 [MART] Executing aggregation query to create {output_table_spend} monthly advertising spend table...")
        bigquery_client.query(query_monthly).result()
        logging.info(f"✅ [MART] Successfully created materialized table {output_table_spend} for monthly advertising spend.")
        print(f"✅ [MART] Successfully created materialized table {output_table_spend} for monthly advertising spend.")
    except Exception as e:
        logging.error(f"❌ [MART] Failed to excute aggregation query to build monthly advertising spend table due to {e}.")
        print(f"❌ [MART] Failed to excute aggregation query to build monthly advertising spend table due to {e}.")
        return

    # 2.1.4. Build and execute reconciliation query
    input_dataset_budget = f"{COMPANY}_dataset_budget_api_mart"
    input_table_budget = f"{PROJECT}.{input_dataset_budget}.{COMPANY}_table_budget_all_monthly"
    output_dataset_recon = f"{COMPANY}_dataset_{PLATFORM}_api_mart"
    output_table_recon = f"{PROJECT}.{output_dataset_recon}.{COMPANY}_table_ads_recon_all"
    query_recon = f"""
        CREATE OR REPLACE TABLE `{output_table_recon}`
        CLUSTER BY ma_ngan_sach_cap_1, chuong_trinh, noi_dung, thang AS
        WITH budget AS (
            SELECT * FROM `{input_table_budget}`
        ),
        cost AS (
            SELECT * FROM `{output_table_spend}`
        ),
        calc AS (
            SELECT
                COALESCE(b.ma_ngan_sach_cap_1, c.ma_ngan_sach_cap_1) AS ma_ngan_sach_cap_1,
                COALESCE(b.chuong_trinh, c.chuong_trinh) AS chuong_trinh,
                COALESCE(b.noi_dung, c.noi_dung) AS noi_dung,
                COALESCE(b.nen_tang, c.nen_tang) AS nen_tang,
                COALESCE(b.hinh_thuc, c.hinh_thuc) AS hinh_thuc,
                COALESCE(b.thang, c.thang) AS thang,
                b.* EXCEPT(ma_ngan_sach_cap_1, chuong_trinh, noi_dung, nen_tang, hinh_thuc, thang),
                c.nhan_su,
                c.so_tien_thuc_tieu,
                c.trang_thai,
                SAFE_DIVIDE(c.so_tien_thuc_tieu, b.ngan_sach_thuc_chi) AS spending_ratio,
                DATE_DIFF(CURRENT_DATE(), b.thoi_gian_bat_dau, DAY) AS days_running,
                DATE_DIFF(b.thoi_gian_ket_thuc, b.thoi_gian_bat_dau, DAY) AS total_days
            FROM budget b
            FULL OUTER JOIN cost c
            ON b.ma_ngan_sach_cap_1 = c.ma_ngan_sach_cap_1
            AND b.chuong_trinh = c.chuong_trinh
            AND b.noi_dung = c.noi_dung
            AND COALESCE(b.nen_tang,"other") = COALESCE(c.nen_tang,"other")
            AND COALESCE(b.hinh_thuc,"other") = COALESCE(c.hinh_thuc,"other")
            AND b.thang = c.thang
        )
        SELECT
            *,
            CASE
                WHEN COALESCE(ngan_sach_thuc_chi, 0) = 0
                    AND COALESCE(so_tien_thuc_tieu, 0) = 0
                THEN "🚫 No Budget"    
                
                WHEN COALESCE(ngan_sach_thuc_chi,0) = 0
                    AND COALESCE(so_tien_thuc_tieu,0) > 0
                THEN CASE 
                        WHEN LOWER(COALESCE(trang_thai,"")) = 'active' THEN "⚠️ Spend without Budget (Running)" 
                        ELSE "⚪ Spend without Budget (Stopped)" 
                    END

                WHEN COALESCE(ngan_sach_thuc_chi, 0) > 0
                    AND CURRENT_DATE() < thoi_gian_bat_dau
                    AND COALESCE(so_tien_thuc_tieu, 0) = 0
                THEN "🕓 Not Yet Started"                     

                WHEN COALESCE(ngan_sach_thuc_chi, 0) > 0
                    AND CURRENT_DATE() >= thoi_gian_bat_dau
                    AND COALESCE(so_tien_thuc_tieu, 0) = 0
                    AND DATE_DIFF(CURRENT_DATE(), thoi_gian_bat_dau, DAY) <= 3
                THEN "⚪ Not Set"

                WHEN COALESCE(ngan_sach_thuc_chi, 0) > 0
                    AND CURRENT_DATE() >= thoi_gian_bat_dau
                    AND COALESCE(so_tien_thuc_tieu, 0) = 0
                    AND DATE_DIFF(CURRENT_DATE(), thoi_gian_bat_dau, DAY) > 3
                THEN "⚠️ Delay"                

                WHEN COALESCE(ngan_sach_thuc_chi,0) > 0
                    AND CURRENT_DATE() < thoi_gian_bat_dau
                    AND COALESCE(so_tien_thuc_tieu,0) > 0
                THEN CASE 
                        WHEN LOWER(COALESCE(trang_thai,"")) != 'active' THEN "⚪ Early Spend (Stopped)" 
                        ELSE "⚠️ Early Spend (Running)" 
                    END

                WHEN COALESCE(ngan_sach_thuc_chi,0) > 0
                    AND SAFE_DIVIDE(so_tien_thuc_tieu, ngan_sach_thuc_chi) < 0.95
                    AND DATE_DIFF(thoi_gian_ket_thuc, thoi_gian_bat_dau, DAY) > 0
                    AND SAFE_DIVIDE(so_tien_thuc_tieu, ngan_sach_thuc_chi) < SAFE_DIVIDE(DATE_DIFF(CURRENT_DATE(), thoi_gian_bat_dau, DAY), DATE_DIFF(thoi_gian_ket_thuc, thoi_gian_bat_dau, DAY)) - 0.3
                THEN CASE 
                        WHEN LOWER(COALESCE(trang_thai,"")) != 'active' THEN "⚪ Inactive" 
                        ELSE "⚠️ Low Spend" 
                    END

                WHEN COALESCE(ngan_sach_thuc_chi,0) > 0
                    AND SAFE_DIVIDE(so_tien_thuc_tieu, ngan_sach_thuc_chi) < 0.95
                    AND DATE_DIFF(thoi_gian_ket_thuc, thoi_gian_bat_dau, DAY) > 0
                    AND SAFE_DIVIDE(so_tien_thuc_tieu, ngan_sach_thuc_chi) > SAFE_DIVIDE(DATE_DIFF(CURRENT_DATE(), thoi_gian_bat_dau, DAY), DATE_DIFF(thoi_gian_ket_thuc, thoi_gian_bat_dau, DAY)) + 0.3
                THEN CASE 
                        WHEN LOWER(COALESCE(trang_thai,"")) != 'active' THEN "⚪ Inactive" 
                        ELSE "⚠️ High Spend" 
                    END

                WHEN COALESCE(ngan_sach_thuc_chi,0) > 0
                    AND SAFE_DIVIDE(so_tien_thuc_tieu, ngan_sach_thuc_chi) BETWEEN 0.95 AND 0.99
                THEN CASE 
                        WHEN LOWER(COALESCE(trang_thai,"")) != 'active' THEN "⚪ Inactive" 
                        ELSE "🟢 Near Completion" 
                    END

                WHEN COALESCE(ngan_sach_thuc_chi,0) > 0
                    AND SAFE_DIVIDE(so_tien_thuc_tieu, ngan_sach_thuc_chi) > 0.99
                    AND so_tien_thuc_tieu < ngan_sach_thuc_chi * 1.01
                THEN CASE 
                        WHEN LOWER(COALESCE(trang_thai,"")) = 'active' THEN "⚠️ Completed but Running"
                        ELSE "🔵 Completed"
                    END

                WHEN COALESCE(ngan_sach_thuc_chi,0) > 0
                    AND so_tien_thuc_tieu >= ngan_sach_thuc_chi * 1.01
                    AND LOWER(COALESCE(trang_thai,"")) = 'active'
                THEN "🔴 Over Budget but Running"

                WHEN COALESCE(ngan_sach_thuc_chi,0) > 0
                    AND so_tien_thuc_tieu >= ngan_sach_thuc_chi * 1.01
                    AND LOWER(COALESCE(trang_thai,"")) != 'active'
                THEN "⚪ Over Budget and Stopped"

                WHEN COALESCE(ngan_sach_thuc_chi,0) > 0
                    AND COALESCE(so_tien_thuc_tieu,0) > 0
                    AND LOWER(COALESCE(trang_thai,"")) = 'active'
                THEN "🟢 In Progress"

                ELSE "❓ Not Recognized"
            END AS trang_thai_chien_dich
        FROM calc
    """
    try:
        print(f"🔁 [MART] Executing aggregation query to create {output_table_recon} reconciled advertising spend table...")
        logging.info(f"🔁 [MART] Executing aggregation query to create {output_table_recon} reconciled advertising spend table...")
        bigquery_client.query(query_recon).result()
        logging.info(f"✅ [MART] Successfully created materialized table {output_table_recon} for reconciled advertising spend.")
        print(f"✅ [MART] Successfully created materialized table {output_table_recon} for reconciled advertising spend.")
    except Exception as e:
        logging.error(f"❌ [MART] Failed to excute aggregation query to build reconciled advertising spend table due to {e}.")
        print(f"❌ [MART] Failed to excute aggregation query to build reconciled advertising spend table due to {e}.")

if __name__ == "__main__":
    mart_recon_all()