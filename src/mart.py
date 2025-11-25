"""
==================================================================
BUDGET MATERIALIZATION MODULE
------------------------------------------------------------------
This module materializes the final layer for Budget Allocation by 
aggregating and transforming data sourced from staging tables 
produced during the raw data ingestion process.

It serves as the final transformation stage, consolidating daily 
performance and cost metrics into analytics-ready BigQuery tables 
optimized for reporting, dashboarding, and business analysis.

✔️ Dynamically identifies all Budget Allocation staging tables  
✔️ Applies data transformation, standardization, and type enforcement  
✔️ Performs daily-level aggregation of campaign performance metrics  
✔️ Creates partitioned and clustered MART tables in Google BigQuery  
✔️ Ensures consistency and traceability across the data pipeline  

⚠️ This module is exclusively responsible for materialized layer  
construction. It does not perform data ingestion, API fetching, 
or enrichment tasks.
==================================================================
"""

# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add Python logging ultilities for integraton
import logging

# Add Python time ultilities for integration
import time

# Add Google Cloud modules for integration
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

# 1. BUILD MONTHLY MATERIALIZED TABLE FOR BUDGET ALLOCATION

# 1.1. Build materialized table for Budget Allocation by union all staging tables
def mart_budget_allocation() -> dict:
    print(f"🚀 [MART] Starting to build materialized table for Budget Allocation...")
    logging.info(f"🚀 [MART] Starting to build materialized table for Budget Allocation...")

    # 1.1.1. Start timing the Budget Allocation materialization
    mart_time_start = time.time()
    mart_sections_status = {}
    mart_sections_time = {}
    print(f"🔍 [MART] Proceeding to build materialized table for Budget Allocation at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [MART] Proceeding to build materialized table for Budget Allocation at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    try:
    
    # 1.1.2. Prepare Google BigQuery table_id for materialization
        mart_section_name = "[MART] Prepare Google BigQuery table_id for materialization"
        mart_section_start = time.time()        
        try: 
            staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
            staging_table_budget = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_allocation_monthly"
            print(f"🔍 [MART] Using staging table {staging_table_budget} to build materialized table for Budget Allocation...")
            logging.info(f"🔍 [MART] Using staging table {staging_table_budget} to build materialized table for Budget Allocation...")
            mart_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_mart"
            mart_table_budget = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{PLATFORM}_all_all_allocation_monthly"
            print(f"🔍 [MART] Preparing to build materialized table {mart_table_budget} for Budget Allocation...")
            logging.info(f"🔍 [MART] Preparing to build materialized table {mart_table_budget} for Budget Allocation...")
            mart_sections_status[mart_section_name] = "succeed"    
            mart_sections_time[mart_section_name] = round(time.time() - mart_section_start, 2)
        finally:
            mart_sections_time[mart_section_name] = round(time.time() - mart_section_start, 2)           

    # 1.1.3. Initialize Google BigQuery client
        mart_section_name = "[MART] Initialize Google BigQuery client"
        mart_section_start = time.time()
        try:
            print(f"🔍 [MART] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [MART] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            google_bigquery_client = bigquery.Client(project=PROJECT)
            print(f"✅ [MART] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [MART] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            mart_sections_status[mart_section_name] = "succeed"
        except Exception as e:
            mart_sections_status[mart_section_name] = "failed"
            print(f"❌ [MART] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [MART] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        finally:
            mart_sections_time[mart_section_name] = round(time.time() - mart_section_start, 2)

    # 1.1.4. Query all staging Budget Allocation table(s)
        mart_section_name = "[MART] Query all staging Budget Allocation table(s)"
        mart_section_start = time.time()    
        try:
            mart_query_budget = f"""
                CREATE OR REPLACE TABLE `{mart_table_budget}`
                CLUSTER BY thang, nhan_su, hang_muc AS
                SELECT
                    SAFE_CAST(enrich_account_name AS STRING) AS tai_khoan,
                    SAFE_CAST(enrich_account_department AS STRING) AS phong_ban,
                    SAFE_CAST(enrich_account_platform AS STRING) AS nen_tang,
                    SAFE_CAST(raw_budget_group AS STRING) AS ma_ngan_sach_cap_1,
                    SAFE_CAST(raw_budget_type AS STRING) AS ma_ngan_sach_cap_2,
                    SAFE_CAST(raw_program_track AS STRING) AS hang_muc,
                    SAFE_CAST(raw_program_group AS STRING) AS chuong_trinh,
                    SAFE_CAST(raw_program_type AS STRING) AS noi_dung,
                    SAFE_CAST(raw_budget_platform AS STRING) AS nen_tang,
                    SAFE_CAST(raw_budget_objective AS STRING) AS hinh_thuc,
                    SAFE_CAST(raw_date_month AS STRING) AS thang,
                    SAFE_CAST(raw_date_start AS TIMESTAMP) AS thoi_gian_bat_dau,
                    SAFE_CAST(raw_date_end AS TIMESTAMP) AS thoi_gian_ket_thuc,
                    SAFE_CAST(enrich_time_total AS INT) AS tong_so_ngay_thuc_chay,
                    SAFE_CAST(enrich_time_passed AS INT) AS tong_so_ngay_da_qua,
                    SAFE_CAST(raw_budget_initial AS INT) AS ngan_sach_ban_dau,
                    SAFE_CAST(raw_budget_adjusted AS INT) AS ngan_sach_dieu_chinh,
                    SAFE_CAST(raw_budget_additiona AS INT) AS ngan_sach_bo_sung,
                    SAFE_CAST(raw_budget_actual AS INT) AS ngan_sach_thuc_chi,
                    SAFE_CAST(enrich_budget_marketing AS INT) AS ngan_sach_he_thong,
                    SAFE_CAST(enrich_budget_supplier AS INT) AS ngan_sach_nha_cung_cap,
                    SAFE_CAST(enrich_budget_retail AS INT) AS ngan_sach_kinh_doanh,
                    SAFE_CAST(enrich_budget_customer AS INT) AS ngan_sach_tien_san,
                    SAFE_CAST(enrich_budget_recruitment AS INT) AS ngan_sach_tuyen_dung,
                FROM `{staging_table_budget}`
            """
            print(f"🔄 [MART] Querying staging Budget Allocation table {staging_table_budget} to create or replace materialized table...")
            logging.info(f"🔄 [MART] Querying staging Budget Allocation table {staging_table_budget} to create or replace materialized table...")
            google_bigquery_client.query(mart_query_budget).result()
            mart_query_count = f"SELECT COUNT(1) AS row_count FROM `{mart_table_budget}`"
            mart_rows_count = list(google_bigquery_client.query(mart_query_count).result())[0]["row_count"]
            print(f"✅ [MART] Successfully created or replace materialized table {mart_table_budget} for Budget Allocation with {mart_rows_count} row(s).")
            logging.info(f"✅ [MART] Successfully created or replace materialized table {mart_table_budget} for Budget Allocation with {mart_rows_count} row(s).")
            mart_sections_status[mart_section_name] = "succeed"
        except Exception as e:
            mart_sections_status[mart_section_name] = "failed"
            print(f"❌ [MART] Failed to create or replace materialized table for Budget Allocation due to {e}.")
            logging.error(f"❌ [MART] Failed to create or replace materialized table for Budget Allocation due to {e}.")
        finally:
            mart_sections_time[mart_section_name] = round(time.time() - mart_section_start, 2)

    # 1.1.5. Summarize materialization results for Budget Allocation
    finally:
        mart_time_elapsed = round(time.time() - mart_time_start, 2)
        mart_sections_total = len(mart_sections_status) 
        mart_sections_failed = [k for k, v in mart_sections_status.items() if v == "failed"] 
        mart_sections_succeeded = [k for k, v in mart_sections_status.items() if v == "succeed"]
        mart_sections_summary = list(dict.fromkeys(
            list(mart_sections_status.keys()) +
            list(mart_sections_time.keys())
        ))
        mart_sections_detail = {
            mart_section_summary: {
                "status": mart_sections_status.get(mart_section_summary, "unknown"),
                "time": round(mart_sections_time.get(mart_section_summary, 0.0), 2),
            }
            for mart_section_summary in mart_sections_summary
        }       
        if len(mart_sections_failed) > 0:
            print(f"❌ [MART] Failed to complete Budget Allocation materialization due to unsuccessful section(s) {', '.join(mart_sections_failed)}.")
            logging.error(f"❌ [MART] Failed to complete Budget Allocation materialization due to unsuccessful section(s) {', '.join(mart_sections_failed)}.")
            mart_status_final = "mart_failed_all"
        else:
            print(f"🏆 [MART] Successfully completed Budget Allocation materialization in {mart_time_elapsed}s.")
            logging.info(f"🏆 [MART] Successfully completed Budget Allocation materialization in {mart_time_elapsed}s.")
            mart_status_final = "mart_succeed_all"
        mart_results_final = {
            "mart_df_final": None,
            "mart_status_final": mart_status_final,
            "mart_summary_final": {
                "mart_time_elapsed": mart_time_elapsed,
                "mart_sections_total": mart_sections_total,
                "mart_sections_succeed": mart_sections_succeeded,
                "mart_sections_failed": mart_sections_failed,
                "mart_sections_detail": mart_sections_detail,
            },
        }
    return mart_results_final