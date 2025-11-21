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

✔️ Dynamically identifies all available Facebook Ads staging tables  
✔️ Applies data transformation, standardization, and type enforcement  
✔️ Performs daily-level aggregation of campaign performance metrics  
✔️ Creates partitioned and clustered MART tables in Google BigQuery  
✔️ Ensures consistency and traceability across the data pipeline  

⚠️ This module is exclusively responsible for materialized layer  
construction. It does not perform data ingestion, API fetching 
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
def mart_budget_all() -> dict:
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
            query = f"""
            where_clause = f"WHERE account = '{ACCOUNT}'"
            query_specific = f"""
                CREATE OR REPLACE TABLE `{mart_table_specific}` AS
                SELECT
                    ma_ngan_sach_cap_1,
                    chuong_trinh,
                    noi_dung,
                    nen_tang,
                    hinh_thuc,
                    thang,
                    thoi_gian_bat_dau,
                    thoi_gian_ket_thuc,
                    tong_so_ngay_thuc_chay,
                    tong_so_ngay_da_qua,
                    ngan_sach_ban_dau,
                    ngan_sach_dieu_chinh,
                    ngan_sach_bo_sung,
                    ngan_sach_thuc_chi,
                    ngan_sach_he_thong,
                    ngan_sach_nha_cung_cap,
                    ngan_sach_kinh_doanh,
                    ngan_sach_tien_san,
                    ngan_sach_tuyen_dung,
                    ngan_sach_khac
                FROM `{staging_table}`
                {where_clause}
            """
            print(f"🔄 [MART] Querying staging budget allocation table {staging_table} to build materialized table {mart_table_specific} for specific case(s)...")
            logging.info(f"🔄 [MART] Querying staging budget allocation table {staging_table} to build materialized table {mart_table_specific} for specific case(s)...")
            google_bigquery_client.query(query_specific).result()
            count_specific = list(google_bigquery_client.query(
                f"SELECT COUNT(1) AS row_count FROM `{mart_table_specific}`"
            ).result())[0]["row_count"]
            print(f"✅ [MART] Successfully (re)built materialized table {mart_table_specific} with {count_specific} row(s).")
            logging.info(f"✅ [MART] Successfully (re)built materialized table {mart_table_specific} with {count_specific} row(s).")
    except Exception as e:
        print(f"❌ [MART] Failed to build materialized table(s) due to {e}.")
        logging.error(f"❌ [MART] Failed to build materialized table(s) due to {e}.")