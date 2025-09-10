"""
==================================================================
BUDGET MATERIALIZATION MODULE
------------------------------------------------------------------
This module builds the MART layer for marketing budget allocation 
by transforming and standardizing data from the staging layer 
(Google Sheets ingestion output) into finalized analytical tables.

It produces two types of MART tables:
1. A consolidated monthly budget table containing all programs  
2. Separate monthly budget tables for each special event program

✔️ Reads environment-based configuration to determine company scope  
✔️ Pulls source data from staging BigQuery tables populated via ingestion  
✔️ Creates or replaces MART tables with standardized column structure  
✔️ Dynamically generates special event MART tables based on 
   `special_event_name` flag in source data  

⚠️ This module is strictly responsible for *MART layer construction*.  
It does not handle raw data ingestion from Google Sheets or upstream ETL.
==================================================================
"""
# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add logging ultilies for integration
import logging

# Add Google Authentication libraries for integration
from google.auth.exceptions import DefaultCredentialsError

# Add Google CLoud libraries for integration
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

# 1. TRANSFORM BUDGET STAGING DATA INTO MONTHLY MATERIALIZED TABLE IN GOOGLE BIGQUERY

# 1.1 Build materialized table for monthly budget allocation by union all staging tables
def mart_budget_allocation():
    print("🚀 [MART] Starting to build materialized table(s) for monthly budget allocation...")
    logging.info("🚀 [MART] Starting to build materialized table(s) for monthly budget allocation...")

    # 1.1.1. Prepare table_id
    try:
        staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
        staging_table = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_allocation_monthly"
        mart_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_mart"
   
    # 1.1.2. Initialize Google BigQuery client
        try:
            print(f"🔍 [MART] Initializing Google BigQuery client for project {PROJECT}...")
            logging.info(f"🔍 [MART] Initializing Google BigQuery client for project {PROJECT}...")
            bigquery_client = bigquery.Client(project=PROJECT)
            print(f"✅ [MART] Successfully initialized Google BigQuery client for {PROJECT}.")
            logging.info(f"✅ [MART] Successfully initialized Google BigQuery client for {PROJECT}.")
        except DefaultCredentialsError as e:
            raise RuntimeError(f"❌ [MART] Failed to initialize Google BigQuery client due to credentials error.") from e
        except Exception as e:
            print(f"❌ [MART] Failed to initialize Google BigQuery client due to {str(e)}.")
            logging.error(f"❌ [MART] Failed to initialize Google BigQuery client due to {str(e)}.")

    # 1.1.3. Create materialized table for montly budget allocation
        print(f"🔄 [MART] Querying staging budget allocation {staging_table} table for monthly budget allocation creation...")
        logging.info(f"🔄 [MART] Querying staging budget allocation {staging_table} table for monthly budget allocation creation...")
        mart_table_all = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{PLATFORM}_all_all_allocation_monthly"
        query_all = f"""
            CREATE OR REPLACE TABLE `{mart_table_all}` AS
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
        """
        bigquery_client.query(query_all).result()
        count_all_all = list(bigquery_client.query(
            f"SELECT COUNT(1) AS row_count FROM `{mart_table_all}`"
        ).result())[0]["row_count"]
        print(f"✅ [MART] Successfully created materialized table {mart_table_all} with {count_all_all} row(s) for monthly budget allocation.")
        logging.info(f"✅ [MART] Successfully created materialized table {mart_table_all} with {count_all_all} row(s) for monthly budget allocation.")

        # 1.1.4. Create materialized table for budget allocation (all departments/accounts)
        if not (DEPARTMENT == "all" and ACCOUNT == "all"):
            mart_table_specific = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_allocation_monthly"
            where_clause = ""
            if DEPARTMENT != "all" and ACCOUNT != "all":
                where_clause = f"WHERE department = '{DEPARTMENT}' AND account = '{ACCOUNT}'"
            elif DEPARTMENT != "all":
                where_clause = f"WHERE department = '{DEPARTMENT}'"
            elif ACCOUNT != "all":
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
                    ngan_sach_thuc_chi
                FROM `{staging_table}`
                {where_clause}
            """
            bigquery_client.query(query_specific).result()
            count_specific = list(bigquery_client.query(
                f"SELECT COUNT(1) AS row_count FROM `{mart_table_specific}`"
            ).result())[0]["row_count"]
            print(f"✅ [MART] Successfully created {mart_table_specific} with {count_specific} row(s).")
            logging.info(f"✅ [MART] Successfully created {mart_table_specific} with {count_specific} row(s).")
    except Exception as e:
        print(f"❌ [MART] Failed to build materialized table(s) due to {e}.")
        logging.error(f"❌ [MART] Failed to build materialized table(s) due to {e}.")