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
def mart_budget_all():
    print("🚀 [MART] Starting to build materialized table(s) for monthly budget allocation...")
    logging.info("🚀 [MART] Starting to build materialized table(s) for monthly budget allocation...")

    try:
        # 1.1.1. Prepare BigQuery client
        try:
            bigquery_client = bigquery.Client(project=PROJECT)
        except DefaultCredentialsError as e:
            raise RuntimeError("❌ [MART] Failed to initialize Google BigQuery client due to credentials error.") from e

        # 1.1.2. Define datasets and base tables
        staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
        staging_table = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_allocation_monthly"
        mart_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_mart"

        # ------------------------
        # 1.2. Create ALL mart table
        # ------------------------
        mart_table_all = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{PLATFORM}_all_all_allocation_monthly"
        print(f"🔍 [MART] Building ALL mart table {mart_table_all} from {staging_table}...")
        logging.info(f"🔍 [MART] Building ALL mart table {mart_table_all} from {staging_table}...")

        query_all = f"""
            CREATE OR REPLACE TABLE `{mart_table_all}` AS
            SELECT
                phong_ban,
                tai_khoan,
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

        # Count rows in ALL mart table
        count_all = list(bigquery_client.query(
            f"SELECT COUNT(1) AS row_count FROM `{mart_table_all}`"
        ).result())[0]["row_count"]
        print(f"✅ [MART] Created ALL mart table {mart_table_all} with {count_all} row(s).")
        logging.info(f"✅ [MART] Created ALL mart table {mart_table_all} with {count_all} row(s).")

        # ------------------------
        # 1.3. Create mart tables per (phong_ban, tai_khoan)
        # ------------------------
        distinct_query = f"""
            SELECT DISTINCT phong_ban, tai_khoan
            FROM `{staging_table}`
            WHERE phong_ban IS NOT NULL AND tai_khoan IS NOT NULL
        """
        distinct_pairs = bigquery_client.query(distinct_query).result()

        for row in distinct_pairs:
            phong_ban = row["phong_ban"]
            tai_khoan = row["tai_khoan"]
            table_name = f"{COMPANY}_table_{PLATFORM}_{phong_ban}_{tai_khoan}_allocation_monthly"
            mart_table = f"{PROJECT}.{mart_dataset}.{table_name}"

            print(f"🔍 [MART] Building mart table {mart_table}...")
            logging.info(f"🔍 [MART] Building mart table {mart_table}...")

            query = f"""
                CREATE OR REPLACE TABLE `{mart_table}` AS
                SELECT *
                FROM `{staging_table}`
                WHERE phong_ban = '{phong_ban}' AND tai_khoan = '{tai_khoan}'
            """
            bigquery_client.query(query).result()

            count = list(bigquery_client.query(
                f"SELECT COUNT(1) AS row_count FROM `{mart_table}`"
            ).result())[0]["row_count"]
            print(f"✅ [MART] Created mart table {mart_table} with {count} row(s).")
            logging.info(f"✅ [MART] Created mart table {mart_table} with {count} row(s).")

    except Exception as e:
        print(f"❌ [MART] Failed to build mart tables for budget allocation due to {e}.")
        logging.error(f"❌ [MART] Failed to build mart tables for budget allocation due to {e}.")

if __name__ == "__main__":
    mart_budget_all()
