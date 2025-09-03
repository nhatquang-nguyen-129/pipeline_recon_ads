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
    print("🚀 [MART] Starting to build materialized table for monthly budget allocation...")
    logging.info("🚀 [MART] Starting to build materialized table for monthly budget allocation...")

    # 1.1.1. Prepare table_id
    try:
        try:
            bigquery_client = bigquery.Client(project=PROJECT)
        except DefaultCredentialsError as e:
            raise RuntimeError(" ❌ [MART] Failed to initialize Google BigQuery client due to credentials error.") from e
        staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_gspread_staging"
        staging_table = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_allocation_monthly"
        print(f"🔍 [MART] Using staging table {staging_table} to build materialized table for budget allocation...")
        logging.info(f"🔍 [MART] Using staging table {staging_table} to build materialized table for budget allocation...")
        mart_dataset = f"{COMPANY}_dataset_{PLATFORM}_gspread_mart"
        mart_table_monthly = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{PLATFORM}_all_all_allocation_monthly"
        print(f"🔍 [INGEST] Preparing to build materialized table {mart_table_monthly} for budget allocation...")
        logging.info(f"🔍 [INGEST] Preparing to build materialized table {mart_table_monthly} for budget allocation...")

    # 1.1.2. Query all staging table(s)
        query = f"""
            CREATE OR REPLACE TABLE `{mart_table_monthly}` AS
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
        bigquery_client.query(query).result()
        count_query = f"SELECT COUNT(1) AS row_count FROM `{mart_table_monthly}`"
        row_count = list(bigquery_client.query(count_query).result())[0]["row_count"]
        print(f"✅ [MART] Successfully created materialized table {mart_table_monthly} with {row_count} row(s) for monthly budget allocation.")
        logging.info(f"✅ [MART] Successfully created materialized table {mart_table_monthly} with {row_count} row(s) for monthly budget allocation.")
    except Exception as e:
        print(f"❌ [MART] Failed to build materialized table for Facebook campaign spending due to {e}.")
        logging.error(f"❌ [MART] Failed to build materialized table for Facebook campaign spending due to {e}.")

# 1.2 Build materialized table for special event(s) budget allocation by union all staging tables
def mart_budget_event():
    print("🚀 [MART] Starting to build materialized table for special event(s) budget allocation...")
    logging.info("🚀 [MART] Starting to build materialized table for special event(s) budget allocation...")

    try:
        # 1.2.1. Prepare full table_id for raw layer in BigQuery
        try:
            bigquery_client = bigquery.Client(project=PROJECT)
        except DefaultCredentialsError as e:
            raise RuntimeError(" ❌ [MART] Failed to initialize Google BigQuery client due to credentials error.") from e
        staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_gspread_staging"
        staging_table = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_allocation_monthly"
        print(f"🔍 [MART] Using {staging_table} staging table to build materialized table for special event(s) budget allocation...")
        logging.info(f"🔍 [MART] Using {staging_table} staging table to build materialized table for special event(s) budget allocation...")
        mart_dataset = f"{COMPANY}_dataset_{PLATFORM}_gspread_mart"
        print(f"🔍 [MART] Preparing to build materialized table for special event(s) budget allocation in {mart_dataset} dataset...")
        logging.info(f"🔍 [MART] Preparing to build materialized table for special event(s) budget allocation in {mart_dataset} dataset...")

        # 1.2.2. Get distinct special_event_name
        try:
            query_get_events = f"""
                SELECT DISTINCT special_event_name
                FROM `{staging_table}`
                WHERE special_event_name IS NOT NULL
                AND special_event_name != 'None'
            """
            query_job = bigquery_client.query(query_get_events)
            results = query_job.result()
            special_events = [row.special_event_name for row in results]
            print(f"✅ [MART] Successfully retrieved {len(special_events)} special event(s) included {special_events}.")
            logging.info(f"✅ [MART] Successfully retrieved {len(special_events)} special event(s) included {special_events}.")
            if not special_events:
                print(f"⚠️ [MART] No special events found in {staging_table} staging table of budget allocation.")
                logging.warning(f"⚠️ [MART] No special events found in {staging_table} staging table of budget allocation.")
                return
        except Exception as e:
            print(f"❌ [MART] Failed while retrieving special events from {staging_table} due to {e}.")
            logging.error(f"❌ [MART] Failed while retrieving special events from {staging_table} due to {e}.")
            raise

        # 1.2.3. Loop through each special event
        for special_event_name in special_events:
            mart_table_event = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{special_event_name}_allocation_monthly"
            print(f"🔍 [MART] Preparing to build materialized budget allocation {mart_table_event} table...")
            logging.info(f"🔍 [MART] Preparing to build materialized budget allocation {mart_table_event} table...")

            query_create_table = f"""
                CREATE OR REPLACE TABLE `{mart_table_event}` AS
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
                WHERE special_event_name = '{special_event_name}'
            """
            bigquery_client.query(query_create_table).result()
            count_query = f"SELECT COUNT(1) AS row_count FROM `{mart_table_event}`"
            row_count = list(bigquery_client.query(count_query).result())[0]["row_count"]
            print(f"✅ [MART] Successfully created materialized table {mart_table_event} with {row_count} row(s) for monthly budget allocation.")
            logging.info(f"✅ [MART] Successfully created materialized table {mart_table_event} with {row_count} row(s) for monthly budget allocation.")
    except Exception as e:
        print(f"❌ [MART] Failed to build materialized table for Facebook creative performance due to {e}.")
        logging.error(f"❌ [MART] Failed to build materialized table for Facebook creative performance due to {e}.")
        raise

# 1.3 Build materialized table for supplier budget allocation by union all staging tables
def mart_budget_supplier():
    print("🚀 [MART] Starting to build materialized table for supplier budget allocation...")
    logging.info("🚀 [MART] Starting to build materialized table for supplier budget allocation...")

    try:
        # 1.3.1. Prepare table_id
        try:
            bigquery_client = bigquery.Client(project=PROJECT)
        except DefaultCredentialsError as e:
            raise RuntimeError(" ❌ [MART] Failed to initialize Google BigQuery client due to credentials error.") from e
        staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_gspread_staging"
        staging_table = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_allocation_monthly"
        print(f"🔍 [MART] Using staging table {staging_table} to build materialized table for supplier budget allocation...")
        logging.info(f"🔍 [MART] Using staging table {staging_table} to build materialized table for supplier budget allocation...")
        mart_dataset = f"{COMPANY}_dataset_{PLATFORM}_gspread_mart"
        mart_table_supplier = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{PLATFORM}_supplier_monthly"
        supplier_list_table = f"{PROJECT}.kids_dataset_budget_gspread_raw.kids_table_budget_supplier_list"
        print(f"🔍 [MART] Preparing to build materialized table {mart_table_supplier} for supplier budget allocation...")
        logging.info(f"🔍 [MART] Preparing to build materialized table {mart_table_supplier} for supplier budget allocation...")

        query = f"""
            CREATE OR REPLACE TABLE `{mart_table_supplier}` AS
            SELECT
                b.ma_ngan_sach_cap_1,
                b.chuong_trinh,
                s.brand_name AS nha_cung_cap,
                b.noi_dung,
                b.nen_tang,
                b.hinh_thuc,
                b.thang,
                b.thoi_gian_bat_dau,
                b.thoi_gian_ket_thuc,
                b.tong_so_ngay_thuc_chay,
                b.tong_so_ngay_da_qua,
                b.ngan_sach_ban_dau,
                b.ngan_sach_dieu_chinh,
                b.ngan_sach_bo_sung,
                b.ngan_sach_thuc_chi,
                b.ngan_sach_he_thong,
                b.ngan_sach_nha_cung_cap,
                b.ngan_sach_kinh_doanh,
                b.ngan_sach_tien_san,
                b.ngan_sach_tuyen_dung,
                b.ngan_sach_khac
            FROM `{staging_table}` b
            LEFT JOIN `{supplier_list_table}` s
              ON REGEXP_CONTAINS(b.chuong_trinh, s.brand_name)  -- match theo brand_name
            WHERE b.ma_ngan_sach_cap_1 = 'NC'
        """
        bigquery_client.query(query).result()
        count_query = f"SELECT COUNT(1) AS row_count FROM `{mart_table_supplier}`"
        row_count = list(bigquery_client.query(count_query).result())[0]["row_count"]
        print(f"✅ [MART] Successfully created materialized table {mart_table_supplier} with {row_count} row(s) for supplier budget allocation.")
        logging.info(f"✅ [MART] Successfully created materialized table {mart_table_supplier} with {row_count} row(s) for supplier budget allocation.")
    except Exception as e:
        print(f"❌ [MART] Failed to build materialized table for supplier budget allocation due to {e}.")
        logging.error(f"❌ [MART] Failed to build materialized table for supplier budget allocation due to {e}.")

if __name__ == "__main__":
    mart_budget_supplier()