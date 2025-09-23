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

# Add Python Pandas library for data processing
import pandas as pd

# Add Google Authentication libraries for integration
from google.api_core.exceptions import (
    GoogleAPICallError,
    Forbidden,
    NotFound,
    PermissionDenied, 
)
from google.auth import default
from google.auth.exceptions import DefaultCredentialsError
from google.auth.transport.requests import AuthorizedSession

# Add Google Spreadsheets API libraries for integration
import gspread
from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound, APIError, GSpreadException

# Add Google CLoud libraries for integration
from google.cloud import bigquery

# Add Google Secret Manager for integration
from google.cloud import secretmanager

# Add UUID libraries for integration
import uuid

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

    try:
    
    # 1.1.1. Prepare id
        staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
        staging_table = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_allocation_monthly"
        mart_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_mart"

    # 1.1.2. Initialize Google BigQuery client
        try:
            print(f"🔍 [MART] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [MART] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            google_bigquery_client = bigquery.Client(project=PROJECT)
            print(f"✅ [MART] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [MART] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        except DefaultCredentialsError as e:
            raise RuntimeError("❌ [MART] Failed to initialize Google BigQuery client due to credentials error.") from e
        except Forbidden as e:
            raise RuntimeError("❌ [MART] Failed to initialize Google BigQuery client due to permission denial.") from e
        except GoogleAPICallError as e:
            raise RuntimeError("❌ [MART] Failed to initialize Google BigQuery client due to API call error.") from e
        except Exception as e:
            raise RuntimeError(f"❌ [MARTT] Failed to initialize Google BigQuery client due to {e}.") from e

    # 1.1.3. Query staging table to build materialized table for supplier
        if DEPARTMENT == "marketing" and ACCOUNT == "supplier":
            mart_table_supplier = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{PLATFORM}_marketing_supplier_allocation_monthly"

            print(f"🔍 [MART] Building materialized table {mart_table_supplier} for supplier using supplier metadata from Google Sheets...")
            logging.info(f"🔍 [MART] Building materialized table {mart_table_supplier} for supplier using supplier metadata from Google Sheets...")

            try:
                print(f"🔍 [MART] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
                logging.info(f"🔍 [MART] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
                google_secret_client = secretmanager.SecretManagerServiceClient()
                print(f"✅ [MART] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
                logging.info(f"✅ [MART] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
            except DefaultCredentialsError as e:
                raise RuntimeError("❌ [MART] Failed to initialize Google Secret Manager client due to credentials error.") from e
            except PermissionDenied as e:
                raise RuntimeError("❌ [MART] Failed to initialize Google Secret Manager client due to permission denial.") from e
            except NotFound as e:
                raise RuntimeError("❌ [MART] Failed to initialize Google Secret Manager client because secret not found.") from e
            except GoogleAPICallError as e:
                raise RuntimeError("❌ [MART] Failed to initialize Google Secret Manager client due to API call error.") from e
            except Exception as e:
                raise RuntimeError(f"❌ [MART] Failed to initialize Google Secret Manager client due to unexpected error {e}.") from e
            
            print(f"🔍 [MART] Retrieving supplier budget information for {ACCOUNT} from Google Secret Manager...")
            logging.info(f"🔍 [MART] Retrieving supplier budget information for {ACCOUNT} from Google Secret Manager...") 
            budget_secret_id = f"{COMPANY}_secret_{DEPARTMENT}_{PLATFORM}_sheet_id_{ACCOUNT}"
            budget_secret_name = f"projects/{PROJECT}/secrets/{budget_secret_id}/versions/latest"
            response = google_secret_client.access_secret_version(name=budget_secret_name)
            sheet_id_supplier = response.payload.data.decode("UTF-8")
            print(f"✅ [MART] Successfully retrieved supplier budget allocation sheet_id {sheet_id_supplier} for environment variable {ACCOUNT} from Google Secret Manager.")
            logging.info(f"✅ [MART] Successfully retrieved supplier budget allocation sheet_id {sheet_id_supplier} for environment variable {ACCOUNT} from Google Secret Manager.")   

            try:
                print(f"🔍 [MART] Initializing Google Sheets client for sheet_id {sheet_id_supplier}...")
                logging.info(f"🔍 [MART] Initializing Google Sheets client for sheet_id {sheet_id_supplier}...")                
                scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
                creds, _ = default(scopes=scopes)
                google_gspread_client = gspread.Client(auth=creds)
                google_gspread_client.session = AuthorizedSession(creds)
                print(f"✅ [MART] Successfully initialized Google Sheets client for sheet_id {sheet_id_supplier} with scopes {scopes}.")
                logging.info(f"✅ [MART] Successfully initialized Google Sheets client for sheet_id {sheet_id_supplier} with scopes {scopes}.")
            except DefaultCredentialsError as e:
                raise RuntimeError("❌ [MART] Failed to initialize Google Sheets client due to credentials error.") from e
            except SpreadsheetNotFound as e:
                raise RuntimeError(f"❌ [MART] Failed to initialize Google Sheets client because spreadsheet {sheet_id_supplier} not found.") from e
            except WorksheetNotFound as e:
                raise RuntimeError(f"❌ [MART] Failed to initialize Google Sheets client because worksheet not found in spreadsheet {sheet_id_supplier}.") from e
            except APIError as e:
                raise RuntimeError("❌ [MART] Failed to initialize Google Sheets client due to API error.") from e
            except GSpreadException as e:
                raise RuntimeError("❌ [MART] Failed to initialize Google Sheets client due to Gspread client error.") from e
            except Exception as e:
                raise RuntimeError(f"❌ [MART] Failed to initialize Google Sheets client due to {e}.") from e         

            try:
                print(f"🔍 [MART] Retrieving budget allocation data in Google Sheets file {sheet_id_supplier} from Google Sheets API...")
                logging.info(f"🔍 [MART] Retrieving budget allocation data in Google Sheets file {sheet_id_supplier} from Google Sheets API...")
                worksheet = google_gspread_client.open_by_key(sheet_id_supplier).worksheet("supplier")
                records = worksheet.get_all_records()
                df_supplier = pd.DataFrame(records)
                print(f"✅ [MART] Retrieved {len(records)} row(s) of supplier metadata from {worksheet} in Google Sheets file {sheet_id_supplier}.")
                logging.info(f"✅ [MART] Retrieved {len(records)} row(s) of supplier metadata from {worksheet} in Google Sheets file {sheet_id_supplier}.")
            except Exception as e:
                print(f"❌ [MART] Failed to fetch supplier metadata from {worksheet} worksheet in {sheet_id_supplier} Google Sheets file due to {e}.")
                logging.error(f"❌ [MART] Failed to fetch supplier metadata from {worksheet} worksheet in {sheet_id_supplier} Google Sheets file due to {e}.")
            if "supplier_name" not in df_supplier.columns:
                raise RuntimeError(f"❌ [MART] Missing 'supplier_name' column in supplier sheet {sheet_id_supplier}.")

            print(f"🔍 [MART] Creating supplier metadata temporary from Google Sheets file {sheet_id_supplier} to build materialized table for supplier budget allocation...")
            logging.info(f"🔍 [MART] Creating supplier metadata temporary from Google Sheets file {sheet_id_supplier} to build materialized table for supplier budget allocation...")            
            temp_table_id = f"{PROJECT}.{mart_dataset}.temp_supplier_{uuid.uuid4().hex[:8]}"
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            google_bigquery_client.load_table_from_dataframe(df_supplier[["supplier_name"]], temp_table_id, job_config=job_config).result()
            print(f"✅ [MART] Successfully created supplier metadata temporary table {temp_table_id} with {len(df_supplier)} row(s).")
            logging.info(f"✅ [MART] Successfully created supplier metadata temporary table {temp_table_id} with {len(df_supplier)} row(s).")            
            try:
                query_supplier = f"""
                    CREATE OR REPLACE TABLE `{mart_table_supplier}` AS
                    SELECT
                        a.ma_ngan_sach_cap_1,
                        a.chuong_trinh,
                        a.noi_dung,
                        a.nen_tang,
                        a.hinh_thuc,
                        a.thang,
                        a.thoi_gian_bat_dau,
                        a.thoi_gian_ket_thuc,
                        a.tong_so_ngay_thuc_chay,
                        a.tong_so_ngay_da_qua,
                        a.ngan_sach_ban_dau,
                        a.ngan_sach_dieu_chinh,
                        a.ngan_sach_bo_sung,
                        a.ngan_sach_thuc_chi,
                        s.supplier_name
                    FROM `{staging_table}` a
                    LEFT JOIN `{temp_table_id}` s
                    ON REGEXP_CONTAINS(a.chuong_trinh, s.supplier_name)
                    WHERE a.department = 'marketing'
                    AND a.account = 'supplier'
                """
                print(f"🔄 [MART] Querying staging budget allocation table {staging_table} to build materialized table {mart_table_supplier} for supplier...")
                logging.info(f"🔄 [MART] Querying staging budget allocation table {staging_table} to build materialized table {mart_table_supplier} for supplier...")
                google_bigquery_client.query(query_supplier).result()
                count_supplier = list(google_bigquery_client.query(
                    f"SELECT COUNT(1) AS row_count FROM `{mart_table_supplier}`"
                ).result())[0]["row_count"]
                print(f"✅ [MART] Successfully (re)built materialized table {mart_table_supplier} for supplier with {count_supplier} row(s).")
                logging.info(f"✅ [MART] Successfully (re)built materialized table {mart_table_supplier} for supplier with {count_supplier} row(s).")
            finally:
                print(f"🔄 [MART] The materialized table rebuild process is finished then supplier metadata temporary table {temp_table_id} deletion will be proceeding...")
                logging.info(f"🔄 [MART] The materialized table rebuild process is finished then supplier metadata temporary table {temp_table_id} deletion will be proceeding...")
                google_bigquery_client.delete_table(temp_table_id, not_found_ok=True)
                print(f"✅ [MART] Successfully deleted supplier metadata temporary {temp_table_id} in Google BigQuery.")
                logging.info(f"✅ [MART] Successfully deleted supplier metadata temporary {temp_table_id} in Google BigQuery.")
        
        # 1.1.4. Query staging table to build materialized table for specific case(s)
        else:
            mart_table_specific = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_allocation_monthly"
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