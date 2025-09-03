"""
==================================================================
BUDGET STAGING MODULE
------------------------------------------------------------------
This module ingests budget allocation data from Google Sheets into 
Google BigQuery, forming the raw data layer of the marketing pipeline.

It reads structured budget data from predefined worksheets, performs 
basic cleaning (e.g. normalizing column names, coercing numeric fields), 
and loads them into partitioned BigQuery tables per sheet/month.

✔️ Uses Google Sheets API via `gspread` with service account auth  
✔️ Supports sheet filtering and naming normalization per config  
✔️ Automatically writes to BigQuery with schema autodetect (WRITE_TRUNCATE)

⚠️ This module is strictly limited to *raw-layer ingestion*.  
It does **not** handle staging, aggregation, or mart-level logic.
==================================================================
"""
# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add logging ultilities for integration
import logging

# Add Python Pandas libraries for integration
import pandas as pd

# Add Python "re" libraríe for integration
import re

# Add Google Authentication libraries for integration
from google.auth.exceptions import DefaultCredentialsError

# Add Google BigQuery library for integration
from google.cloud import bigquery

# Add internal Budget module for handling
from config.schema import ensure_table_schema

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

# 1. TRANSFORM BUDGET RAW DATA INTO CLEANED STAGING TABLES FOR MODELING AND ANALYSIS

# 1.1. TRANSFORM BUDGET RAW DATA INTO STAGNG TABLE WIH TIME PARTITIONING
def staging_budget_allocation():
    print("🚀 [STAGING] Starting unified staging process for all budget raw tables...")
    logging.info("🚀 [STAGING] Starting unified staging process for all budget raw tables...")

    # 1.1.1. Prepare id for raw layer in Google BigQuery
    try:
        try:
            client = bigquery.Client(project=PROJECT)
        except DefaultCredentialsError as e:
            raise RuntimeError(" ❌ [INGEST] Failed to initialize Google BigQuery client due to credentials error.") from e
        raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
        print(f"🔍 [STAGING] Using raw dataset {raw_dataset} to build staging table for budget allocation...")
        logging.info(f"🔍 [STAGING] Using raw dataset {raw_dataset} to build staging table for budget allocation...")
        staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
        staging_table_budget = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_allocation_monthly"
        print(f"🔍 [STAGING] Using staging dataset {raw_dataset} to build staging table for budget allocation...")
        logging.info(f"🔍 [STAGING] Using staging dataset {raw_dataset} to build staging table for budget allocation...")

    # 1.1.2. Scan all Facebook raw campaign insights table(s)
        print("🔍 [STAGING] Scanning all raw budget allocation table(s)...")
        logging.info("🔍 [STAGING] Scanning all raw budget allocation table(s)...")
        tables = client.list_tables(f"{PROJECT}.{raw_dataset}")
        raw_tables = [table.table_id for table in tables]
        if not raw_tables:
            print(f"⚠️ [STAGING] No raw budget allocation table(s) found for {COMPANY} company then staging is skipped.")
            logging.warning(f"⚠️ [STAGING] No raw budget allocation table(s) found for {COMPANY} company then staging is skipped.")
            return
        print(f"✅ [STAGING] Successfully found {len(raw_tables)} raw budget table(s) for {COMPANY} company.")
        logging.info(f"✅ [STAGING] Successfully found {len(raw_tables)} raw budget table(s) for {COMPANY} company.")

        # 1.1.3. Query and join all Facebook campaign insights tables
        print(f"🔍 [STAGING] Preparing to build staging table {staging_table_budget} for budget allocation...")
        logging.info(f"🔍 [STAGING] Preparing to build staging table {staging_table_budget} for budget allocation...")
        all_dfs = []
        for table in raw_tables:
            table_id = f"{PROJECT}.{raw_dataset}.{table}"
            print(f"🔄 [STAGING] Querying raw budget allocation table {table_id}...")
            logging.info(f"🔄 [STAGING] Querying raw budget allocation table {table_id}...")
            worksheet_name = table.split('_')[4].lower() if len(table.split('_')) >= 5 else ""
            if worksheet_name == "supplier":
                print(f"⚠️ [STAGING] Found supplier table {table} then it is skipped.")
                logging.warning(f"⚠️ [STAGING] Found supplier table {table} then it is skipped.")
                continue
            is_monthly = bool(re.match(r"^m\d{6}$", worksheet_name))
            is_special = bool(re.match(r"^[a-z]+202\d$", worksheet_name)) 
            if not (is_monthly or is_special):
                print(f"⚠️ [STAGING] Found unknown format table {table} then it is skipped.")
                logging.warning(f"⚠️ [STAGING] Found unknown format table {table} then it is skipped.")
                continue
            try:
                df = client.query(f"SELECT * FROM `{table_id}`").result().to_dataframe()
                if df.empty:
                    print(f"⚠️ [STAGING] Budget allocation table {table} is empty then query is skipped.")
                    logging.warning(f"⚠️ [STAGING] Budget allocation table {table} is empty then query is skipped.")
                    continue
                parts = table.split('_')
                if len(parts) < 5:
                    print(f"⚠️ [STAGING] Unexpected raw budget allocation table name format {table} then query is skipped.")
                    logging.warning(f"⚠️ [STAGING] Unexpected raw budget allocation table name format {table} then query is skipped.")
                    continue
                department = parts[3] 
                worksheet_name = parts[4].lower()
                is_monthly = bool(re.match(r"^m\d{6}$", worksheet_name))
                if is_monthly:
                    yyyy = worksheet_name[3:7]
                    mm = worksheet_name[1:3]
                    df["thang"] = f"{yyyy}-{mm}"
                    df["special_event_name"] = None
                else:
                    if "thang" not in df.columns:
                        df["thang"] = "unknown"
                    df["special_event_name"] = worksheet_name
                df["department"] = department      
                all_dfs.append(df)
            except Exception as e:
                print(f"❌ [STAGING] Failed to query raw budget allocation table {table_id} due to {e}.")
                logging.warning(f"❌ [STAGING] Failed to query raw budget allocation table {table_id} due to {e}.")
                continue
        df_all = pd.concat(all_dfs, ignore_index=True)
        print(f"✅ [STAGING] Succssfully combined {len(df_all)} rows from raw tables of {COMPANY} company.")
        logging.info(f"✅ [STAGING] Succssfully combined {len(df_all)} rows from raw tables of {COMPANY} company.")
    
    # 1.1.4. Enrich budget allocation
        try:
            print(f"🔄 [STAGING] Enriching fields for {len(df_all)} row(s) of staging budget allocation field(s)...")
            logging.info(f"🔄 [STAGING] Enriching fields for {len(df_all)} row(s) of staging budget allocation field(s)...")
            for col in ["ngan_sach_ban_dau", "ngan_sach_dieu_chinh", "ngan_sach_bo_sung"]:
                if col in df_all.columns:
                    df_all[col] = pd.to_numeric(df_all[col], errors="coerce").fillna(0).astype(int)
                else:
                    df_all[col] = 0
            df_all["ngan_sach_thuc_chi"] = df_all["ngan_sach_ban_dau"] + df_all["ngan_sach_dieu_chinh"] + df_all["ngan_sach_bo_sung"]
            df_all["thoi_gian_bat_dau"] = pd.to_datetime(df_all.get("thoi_gian_bat_dau"), errors="coerce")
            df_all["thoi_gian_ket_thuc"] = pd.to_datetime(df_all.get("thoi_gian_ket_thuc"), errors="coerce")
            today = pd.to_datetime("today").normalize()
            df_all["tong_so_ngay_thuc_chay"] = (df_all["thoi_gian_ket_thuc"] - df_all["thoi_gian_bat_dau"]).dt.days
            df_all["tong_so_ngay_da_qua"] = ((today - df_all["thoi_gian_bat_dau"]).dt.days.clip(lower=0))
            df_all["ngan_sach_he_thong"] = (df_all["ma_ngan_sach_cap_1"] == "KP") * df_all["ngan_sach_thuc_chi"]
            df_all["ngan_sach_nha_cung_cap"] = (df_all["ma_ngan_sach_cap_1"] == "NC") * df_all["ngan_sach_thuc_chi"]
            df_all["ngan_sach_kinh_doanh"] = (df_all["ma_ngan_sach_cap_1"] == "KD") * df_all["ngan_sach_thuc_chi"]
            df_all["ngan_sach_tien_san"] = (df_all["ma_ngan_sach_cap_1"] == "CS") * df_all["ngan_sach_thuc_chi"]
            df_all["ngan_sach_tuyen_dung"] = (df_all["ma_ngan_sach_cap_1"] == "HC") * df_all["ngan_sach_thuc_chi"]
            df_all["ngan_sach_khac"] = df_all["ngan_sach_tien_san"] + df_all["ngan_sach_tuyen_dung"]
            df_all = ensure_table_schema(df_all, "staging_budget_allocation")
            print(f"✅ [STAGING] Successfully enriched {len(df_all)} row(s) of staging budget allocation.")
            logging.info(f"✅ [STAGING] Successfully enriched {len(df_all)} row(s) of staging budget allocation.")  
        except Exception as e:
            print(f"❌ [STAGING] Failed to enrich staging budget allocation due to {e}.")
            logging.error(f"❌ [STAGING] Failed to enrich staging budget allocation due to {e}.")
            raise

    # 1.1.5. Enforce schema for Facebook staging campaign insights
        try:
            print(f"🔄 [STAGING] Enforcing schema for {len(df_all)} row(s) of staging budget allocation...")
            logging.info(f"🔄 [STAGING] Enforcing schema for {len(df_all)} row(s) of staging budget allocation...")
            df_all = ensure_table_schema(df_all, "staging_budget_allocation")
            print(f"✅ [STAGING] Successfully enforced {len(df_all)} row(s) of staging budget allocation.")
            logging.info(f"✅ [STAGING] Successfully enforced {len(df_all)} row(s) of Ftaging budget allocation.")
        except Exception as e:
            print(f"❌ [INGEST] Failed to enforce schema for {len(df_all)} row(s) of staging budget allocation due to {e}.")
            logging.error(f"❌ [INGEST] Failed to enforce schema for {len(df_all)} row(s) of staging budget allocation due to {e}.")
            raise        

    # 1.1.6. Upload Facebook staging campaign insights to Google BigQuery raw table        
        try:
            print(f"🔍 [STAGING] Uploading {len(df_all)} row(s) of staging budget allocation table {staging_table_budget}...")
            logging.info(f"🔍 [STAGING] Uploading {len(df_all)} row(s) of staging budget allocation table {staging_table_budget}...")
            try:
                client = bigquery.Client(project=PROJECT)
            except DefaultCredentialsError as e:
                raise RuntimeError("❌ [STAGING] Failed to initialize Google BigQuery client due to credentials error.") from e
            if "special_event_name" in df_all.columns:
                df_all["special_event_name"] = df_all["special_event_name"].where(
                    pd.notnull(df_all["special_event_name"]), None
                )
            clustering_fields = [
                f for f in ["ma_ngan_sach_cap_1", "chuong_trinh", "thang", "nen_tang"]
                if f in df_all.columns
            ]
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
                source_format=bigquery.SourceFormat.PARQUET,
                clustering_fields=clustering_fields if clustering_fields else None
            )
            load_job = client.load_table_from_dataframe(
                df_all,
                staging_table_budget,
                job_config=job_config
            )
            load_job.result()
            print(f"✅ [STAGING] Successfully uploaded {len(df_all)} row(s) of staging budget allocation to table {staging_table_budget}.")
            logging.info(f"✅ [STAGING] Successfully uploaded {len(df_all)} row(s) of staging budget allocation to table {staging_table_budget}.")
        except Exception as e:
            print(f"❌ [STAGING] Failed to upload staging budget allocation due to {e}.")
            logging.error(f"❌ [STAGING] Failed to upload staging budget allocation due to {e}.")
    except Exception as e:
        print(f"❌ [STAGING] Faild to unify staging budget allocation due to {e}.")
        logging.error(f"❌ [STAGING] Faild to unify staging budget allocation due to {e}.")

if __name__ == "__main__":
    staging_budget_allocation()

