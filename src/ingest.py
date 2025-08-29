"""
==================================================================
BUDGET INGESTION MODULE
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

# Add datetime utilities for integration
from datetime import datetime

# Add logging ultilities for integration
import logging

# Add Python Pandas library for integration
import pandas as pd

# Add timezone ultilites for integration
import pytz

# Add UUID libraries for integration
import uuid

# Add Google Authentication libraries for integration
from google.auth import default
from google.auth.exceptions import DefaultCredentialsError

# Add Google API core library for integration
from google.api_core.exceptions import NotFound

# Add Google Cloud library for integration
from google.cloud import bigquery

# Add Google Sheet libraries for integration
import gspread

# Add internal Google Sheet module for configuration
from config.schema import ensure_table_schema

# Add internal Google Sheet module for handing
from src.enrich import enrich_budget_insights
from src.fetch import fetch_budget_allocation

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

# 1. INGEST BUDGET ALLOCATION FROM GOOGLE SHEETS TO GOOGLE BIGQUERY RAW TABLE

# 1.1. Ingest budget allocation from Google Sheets to Google BigQuery raw table
def ingest_budget_allocation(
    sheet_id: str,
    worksheet_name: str,
    thang: str,
) -> pd.DataFrame:
    if not COMPANY or not ACCOUNT:
        raise ValueError("Missing COMPANY or PLATFORM or ACCOUNT environment variable.")

    print(f"🚀 [INGEST] Starting to ingest budget allocation for month {thang}...")
    logging.info(f"🚀 [INGEST] Starting to ingest budget allocation for month {thang}...")

    # 1.1.1. Call Google Sheets API
    try:
        print(f"🔍 [INGEST] Fetching budget allocation for month {thang} from API...")
        logging.info(f"🔍 [INGEST] Fetching budget allocation for month {thang} from API...")
        try:
            scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
            creds, _ = default(scopes=scopes)
            gc = gspread.Client(auth=creds)
            gc.session = gspread.requests.AuthorizedSession(creds)
        except DefaultCredentialsError as e:
            raise RuntimeError("❌ [INGEST] Failed to initialize Google Sheets client due to credentials error.") from e
        except Exception as e:
            raise RuntimeError(f"❌ [INGEST] Failed to initialize Google Sheets client due to {e}.") from e
        df = fetch_budget_allocation(gc, sheet_id, worksheet_name)
        print(f"✅ [INGEST] Successfully fetching {len(df)} rows(s) of budget allocation.")
        logging.info(f"✅ [INGEST] Successfully fetching {len(df)} rows(s) of budget allocation.")
        if df.empty:
            print("⚠️ [INGEST] Empty budget allocation returned.")
            logging.warning("⚠️ [INGEST] Empty budget allocation returned.")
            return df
    except Exception as e:
        print(f"❌ [INGEST] Failed to fetch budget allocation due to {e}.")
        logging.error(f"❌ [INGEST] Failed to fetch budget allocation due to {e}.")
        return pd.DataFrame()

        # 1.1.2 Enrich dataframe
    try:
        print(f"🔁 [INGEST] Enriching budget allocation for month {thang} with {len(df)} row(s)...")
        logging.info(f"🔁 [INGEST] Enriching budget allocation for month {thang} with {len(df)} row(s)...")
        df = enrich_budget_insights(df)
        df["last_updated_at"] = datetime.utcnow().replace(tzinfo=pytz.UTC)
        print(f"✅ [INGEST] Successfully enriched budget allocation for {thang} with {len(df)} row(s).")
        logging.info(f"✅ [INGEST] Successfully enriched budget allocation for {thang} with {len(df)} row(s).")
    except Exception as e:
        print(f"❌ [INGEST] Failed to enrich budget allocation for {thang} due to {e}.")
        logging.error(f"❌ [INGEST] Failed to enrich budget allocation for {thang} due to {e}.")
        raise

    # 1.1.3. Prepare table_id
    raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
    table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_allocation_{worksheet_name}"
    print(f"🔍 [INGEST] Proceeding to ingest budget allocation for {thang} with {table_id} table_id...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest budget allocation for {thang} with {table_id} table_id...")
    
    # 1.1.4. Enforce schema
    try:
        print(f"🔄 [INGEST] Enforcing schema for {len(df)} row(s) of budget allocation...")
        logging.info(f"🔄 [INGEST] Enforcing schema for {len(df)} row(s) of budget allocation...")
        df = ensure_table_schema(df, "ingest_budget_allocation")
        print(f"✅ [INGEST] Successfully enforced schema for {len(df)} row(s) of budget allocation.")
        logging.info(f"✅ [INGEST] Successfully enforced schema for {len(df)} row(s) of budget allocation.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to enforce schema for budget allocation due to {e}.")
        logging.error(f"❌ [INGEST] Failed to enforce schema for budget allocation due to {e}.")
        raise

    # 1.1.5. Delete existing row(s) by thang or create new table if not exist
    try:
        print(f"🔍 [INGEST] Checking budget allocation table {table_id} existence...")
        logging.info(f"🔍 [INGEST] Checking budget allocation table {table_id} existence...")
        df = df.drop_duplicates()
        try:
            client = bigquery.Client(project=PROJECT)
        except DefaultCredentialsError as e:
            raise RuntimeError(" ❌ [INGEST] Failed to initialize Google BigQuery client due to credentials error.") from e
        try:
            client.get_table(table_id)
            table_exists = True
        except Exception:
            table_exists = False
        if not table_exists:
            print(f"⚠️ [INGEST] Budget allocation table {table_id} not found then table creation will be proceeding...")
            logging.info(f"⚠️ [INGEST] Budget allocation table {table_id} not found then table creation will be proceeding...")
            schema = []
            for col, dtype in df.dtypes.items():
                if dtype.name.startswith("int"):
                    bq_type = "INT64"
                elif dtype.name.startswith("float"):
                    bq_type = "FLOAT64"
                elif dtype.name == "bool":
                    bq_type = "BOOL"
                elif "datetime" in dtype.name:
                    bq_type = "TIMESTAMP"
                else:
                    bq_type = "STRING"
                schema.append(bigquery.SchemaField(col, bq_type))
            table = bigquery.Table(table_id, schema=schema)
            effective_partition = "date" if "date" in df.columns else None
            if effective_partition:
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=effective_partition
                )
            clustering_fields = ["thang"]
            filtered_clusters = [f for f in clustering_fields if f in df.columns]
            if filtered_clusters:
                table.clustering_fields = filtered_clusters
                print(f"🔍 [INGEST] Creating table {table_id} with clustering {filtered_clusters} field(s)...")
                logging.info(f"🔍 [INGEST] Creating table {table_id} with clustering {filtered_clusters} field(s)...")
            table = client.create_table(table)
            print(f"✅ [INGEST] Successfully created table {table_id} with clustering {clustering_fields}.")
            logging.info(f"✅ [INGEST] Successfully created table {table_id} with clustering {clustering_fields}.")
        else:
            print(f"⚠️ [INGEST] Budget allocation table {table_id} exists then existing row(s) deletion with unique key {thang} will be proceeding...")
            logging.info(f"⚠️ [INGEST] Budget allocation table {table_id} exists then existing row(s) deletion with unique key {thang} will be proceeding...")
            unique_keys = pd.DataFrame({"thang": [thang]}).dropna().drop_duplicates()
            if not unique_keys.empty:
                temp_table_id = f"{PROJECT}.{raw_dataset}.temp_table_budget_allocation_delete_keys_{uuid.uuid4().hex[:8]}"
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
                client.load_table_from_dataframe(unique_keys, temp_table_id, job_config=job_config).result()
                delete_query = f"""
                    DELETE FROM `{table_id}` AS main
                    WHERE EXISTS (
                        SELECT 1 FROM `{temp_table_id}` AS temp
                        WHERE CAST(main.thang AS STRING) = CAST(temp.thang AS STRING)
                    )
                """
                result = client.query(delete_query).result()
                client.delete_table(temp_table_id, not_found_ok=True)
                deleted_rows = result.num_dml_affected_rows
                print(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) for unique key {thang} from budget allocation table {table_id}.")
                logging.info(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) for unique key {thang} from budget allocation table {table_id}.")
            else:
                print(f"⚠️ [INGEST] No unique key {thang} found then existing row(s) deletion for budget allocation table {table_id} is skipped.")
                logging.warning(f"⚠️ [INGEST] No unique key {thang} found then existing row(s) deletion for budget allocation table {table_id} is skipped.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to ingest budget allocation for table {table_id} due to {e}.")
        logging.error(f"❌ [INGEST] Failed to ingest budget allocation for table {table_id} due to {e}.")
        raise

    # 1.1.6. Upload to BigQuery
    try:
        print(f"🔍 [INGEST] Uploading {len(df)} row(s) of budget allocation to table {table_id}...")
        logging.info(f"🔍 [INGEST] Uploading {len(df)} row(s) of budget allocation to table {table_id}...")
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
        print(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of budget allocation to {table_id}.")
        logging.info(f"✅ [INGEST] Successfully ingested {len(df)} row(s) of budget allocation into {table_id}.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to load budget allocation into {table_id} due to {e}.")
        logging.error(f"❌ [INGEST] Failed to load budget allocation into {table_id} due to {e}.")
        raise
    return df

if __name__ == "__main__":
    # Thông tin sheet muốn ingest
    sheet_id = "1tWQ19_3cGFoCeYE5bG4P7OYPEz_qXZncAVojtmJ3Gd8"
    worksheet_name = "m082025"  # tab cụ thể trên Google Sheet
    thang = "2025-08"  # hardcode để test

    ingest_budget_allocation(
        sheet_id=sheet_id,
        worksheet_name=worksheet_name,
    )

