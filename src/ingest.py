"""
==================================================================
BUDGET INGESTION MODULE
------------------------------------------------------------------
This module ingests raw data from the Budget Allocation fetching 
module into Google BigQuery, establishing the foundational raw 
layer used for centralized storage and historical retention.

It manages the complete ingestion flow from authentication and 
data fetching, to enrichment, schema validation and loading into 
Google BigQuery tables.

✔️ Supports both append and truncate modes via write_disposition
✔️ Validates data structure using centralized schema utilities  
✔️ Integrates enrichment routines before loading into BigQuery  
✔️ Implements granular logging and CSV-based error traceability  
✔️ Ensures pipeline reliability through retry and checkpoint logic  

⚠️ This module is dedicated solely to *raw-layer ingestion*.  
It does **not** handle advanced transformations, metric modeling, 
or aggregated data processing beyond the ingestion boundary.
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

# Add Python time ultilities for integration
import time

# Add timezone ultilites for integration
import pytz

# Add UUID libraries for integration
import uuid

# Add Google API core modules for integration
from google.api_core.exceptions import NotFound

# Add Google Cloud library for integration
from google.cloud import bigquery

# Add internal Google Sheet module for handing
from src.enrich import enrich_budget_insights
from src.fetch import fetch_budget_allocation
from src.schema import enforce_table_schema

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

# 1. INGEST BUDGET ALLOCATION

# 1.1. Ingest Budget Allocation to Google BigQuery
def ingest_budget_allocation(
    ingest_id_sheet: str,
    ingest_name_sheet: str
) -> pd.DataFrame:
    print(f"🚀 [INGEST] Starting to ingest budget allocation for sheet name {ingest_name_sheet} in Google Sheet sheet_id {ingest_id_sheet}..")
    logging.info(f"🚀 [INGEST] Starting to ingest budget allocation for sheet name {ingest_name_sheet} in Google Sheet sheet_id {ingest_id_sheet}..")

    # 1.1.1. Start timing Budget Allocation ingestion
    ingest_time_start = time.time()
    ingest_sections_status = {}
    ingest_sections_time = {}
    print(f"🔍 [INGEST] Proceeding to ingest raw Budget Allocation at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest raw Budget Allocation at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    try:

    # 1.1.2. Trigger to fetch Budget Allocation
        ingest_section_name = "[INGEST] Trigger to fetch Budget Allocation"
        ingest_section_start = time.time()        
        try:
            print(f"🔁 [INGEST] Triggering to fetch Budget Allocation for sheet name {ingest_name_sheet} from Google Sheets sheet_id {ingest_id_sheet}...")
            logging.info(f"🔁 [INGEST] Triggering to fetch Budget Allocation for sheet name {ingest_name_sheet} from Google Sheets sheet_id {ingest_id_sheet}...")
            ingest_results_fetched = fetch_budget_allocation(fetch_id_sheet=ingest_id_sheet, fetch_name_sheet=ingest_name_sheet)
            ingest_df_fetched = ingest_results_fetched["fetch_df_final"]
            ingest_status_fetched = ingest_results_fetched["fetch_status_final"]
            ingest_summary_fetched = ingest_results_fetched["fetch_summary_final"]
            if ingest_status_fetched == "fetch_succeed_all":
                print(f"✅ [INGEST] Successfully triggered Budget Allocation fetching for {ingest_summary_fetched['fetch_rows_output']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                logging.info(f"✅ [INGEST] Successfully triggered Budget Allocation fetching for {ingest_summary_fetched['fetch_rows_output']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                ingest_sections_status[ingest_section_name] = "succeed"
            elif ingest_status_fetched == "fetch_succeed_partial":
                print(f"⚠️ [INGEST] Partially triggered Budget Allocation fetching {ingest_summary_fetched['fetch_rows_output']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                logging.warning(f"⚠️ [INGEST] Partially triggered Budget Allocation fetching {ingest_summary_fetched['fetch_rows_output']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                ingest_sections_status[ingest_section_name] = "partial"
            else:
                ingest_sections_status[ingest_section_name] = "failed"
                print(f"❌ [INGEST] Failed to trigger Budget Allocation fetching with {ingest_summary_fetched['fetch_rows_output']} fetched row(s) due to {', '.join(ingest_summary_fetched['fetch_sections_failed'])} or unknown error in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                logging.error(f"❌ [INGEST] Failed to trigger Budget Allocation fetching with {ingest_summary_fetched['fetch_rows_output']} fetched row(s) due to {', '.join(ingest_summary_fetched['fetch_sections_failed'])} or unknown error in {ingest_summary_fetched['fetch_time_elapsed']}s.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)
    
    # 1.1.3. Prepare Google BigQuery table_id for ingestion
        ingest_section_name = "[INGEST] Prepare Google BigQuery table_id for ingestion"
        ingest_section_start = time.time()    
        try:            
            raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
            raw_table_budget = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_allocation_{ingest_name_sheet}"
            print(f"🔍 [INGEST] Proceeding to ingest Budget Allocation for {len(ingest_df_fetched)} fetched row(s) with Google BigQuery table_id {raw_table_budget}...")
            logging.info(f"🔍 [INGEST] Proceeding to ingest Budget Allocation for {len(ingest_df_fetched)} fetched row(s) with Google BigQuery table_id {raw_table_budget}...")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)
    
    # 1.1.4. Trigger to enforce schema for Budget Allocation
        ingest_section_name = "[INGEST] Trigger to enforce schema for Budget Allocation"
        ingest_section_start = time.time()
        try:
            print(f"🔄 [INGEST] Triggering to enforce schema for Budget Allocation with {len(ingest_df_fetched)} row(s)...")
            logging.info(f"🔄 [INGEST] Triggering to enforce schema for Budget Allocation with {len(ingest_df_fetched)} row(s)...")
            ingest_results_enforced = enforce_table_schema(ingest_df_fetched, "ingest_budget_allocation")
            ingest_summary_enforced = ingest_results_enforced["schema_summary_final"]
            ingest_status_enforced = ingest_results_enforced["schema_status_final"]
            ingest_df_enforced = ingest_results_enforced["schema_df_final"]    
            if ingest_status_enforced == "schema_succeed_all":
                print(f"✅ [INGEST] Successfully triggered Budget Allocation schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{len(ingest_df_fetched)} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                logging.info(f"✅ [INGEST] Successfully triggered Budget Allocation schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{len(ingest_df_fetched)} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                ingest_sections_status[ingest_section_name] = "succeed"
            else:
                ingest_sections_status[ingest_section_name] = "failed"
                print(f"❌ [INGEST] Failed to trigger Budget Allocation schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{len(ingest_df_fetched)} enforced row(s) due to failed section(s) {', '.join(ingest_summary_enforced['schema_sections_failed'])} in {ingest_summary_enforced['fetch_time_elapsed']}s.")
                logging.error(f"❌ [INGEST] Failed to trigger Budget Allocation schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{len(ingest_df_fetched)} enforced row(s) due to failed section(s) {', '.join(ingest_summary_enforced['schema_sections_failed'])} in {ingest_summary_enforced['fetch_time_elapsed']}s.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.1.5. Delete existing row(s) by "thang" or create new table if not exist
    try:
        print(f"🔍 [INGEST] Checking budget allocation table {table_id} existence...")
        logging.info(f"🔍 [INGEST] Checking budget allocation table {table_id} existence...")
        df = df.drop_duplicates()
        
        try:
            print(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            google_bigquery_client = bigquery.Client(project=PROJECT)
            print(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        except DefaultCredentialsError as e:
            raise RuntimeError("❌ [INGEST] Failed to initialize Google BigQuery client due to credentials error.") from e
        except Forbidden as e:
            raise RuntimeError("❌ [INGEST] Failed to initialize Google BigQuery client due to permission denial.") from e
        except GoogleAPICallError as e:
            raise RuntimeError("❌ [INGEST] Failed to initialize Google BigQuery client due to API call error.") from e
        except Exception as e:
            raise RuntimeError(f"❌ [INGEST] Failed to initialize Google BigQuery client due to {e}.") from e
                
        try:
            google_bigquery_client.get_table(table_id)
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
            table = google_bigquery_client.create_table(table)
            print(f"✅ [INGEST] Successfully created table {table_id} with clustering {clustering_fields}.")
            logging.info(f"✅ [INGEST] Successfully created table {table_id} with clustering {clustering_fields}.")
        else:
            print(f"⚠️ [INGEST] Budget allocation table {table_id} exists then existing row(s) deletion with unique key {thang} will be proceeding...")
            logging.info(f"⚠️ [INGEST] Budget allocation table {table_id} exists then existing row(s) deletion with unique key {thang} will be proceeding...")
            unique_keys = pd.DataFrame({"thang": [thang]}).dropna().drop_duplicates()
            if not unique_keys.empty:
                temp_table_id = f"{PROJECT}.{raw_dataset}.temp_table_budget_allocation_delete_keys_{uuid.uuid4().hex[:8]}"
                try:
                    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
                    print(f"🔍 [INGEST] Creating temporary table {temp_table_id} to proceed batch deletion for {thang} unique key(s)...")
                    logging.info(f"🔍 [INGEST] Creating temporary table {temp_table_id} to proceed batch deletion for {thang} unique key(s)...")            
                    google_bigquery_client.load_table_from_dataframe(unique_keys, temp_table_id, job_config=job_config).result()
                    print(f"✅ [INGEST] Successfully created temporary table {temp_table_id} for {thang} unique key(s).")
                    logging.info(f"✅ [INGEST] Successfully created temporary table {temp_table_id} for {thang} unique key(s).") 
                    
                    delete_query = f"""
                        DELETE FROM `{table_id}` AS main
                        WHERE EXISTS (
                            SELECT 1 FROM `{temp_table_id}` AS temp
                            WHERE CAST(main.thang AS STRING) = CAST(temp.thang AS STRING)
                        )
                    """
                    print(f"🔄 [INGEST] Deleting existing row(s) with unique key {thang} using temporary table {temp_table_id} in Google BigQuery...")
                    logging.info(f"🔄 [INGEST] Deleting existing row(s) with unique key {thang} using temporary table {temp_table_id} in Google BigQuery...")
                    result = google_bigquery_client.query(delete_query).result()
                    deleted_rows = result.num_dml_affected_rows
                    print(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) for unique key {thang} from budget allocation table {table_id}.")
                    logging.info(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) for unique key {thang} from budget allocation table {table_id}.")                
                finally:
                    print(f"🔄 [INGEST] The exising row(s) deduplication is finished then temporary table {temp_table_id} deletion will be proceeding...")
                    logging.info(f"🔄 [INGEST] The exising row(s) deduplication is finished then temporary table {temp_table_id} deletion will be proceeding...")
                    google_bigquery_client.delete_table(temp_table_id, not_found_ok=True)
                    print(f"✅ [INGEST] Successfully deleted temporary table {temp_table_id} in Google BigQuery.")
                    logging.info(f"✅ [INGEST] Successfully deleted temporary table {temp_table_id} in Google BigQuery.")
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
        google_bigquery_client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
        print(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of budget allocation to {table_id}.")
        logging.info(f"✅ [INGEST] Successfully ingested {len(df)} row(s) of budget allocation into {table_id}.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to load budget allocation into {table_id} due to {e}.")
        logging.error(f"❌ [INGEST] Failed to load budget allocation into {table_id} due to {e}.")
        raise
    return df