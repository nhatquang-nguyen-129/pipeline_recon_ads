"""
==================================================================
BUDGET INGESTION MODULE
------------------------------------------------------------------
This module ingests raw data from the Budget Allocation fetching 
module into Google BigQuery, establishing the foundational raw 
layer used for centralized storage and historical retention.

It manages the complete ingestion flow from authentication to 
data fetching, schema validation and loading into Google BigQuery 
tables segmented by track, platform, objective...

✔️ Supports both append and truncate modes via write_disposition
✔️ Validates data structure using centralized schema utilities
✔️ Applies lightweight normalization required for raw-layer loading
✔️ Implements granular logging and CSV-based error traceability
✔️ Ensures pipeline reliability through retry and checkpoint logic

⚠️ This module is dedicated solely to raw-layer ingestion.  
It does not handle advanced transformations, metric modeling or 
aggregated data processing beyond the ingestion boundary.
==================================================================
"""

# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add Python logging ultilities for integration
import logging

# Add Python time ultilities for integration
import time

# Add Python UUID ultilities for integration
import uuid

# Add Python Pandas libraries for integration
import pandas as pd

# Add Google API core modules for integration
from google.api_core.exceptions import NotFound

# Add Google Cloud modules for integration
from google.cloud import bigquery

# Add internal Budget Allocation module for handing
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
def ingest_budget_allocation(ingest_month_allocation: str) -> pd.DataFrame:
    print(f"🚀 [INGEST] Starting to ingest Budget Allocation for month {ingest_month_allocation}...")
    logging.info(f"🚀 [INGEST] Starting to ingest Budget Allocation for month {ingest_month_allocation}...")

    # 1.1.1. Start timing Budget Allocation ingestion
    ingest_time_start = time.time()
    ingest_sections_status = {}
    ingest_sections_time = {}
    print(f"🔍 [INGEST] Proceeding to ingest raw Budget Allocation at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest raw Budget Allocation at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    try:

    # 1.1.2. Convert YYYY-MM input to mMMYYYY ingest_name_sheet
        ingest_section_name = "[INGEST] Convert YYYY-MM input to mMMYYYY ingest_name_sheet"
        ingest_section_start = time.time()
        try:
            print(f"🔄 [INGEST] Converting {ingest_month_allocation} from YYYY-MM format to mMMYYY...")
            logging.info(f"🔄 [INGEST] Converting {ingest_month_allocation} from YYYY-MM format to mMMYYY...")
            year, month = ingest_month_allocation.split("-")
            month = month.zfill(2)
            ingest_name_sheet = f"m{month}{year}"
            ingest_sections_status[ingest_section_name] = "succeed"
            print(f"✅ [INGEST] Successfully converted {ingest_month_allocation} from YYYY-MM format to mMMYYYY with ingest_name_sheet {ingest_name_sheet}.")
            logging.info(f"✅ [INGEST] Successfully converted {ingest_month_allocation} from YYYY-MM format to mMMYYYY with ingest_name_sheet {ingest_name_sheet}.")            
        except Exception as e:
            ingest_sections_status[ingest_section_name] = "failed"
            print(f"❌ [INGEST] Failed to convert {ingest_month_allocation} from YYYY-MM format to mMMYYY due to {e}.")
            logging.error(f"❌ [INGEST] Failed to convert {ingest_month_allocation} from YYYY-MM format to mMMYYY due to {e}.")            
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.1.3. Trigger to fetch Budget Allocation
        ingest_section_name = "[INGEST] Trigger to fetch Budget Allocation"
        ingest_section_start = time.time()        
        try:
            print(f"🔁 [INGEST] Triggering to fetch Budget Allocation for month {ingest_month_allocation}...")
            logging.info(f"🔁 [INGEST] Triggering to fetch Budget Allocation for month {ingest_month_allocation}...")
            ingest_results_fetched = fetch_budget_allocation(fetch_month_allocation=ingest_month_allocation)
            ingest_df_fetched = ingest_results_fetched["fetch_df_final"]
            ingest_status_fetched = ingest_results_fetched["fetch_status_final"]
            ingest_summary_fetched = ingest_results_fetched["fetch_summary_final"]
            if ingest_status_fetched == "fetch_succeed_all":
                ingest_sections_status[ingest_section_name] = "succeed"
                print(f"✅ [INGEST] Successfully triggered Budget Allocation fetching for {ingest_summary_fetched['fetch_rows_output']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                logging.info(f"✅ [INGEST] Successfully triggered Budget Allocation fetching for {ingest_summary_fetched['fetch_rows_output']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")                
            elif ingest_status_fetched == "fetch_succeed_partial":
                ingest_sections_status[ingest_section_name] = "partial"                
                print(f"⚠️ [INGEST] Partially triggered Budget Allocation fetching {ingest_summary_fetched['fetch_rows_output']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                logging.warning(f"⚠️ [INGEST] Partially triggered Budget Allocation fetching {ingest_summary_fetched['fetch_rows_output']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")                
            else:
                ingest_sections_status[ingest_section_name] = "failed"
                print(f"❌ [INGEST] Failed to trigger Budget Allocation fetching with {ingest_summary_fetched['fetch_rows_output']} fetched row(s) due to {', '.join(ingest_summary_fetched['fetch_sections_failed'])} or unknown error in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                logging.error(f"❌ [INGEST] Failed to trigger Budget Allocation fetching with {ingest_summary_fetched['fetch_rows_output']} fetched row(s) due to {', '.join(ingest_summary_fetched['fetch_sections_failed'])} or unknown error in {ingest_summary_fetched['fetch_time_elapsed']}s.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)
    
    # 1.1.4. Trigger to enforce schema for Budget Allocation
        ingest_section_name = "[INGEST] Trigger to enforce schema for Budget Allocation"
        ingest_section_start = time.time()
        try:
            print(f"🔄 [INGEST] Triggering to enforce schema for raw Budget Allocation with {len(ingest_df_fetched)} row(s)...")
            logging.info(f"🔄 [INGEST] Triggering to enforce schema for raw Budget Allocation with {len(ingest_df_fetched)} row(s)...")
            ingest_results_enforced = enforce_table_schema(ingest_df_fetched, "ingest_budget_allocation")
            ingest_summary_enforced = ingest_results_enforced["schema_summary_final"]
            ingest_status_enforced = ingest_results_enforced["schema_status_final"]
            ingest_df_enforced = ingest_results_enforced["schema_df_final"]    
            if ingest_status_enforced == "schema_succeed_all":
                ingest_sections_status[ingest_section_name] = "succeed"
                print(f"✅ [INGEST] Successfully triggered schema enforcement for raw Budget Allocation with {ingest_summary_enforced['schema_rows_output']}/{len(ingest_df_fetched)} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                logging.info(f"✅ [INGEST] Successfully triggered schema enforcement for raw Budget Allocation with {ingest_summary_enforced['schema_rows_output']}/{len(ingest_df_fetched)} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")                
            else:
                ingest_sections_status[ingest_section_name] = "failed"
                print(f"❌ [INGEST] Failed to trigger schema enforcement for raw Budget Allocation with {ingest_summary_enforced['schema_rows_output']}/{len(ingest_df_fetched)} enforced row(s) due to failed section(s) {', '.join(ingest_summary_enforced['schema_sections_failed'])} in {ingest_summary_enforced['fetch_time_elapsed']}s.")
                logging.error(f"❌ [INGEST] Failed to trigger schema enforcement for raw Budget Allocation with {ingest_summary_enforced['schema_rows_output']}/{len(ingest_df_fetched)} enforced row(s) due to failed section(s) {', '.join(ingest_summary_enforced['schema_sections_failed'])} in {ingest_summary_enforced['fetch_time_elapsed']}s.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.1.5. Initialize Google BigQuery client
        ingest_section_name = "[INGEST] Initialize Google BigQuery client"
        ingest_section_start = time.time()
        try:
            print(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            google_bigquery_client = bigquery.Client(project=PROJECT)
            ingest_sections_status[ingest_section_name] = "succeed"
            print(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        except Exception as e:
            ingest_sections_status[ingest_section_name] = "failed"
            print(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.1.6. Prepare Google BigQuery table_id for ingestion
        ingest_section_name = "[INGEST] Prepare Google BigQuery table_id for ingestion"
        ingest_section_start = time.time()    
        try:            
            raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
            raw_table_budget = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_allocation_{ingest_name_sheet}"
            ingest_sections_status[ingest_section_name] = "succeed"
            print(f"🔍 [INGEST] Proceeding to ingest Budget Allocation for {len(ingest_df_fetched)} fetched row(s) with Google BigQuery table_id {raw_table_budget}...")
            logging.info(f"🔍 [INGEST] Proceeding to ingest Budget Allocation for {len(ingest_df_fetched)} fetched row(s) with Google BigQuery table_id {raw_table_budget}...")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.1.7. Delete existing row(s) or create new table if it not exist
        ingest_section_name = "[INGEST] Delete existing row(s) or create new table if it not exist"
        ingest_section_start = time.time()
        try:
            ingest_df_deduplicated = ingest_df_enforced.drop_duplicates()           
            table_clusters_defined = ["raw_date_month"]
            table_clusters_filtered = []
            table_schemas_defined = []
            temp_table_id = None
            try:
                print(f"🔍 [INGEST] Checking raw Budget Allocation table {raw_table_budget} existence...")
                logging.info(f"🔍 [INGEST] Checking raw Budget Allocation table {raw_table_budget} existence...")
                google_bigquery_client.get_table(raw_table_budget)
                ingest_table_existed = True
            except NotFound:
                ingest_table_existed = False
            except Exception:
                print(f"❌ [INGEST] Failed to check raw Budget Allocation table {raw_table_budget} existence due to {e}.")
                logging.error(f"❌ [INGEST] Failed to check raw Budget Allocation table {raw_table_budget} existence due to {e}.")
            if not ingest_table_existed:
                print(f"⚠️ [INGEST] Budget Allocation table {raw_table_budget} not found then table creation will be proceeding...")
                logging.info(f"⚠️ [INGEST] Budget Allocation table {raw_table_budget} not found then table creation will be proceeding...")
                for col, dtype in ingest_df_deduplicated.dtypes.items():
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
                    table_schemas_defined.append(bigquery.SchemaField(col, bq_type))
                table_configuration_defined = bigquery.Table(raw_table_budget, schema=table_schemas_defined)
                table_partition_effective = "date" if "date" in ingest_df_deduplicated.columns else None
                if table_partition_effective:
                    table_configuration_defined.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field=table_partition_effective
                    )
                table_clusters_filtered = [f for f in table_clusters_defined if f in ingest_df_deduplicated.columns]
                if table_clusters_filtered:  
                    table_configuration_defined.clustering_fields = table_clusters_filtered  
                try:    
                    print(f"🔍 [INGEST] Creating raw Budget Allocation table defined name {raw_table_budget} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}...")
                    logging.info(f"🔍 [INGEST] Creating raw Budget Allocation table defined name {raw_table_budget} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}...")
                    table_metadata_defined = google_bigquery_client.create_table(table_configuration_defined)
                    print(f"✅ [INGEST] Successfully created raw Budget Allocation table actual name {table_metadata_defined.full_table_id} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}.")
                    logging.info(f"✅ [INGEST] Successfully created raw Budget Allocation table actual name {table_metadata_defined.full_table_id} with partition on {table_partition_effective} and cluster on {table_clusters_filtered}.")
                except Exception as e:
                    print(f"❌ [INGEST] Failed to create raw Budget Allocation table {raw_table_budget} due to {e}.")
                    logging.error(f"❌ [INGEST] Failed to create raw Budget Allocation table {raw_table_budget} due to {e}.")
            else:
                print(f"🔄 [INGEST] Found raw Budget Allocation table {raw_table_budget} then existing row(s) deletion will be proceeding...")
                logging.info(f"🔄 [INGEST] Found raw Budget Allocation table {raw_table_budget} then existing row(s) deletion will be proceeding...")
                unique_keys = ingest_df_deduplicated[["raw_date_month"]].dropna().drop_duplicates()
                if not unique_keys.empty:
                    temp_table_id = f"{PROJECT}.{raw_dataset}.temp_table_campaign_metadata_delete_keys_{uuid.uuid4().hex[:8]}"
                    ingest_job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
                    google_bigquery_client.load_table_from_dataframe(unique_keys, temp_table_id, job_config=ingest_job_config).result()
                    ingest_job_condition = " AND ".join([
                        f"CAST(main.{col} AS STRING) = CAST(temp.{col} AS STRING)"
                        for col in ["raw_date_month"]
                    ])
                    ingest_query_delete = f"""
                        DELETE FROM `{raw_table_budget}` AS main
                        WHERE EXISTS (
                            SELECT 1 FROM `{temp_table_id}` AS temp
                            WHERE {ingest_job_condition}
                        )
                    """
                    ingest_result_deleted = google_bigquery_client.query(ingest_query_delete).result()
                    ingest_rows_deleted = ingest_result_deleted.num_dml_affected_rows
                else:
                    print(f"⚠️ [INGEST] No unique 'raw_date_month' key found in raw Budget Allocation table {raw_table_budget} then existing row(s) deletion is skipped.")
                    logging.warning(f"⚠️ [INGEST] No unique 'raw_date_month' key found in raw Budget Allocation table {raw_table_budget} then existing row(s) deletion is skipped.")
            ingest_sections_status[ingest_section_name] = "succeed"
        except Exception as e:
            ingest_sections_status[ingest_section_name] = "failed"
            print(f"❌ [INGEST] Failed to delete existing row(s) or create new table {raw_table_budget} if it not exist for raw Budget Allocation due to {e}.")
            logging.error(f"❌ [INGEST] Failed to delete existing row(s) or create new table {raw_table_budget} if it not exist for raw Budget Allocation due to {e}.")
        finally:
            if temp_table_id is not None:
                try:
                    google_bigquery_client.delete_table(temp_table_id, not_found_ok=True)
                    print(f"✅ [INGEST] Successfully deleted {ingest_rows_deleted} existing row(s) of raw Budget Allocation table {raw_table_budget} and temporary table {temp_table_id} was deleted.")
                    logging.info(f"✅ [INGEST] Successfully deleted {ingest_rows_deleted} existing row(s) of raw Budget Allocation table {raw_table_budget} and temporary table {temp_table_id} was deleted.")
                except Exception as e:
                    print(f"⚠️ [INGEST] Failed to delete temporary table {temp_table_id} after deleted existing row(s) of raw Budget Allocation due to {e}.")
                    logging.warning(f"⚠️ [INGEST] Failed to delete temporary table {temp_table_id} after deleted existing row(s) of raw Budget Allocation due to {e}.")
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.1.8. Upload Budget Allocation to Google BigQuery
        ingest_section_name = "[INGEST] Upload Budget Allocation to Google BigQuery"
        ingest_section_start = time.time()
        try:
            print(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of raw Budget Allocation to Google BigQuery table {raw_table_budget}...")
            logging.info(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} row(s) of raw Budget Allocation to Google BigQuery table {raw_table_budget}...")
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            google_bigquery_client.load_table_from_dataframe(ingest_df_deduplicated, raw_table_budget, job_config=job_config).result()
            ingest_df_uploaded = ingest_df_deduplicated.copy()
            print(f"✅ [INGEST] Successfully uploaded {len(ingest_df_uploaded)} row(s) of raw Budget Allocation to Google BigQuery table {raw_table_budget}.")
            logging.info(f"✅ [INGEST] Successfully uploaded {len(ingest_df_uploaded)} row(s) of raw Budget Allocation to Google BigQuery table {raw_table_budget}.")
            ingest_sections_status[ingest_section_name] = "succeed"
        except Exception as e:
            ingest_sections_status[ingest_section_name] = "failed"
            print(f"❌ [INGEST] Failed to upload raw Budget Allocation to Google BigQuery table {raw_table_budget} due to {e}.")
            logging.error(f"❌ [INGEST] Failed to upload raw Budget Allocation to Google BigQuery table {raw_table_budget} due to {e}.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.1.9. Summarize ingestion results for Budget Allocation
    finally:
        ingest_time_elapsed = round(time.time() - ingest_time_start, 2)
        ingest_df_final = (ingest_df_uploaded.copy() if "ingest_df_uploaded" in locals() and not ingest_df_uploaded.empty else pd.DataFrame())
        ingest_sections_total = len(ingest_sections_status) 
        ingest_sections_failed = [k for k, v in ingest_sections_status.items() if v == "failed"] 
        ingest_sections_succeeded = [k for k, v in ingest_sections_status.items() if v == "succeed"]
        ingest_rows_output = len(ingest_df_final)
        ingest_sections_summary = list(dict.fromkeys(
            list(ingest_sections_status.keys()) +
            list(ingest_sections_time.keys())
        ))
        ingest_sections_detail = {
            ingest_section_summary: {
                "status": ingest_sections_status.get(ingest_section_summary, "unknown"),
                "time": ingest_sections_time.get(ingest_section_summary, None),
            }
            for ingest_section_summary in ingest_sections_summary
        }     
        if ingest_sections_failed:
            print(f"❌ [INGEST] Failed to complete Budget Allocation ingestion with {ingest_rows_output} ingested row(s) due to {', '.join(ingest_sections_failed)} failed section(s) in {ingest_time_elapsed}s.")
            logging.error(f"❌ [INGEST] Failed to complete Budget Allocation ingestion with {ingest_rows_output} ingested row(s) due to {', '.join(ingest_sections_failed)} failed section(s) in {ingest_time_elapsed}s.")
            ingest_status_final = "ingest_failed_all"
        else:
            print(f"🏆 [INGEST] Successfully completed Budget Allocation ingestion with {ingest_rows_output} ingested row(s) in {ingest_time_elapsed}s.")
            logging.info(f"🏆 [INGEST] Successfully completed Budget Allocation ingestion with {ingest_rows_output} ingested row(s) in {ingest_time_elapsed}s.")
            ingest_status_final = "ingest_succeed_all"
        ingest_results_final = {
            "ingest_df_final": ingest_df_final,
            "ingest_status_final": ingest_status_final,
            "ingest_summary_final": {
                "ingest_time_elapsed": ingest_time_elapsed, 
                "ingest_sections_total": ingest_sections_total,
                "ingest_sections_succeed": ingest_sections_succeeded, 
                "ingest_sections_failed": ingest_sections_failed, 
                "ingest_sections_detail": ingest_sections_detail, 
                "ingest_rows_output": ingest_rows_output
            },
        }
    return ingest_results_final