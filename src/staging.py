"""
==================================================================
BUDGET STAGING MODULE
------------------------------------------------------------------
This module transforms raw Budget Allocation data into enriched,  
normalized staging tables in BigQuery, acting as the bridge  
between raw API ingestion and final materialized analytics.

It combines track, program, type data, applies business logic  
and included parsing naming conventions, standardizing fields to
prepares clean datasets for downstream consumption.

✔️ Joins raw budget allocation with track and program data
✔️ Enriches fields such as owner, placement and format  
✔️ Normalizes and writes standardized tables into dataset  
✔️ Validates data integrity and ensures field completeness  
✔️ Supports modular extension for new Budget Allocation entities  

⚠️ This module is strictly responsible for data transformation
into staging format. It does not handle API ingestion or final  
materialized aggregations.
==================================================================
"""

# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add Python datetime utilities for integration
from datetime import datetime

# Add Python logging ultilities for integration
import logging

# Add Python time ultilities for integration
import time

# Add Python IANA time zone ultilities for integration
from zoneinfo import ZoneInfo

# Add Python Pandas libraries for integration
import pandas as pd

# Add Google Cloud modules for integration
from google.cloud import bigquery

# Add internal Budget Allocation modules for handling
from src.schema import enforce_table_schema
from src.enrich import enrich_budget_fields

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

# 1. TRANSFORM BUDGET ALLOCATION DATA INTO CLEANED STAGING TABLES

# 1.1. Transform Budget Allocation from tables into cleaned staging tables
def staging_budget_allocation() -> dict:
    print("🚀 [STAGING] Starting to build staging Budget Allocation table...")
    logging.info("🚀 [STAGING] Starting to build staging Budget Allocation table...")
    
    # 1.1.1. Start timing the Budget Allocation staging
    ICT = ZoneInfo("Asia/Ho_Chi_Minh")    
    raw_tables_budget = []
    staging_time_start = time.time()
    staging_tables_queried = []
    staging_df_concatenated = pd.DataFrame()
    staging_df_uploaded = pd.DataFrame()    
    staging_sections_status = {}
    staging_sections_time = {}
    print(f"🔍 [STAGING] Proceeding to transform Budget Allocation into cleaned staging table at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")
    logging.info(f"🔍 [STAGING] Proceeding to transform Budget Allocation into cleaned staging table at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")

    try:
    
    # 1.1.2. Prepare table_id for Budget Allocation staging
        staging_section_name = "[STAGING] Prepare table_id for Budget Allocation staging"
        staging_section_start = time.time()   
        try:            
            raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
            print(f"🔍 [STAGING] Using raw dataset {raw_dataset} to build staging table for Budget Allocation...")
            logging.info(f"🔍 [STAGING] Using raw dataset {raw_dataset} to build staging table for Budget Allocation...")
            staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
            staging_table_budget = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_allocation_monthly"
            print(f"🔍 [STAGING] Using staging dataset {raw_dataset} to build staging table for Budget Allocation...")
            logging.info(f"🔍 [STAGING] Using staging dataset {raw_dataset} to build staging table for Budget Allocation...")
            staging_sections_status[staging_section_name] = "succeed"
        finally:
            staging_sections_time[staging_section_name] = round(time.time() - staging_section_start, 2)            

    # 1.1.3. Initialize Google BigQuery client
        staging_section_name = "[STAGING] Initialize Google BigQuery client"
        staging_section_start = time.time()    
        try:
            print(f"🔍 [STAGING] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [STAGING] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            google_bigquery_client = bigquery.Client(project=PROJECT)
            staging_sections_status[staging_section_name] = "succeed"
            print(f"✅ [STAGING] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [STAGING] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")            
        except Exception as e:
            staging_sections_status[staging_section_name] = "failed"
            print(f"❌ [STAGING] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [STAGING] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        finally:
            staging_sections_time[staging_section_name] = round(time.time() - staging_section_start, 2)

    # 1.1.4. Scan all budget allocation tables
        staging_section_name = "[STAGING] Scan budget allocation tables"
        staging_section_start = time.time()            
        try:
            query_select_config = f"""
                SELECT table_name
                FROM `{PROJECT}.{raw_dataset}.INFORMATION_SCHEMA.TABLES`
                WHERE REGEXP_CONTAINS(
                    table_name,
                    r'^{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_allocation_m[0-1][0-9][0-9]{{4}}$'
                )
            """   
            print(f"🔍 [STAGING] Scanning all Budget Allocation tables from Google BigQuery dataset {raw_dataset}...")
            logging.info(f"🔍 [STAGING] Scanning all Budget Allocation tables from Google BigQuery dataset {raw_dataset}...")
            query_select_load = google_bigquery_client.query(query_select_config)
            query_select_result = query_select_load.result()
            raw_tables_name = [row.table_name for row in query_select_result]
            raw_tables_budget = [f"{PROJECT}.{raw_dataset}.{t}" for t in raw_tables_name]
            staging_sections_status[staging_section_name] = "succeed"
            print(f"✅ [STAGING] Successfully found {len(raw_tables_budget)} Budget Allocation table(s).")
            logging.info(f"✅ [STAGING] Successfully found {len(raw_tables_budget)} Budget Allocation table(s).")            
        except Exception as e:
            staging_sections_status[staging_section_name] = "failed"
            print(f"❌ [STAGING] Failed to scan Budget Allocation tables due to {e}.")
            logging.error(f"❌ [STAGING] Failed to scan Budget Allocation tables due to {e}.")
        finally:
            staging_sections_time[staging_section_name] = round(time.time() - staging_section_start, 2)    

    # 1.1.5. Query all Budget Allocation tables
        staging_section_name = "[STAGING] Query all Budget Allocation tables"
        staging_section_start = time.time()
        try:
            for raw_table_budget in raw_tables_budget:
                try:
                    query_select_config = f"""
                        SELECT *
                        FROM `{raw_table_budget}`
                    """
                    print(f"🔄 [STAGING] Querying raw Budget Allocation table {raw_table_budget}...")
                    logging.info(f"🔄 [STAGING] Querying raw Budget Allocation table {raw_table_budget}...")                    
                    query_select_load = google_bigquery_client.query(query_select_config)
                    staging_df_queried = query_select_load.to_dataframe()
                    staging_tables_queried.append({"raw_table_budget": raw_table_budget, "staging_df_queried": staging_df_queried})
                    print(f"✅ [STAGING] Successfully queried {len(staging_df_queried)} row(s) from Budget Allocation table {raw_table_budget}.")
                    logging.info(f"✅ [STAGING] Successfully queried {len(staging_df_queried)} row(s) from Budget Allocation table {raw_table_budget}.")
                except Exception as e:
                    print(f"❌ [STAGING] Failed to query Budget Allocation table {raw_table_budget} due to {e}.")
                    logging.warning(f"❌ [STAGING] Failed to query Budget Allocation table {raw_table_budget} due to {e}.")
                    continue
        finally:
            staging_sections_time[staging_section_name] = round(time.time() - staging_section_start, 2)                     
        if len(staging_tables_queried) == len(raw_tables_budget):
            staging_sections_status[staging_section_name] = "succeed"
        elif len(staging_tables_queried) == 0:
            staging_sections_status[staging_section_name] = "failed"
        else:
            staging_sections_status[staging_section_name] = "partial"

    # 1.1.6. Trigger to enrich staging Budget Allocation
        staging_section_name = "[STAGING] Trigger to enrich staging Budget Allocation"
        staging_section_start = time.time()         
        try:
            staging_tables_enriched = []
            staging_dfs_enriched = []              
            for staging_table_queried in staging_tables_queried:
                raw_table_budget = staging_table_queried["raw_table_budget"]
                staging_df_queried = staging_table_queried["staging_df_queried"]
                print(f"🔄 [STAGING] Trigger to enrich Budget Allocation for {len(staging_df_queried)} queried row(s) from Google BigQuery table {raw_table_budget}...")
                logging.info(f"🔄 [STAGING] Trigger to enrich Budget Allocation for {len(staging_df_queried)} queried row(s) from Google BigQuery table {raw_table_budget}...")
                staging_results_enriched = enrich_budget_fields(staging_df_queried, enrich_table_id=raw_table_budget)
                staging_df_enriched = staging_results_enriched["enrich_df_final"]                
                staging_summary_enriched = staging_results_enriched["enrich_summary_final"]
                staging_status_enriched = staging_results_enriched["enrich_status_final"]
                if staging_status_enriched == "enrich_succeed_all":
                    print(f"✅ [STAGING] Successfully triggered Budget Allocation enrichment with {staging_summary_enriched['enrich_rows_output']}/{staging_summary_enriched['enrich_rows_input']} enriched row(s) in {staging_summary_enriched['enrich_time_elapsed']}s.")
                    logging.info(f"✅ [STAGING] Successfully triggered Budget Allocation enrichment with {staging_summary_enriched['enrich_rows_output']}/{staging_summary_enriched['enrich_rows_input']} enriched row(s) in {staging_summary_enriched['enrich_time_elapsed']}s.")
                    staging_tables_enriched.append(raw_table_budget)
                    staging_dfs_enriched.append(staging_df_enriched)
                elif staging_status_enriched == "enrich_succeed_partial":
                    print(f"⚠️ [STAGING] Partially triggered Budget Allocation enrichment with {staging_summary_enriched['enrich_rows_output']}/{staging_summary_enriched['enrich_rows_input']} enriched row(s) in {staging_summary_enriched['enrich_time_elapsed']}s.")
                    logging.info(f"⚠️ [STAGING] Partially triggered Budget Allocation enrichment with {staging_summary_enriched['enrich_rows_output']}/{staging_summary_enriched['enrich_rows_input']} enriched row(s) in {staging_summary_enriched['enrich_time_elapsed']}s.")
                    staging_tables_enriched.append(raw_table_budget)
                    staging_dfs_enriched.append(staging_df_enriched)
                else:
                    print(f"❌ [STAGING] Failed to trigger Budget Allocation enrichment with {staging_summary_enriched['enrich_rows_output']}/{staging_summary_enriched['enrich_rows_input']} enriched row(s) in {staging_summary_enriched['enrich_time_elapsed']}s.")
                    logging.error(f"❌ [STAGING] Failed to trigger Budget Allocation enrichment with {staging_summary_enriched['enrich_rows_output']}/{staging_summary_enriched['enrich_rows_input']} enriched row(s) in {staging_summary_enriched['enrich_time_elapsed']}s.")
        finally:
            staging_sections_time[staging_section_name] = round(time.time() - staging_section_start, 2)                        
        if len(staging_tables_enriched) == len(staging_tables_queried):
            staging_sections_status[staging_section_name] = "succeed"
        elif len(staging_tables_enriched) == 0:
            staging_sections_status[staging_section_name] = "failed"
        else:
            staging_sections_status[staging_section_name] = "parital"

    # 1.1.7. Concatenate enriched staging Budget Allocation
        staging_section_name = "[STAGING] Concatenate enriched staging Budget Allocation"
        staging_section_start = time.time()
        try:        
            if staging_dfs_enriched:
                staging_df_concatenated = pd.concat(staging_dfs_enriched, ignore_index=True)
                staging_sections_status[staging_section_name] = "succeed"
                print(f"✅ [STAGING] Successully concatenated staging Budget Allocation with {len(staging_df_concatenated)} enriched row(s) from {len(staging_dfs_enriched)} DataFrame(s).")
                logging.info(f"✅ [STAGING] Successully concatenated staging Budget Allocation with {len(staging_df_concatenated)} enriched row(s) from {len(staging_dfs_enriched)} DataFrame(s).")                
            else:
                staging_sections_status[staging_section_name] = "failed"
                print("⚠️ [STAGING] No enriched DataFrame found for staging Budget Allocation then concatenation is failed.")
                logging.warning("⚠️ [STAGING] No enriched DataFrame found for staging Budget Allocation then concatenation is failed.")                
        finally:
            staging_sections_time[staging_section_name] = round(time.time() - staging_section_start, 2)     

    # 1.1.8. Trigger to enforce schema for staging Budget Allocation
        staging_section_name = "[STAGING] Trigger to enforce schema for staging Budget Allocation"
        staging_section_start = time.time()        
        try:
            print(f"🔁 [STAGING] Triggering to enforce schema for Budget Allocation for {len(staging_df_concatenated)} row(s)...")
            logging.info(f"🔁 [STAGING] Triggering to enforce schema for Budget Allocation for {len(staging_df_concatenated)} row(s)...")
            staging_results_enforced = enforce_table_schema(schema_df_input=staging_df_concatenated,schema_type_mapping="staging_budget_allocation")
            staging_df_enforced = staging_results_enforced["schema_df_final"]            
            staging_summary_enforced = staging_results_enforced["schema_summary_final"]
            staging_status_enforced = staging_results_enforced["schema_status_final"]
            if staging_status_enforced == "schema_succeed_all":
                staging_sections_status[staging_section_name] = "succeed"
                print(f"✅ [STAGING] Successfully triggered Budget Allocation schema enforcement with {staging_summary_enforced['schema_rows_output']}/{staging_summary_enforced['schema_rows_input']} enforced row(s) in {staging_summary_enforced['schema_time_elapsed']}s.")
                logging.info(f"✅ [STAGING] Successfully triggered Budget Allocation schema enforcement with {staging_summary_enforced['schema_rows_output']}/{staging_summary_enforced['schema_rows_input']} enforced row(s) in {staging_summary_enforced['schema_time_elapsed']}s.")
            elif staging_status_enforced == "schema_succeed_partial":
                staging_sections_status[staging_section_name] = "partial"
                print(f"⚠️ [FETCH] Partially triggered Budget Allocation schema enforcement with {staging_summary_enforced['schema_rows_output']}/{staging_summary_enforced['schema_rows_input']} enforced row(s) in {staging_summary_enforced['schema_time_elapsed']}s.")
                logging.warning(f"⚠️ [FETCH] Partially triggered Budget Allocation schema enforcement with {staging_summary_enforced['schema_rows_output']}/{staging_summary_enforced['schema_rows_input']} enforced row(s) in {staging_summary_enforced['schema_time_elapsed']}s.")
            else:
                staging_sections_status[staging_section_name] = "failed"
                print(f"❌ [STAGING] Failed to trigger Budget Allocation schema enforcement with {staging_summary_enforced['schema_rows_output']}/{staging_summary_enforced['schema_rows_input']} enforced row(s) in {staging_summary_enforced['schema_time_elapsed']}s.")
                logging.error(f"❌ [STAGING] Failed to trigger Budget Allocation schema enforcement with {staging_summary_enforced['schema_rows_output']}/{staging_summary_enforced['schema_rows_input']} enforced row(s) in {staging_summary_enforced['schema_time_elapsed']}s.")
        finally:
            staging_sections_time[staging_section_name] = round(time.time() - staging_section_start, 2)       

    # 1.1.9. Create new staging Budget Allocation table
        staging_section_name = "[STAGING] Create new staging Budget Allocation table"
        staging_section_start = time.time()     
        try:
            staging_df_deduplicated = staging_df_enforced.drop_duplicates()
            table_clusters_defined = ["raw_budget_group", "raw_category_group", "raw_program_group"]
            table_partition_defined = "date"
            table_schemas_defined = []            
            try:
                print(f"🔍 [STAGING] Checking staging Budget Allocation table {staging_table_budget} existence...")
                logging.info(f"🔍 [STAGING] Checking staging Budget Allocation table {staging_table_budget} existence...")
                google_bigquery_client.get_table(staging_table_budget)
                staging_table_exists = True
            except Exception:
                staging_table_exists = False
            if not staging_table_exists:
                try:
                    print(f"⚠️ [STAGING] Staging Budget Allocation table {staging_table_budget} not found then new table creation will be proceeding...")
                    logging.warning(f"⚠️ [STAGING] Staging Budget Allocation table {staging_table_budget} not found then new table creation will be proceeding...")
                    for col, dtype in staging_df_deduplicated.dtypes.items():
                        if dtype.name.startswith("int"):
                            google_bigquery_type = "INT64"
                        elif dtype.name.startswith("float"):
                            google_bigquery_type = "FLOAT64"
                        elif dtype.name == "bool":
                            google_bigquery_type = "BOOL"
                        elif "datetime" in dtype.name:
                            google_bigquery_type = "TIMESTAMP"
                        else:
                            google_bigquery_type = "STRING"
                        table_schemas_defined.append(bigquery.SchemaField(col, google_bigquery_type))
                    table_configuration_defined = bigquery.Table(staging_table_budget, schema=table_schemas_defined)
                    table_partition_effective = table_partition_defined if table_partition_defined in staging_df_deduplicated.columns else None
                    if table_partition_effective:
                        table_configuration_defined.time_partitioning = bigquery.TimePartitioning(
                            type_=bigquery.TimePartitioningType.DAY,
                            field=table_partition_effective
                        )
                    table_clusters_effective = [table_cluster_defined for table_cluster_defined in table_clusters_defined if table_cluster_defined in staging_df_deduplicated.columns]
                    if table_clusters_effective:
                            table_configuration_defined.clustering_fields = table_clusters_effective
                    staging_table_create = google_bigquery_client.create_table(table_configuration_defined)
                    staging_table_id = staging_table_create.full_table_id
                    staging_sections_status[staging_section_name] = "succeed"
                    print(f"✅ [STAGING] Successfully created staging Budget Allocation table with actual name {staging_table_id} with partition on {table_partition_effective} and cluster on {table_clusters_effective}.")
                    logging.info(f"✅ [STAGING] Successfully created staging Budget Allocation table with actual name {staging_table_id} with partition on {table_partition_effective} and cluster on {table_clusters_effective}.")
                except Exception as e:
                    staging_sections_status[staging_section_name] = "failed"
                    print(f"❌ [STAGING] Failed to create staging Budget Allocation table {staging_table_budget} due to {e}.")
                    logging.error(f"❌ [STAGING] Failed to create staging Budget Allocation table {staging_table_budget} due to {e}.")
            else:
                staging_sections_status[staging_section_name] = "succeed"
                print(f"⚠️ [STAGING] Staging Budget Allocation table {staging_table_budget} already exists then creation will be skipped.")
                logging.info(f"⚠️ [STAGING] Staging Budget Allocation table {staging_table_budget} already exists then creation will be skipped.")                
        finally:
            staging_sections_time[staging_section_name] = round(time.time() - staging_section_start, 2)

    # 1.1.10. Upload staging Budget Allocation
        staging_section_name = "[STAGING] Upload staging Budget Allocation"
        staging_section_start = time.time()
        try:            
            if not staging_table_exists:
                try: 
                    print(f"🔍 [STAGING] Uploading {len(staging_df_deduplicated)} deduplicated row(s) of staging Budget Allocation to new Google BigQuery table {staging_table_id}...")
                    logging.warning(f"🔍 [STAGING] Uploading {len(staging_df_deduplicated)} deduplicated row(s) of staging Budget Allocation to new Google BigQuery table {staging_table_id}...")
                    job_load_config = bigquery.LoadJobConfig(
                        write_disposition="WRITE_APPEND",
                        time_partitioning=bigquery.TimePartitioning(
                            type_=bigquery.TimePartitioningType.DAY,
                            field="date"
                        ),
                        clustering_fields=table_clusters_effective if table_clusters_effective else None
                    )
                    job_load_load = google_bigquery_client.load_table_from_dataframe(
                        staging_df_deduplicated,
                        staging_table_budget, 
                        job_config=job_load_config
                    )
                    job_load_result = job_load_load.result()
                    staging_rows_uploaded = job_load_load.output_rows
                    staging_df_uploaded = staging_df_deduplicated.copy()
                    staging_sections_status[staging_section_name] = "succeed"
                    print(f"✅ [STAGING] Successfully uploaded {staging_rows_uploaded} deduplicated row(s) of staging Budget Allocation to new Google BigQuery table {staging_table_id}.")
                    logging.info(f"✅ [STAGING] Successfully uploaded {staging_rows_uploaded} deduplicated row(s) of staging Budget Allocation to new Google BigQuery table {staging_table_id}.")
                except Exception as e:
                    staging_sections_status[staging_section_name] = "failed"
                    print(f"❌ [STAGING] Failed to upload {len(staging_df_deduplicated)} deduplicated row(s) of staging Budget Allocation to Google BigQuery table {staging_table_id} due to {e}.")
                    logging.error(f"❌ [STAGING] Failed to upload {len(staging_df_deduplicated)} deduplicated row(s) of staging Budget Allocation to Google BigQuery table {staging_table_id} due to {e}.")
            else:
                try:
                    print(f"🔍 [STAGING] Found existing Google BigQuery table {staging_table_budget} and {len(staging_df_enforced)} row(s) of staging Budget Allocation will be overwritten...")
                    logging.warning(f"🔍 [STAGING] Found existing Google BigQuery table {staging_table_budget} and {len(staging_df_enforced)} row(s) of staging Budget Allocation will be overwritten...")
                    job_load_config = bigquery.LoadJobConfig(
                        write_disposition="WRITE_TRUNCATE",
                    )
                    job_load_load = google_bigquery_client.load_table_from_dataframe(
                        staging_df_deduplicated,
                        staging_table_budget, 
                        job_config=job_load_config
                    )
                    job_load_result = job_load_load.result()
                    staging_rows_uploaded = job_load_load.output_rows
                    staging_df_uploaded = staging_df_deduplicated.copy()
                    staging_sections_status[staging_section_name] = "succeed"
                    print(f"✅ [STAGING] Successfully overwrote {staging_rows_uploaded} deduplicated row(s) of staging Budget Allocation to existing Google BigQuery table {staging_table_budget}.")
                    logging.info(f"✅ [STAGING] Successfully overwrote {staging_rows_uploaded} deduplicated row(s) of staging Budget Allocation to existing Google BigQuery table {staging_table_budget}.")
                except Exception as e:
                    staging_sections_status[staging_section_name] = "failed"
                    print(f"❌ [STAGING] Failed to overwrite {len(staging_df_deduplicated)} deduplicated row(s) of staging Budget Allocation to existing Google BigQuery table {staging_table_budget} due to {e}.")
                    logging.error(f"❌ [STAGING] Failed to overwrite {len(staging_df_deduplicated)} deduplicated row(s) of staging Budget Allocation to existing Google BigQuery table {staging_table_budget} due to {e}.")
        finally:
            staging_sections_time[staging_section_name] = round(time.time() - staging_section_start, 2)   

    # 1.1.11. Summarize staging results of Budget Allocation
    finally:
        staging_time_elapsed = round(time.time() - staging_time_start, 2)
        staging_df_final = staging_df_uploaded.copy() if not staging_df_uploaded.empty else pd.DataFrame()
        staging_sections_total = len(staging_sections_status)
        staging_sections_succeed = [k for k, v in staging_sections_status.items() if v == "succeed"]
        staging_sections_failed = [k for k, v in staging_sections_status.items() if v == "failed"]
        staging_tables_input = len(raw_tables_budget)
        staging_tables_output = len(staging_tables_queried)
        staging_tables_failed = staging_tables_input - staging_tables_output
        staging_rows_output = staging_rows_uploaded
        staging_sections_summary = list(dict.fromkeys(
            list(staging_sections_status.keys()) +
            list(staging_sections_time.keys())
        ))
        staging_sections_detail = {
            staging_section_summary: {
                "status": staging_sections_status.get(staging_section_summary, "unknown"),
                "time": round(staging_sections_time.get(staging_section_summary, 0.0), 2),
            }
            for staging_section_summary in staging_sections_summary
        }
        if staging_sections_failed:
            staging_status_final = "staging_failed_all"
            print(f"❌ [STAGING] Failed to complete Budget Allocation staging with {staging_tables_output}/{staging_tables_input} queried table(s) and {staging_rows_output} uploaded row(s) due to {', '.join(staging_sections_failed)} failed section(s) in {staging_time_elapsed}s.")
            logging.error(f"❌ [STAGING] Failed to complete Budget Allocation staging with {staging_tables_output}/{staging_tables_input} queried table(s) and {staging_rows_output} uploaded row(s) due to {', '.join(staging_sections_failed)} failed section(s) in {staging_time_elapsed}s.")
        elif staging_tables_output == staging_tables_input:
            staging_status_final = "staging_succeed_all"
            print(f"🏆 [STAGING] Successfully completed Budget Allocation staging with {staging_tables_output}/{staging_tables_input} queried table(s) and {staging_rows_output} uploaded row(s) in {staging_time_elapsed}s.")
            logging.info(f"🏆 [STAGING] Successfully completed Budget Allocation staging with {staging_tables_output}/{staging_tables_input} queried table(s) and {staging_rows_output} uploaded row(s) in {staging_time_elapsed}s.")
        else:            
            staging_status_final = "staging_failed_partial"            
            print(f"⚠️ [STAGING] Partially completed Budget Allocation staging with {staging_tables_output}/{staging_tables_input} queried table(s) and {staging_rows_output} uploaded row(s) in {staging_time_elapsed}s.")
            logging.warning(f"⚠️ [STAGING] Partially completed Budget Allocation staging with {staging_tables_output}/{staging_tables_input} queried table(s) and {staging_rows_output} uploaded row(s) in {staging_time_elapsed}s.")
        staging_results_final = {
            "staging_df_final": staging_df_final,
            "staging_status_final": staging_status_final,
            "staging_summary_final": {
                "staging_time_elapsed": staging_time_elapsed,
                "staging_sections_total": staging_sections_total,
                "staging_sections_succeed": staging_sections_succeed,
                "staging_sections_failed": staging_sections_failed,
                "staging_sections_detail": staging_sections_detail,
                "staging_tables_input": staging_tables_input,
                "staging_tables_output": staging_tables_output,
                "staging_tables_failed": staging_tables_failed,
                "staging_rows_output": staging_rows_output,
            }
        }
    return staging_results_final