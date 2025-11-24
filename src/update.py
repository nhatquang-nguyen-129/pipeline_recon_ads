"""
==================================================================
BUDGET UPDATE MODULE
------------------------------------------------------------------
This module performs incremental updates to Budget Allocation data 
at the raw layer, providing an efficient mechanism for refreshing  
recent or specific-date datasets without the need for full reloads.

By supporting targeted updates (per day, layer, or entity), it  
enables faster turnaround for near-real-time dashboards and daily  
data sync jobs while maintaining historical accuracy and integrity.

✔️ Handles incremental data ingestion from the GSpread API
✔️ Supports selective updates for campaign, adset, ad or creative  
✔️ Preserves schema alignment with staging and MART layers  
✔️ Implements error handling and retry logic for partial failures  
✔️ Designed for integration in daily or on-demand sync pipelines  

⚠️ This module is strictly responsible for *RAW layer updates only*.  
It does not perform transformations, enrichment, or aggregations.  
Processed data is consumed by the STAGING and MART modules.
==================================================================
"""

# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add logging capability for tracking process execution and errors
import logging


# Add Python "re" library for expression matching
import re

# Add Python 'time' library for tracking execution time and implementing delays
import time

# Add internal Budget service for data handling
from src.ingest import ingest_budget_allocation
from src.staging import staging_budget_allocation
from src.mart import (
    mart_budget_allocation
)

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

# 1. UPDATE BUDGET ALLOCATION FOR A GIVEN DATE RANGE

# 1.1. Update budget allocation data for a given date range
def update_budget_allocation(update_month_allocation: str) -> None:
    print(f"🚀 [UPDATE] Starting to update Budget Allocation for month {update_month_allocation}...")
    logging.info(f"🚀 [UPDATE] Starting to update Budget Allocation for month {update_month_allocation}...")

    # 1.1.1. Start timing TikTok Ads campaign insights update
    update_time_start = time.time()
    update_sections_status = {}
    update_sections_time = {}
    print(f"🔍 [UPDATE] Proceeding to update Budget Allocation for month {update_month_allocation} at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [UPDATE] Proceeding to update Budget Allocation for month {update_month_allocation} at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    try:  

    # 1.1.2. Trigger to ingest Budget Allocation
        update_section_name = "[UPDATE] Trigger to ingest Budget Allocation"
        update_section_start = time.time()
        try:
            print(f"🔄 [UPDATE] Triggering to ingest Budget Allocation for month {update_month_allocation}...")
            logging.info(f"🔄 [UPDATE] Triggering to ingest Budget Allocation for month {update_month_allocation}...")
            ingest_results_insights = ingest_budget_allocation(ingest_month_allocation=update_month_allocation)
            ingest_df_insights = ingest_results_insights["ingest_df_final"]
            ingest_status_insights = ingest_results_insights["ingest_status_final"]
            ingest_summary_insights = ingest_results_insights["ingest_summary_final"]
            if ingest_status_insights == "ingest_succeed_all":
                print(f"✅ [UPDATE] Successfully triggered Budget Allocation ingestion for month {update_month_allocation} with {ingest_summary_insights['ingest_rows_output']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")
                logging.info(f"✅ [UPDATE] Successfully triggered Budget Allocation ingestion for month {update_month_allocation} with {ingest_summary_insights['ingest_rows_output']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")
                update_sections_status[update_section_name] = "succeed"
            else:
                update_sections_status[update_section_name] = "failed"
                print(f"❌ [UPDATE] Failed to trigger Budget Allocation ingestion for month {update_month_allocation} {ingest_summary_insights['ingest_rows_output']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")
                logging.error(f"❌ [UPDATE] Failed to trigger Budget Allocation ingestion for month {update_month_allocation} {ingest_summary_insights['ingest_rows_output']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")
        finally:
            update_sections_time[update_section_name] = round(time.time() - update_section_start, 2)

    # 1.1.3. Trigger to build staging Budget Allocation
        update_section_name = "[UPDATE] Trigger to build staging Budget Allocation"
        update_section_start = time.time()
        try:
            print("🔄 [UPDATE] Triggering to create or overwrite staging Budget Allocation table...")
            logging.info("🔄 [UPDATE] Triggering to create or overwrite staging Budget Allocation table...")
            staging_results_campaign = staging_budget_allocation()
            staging_status_campaign = staging_results_campaign["staging_status_final"]
            staging_summary_campaign = staging_results_campaign["staging_summary_final"]
            if staging_status_campaign == "staging_succeed_all":
                print(f"✅ [UPDATE] Successfully triggered Budget Allocation staging with {staging_summary_campaign['staging_tables_output']}/{staging_summary_campaign['staging_tables_input']} table(s) on {staging_summary_campaign['staging_tables_input']} queried table(s) and {staging_summary_campaign['staging_rows_output']} uploaded row(s) in {staging_summary_campaign['staging_time_elapsed']}s.")
                logging.info(f"✅ [UPDATE] Successfully triggered Budget Allocation staging with {staging_summary_campaign['staging_tables_output']}/{staging_summary_campaign['staging_tables_input']} table(s) on {staging_summary_campaign['staging_tables_input']} queried table(s) and {staging_summary_campaign['staging_rows_output']} uploaded row(s) in {staging_summary_campaign['staging_time_elapsed']}s.")
                update_sections_status[update_section_name] = "succeed"
            elif staging_status_campaign == "staging_failed_partial":
                print(f"⚠️ [UPDATE] Partially triggered Budget Allocation staging with {staging_summary_campaign['staging_tables_output']}/{staging_summary_campaign['staging_tables_input']} table(s) on {staging_summary_campaign['staging_tables_input']} queried table(s) and {staging_summary_campaign['staging_rows_output']} uploaded row(s) in {staging_summary_campaign['staging_time_elapsed']}s.")
                logging.warning(f"⚠️ [UPDATE] Partially triggered Budget Allocation staging with {staging_summary_campaign['staging_tables_output']}/{staging_summary_campaign['staging_tables_input']} table(s) on {staging_summary_campaign['staging_tables_input']} queried table(s) and {staging_summary_campaign['staging_rows_output']} uploaded row(s) in {staging_summary_campaign['staging_time_elapsed']}s.")
                update_sections_status[update_section_name] = "partial"
            else:
                print(f"❌ [UPDATE] Failed to trigger Budget Allocation staging with {staging_summary_campaign['staging_tables_output']}/{staging_summary_campaign['staging_tables_input']} table(s) on {staging_summary_campaign['staging_tables_input']} queried table(s) and {staging_summary_campaign['staging_rows_output']} uploaded row(s) in {staging_summary_campaign['staging_time_elapsed']}s.")
                logging.error(f"❌ [UPDATE] Failed to trigger Budget Allocation staging with {staging_summary_campaign['staging_tables_output']}/{staging_summary_campaign['staging_tables_input']} table(s) on {staging_summary_campaign['staging_tables_input']} queried table(s) and {staging_summary_campaign['staging_rows_output']} uploaded row(s) in {staging_summary_campaign['staging_time_elapsed']}s.")
                update_sections_status[update_section_name] = "failed"
        finally:
            update_sections_time[update_section_name] = round(time.time() - update_section_start, 2)

    # 1.1.4. Trigger to materialize Budget Allocation
        update_section_name = "[UPDATE] Trigger to materialize Budget Allocation"
        update_section_start = time.time()
        try:
            if staging_status_campaign in ["staging_succeed_all", "staging_failed_partial"]:
                print("🔄 [UPDATE] Triggering to build materialized Facebook Ads campaign performance table...")
                logging.info("🔄 [UPDATE] Triggering to build materialized Facebook Ads campaign performance table...")               
                mart_results_all = mart_budget_allocation()
                mart_status_all = mart_results_all["mart_status_final"]
                mart_summary_all = mart_results_all["mart_summary_final"]                
                if mart_status_all == "mart_succeed_all":
                    print(f"✅ [UPDATE] Successfully completed Facebook Ads campaign performance materialization in {mart_summary_all['mart_time_elapsed']}s.")
                    logging.info(f"✅ [UPDATE] Successfully completed Facebook Ads campaign performance materialization in {mart_summary_all['mart_time_elapsed']}s.")
                    update_sections_status[update_section_name] = "succeed"
                elif mart_status_all == "mart_failed_all":
                    print(f"❌ [UPDATE] Failed to complete Facebook Ads campaign performance materialization due to unsuccessful section(s) of {', '.join(mart_summary_all['mart_sections_failed']) if mart_summary_all['mart_sections_failed'] else 'unknown'}.")
                    logging.error(f"❌ [UPDATE] Failed to complete Facebook Ads campaign performance materialization due to unsuccessful section(s) of {', '.join(mart_summary_all['mart_sections_failed']) if mart_summary_all['mart_sections_failed'] else 'unknown'}.")
                    update_sections_status[update_section_name] = "failed"
            else:
                print("⚠️ [UPDATE] No data returned from Facebook Ads campaign insights staging then materialization is skipped.")
                logging.warning("⚠️ [UPDATE] No data returned from Facebook Ads campaign insights staging then materialization is skipped.")
                update_sections_status[update_section_name] = "failed"
        finally:
            update_sections_time[update_section_name] = round(time.time() - update_section_start, 2)