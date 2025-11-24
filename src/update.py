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
def update_budget_allocation(thang: str) -> None:
    print(f"🚀 [UPDATE] Starting to update Budget Allocation for month {thang}...")
    logging.info(f"🚀 [UPDATE] Starting to update Budget Allocation for month {thang}...")

    # 1.1.1. Start timing TikTok Ads campaign insights update
    update_time_start = time.time()
    update_sections_status = {}
    update_sections_time = {}
    print(f"🔍 [UPDATE] Proceeding to update Budget Allocation for month {thang} at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [UPDATE] Proceeding to update Budget Allocation for month {thang} at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    try:  

    # 1.1.2. Trigger to ingest Budget Allocation
        update_section_name = "[UPDATE] Trigger to ingest Budget Allocation"
        update_section_start = time.time()
        try:
            print(f"🔄 [UPDATE] Triggering to ingest Budget Allocation for month {thang}...")
            logging.info(f"🔄 [UPDATE] Triggering to ingest Budget Allocation for month {thang}...")
            ingest_results_insights = ingest_budget_allocation(start_date=start_date, end_date=end_date)
            ingest_df_insights = ingest_results_insights["ingest_df_final"]
            ingest_status_insights = ingest_results_insights["ingest_status_final"]
            ingest_summary_insights = ingest_results_insights["ingest_summary_final"]
            updated_ids_campaign = set(ingest_df_insights["campaign_id"].dropna().unique())
            if ingest_status_insights == "ingest_succeed_all":
                print(f"✅ [UPDATE] Successfully triggered Facebook Ads campaign insights ingestion from {start_date} to {end_date} with {ingest_summary_insights['ingest_dates_output']}/{ingest_summary_insights['ingest_dates_input']} ingested day(s) and {ingest_summary_insights['ingest_rows_output']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")
                logging.info(f"✅ [UPDATE] Successfully triggered Facebook Ads campaign insights ingestion from {start_date} to {end_date} with {ingest_summary_insights['ingest_dates_output']}/{ingest_summary_insights['ingest_dates_input']} ingested day(s) and {ingest_summary_insights['ingest_rows_output']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")
                update_sections_status[update_section_name] = "succeed"
            elif ingest_status_insights == "ingest_succeed_partial":
                print(f"⚠️ [UPDATE] Partially triggered Facebook Ads campaign insights ingestion from {start_date} to {end_date} with {ingest_summary_insights['ingest_dates_output']}/{ingest_summary_insights['ingest_dates_input']} ingested day(s) and {ingest_summary_insights['ingest_rows_output']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")
                logging.warning(f"⚠️ [UPDATE] Partially triggered Facebook Ads campaign insights ingestion from {start_date} to {end_date} with {ingest_summary_insights['ingest_dates_output']}/{ingest_summary_insights['ingest_dates_input']} ingested day(s) and {ingest_summary_insights['ingest_rows_output']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")
                update_sections_status[update_section_name] = "partial"
            else:
                update_sections_status[update_section_name] = "failed"
                print(f"❌ [UPDATE] Failed to trigger Facebook Ads campaign insights ingestion from {start_date} to {end_date} with with {ingest_summary_insights['ingest_dates_output']}/{ingest_summary_insights['ingest_dates_input']} ingested day(s) and {ingest_summary_insights['ingest_rows_output']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")
                logging.error(f"❌ [UPDATE] Failed to trigger Facebook Ads campaign insights ingestion from {start_date} to {end_date} with with {ingest_summary_insights['ingest_dates_output']}/{ingest_summary_insights['ingest_dates_input']} ingested day(s) and {ingest_summary_insights['ingest_rows_output']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")
        finally:
            update_sections_time[update_section_name] = round(time.time() - update_section_start, 2)

    # 1.1.6. Rebuild staging budget allocation table
    has_monthly = df_monthly is not None and len(df_monthly) > 0
    has_special = len(df_specials) > 0
    if has_monthly or has_special:
        print("🔄 [UPDATE] Triggering to rebuild staging budget allocation table...")
        logging.info("🔄 [UPDATE] Triggering to rebuild staging budget allocation table...")            
        try:
            staging_budget_allocation()
        except Exception as e:
            print(f"❌ [UPDATE] Failed to trigger staging table rebuild for budget allocation due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to trigger staging table rebuild for budget allocation due to {e}.")  
    else:
        print(f"⚠️ [UPDATE] No updates for {thang} in budget allocation then staging table rebuild is skipped.")
        logging.warning(f"⚠️ [UPDATE] No updates for {thang} in budget allocation then staging table rebuild is skipped.")

    # 1.1.7. Rebuild materialized budget allocation table
    if has_monthly or has_special:
        print("🔄 [UPDATE] Triggering to rebuild materialized budget allocation table...")
        logging.info("🔄 [UPDATE] Triggering to rebuild materialized budget allocation table...")     
        try:
            mart_budget_allocation()
        except Exception as e:
            print(f"❌ [UPDATE] Failed to trigger materialized table rebuild for budget allocation due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to trigger materialized table rebuild for budget allocation due to {e}.")          
        
    # 1.1.8. Measure the total execution time    
    elapsed = time.time() - start_time
    print(f"✅ [UPDATE] Completed budget allocation update for {thang} in {elapsed:.2f}s.")
    logging.info(f"✅ [UPDATE] Completed budget allocation update for {thang} in {elapsed:.2f}s.")