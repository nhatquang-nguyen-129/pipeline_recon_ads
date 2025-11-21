"""
==================================================================
BUDGET UPDATE MODULE
------------------------------------------------------------------
This module performs incremental updates to Budget Allocation data 
at the raw layer, providing an efficient mechanism for refreshing  
recent or specific-date datasets without the need for full reloads.

By supporting targeted updates (per month, layer or entity), it  
enables faster turnaround for near-real-time dashboards and daily  
data sync jobs while maintaining historical accuracy and integrity.

✔️ Handles incremental data ingestion from the Google Sheets API
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

# Add Google API Core libraries for integration
from google.api_core.exceptions import NotFound

# Add Google Spreadsheets API libraries for integration
import gspread
from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound, APIError, GSpreadException

# Add Google Secret Manager libraries for integration
from google.cloud import secretmanager

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
    

    # 1.1.4. Ingest monthly budget
    df_monthly = None
    if monthly_sheets:
        try:
            print(f"🔄 [UPDATE] Triggering to ingest monthly budget allocation sheet {worksheet_monthly}...")
            logging.info(f"🔄 [UPDATE] Triggering to ingest monthly budget allocation sheet {worksheet_monthly}...")
            df_monthly = ingest_budget_allocation(sheet_id, worksheet_monthly, thang) \
                .query("thang == @thang")
        except Exception as e:
            print(f"❌ [UPDATE] Failed to trigger monthly budget allocation sheet {worksheet_monthly} due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to trigger monthly budget allocation sheet {worksheet_monthly} due to {e}.")
    # 1.1.5. Ingest special budget
    df_specials = []
    for ws in special_sheets:
        try:
            print(f"🔄 [UPDATE] Triggering to ingest special budget allocation sheet {ws}...")
            logging.info(f"🔄 [UPDATE] Triggering to ingest special budget allocation sheet {ws}...")            
            df_special = ingest_budget_allocation(sheet_id, ws, thang)
            df_special = df_special.query("thang == @thang")            
            if len(df_special) > 0:
                df_specials.append(df_special)
            else:
                print(f"⚠️ [UPDATE] No rows matched 'thang' to {thang} in special sheet {ws} then ingestion is skipped.")
                logging.warning(f"⚠️ [UPDATE] No rows matched 'thang' to {thang} in special sheet {ws} then ingestion is skipped.")        
        except Exception as e:
            print(f"❌ [UPDATE] Failed to trigger special budget allocation sheet {ws} ingestion due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to ingest special budget sheet {ws} due to {e}.")

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