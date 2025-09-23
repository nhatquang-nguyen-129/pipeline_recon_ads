"""
==================================================================
BUDGET UPDATE MODULE
------------------------------------------------------------------
This module performs **incremental updates** to budget allocation  
data at the raw layer, enabling ingestion from multiple sources  
and departments into Google BigQuery without reloading the entire  
dataset.

It is designed to support scheduled refreshes, cross-department  
budget consolidation, and ad-hoc adjustments to allocation data.

✔️ Supports multi-source ingestion (Google Sheets, CSV, APIs, etc.)  
✔️ Handles department-level budget segmentation and mapping  
✔️ Loads data incrementally to ensure minimal latency and freshness  

⚠️ This module is responsible for *RAW layer updates only*. It does  
not perform advanced transformations or generate staging/MART tables  
directly.
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
    print(f"🚀 [UPDATE] Starting to update budget allocation for {thang}...")
    logging.info(f"🚀 [UPDATE] Starting to update budget allocation for {thang}...")

    # 1.1.1. Start timing the update process
    start_time = time.time()
    
    # 1.1.2. Initialize Google Secret Manager client
    try:
        print(f"🔍 [UPDATE] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
        logging.info(f"🔍 [UPDATE] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
        google_secret_client = secretmanager.SecretManagerServiceClient()
        print(f"✅ [UPDATE] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
        logging.info(f"✅ [UPDATE] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
    except DefaultCredentialsError as e:
        raise RuntimeError("❌ [UPDATE] Failed to initialize Google Secret Manager client due to credentials error.") from e
    except PermissionDenied as e:
        raise RuntimeError("❌ [UPDATE] Failed to initialize Google Secret Manager client due to permission denial.") from e
    except NotFound as e:
        raise RuntimeError("❌ [UPDATE] Failed to initialize Google Secret Manager client because secret not found.") from e
    except GoogleAPICallError as e:
        raise RuntimeError("❌ [UPDATE] Failed to initialize Google Secret Manager client due to API call error.") from e
    except Exception as e:
        raise RuntimeError(f"❌ [UPDATE] Failed to initialize Google Secret Manager client due to unexpected error {e}.") from e
    
    # 1.1.3. Prepare Google Secret Manager id(s)
    print(f"🔍 [UPDATE] Retrieving budget information for {ACCOUNT} from Google Secret Manager...")
    logging.info(f"🔍 [UPDATE] Retrieving budget information for {ACCOUNT} from Google Secret Manager...") 
    secret_id = f"{COMPANY}_secret_{DEPARTMENT}_{PLATFORM}_sheet_id_{ACCOUNT}"
    secret_name = f"projects/{PROJECT}/secrets/{secret_id}/versions/latest"
    response = google_secret_client.access_secret_version(request={"name": secret_name})
    sheet_id = response.payload.data.decode("UTF-8")
    print(f"✅ [UPDATE] Successfully retrieved budget allocation sheet_id {sheet_id} for environment variable {ACCOUNT} from Google Secret Manager.")
    logging.info(f"✅ [UPDATE] Successfully retrieved budget allocation sheet_id {sheet_id} for environment variable {ACCOUNT} from Google Secret Manager.")
    
    # 1.1.4. Initialize Google Sheets client
    try:
        print(f"🔍 [UPDATE] Initializing Google Sheets client for sheet_id {sheet_id}...")
        logging.info(f"🔍 [UPDATE] Initializing Google Sheets client for sheet_id {sheet_id}...")                
        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        creds, _ = default(scopes=scopes)
        google_gspread_client = gspread.Client(auth=creds)
        google_gspread_client.session = AuthorizedSession(creds)
        print(f"✅ [MART] Successfully initialized Google Sheets client for sheet_id {sheet_id} with scopes {scopes}.")
        logging.info(f"✅ [MART] Successfully initialized Google Sheets client for sheet_id {sheet_id} with scopes {scopes}.")
    except DefaultCredentialsError as e:
        raise RuntimeError("❌ [MART] Failed to initialize Google Sheets client due to credentials error.") from e
    except SpreadsheetNotFound as e:
        raise RuntimeError(f"❌ [MART] Failed to initialize Google Sheets client because spreadsheet {sheet_id} not found.") from e
    except WorksheetNotFound as e:
        raise RuntimeError(f"❌ [MART] Failed to initialize Google Sheets client because worksheet not found in spreadsheet {sheet_id}.") from e
    except APIError as e:
        raise RuntimeError("❌ [MART] Failed to initialize Google Sheets client due to API error.") from e
    except GSpreadException as e:
        raise RuntimeError("❌ [MART] Failed to initialize Google Sheets client due to Gspread client error.") from e
    except Exception as e:
        raise RuntimeError(f"❌ [MART] Failed to initialize Google Sheets client due to {e}.") from e    

    # 1.1.5. Call Google Sheets API
    try:
        print(f"🔍 [UPDATE] Retrieving all worksheet(s) in Google Sheets file {sheet_id} for classification...")
        logging.info(f"🔍 [UPDATE] Retrieving all worksheet(s) in Google Sheets file {sheet_id} for classification...")
        sh = google_gspread_client.open_by_key(sheet_id)
        worksheet_list = [ws.title for ws in sh.worksheets()]
        print(f"✅ [UPDATE] Successfully retrieved {len(worksheet_list)} worksheet(s) from Google Sheets file {sheet_id}.")
        logging.info(f"✅ [UPDATE] Successfully retrieved {len(worksheet_list)} worksheet(s) from Google Sheets file {sheet_id}.")
    except Exception as e:
        print(f"❌ [UPDATE] Failed to fetch data from worksheet(s) in {sheet_id} file due to {e}.")
        logging.error(f"❌ [UPDATE] Failed to fetch data from worksheet(s) in {sheet_id} file due to {e}.")

    # 1.1.6. Prepare id(s)
    year, month = thang.split("-")
    worksheet_monthly = f"m{int(month):02d}{year}"
    pattern_special = re.compile(rf".*{year}$")
    monthly_sheets = [ws for ws in worksheet_list if ws == worksheet_monthly]
    special_sheets = [ws for ws in worksheet_list if pattern_special.match(ws) and not ws.startswith("m")]
    print(f"🔍 [UPDATE] Preparing to ingest {len(monthly_sheets)} monthly sheet(s) included {monthly_sheets} "
          f"and {len(special_sheets)} special sheet(s) included {special_sheets}.")
    logging.info(f"🔍 [UPDATE] Preparing to ingest {len(monthly_sheets)} monthly sheet(s) included {monthly_sheets} "
          f"and {len(special_sheets)} special sheet(s) included {special_sheets}.")

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