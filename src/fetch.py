"""
==================================================================
BUDGET FETCHING MODULE
------------------------------------------------------------------
This module handles direct, authenticated access to predefined 
Google Sheets sources, serving as the unified interface to 
retrieve marketing budget allocations across different scopes.

It enables structured, centralized logic for reading and normalizing 
budget data by category (e.g., system-wide, supplier co-op, local), 
intended to be used as part of the ETL pipeline's extraction layer.

✔️ Authenticates securely via Service Account credentials  
✔️ Loads budget data from configured Google Sheets tabs  
✔️ Maps sheet-to-category via hardcoded internal mapping  
✔️ Returns clean pandas DataFrames for further processing
✔️ Logs detailed runtime information for monitoring and debugging

⚠️ This module is focused solely on *budget data retrieval*.  
It does not perform downstream validation, transformation, or 
data warehouse operations such as BigQuery ingestion.
==================================================================
"""

# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add Python logging ultilties for integration
import logging

# Add Python Pandas libraries for integration
import pandas as pd

# Add Python time ultilities for integration
import time

# Add Google Authentication libraries for integration
from google.auth import default
from google.auth.transport.requests import AuthorizedSession

# Add Google Secret Manager modules for integration
from google.cloud import secretmanager

# Add Google Spreadsheets API modules for integration
import gspread

# Add internal Budget module for handling
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

# 1. FETCH BUDGET ALLCATION

# 1.1. Fetch Budget Allcation from Google Sheets
def fetch_budget_allocation(fetch_month_allocation: str) -> pd.DataFrame:
    print(f"🚀 [FETCH] Starting to fetch budget allocation for month {fetch_month_allocation}...")
    logging.info(f"🚀 [FETCH] Starting to fetch budget allocation for month {fetch_month_allocation}...")

    # 1.1.1. Start timing the Budget Allocation fetching
    fetch_time_start = time.time()   
    fetch_sections_status = {}
    fetch_sections_time = {}
    print(f"🔍 [FETCH] Proceeding to fetch raw Budget Allocation at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [FETCH] Proceeding to fetch raw Budget Allocation at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    try:

    # 1.1.2. Convert YYYY-MM input to mMMYYYY fetch_name_sheet
        fetch_section_name = "[FETCH] Convert YYYY-MM input to mMMYYYY fetch_name_sheet"
        fetch_section_start = time.time()
        try:
            print(f"🔄 [FETCH] Converting {fetch_month_allocation} from YYYY-MM format to mMMYYY...")
            logging.info(f"🔄 [FETCH] Converting {fetch_month_allocation} from YYYY-MM format to mMMYYY...")
            year, month = fetch_month_allocation.split("-")
            month = month.zfill(2)
            fetch_name_sheet = f"m{month}{year}"
            print(f"✅ [FETCH] Successfully converted {fetch_month_allocation} from YYYY-MM format to mMMYYYY with fetch_name_sheet {fetch_name_sheet}.")
            logging.info(f"✅ [FETCH] Successfully converted {fetch_month_allocation} from YYYY-MM format to mMMYYYY with fetch_name_sheet {fetch_name_sheet}.")
            fetch_sections_status[fetch_section_name] = "succeed"
        except Exception as e:
            print(f"❌ [FETCH] Failed to convert {fetch_month_allocation} from YYYY-MM format to mMMYYY due to {e}.")
            logging.error(f"❌ [FETCH] Failed to convert {fetch_month_allocation} from YYYY-MM format to mMMYYY due to {e}.")
            fetch_sections_status[fetch_section_name] = "failed"
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 1.1.2. Initialize Google Secret Manager client
        fetch_section_name = "[FETCH] Initialize Google Secret Manager client"
        fetch_section_start = time.time()                
        try:
            print(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            google_secret_client = secretmanager.SecretManagerServiceClient()
            print(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
            logging.info(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
            fetch_sections_status[fetch_section_name] = "succeed"
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to initialize Google Secret Manager client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [FETCH] Failed to initialize Google Secret Manager client for Google Cloud Platform project {PROJECT} due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)      

    # 1.1.3. Get Budget Allocation from Google Secret Manager
        fetch_section_name = "[FETCH] Get Budget Allocation from Google Secret Manager"
        fetch_section_start = time.time()         
        try:
            print(f"🔍 [FETCH] Retrieving Budget Allocation sheet_id for account {ACCOUNT} from Google Secret Manager...")
            logging.info(f"🔍 [FETCH] Retrieving Budget Allocation sheet_id for account {ACCOUNT} from Google Secret Manager...")
            sheet_secret_id = f"{COMPANY}_secret_{DEPARTMENT}_{PLATFORM}_sheet_id_{ACCOUNT}"
            sheet_secret_name = f"projects/{PROJECT}/secrets/{sheet_secret_id}/versions/latest"
            sheet_secret_response = google_secret_client.access_secret_version(request={"name": sheet_secret_name})
            fetch_id_sheet = sheet_secret_response.payload.data.decode("utf-8")
            print(f"✅ [FETCH] Successfully retrieved Budget Allocation sheet_id {fetch_id_sheet} for account {ACCOUNT} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved Budget Allocation sheet_id {fetch_id_sheet} for account {ACCOUNT} from Google Secret Manager.")
            fetch_sections_status[fetch_section_name] = "succeed"
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to retrieve Budget Allocation sheet_id for account {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve Budget Allocation sheet_id for account {ACCOUNT} from Google Secret Manager due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 1.1.4. Initialize Google Sheets client
        fetch_section_name = "[FETCH] Initialize Google Sheets client"
        fetch_section_start = time.time()            
        try:
            print(f"🔍 [FETCH] Initializing Google Sheets client for sheet_id {fetch_id_sheet}...")
            logging.info(f"🔍 [FETCH] Initializing Google Sheets client for sheet_id {fetch_id_sheet}...")                
            scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
            creds, _ = default(scopes=scopes)
            google_gspread_client = gspread.Client(auth=creds)
            google_gspread_client.session = AuthorizedSession(creds)
            print(f"✅ [FETCH] Successfully initialized Google Sheets client for sheet_id {fetch_id_sheet} with scopes {scopes}.")
            logging.info(f"✅ [FETCH] Successfully initialized Google Sheets client for sheet_id {fetch_id_sheet} with scopes {scopes}.")
            fetch_sections_status[fetch_section_name] = "succeed"
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to initialize Google Sheets client due to {e}.")
            logging.error(f"❌ [FETCH] Failed to initialize Google Sheets client due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2) 
    
    # 1.1.5. Make Google Sheets API call for worksheet recording
        fetch_section_name = "[FETCH] Make Google Sheets API call for worksheet recording"
        fetch_section_start = time.time()             
        try:
            print(f"🔍 [FETCH] Retrieving {fetch_name_sheet} in Google Sheets file {fetch_id_sheet} from Google Sheets API...")
            logging.info(f"🔍 [FETCH] Retrieving {fetch_name_sheet} in Google Sheets file {fetch_id_sheet} from Google Sheets API...")
            fetch_worksheet_budget = google_gspread_client.open_by_key(fetch_id_sheet).worksheet(fetch_name_sheet)
            fetch_records_responsed = fetch_worksheet_budget.get_all_records()
            print(f"✅ [FETCH] Successfully retrieved {len(fetch_records_responsed)} row(s) from {fetch_name_sheet} in Google Sheets file {fetch_id_sheet}.")
            logging.info(f"✅ [FETCH] Successfully retrieved {len(fetch_records_responsed)} row(s) from {fetch_name_sheet} in Google Sheets file {fetch_id_sheet}.")
            if not fetch_records_responsed:
                print(f"⚠️ [FETCH] No data found in {fetch_name_sheet} worksheet then empty DataFrame will be returned.")
                logging.warning(f"⚠️ [FETCH] No data found in {fetch_name_sheet} worksheet then empty DataFrame will be returned.")
                return pd.DataFrame()
            fetch_df_flattened = pd.DataFrame(fetch_records_responsed).replace("", None)
            fetch_sections_status[fetch_section_name] = "succeed"
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to retrieve worksheet record(s) from Google Sheets API due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve worksheet record(s) from Google Sheets API due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)
   
    # 1.1.6. Trigger to enforce schema for Budget Allocation
        fetch_section_name = "[FETCH] Trigger to enforce schema for Budget Allocation"
        fetch_section_start = time.time()              
        try:            
            print(f"🔄 [FETCH] Trigger to enforce schema for Budget Allocation with {len(fetch_df_flattened)} row(s)...")
            logging.info(f"🔄 [FETCH] Trigger to enforce schema for Budget Allocation with {len(fetch_df_flattened)} row(s)...")
            fetch_results_schema = enforce_table_schema(fetch_df_flattened, "fetch_budget_allocation")
            fetch_summary_enforced = fetch_results_schema["schema_summary_final"]
            fetch_status_enforced = fetch_results_schema["schema_status_final"]
            fetch_df_enforced = fetch_results_schema["schema_df_final"]    
            if fetch_status_enforced == "schema_succeed_all":
                print(f"✅ [FETCH] Successfully triggered to enforce schema for Budget Allocation with {fetch_summary_enforced['schema_rows_output']} row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
                logging.info(f"✅ [FETCH] Successfully triggered to enforce schema for Budget Allocation with {fetch_summary_enforced['schema_rows_output']} row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
                fetch_sections_status[fetch_section_name] = "succeed"
            else:
                fetch_sections_status[fetch_section_name] = "failed"
                print(f"❌ [FETCH] Failed to retrieve schema enforcement final results(s) for Budget Allocation with failed sections "f"{', '.join(fetch_summary_enforced['schema_sections_failed'])}.")
                logging.error(f"❌ [FETCH] Failed to retrieve schema enforcement final results(s) for Budget Allocation with failed sections "f"{', '.join(fetch_summary_enforced['schema_sections_failed'])}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 1.1.7. Summarize fetch results for Budget Allocation
    finally:
        fetch_time_elapsed = round(time.time() - fetch_time_start, 2)
        fetch_df_final = fetch_df_enforced.copy() if "fetch_df_enforced" in locals() and not fetch_df_enforced.empty else pd.DataFrame()
        fetch_sections_total = len(fetch_sections_status) 
        fetch_sections_failed = [k for k, v in fetch_sections_status.items() if v == "failed"] 
        fetch_sections_succeeded = [k for k, v in fetch_sections_status.items() if v == "succeed"]
        fetch_rows_output = len(fetch_df_final)
        fetch_sections_summary = list(dict.fromkeys(
            list(fetch_sections_status.keys()) +
            list(fetch_sections_time.keys())
        ))
        fetch_sections_detail = {
            fetch_section_summary: {
                "status": fetch_sections_status.get(fetch_section_summary, "unknown"),
                "time": fetch_sections_time.get(fetch_section_summary, None),
            }
            for fetch_section_summary in fetch_sections_summary
        }          
        if fetch_sections_failed:
            print(f"❌ [FETCH] Failed to complete Budget Allocation fetching with {fetch_rows_output} fetched row(s) due to  {', '.join(fetch_sections_failed)} failed section(s) in {fetch_time_elapsed}s.")
            logging.error(f"❌ [FETCH] Failed to complete Budget Allocation fetching with {fetch_rows_output} fetched row(s) due to  {', '.join(fetch_sections_failed)} failed section(s) in {fetch_time_elapsed}s.")
            fetch_status_final = "fetch_failed_all"
        else:
            print(f"🏆 [FETCH] Successfully completed Budget Allocation fetching with {fetch_rows_output} fetched row(s) in {fetch_time_elapsed}s.")
            logging.info(f"🏆 [FETCH] Successfully completed Budget Allocation fetching with {fetch_rows_output} fetched row(s) in {fetch_time_elapsed}s.")
            fetch_status_final = "fetch_succeed_all"    
        fetch_results_final = {
            "fetch_df_final": fetch_df_final,
            "fetch_status_final": fetch_status_final,
            "fetch_summary_final": {
                "fetch_time_elapsed": fetch_time_elapsed, 
                "fetch_sections_total": fetch_sections_total,
                "fetch_sections_succeed": fetch_sections_succeeded, 
                "fetch_sections_failed": fetch_sections_failed, 
                "fetch_sections_detail": fetch_sections_detail, 
                "fetch_rows_output": fetch_rows_output
            },
        }
    return fetch_results_final