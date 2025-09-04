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
from google.auth import default
from google.auth.exceptions import DefaultCredentialsError
from google.auth.transport.requests import AuthorizedSession

# Add Google API Core libraries for integration
from google.api_core.exceptions import NotFound

# Add Google Spreadsheets API libraries for integration
import gspread

# Add Google CLoud libraries for integration
from google.cloud import bigquery

# Add Google Secret Manager libraries for integration
from google.cloud import secretmanager

# Add Python "re" library for expression matching
import re

# Add Python 'time' library for tracking execution time and implementing delays
import time

# Add Python 'datetime' library for datetime manipulation and timezone handling
from datetime import (
    datetime,
    timedelta,
    timezone
)

# Add internal Budget service for data handling
from src.ingest import ingest_budget_allocation
from src.staging import staging_budget_allocation
from src.mart import (
    mart_budget_all,
    mart_budget_event
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
    print(f"🚀 [UPDATE] Starting to update budget allocation for {thang} month...")
    logging.info(f"🚀 [UPDATE] Starting to update budget allocation for {thang} month...")

    # 1.1.1. Start timing the update process
    start_time = time.time()
   
    # 1.1.2. Prepare raw_table_id in BigQuery  
    raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"  
    year, month = thang.split("-")
    raw_table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_budget_{ACCOUNT}_m{int(month):02d}{year}"
    print(f"🔍 [UPDATE] Verifying raw budget allocation table {raw_table_id}...")
    logging.info(f"🔍 [UPDATE] Verifying raw budget allocation table {raw_table_id}...")

    # 1.1.3. Iterate over input date range to verify data freshness
    should_ingest = False
    try:
        try:
            client = bigquery.Client(project=PROJECT)
        except DefaultCredentialsError as e:
            raise RuntimeError(" ❌ [UPDATE] Failed to initialize Google BigQuery client due to credentials error.") from e
        client.get_table(raw_table_id)
    except NotFound:
        print(f"⚠️ [UPDATE] Raw budget allocation table {raw_table_id} not found then ingestion will be starting...")
        logging.warning(f"⚠️ [UPDATE] Raw budget allocation table {raw_table_id} not found then ingestion will be starting...")
        should_ingest = True
    else:
        query = f"SELECT MAX(last_updated_at) as last_updated FROM `{raw_table_id}`"
        try:
            result = client.query(query).result()
            last_updated = list(result)[0]["last_updated"]
            if not last_updated:
                print(f"⚠️ [UPDATE] No last_update_at found in raw budget allocation table {raw_table_id} then ingestion will be starting...")
                logging.warning(f"⚠️ [UPDATE] No last_update_at found in raw budget allocation table {raw_table_id} then ingestion will be starting...")
                should_ingest = True
            else:
                delta = datetime.now(timezone.utc) - last_updated
                if delta > timedelta(hours=1):
                    print(f"⚠️ [UPDATE] Raw budget table {raw_table_id} is outdated with last_update_at is {last_updated} then ingestion will be starting...")
                    logging.warning(f"⚠️ [UPDATE] Raw budget table {raw_table_id} is outdated with last_update_at is {last_updated} then ingestion will be starting...")
                    should_ingest = True
                else:
                    print(f"✅ [UPDATE] Raw budget table {raw_table_id} is up to date with last_update_at is {last_updated} then ingestion is skipped.")
                    logging.info(f"✅ [UPDATE] Raw budget table {raw_table_id} is up to date with last_update_at is {last_updated} then ingestion is skipped.")
        except Exception as e:
            print(f"❌ [UPDATE] Failed to verify freshness of {raw_table_id} due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to verify freshness of {raw_table_id} due to {e}.")
            should_ingest = False
    if not should_ingest:
        print(f"✅ [UPDATE] Raw budget allocation table for {thang} month is up to date then ingestion is skipped.")
        logging.info(f"✅ [UPDATE] Raw budget allocation table for {thang} month is up to date then ingestion is skipped.")
        return

    # 1.1.4. Get sheet_id from Google Secret Manager
    try:
        secret_client = secretmanager.SecretManagerServiceClient()
        secret_id = f"{COMPANY}_secret_{DEPARTMENT}_{PLATFORM}_sheet_id_{ACCOUNT}"
        secret_name = f"projects/{PROJECT}/secrets/{secret_id}/versions/latest"
        response = secret_client.access_secret_version(request={"name": secret_name})
        sheet_id = response.payload.data.decode("UTF-8")
        print(f"🚀 [UPDATE] Using sheet {sheet_id} to update budget allocation for {thang}...")
        logging.info(f"🚀 [UPDATE] Using sheet {sheet_id} to update budget allocation for {thang}...")
    except Exception as e:
        raise RuntimeError(
            f"❌ [UPDATE] Failed to fetch sheet_id from Secret Manager for {COMPANY} - {PLATFORM} - {ACCOUNT} due to {e}."
        ) from e

    # 1.1.5. Ingest monthly budget
    worksheet_monthly = f"m{int(month):02d}{year}"
    try:
        print(f"🔄 [UPDATE] Triggering monthly budget ingestion for {worksheet_monthly} sheet for {thang} month...")
        logging.info(f"🔄 [UPDATE] Triggering monthly budget ingestion for {worksheet_monthly} sheet for {thang} month...")
        df_monthly = ingest_budget_allocation(sheet_id, worksheet_monthly, thang)
        print(f"✅ [UPDATE] Successfully ingested monthly budget {worksheet_monthly} with {len(df_monthly)} row(s) for {thang} month.")
        logging.info(f"✅ [UPDATE] Successfully ingested monthly budget {worksheet_monthly} with {len(df_monthly)} row(s) for {thang} month.")
    except Exception as e:
        print(f"❌ [UPDATE] Failed to ingest monthly budget {worksheet_monthly} due to {e}.")
        logging.error(f"❌ [UPDATE] Failed to ingest monthly budget {worksheet_monthly} due to {e}.")
        raise

    # 1.1.6. Ingest special event(s) budget
    df_specials = []
    try:
        try:
            scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
            creds, _ = default(scopes=scopes)
            gc = gspread.Client(auth=creds)
            gc.session = AuthorizedSession(creds)
        except DefaultCredentialsError as e:
            raise RuntimeError("❌ [UPDATE] Failed to initialize Google Sheets client due to credentials error.") from e
        except Exception as e:
            raise RuntimeError(f"❌ [UPDATE] Failed to initialize Google Sheets client due to {e}.") from e
        sh = gc.open_by_key(sheet_id)
        worksheet_list = [ws.title for ws in sh.worksheets()]
        logging.info(f"[UPDATE] 📑 Found worksheets in sheet {sheet_id}: {worksheet_list}")
    except Exception as e:
        print(f"❌ [UPDATE] Failed to fetch worksheet list from sheet {sheet_id} due to {e}.")
        logging.error(f"❌ [UPDATE] Failed to fetch worksheet list from sheet {sheet_id} due to {e}.")
        raise
    pattern = re.compile(rf".*{year}$")
    special_sheets = [ws for ws in worksheet_list if pattern.match(ws) and not ws.startswith("m")]
    for ws in special_sheets:
        try:
            print(f"🔄 [UPDATE] Triggering special budget ingestion for worksheet {ws} (thang={thang})...")
            logging.info(f"🔄 [UPDATE] Triggering special budget ingestion for worksheet {ws} (thang={thang})...")
            df_special = ingest_budget_allocation(sheet_id, ws, thang)
            df_specials.append(df_special)
            print(f"✅ [UPDATE] Successfully ingested special budget {ws}.")
            logging.info(f"✅ [UPDATE] Successfully ingested special budget {ws}.")
        except Exception as e:
            print(f"❌ [UPDATE] Failed to ingest special budget {ws} due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to ingest special budget {ws} due to {e}.")
            raise
    has_monthly = df_monthly is not None and len(df_monthly) > 0
    has_special = any(df is not None and len(df) > 0 for df in df_specials)

    # 1.1.7. Rebuild budget allocation staging table
    if has_monthly or has_special:
        print("🔄 [UPDATE] Triggering to rebuild staging table for budget allocation...")
        logging.info("🔄 [UPDATE] Triggering to rebuild staging table for budget allocation...")
        try:
            staging_budget_allocation()
            print("✅ [UPDATE] Successfully rebuilt staging table for budget allocation.")
            logging.info("✅ [UPDATE] Successfully rebuilt staging table for budget allocation.")
        except Exception as e:
            print(f"❌ [UPDATE] Failed to rebuild staging table for budget allocation due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to rebuild staging table for budget allocation due to {e}.")       
    else:
        print(f"⚠️ [UPDATE] No monthly or special budget allocation ingested for {thang} then staging table building is skipped.")
        logging.warning(f"⚠️ [UPDATE] No monthly or special budget allocation ingested for {thang} then staging table building is skipped.")

    # 1.1.8. Rebuild budget allocation materialized table
    if has_monthly or has_special:
        try: 
            print("🔄 [UPDATE] Triggering to rebuild materialized table for monthly budget allocation...")
            logging.info("🔄 [UPDATE] Triggering to rebuild materialized table for monthly budget allocation...")
            mart_budget_all()
            print("✅ [UPDATE] Successfully rebuilt materialized table for monthly budget allocation.")
            logging.info("✅ [UPDATE] Successfully rebuilt materialized table for monthly budget allocation.")
        except Exception as e:
            print(f"❌ [UPDATE] Failed to rebuild materialized table for monthly budget allocation due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to rebuild materialized table for monthly budget allocation due to {e}.")       
        try:    
            print("🔄 [UPDATE] Triggering to rebuild materialized table for special event(s) budget allocation...")
            logging.info("🔄 [UPDATE] Triggering to rebuild materialized table for special event(s) budget allocation...")
            mart_budget_event()
            print("✅ [UPDATE] Successfully rebuilt materialized table for special event(s) budget allocation.")
            logging.info("✅ [UPDATE] Successfully rebuilt materialized table for special event(s) budget allocation.")
        except Exception as e:
            print(f"❌ [UPDATE] Failed to rebuild materialized table for special event(s) budget allocation due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to rebuild materialized table for special event(s) budget allocation due to {e}  .")             
    else:
        print(f"⚠️ [UPDATE] Skip staging & mart because no monthly/special data ingested for {thang}.")
        logging.warning(f"⚠️ [UPDATE] Skip staging & mart because no monthly/special data ingested for {thang}.")


    # 1.1.9. Measure the total execution time of Facebook campaign insights update process
    elapsed = time.time() - start_time
    print(f"✅ [UPDATE] Successfully completed budget allocation update in {elapsed}s.")
    logging.info(f"✅ [UPDATE] Successfully completed budget allocation update in {elapsed}s.")

if __name__ == "__main__":
    import argparse
    import os

    parser = argparse.ArgumentParser(description="Facebook Ads Data Update CLI")
    parser.add_argument("--thang", required=True, help="Start date in format YYYY-MM-DD")
    args = parser.parse_args()

    update_budget_allocation(args.thang)
