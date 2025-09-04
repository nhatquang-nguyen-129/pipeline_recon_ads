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
    mart_budget_all
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

    start_time = time.time()
    year, month = thang.split("-")

    # 1.1. Lấy sheet_id từ Secret Manager
    secret_client = secretmanager.SecretManagerServiceClient()
    secret_id = f"{COMPANY}_secret_{DEPARTMENT}_{PLATFORM}_sheet_id_{ACCOUNT}"
    secret_name = f"projects/{PROJECT}/secrets/{secret_id}/versions/latest"
    response = secret_client.access_secret_version(request={"name": secret_name})
    sheet_id = response.payload.data.decode("UTF-8")

    # 1.2. Khởi tạo Google Sheets client
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        creds, _ = default(scopes=scopes)
        gc = gspread.Client(auth=creds)
        gc.session = AuthorizedSession(creds)
        sh = gc.open_by_key(sheet_id)
        worksheet_list = [ws.title for ws in sh.worksheets()]
    except Exception as e:
        raise RuntimeError(f"❌ [UPDATE] Failed to init Google Sheets client due to {e}")

    # 1.3. Xác định sheet monthly + special
    worksheet_monthly = f"m{int(month):02d}{year}"  # dạng mMMYYYY
    pattern_special = re.compile(rf".*{year}$")     # dạng fesYYYY, snYYYY...
    monthly_sheets = [ws for ws in worksheet_list if ws == worksheet_monthly]
    special_sheets = [ws for ws in worksheet_list if pattern_special.match(ws) and not ws.startswith("m")]

    # 2. Ingest monthly budget
    df_monthly = None
    if monthly_sheets:
        print(f"🔄 [UPDATE] Ingesting monthly budget sheet {worksheet_monthly}...")
        df_monthly = ingest_budget_allocation(sheet_id, worksheet_monthly, thang) \
            .query("thang == @thang")  # lọc đúng tháng
        print(f"✅ [UPDATE] Loaded {len(df_monthly)} row(s) from monthly {worksheet_monthly} for {thang}.")

    # 3. Ingest special budgets (full load, lọc theo thang trong data)
    df_specials = []
    for ws in special_sheets:
        print(f"🔄 [UPDATE] Ingesting special budget sheet {ws} (full load)...")
        df_special = ingest_budget_allocation(sheet_id, ws, thang)
        df_special = df_special.query("thang == @thang")
        if len(df_special) > 0:
            df_specials.append(df_special)
            print(f"✅ [UPDATE] Loaded {len(df_special)} row(s) from special {ws} for {thang}.")
        else:
            print(f"⚠️ [UPDATE] No rows matched thang={thang} in special sheet {ws}.")

    # 4. Rebuild staging + mart
    has_monthly = df_monthly is not None and len(df_monthly) > 0
    has_special = len(df_specials) > 0

    if has_monthly or has_special:
        print("🔄 [UPDATE] Rebuilding staging & mart for budget allocation...")
        staging_budget_allocation()
        mart_budget_all()
        print("✅ [UPDATE] Rebuilt staging & mart successfully.")
    else:
        print(f"⚠️ [UPDATE] No data ingested for {thang}, skip staging & mart.")

    elapsed = time.time() - start_time
    print(f"✅ [UPDATE] Completed budget allocation update for {thang} in {elapsed:.2f}s.")
    logging.info(f"✅ [UPDATE] Completed budget allocation update for {thang} in {elapsed:.2f}s.")

if __name__ == "__main__":
    import argparse
    import os

    parser = argparse.ArgumentParser(description="Facebook Ads Data Update CLI")
    parser.add_argument("--thang", required=True, help="Start date in format YYYY-MM-DD")
    args = parser.parse_args()

    update_budget_allocation(args.thang)
