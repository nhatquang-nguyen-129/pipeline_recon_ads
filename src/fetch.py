#services/budget/fetch.py
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

⚠️ This module is focused solely on *budget data retrieval*.  
It does not perform downstream validation, transformation, or 
data warehouse operations such as BigQuery ingestion.
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

# Add Python "re" library for expression matching
import re

# Add internal Google BigQuery module for integration
from infrastructure.bigquery.schema import normalize_string_snake
from infrastructure.bigquery.schema import remove_string_accents

# Add internal Budget module for data handling
from services.budget.config import MAPPING_BUDGET_GSPREAD
from services.budget.schema import ensure_table_schema

# 1. FETCH BUDGET SHEETS FOR FACT TABLES

# 1.1. Fetch all valid worksheets (excluding filters) from Google Sheet defined in config and return them as a dictionary
def fetch_budget_allocation(gc, sheet_id: str, worksheet_name: str, selected_month: str | None = None) -> pd.DataFrame:
    print(f"🚀 [FETCH] Fetching budget allocation from {worksheet_name} sheet in {sheet_id} file...")
    logging.info(f"🚀 [FETCH] Fetching budget allocation from {worksheet_name} sheet in {sheet_id} file...")

    # 1.1.1. Call Google Sheets API
    try:
        ws = gc.open_by_key(sheet_id).worksheet(worksheet_name)
        records = ws.get_all_records()
        print(f"✅ [FETCH] Retrieved {len(records)} row(s) from {worksheet_name} in Google Sheets file {sheet_id}.")
        logging.info(f"✅ [FETCH] Retrieved {len(records)} row(s) from worksheet {worksheet_name} in Google Sheets file {sheet_id}.")
        if not records:
            print(f"⚠️ [FETCH] No data found in {worksheet_name} worksheet.")
            logging.warning(f"⚠️ [FETCH] No data found in {worksheet_name} worksheet.")
            return pd.DataFrame()
        df = pd.DataFrame(records).replace("", None)
    except Exception as e:
        print(f"❌ [FETCH] Cannot fetch data from {worksheet_name} worksheet in {sheet_id} file due to {e}.")
        logging.error(f"❌ [FETCH] Cannot fetch data from {worksheet_name} worksheet in {sheet_id} file due to {e}.")
        return pd.DataFrame()

    # 1.1.2. Normailize column names to snake_case
    try:
        print(f"🔄 [FETCH] Normalizing name for {len(df.columns)} column(s) in budget allocation...")
        logging.info(f"🔄 [FETCH] Normalizing name for {len(df.columns)} column(s) in budget allocation...")
        df.columns = [normalize_string_snake(col) for col in df.columns]
        print(f"✅ [FETCH] Successfully normalized name for {len(df.columns)} column(s) in budget allocation.")
        logging.info(f"✅ [FETCH] Successfully normalized name for {len(df.columns)} column(s) in budget allocation.")
        if df.empty:
            print("⚠️ [FETCH] Empty dataframe returned from budget allocation then normalization is skipped.")
            logging.warning("⚠️ [FETCH] Empty dataframe returned from budget allocation then normalization is skipped.")   
    except Exception as e:
        print(f"❌ [FETCH] Failed to normalize column name(s) from budget allocation due to {e}.")
        logging.error(f"❌ [FETCH] Failed to normalize column name(s) from budget allocation due to {e}.")

    # 1.1.3. Remove unicode accents from budget column name(s)
    try:
        print(f"🔄 [FETCH] Removing unicode accents for {len(df.columns)} column name(s) in budget allocation...")
        logging.info(f"🔄 [FETCH] Removing unicode accents for {len(df.columns)} column name(s) in budget allocation...")
        df.columns = [remove_string_accents(col) for col in df.columns]
        print(f"✅ [FETCH] Successfully removed unicode accents for {len(df.columns)} column name(s) in budget allocation.")
        logging.info(f"✅ [FETCH] Successfully removed unicode accents for {len(df.columns)} column name(s) in budget allocation.")
        if df.empty:
            print("⚠️ [FETCH] Empty dataframe returned from budget allocation then removal is skipped.")
            logging.warning("⚠️ [FETCH] Empty dataframe returned from budget allocation then removal is skipped.")   
    except Exception as e:
        print(f"❌ [FETCH] Failed to remove unicode accents from budget column name(s) due to {e}.")
        logging.error(f"❌ [FETCH] Failed to remove unicode accents from budget column name(s) due to {e}.")
    
    # 1.1.4. Enforce schema for budget allocation
    try:
        print(f"🔄 [INGEST] Enforcing schema for {len(df)} row(s) of budget allocation...")
        logging.info(f"🔄 [INGEST] Enforcing schema for {len(df)} row(s) of budget allocation...")
        df = ensure_table_schema(df, "fetch_budget_allocation")
        print(f"✅ [FETCH] Successfully enforced {len(df)} row(s) of budget allocation for {selected_month}.")
        logging.info(f"✅ [FETCH] Successfully enforced {len(df)} row(s) of budget allocation for {selected_month}.")
        if df.empty:
            print("⚠️ [FETCH] Empty dataframe returned from budget allocation then enforcement is skipped.")
            logging.warning("⚠️ [FETCH] Empty dataframe returned from budget allocation then enforcement is skipped.")                   
    except Exception as e:
        print(f"❌ [FETCH] Failed to enforce schema for budget allocation due to {e}.")
        logging.error(f"❌ [FETCH] Failed to enforce schema for budget allocation due to {e}.")
    return df