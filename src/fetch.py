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

# Add internal Budget module for data handling
from config.schema import ensure_table_schema

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

# 1. FETCH BUDGET SHEETS FOR FACT TABLES

# 1.1. Fetch all valid worksheets (excluding filters)
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

    # 1.1.2. Normalize column names to snake_case
    try:
        print(f"🔄 [FETCH] Normalizing name for {len(df.columns)} column(s) in budget allocation...")
        logging.info(f"🔄 [FETCH] Normalizing name for {len(df.columns)} column(s) in budget allocation...")
        df.columns = [
            re.sub(r'(?<!^)(?=[A-Z])', '_', col.strip()).replace(" ", "_").lower()
            for col in df.columns
        ]
        print(f"✅ [FETCH] Successfully normalized name for {len(df.columns)} column(s) in budget allocation.")
        logging.info(f"✅ [FETCH] Successfully normalized name for {len(df.columns)} column(s) in budget allocation.")
        if df.empty:
            print("⚠️ [FETCH] Empty dataframe returned from budget allocation then normalization is skipped.")
            logging.warning("⚠️ [FETCH] Empty dataframe returned from budget allocation then normalization is skipped.")   
    except Exception as e:
        print(f"❌ [FETCH] Failed to normalize column name(s) from budget allocation due to {e}.")
        logging.error(f"❌ [FETCH] Failed to normalize column name(s) from budget allocation due to {e}.")

    # 1.1.3. Remove unicode accents
    try:
        print(f"🔄 [FETCH] Removing unicode accents for {len(df.columns)} column name(s) in budget allocation...")
        logging.info(f"🔄 [FETCH] Removing unicode accents for {len(df.columns)} column name(s) in budget allocation...")
        vietnamese_map = {
            'á': 'a', 'à': 'a', 'ả': 'a', 'ã': 'a', 'ạ': 'a',
            'ă': 'a', 'ắ': 'a', 'ằ': 'a', 'ẳ': 'a', 'ẵ': 'a', 'ặ': 'a',
            'â': 'a', 'ấ': 'a', 'ầ': 'a', 'ẩ': 'a', 'ẫ': 'a', 'ậ': 'a',
            'đ': 'd',
            'é': 'e', 'è': 'e', 'ẻ': 'e', 'ẽ': 'e', 'ẹ': 'e',
            'ê': 'e', 'ế': 'e', 'ề': 'e', 'ể': 'e', 'ễ': 'e', 'ệ': 'e',
            'í': 'i', 'ì': 'i', 'ỉ': 'i', 'ĩ': 'i', 'ị': 'i',
            'ó': 'o', 'ò': 'o', 'ỏ': 'o', 'õ': 'o', 'ọ': 'o',
            'ô': 'o', 'ố': 'o', 'ồ': 'o', 'ổ': 'o', 'ỗ': 'o', 'ộ': 'o',
            'ơ': 'o', 'ớ': 'o', 'ờ': 'o', 'ở': 'o', 'ỡ': 'o', 'ợ': 'o',
            'ú': 'u', 'ù': 'u', 'ủ': 'u', 'ũ': 'u', 'ụ': 'u',
            'ư': 'u', 'ứ': 'u', 'ừ': 'u', 'ử': 'u', 'ữ': 'u', 'ự': 'u',
            'ý': 'y', 'ỳ': 'y', 'ỷ': 'y', 'ỹ': 'y', 'ỵ': 'y',
        }
        vietnamese_map_upper = {k.upper(): v.upper() for k, v in vietnamese_map.items()}
        full_map = {**vietnamese_map, **vietnamese_map_upper}
        df.columns = [
            ''.join(full_map.get(c, c) for c in col) if isinstance(col, str) else col
            for col in df.columns
        ]
        print(f"✅ [FETCH] Successfully removed unicode accents for {len(df.columns)} column name(s) in budget allocation.")
        logging.info(f"✅ [FETCH] Successfully removed unicode accents for {len(df.columns)} column name(s) in budget allocation.")
        if df.empty:
            print("⚠️ [FETCH] Empty dataframe returned from budget allocation then removal is skipped.")
            logging.warning("⚠️ [FETCH] Empty dataframe returned from budget allocation then removal is skipped.")   
    except Exception as e:
        print(f"❌ [FETCH] Failed to remove unicode accents from budget column name(s) due to {e}.")
        logging.error(f"❌ [FETCH] Failed to remove unicode accents from budget column name(s) due to {e}.")
    
    # 1.1.4. Enforce schema
    try:
        print(f"🔄 [INGEST] Enforcing schema for {len(df)} row(s) of budget allocation...")
        logging.info(f"🔄 [INGEST] Enforcing schema for {len(df)} row(s) of budget allocation...")
        df = ensure_table_schema(df, "fetch_budget_allocation")
        if df.empty:
            print("⚠️ [FETCH] Empty dataframe returned from budget allocation then enforcement is skipped.")
            logging.warning("⚠️ [FETCH] Empty dataframe returned from budget allocation then enforcement is skipped.")                   
    except Exception as e:
        print(f"❌ [FETCH] Failed to enforce schema for budget allocation due to {e}.")
        logging.error(f"❌ [FETCH] Failed to enforce schema for budget allocation due to {e}.")
    return df