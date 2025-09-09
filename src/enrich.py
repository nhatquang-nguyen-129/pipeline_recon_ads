"""
==================================================================
BUDGET ENRICHMENT MODULE
------------------------------------------------------------------
This module enriches raw or staging-level budget data with 
standardized metadata, mappings, and business logic to produce 
a clean, analysis-ready mart for downstream usage.

It encapsulates data cleaning, normalization, and mapping steps 
to ensure consistent keys across platforms (e.g. chuong_trinh, 
noi_dung, ma_ngan_sach_cap_1), allowing seamless joins with 
marketing performance data (e.g. Facebook, Google, TikTok).

✔️ Merges raw/staging budget data with dimension tables for unified identifiers  
✔️ Applies naming conventions, formats fields, and parses month/year info  
✔️ Adds audit columns like `is_deleted`, `created_at`, `source` for traceability  
✔️ Returns a fully structured DataFrame ready for loading into the budget mart

⚠️ This module does not fetch or write data from/to external systems.  
It expects staging data as input and returns enriched data for downstream consumption.
==================================================================
"""
# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add datetime utilities for integration
from datetime import datetime

# Add logging ultilities for integration
import logging

# Add timezone ultilites for integration
import pytz

# Add Python Pandas libraries for integration
import pandas as pd

# Add Python "re" library for handling
import re

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

# 1. ENRICH BUDGET INSIGHTS FROM INGESTION PHASE

# 1.1. Enrich budget insights included standarization, timestamp formatting and source tagging
def enrich_budget_insights(df: pd.DataFrame) -> pd.DataFrame:
    print("🚀 [ENRICH] Starting to enrich raw budget data...")
    logging.info("🚀 [ENRICH] Starting to enrich raw budget data...")
    
    if df.empty:
        print("⚠️ [ENRICH] Budget input dataframe is empty then enrichment is skipped.")
        logging.warning("⚠️ [ENRICH] Budget input dataframe is empty then enrichment is skipped.")
        return df
    
    # 1.1.1. Add ingestion timestamp with last_update_at
    try:
        print(f"🔄 [ENRICH] Adding budget ingestion timestamp for {len(df)} row(s) with last_update_at...")
        logging.info(f"🔄 [ENRICH] Adding budget ingestion timestamp for {len(df)} row(s) with last_update_at...")
        df["last_updated_at"] = datetime.utcnow().replace(tzinfo=pytz.UTC)
        print(f"✅ [ENRICH] Successfully added budget ingestion timestamp for {len(df)} row(s) with last_update_at.")
        logging.info(f"✅ [ENRICH] Successfully added budget ingestion timestamp for {len(df)} row(s) with last_update_at.")
    except Exception as e:
        print(f"❌ [ENRICH] Failed to add budget ingestion timestamp due to {e}.")
        logging.exception("❌ [ENRICH] Failed to add budget ingestion timestamp due to {e}.")
    return df

# 2. ENRICH BUDGET FIELDS FROM STAGING PHASE

# 2.1. Enrich budget fields by adding derived fields such as month_key, classification fields and mapping keys
def enrich_budget_fields(df: pd.DataFrame, table_id: str) -> pd.DataFrame:
    print("🚀 [ENRICH] Starting to enrich budget staging data...")
    logging.info("🚀 [ENRICH] Starting to enrich budget staging data...")    

    if df.empty:
        print("⚠️ [ENRICH] Budget input dataframe is empty then enrichment is skipped.")
        logging.warning("⚠️ [ENRICH] Budget input dataframe is empty then enrichment is skipped.")
        return df

    # 2.1.1. Standardize platform classification
    if "nen_tang" in df.columns:
        print("🔍 [ENRICH] Standardizing budget platform classification by 'nen_tang'...")
        logging.info("🔍 [ENRICH] Standardizing budget platform classification by 'nen_tang'...")          
        df["nen_tang"] = df["nen_tang"].astype(str).str.strip().str.lower()
        print("✅ [ENRICH] Successfully standardized budget platform classification by 'nen_tang'.")
        logging.info("✅ [ENRICH] Successfully standardized budget platform classification by 'nen_tang'.")
    else:
        print("⚠️ [ENRICH] Budget column 'nen_tang' not found then standardization is skipped.")
        logging.warning("⚠️ [ENRICH] Budget column 'nen_tang' not found then standardization is skipped.")

    # 2.1.2. Add composite key for join
    required_cols = {"chuong_trinh", "noi_dung", "thang"}
    print(f"🔍 [ENRICH] Adding budget composite key {required_cols}...")
    logging.info(f"🔍 [ENRICH] Adding budget composite key {required_cols}...")
    if required_cols.issubset(df.columns):
        df["budget_key"] = (
            df["chuong_trinh"].str.strip().str.upper() + "__" +
            df["noi_dung"].str.strip().str.upper() + "__" +
            df["thang"]
        )
        print(f"✅ [ENRICH] Successfully created composite key {required_cols}.")
        logging.info(f"✅ [ENRICH] Successfully created composite key {required_cols}.")
    else:
        missing = required_cols - set(df.columns)
        print(f"⚠️ [ENRICH] Missing columns for budget {missing}.")
        logging.warning(f"⚠️ [ENRICH] Missing columns for budget {missing}.")

    # 2.1.3. Enrich metadata from table_id
    try:
        table_name = table_id.split(".")[-1]
        match = re.search(
            r"^(?P<company>\w+)_table_budget_(?P<department>\w+)_(?P<account>\w+)_allocation_(?P<worksheet>\w+)$",
            table_name
        )
        if match:
            df["department"] = match.group("department")
            df["account"] = match.group("account")
            df["worksheet"] = match.group("worksheet")
            print("✅ [ENRICH] Successfully enriched budget metadata from table_id.")
            logging.info("✅ [ENRICH] Successfully enriched budget metadata from table_id.")
    except Exception as e:
        print(f"⚠️ [ENRICH] Failed to enrich budget metadata: {e}")
        logging.warning(f"⚠️ [ENRICH] Failed to enrich budget metadata: {e}")
    return df