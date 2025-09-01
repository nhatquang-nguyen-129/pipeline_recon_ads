"""
==================================================================
BUDGET ENRICHMENT MODULE
------------------------------------------------------------------
This module enriches raw or staging-level budget data with 
standardized metadata, mappings, and business logic to produce 
a clean, analysis-ready mart for downstream usage.

It encapsulates data cleaning, normalization, and mapping steps 
to ensure consistent keys across platforms (e.g. ma_chuong_trinh, 
ma_noi_dung, ma_ngan_sach_cap_1), allowing seamless joins with 
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
import datetime

# Add logging ultilities for integration
import logging

# Add timezone ultilites for integration
import pytz

# Add Python Pandas libraries for integration
import pandas as pd

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
def enrich_budget_fields(df: pd.DataFrame) -> pd.DataFrame:
    print("🚀 [ENRICH] Starting to enrich budget staging data...")
    logging.info("🚀 [ENRICH] Starting to enrich budget staging data...")    

    if df.empty:
        print("⚠️ [ENRICH] Budget input dataframe is empty then enrichment is skipped.")
        logging.warning("⚠️ [ENRICH] Budget input dataframe is empty then enrichment is skipped.")
        return df

    # 2.1.1. Standardize platform classification for budget
    if "nen_tang" in df.columns:
        print("🔍 [ENRICH] Standardizing budget platform classification by 'nen_tang'...")
        logging.info("🔍 [ENRICH] Standardizing budget platform classification by 'nen_tang'...")          
        df["platform"] = df["nen_tang"].astype(str).str.strip().str.lower()
        print("✅ [ENRICH] Successfully standardized budget platform classification by 'nen_tang'.")
        logging.info("✅ [ENRICH] Successfully standardized budget platform classification by 'nen_tang'.")
    else:
        print("⚠️ [ENRICH] Budget column 'nen_tang' not found then standardization is skipped.")
        logging.warning("⚠️ [ENRICH] Budget column 'nen_tang' not found then standardization is skipped.")

    # 2.1.2. Add composite key for join
    required_cols = {"ma_chuong_trinh", "ma_noi_dung", "month"}
    print(f"🔍 [ENRICH] Adding budget composite key {required_cols}...")
    logging.info(f"🔍 [ENRICH] Adding budget composite key {required_cols}...")
    if required_cols.issubset(df.columns):
        df["budget_key"] = (
            df["ma_chuong_trinh"].str.strip().str.upper() + "__" +
            df["ma_noi_dung"].str.strip().str.upper() + "__" +
            df["month"]
        )
        print(f"✅ [ENRICH] Successfully created composite key {required_cols}.")
        logging.info(f"✅ [ENRICH] Successfully created composite key {required_cols}.")
    else:
        missing = required_cols - set(df.columns)
        print(f"⚠️ [ENRICH] Missing columns for budget {missing}.")
        logging.warning(f"⚠️ [ENRICH] Missing columns for budget {missing}.")
    return df