"""
==================================================================
BUDGET ENRICHMENT MODULE
------------------------------------------------------------------
This module is responsible for transforming raw Budget Allocation 
data with standardized metadata, mappings, business logic to 
produce a clean and analysis-ready mart for downstream usage.

By centralizing enrichment rules, this module ensures transparency, 
consistency, and maintainability across the marketing data pipeline 
to build insight-ready tables.

✔️ Merges budget data with dimension tables for unified identifiers  
✔️ Standardizes program track, type and group naming conventions  
✔️ Extracts and normalizes key performance metrics across campaigns  
✔️ Cleans and validates data to ensure schema and field consistency  
✔️ Reduces payload size by removing redundant or raw field(s)

⚠️ This module focuses *only* on enrichment and transformation logic.  
It does **not** handle data fetching, ingestion or staging
==================================================================
"""

# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add Python datetime utilities for integration
from datetime import datetime

# Add Python logging ultilities for integraton
import logging

# Add Python Pandas libraries for integration
import pandas as pd

# Add Python timezone ultilities for integration
import pytz

# Add Python "re" libraries for integraton
import re

# Add Python time ultilities for integration
import time

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

# 1. ENRRICH BUDGET ALLOCATION FROM INGESTION PHASE

# 1.1. Enrich Budget Allocation from ingestion phase
def enrich_budget_insights(enrich_df_input: pd.DataFrame) -> pd.DataFrame:
    print(f"🚀 [ENRICH] Starting to enrich raw Budget Allocation for {len(enrich_df_input)} row(s)...")
    logging.info(f"🚀 [ENRICH] Starting to enrich raw Budget Allocation for {len(enrich_df_input)} row(s)...")

    # 1.1.1. Start timing the raw Budget Allocation enrichment
    enrich_time_start = time.time()   
    enrich_sections_status = {}
    enrich_sections_time = {}
    print(f"🔍 [ENRICH] Proceeding to enrich raw Budget Allocation for {len(enrich_df_input)} row(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [ENRICH] Proceeding to enrich raw Budget Allocation for {len(enrich_df_input)} row(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    
    try:
    
    # 1.1.2. Enrich column(s) name by normalizing to snake_case
        enrich_section_name = "[ENRICH] Enrich column(s) name by normalizing to snake_case"
        enrich_section_start = time.time()    
        try:
            print(f"🔄 [ENRICH] Normalizing name for {len(enrich_df_input.columns)} column(s) of Budget Allocation...")
            logging.info(f"🔄 [ENRICH] Normalizing name for {len(enrich_df_input.columns)} column(s) of Budget Allocation...")
            enrich_df_normalized = enrich_df_input.copy()            
            enrich_df_normalized.columns = [
                re.sub(r'(?<!^)(?=[A-Z])', '_', col.strip()).replace(" ", "_").lower()
                for col in enrich_df_normalized.columns
            ]
            print(f"✅ [ENRICH] Successfully normalized name for {len(enrich_df_normalized.columns)} column(s) in budget allocation.")
            logging.info(f"✅ [ENRICH] Successfully normalized name for {len(enrich_df_normalized.columns)} column(s) in budget allocation.")
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to normalize column(s) name of Budget Allocation due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to normalize column(s) name of Budget Allocation due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)

    # 1.1.3. Enrich column(s) name by unicode accent removal
        enrich_section_name = "[ENRICH] Enrich column(s) name by unicode accent removal"
        enrich_section_start = time.time()      
        try:
            print(f"🔄 [FETCH] Removing unicode accent(s) for {len(enrich_df_normalized.columns)} column(s) name(s) in budget allocation...")
            logging.info(f"🔄 [FETCH] Removing unicode accent(s) for {len(enrich_df_normalized.columns)} column(s) name(s) in budget allocation...")
            enrich_df_accent = enrich_df_normalized.copy()
            vietnamese_map_all = {
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
            vietnamese_map_upper = {k.upper(): v.upper() for k, v in vietnamese_map_all.items()}
            full_map = {**vietnamese_map_all, **vietnamese_map_upper}
            enrich_df_accent.columns = [
                ''.join(full_map.get(c, c) for c in col) if isinstance(col, str) else col
                for col in enrich_df_accent.columns
            ]
            print(f"✅ [ENRICH] Successfully removed unicode accent(s) for {len(enrich_df_accent.columns)} column(s) name in Budget allocation.")
            logging.info(f"✅ [ENRICH] Successfully removed unicode accent(s) for {len(enrich_df_accent.columns)} column(s) name in Budget allocation.")
            enrich_sections_status[enrich_section_name] = "succeed"
        except Exception as e:
            print(f"❌ [FETCH] Failed to remove unicode accent(s) from Budget Allocation column name due to {e}.")
            logging.error(f"❌ [FETCH] Failed to remove unicode accent(s) from Budget Allocation column name due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)                    

    # 1.1.4. Summarize enrich result(s) for raw Budget Allocation
    finally:
        enrich_time_elapsed = round(time.time() - enrich_time_start, 2)
        enrich_df_final = enrich_df_accent.copy() if not enrich_df_accent.empty else pd.DataFrame()
        enrich_sections_total = len(enrich_sections_status)
        enrich_sections_failed = [k for k, v in enrich_sections_status.items() if v == "failed"]
        enrich_sections_succeeded = [k for k, v in enrich_sections_status.items() if v == "succeed"]
        enrich_rows_input = len(enrich_df_input)
        enrich_rows_output = len(enrich_df_final)
        enrich_sections_summary = list(dict.fromkeys(
            list(enrich_sections_status.keys()) +
            list(enrich_sections_time.keys())
        ))
        enrich_sections_detail = {
            enrich_section_summary: {
                "status": enrich_sections_status.get(enrich_section_summary, "unknown"),
                "time": enrich_sections_time.get(enrich_section_summary, None),
            }
            for enrich_section_summary in enrich_sections_summary
        }        
        if any(v == "failed" for v in enrich_sections_status.values()):
            print(f"❌ [ENRICH] Failed to complete raw Budget Allocation enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) due to section(s) {', '.join(enrich_sections_failed)} in {enrich_time_elapsed}s.")
            logging.error(f"❌ [ENRICH] Failed to complete raw Budget Allocation enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) due to section(s) {', '.join(enrich_sections_failed)} in {enrich_time_elapsed}s.")
            enrich_status_final = "enrich_failed_all"        
        else:
            print(f"🏆 [ENRICH] Successfully completed raw Budget Allocation enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) in {enrich_time_elapsed}s.")
            logging.info(f"🏆 [ENRICH] Successfully completed raw Budget Allocation enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) in {enrich_time_elapsed}s.")
            enrich_status_final = "enrich_succeed_all"                 
        enrich_results_final = {
            "enrich_df_final": enrich_df_final,
            "enrich_status_final": enrich_status_final,
            "enrich_summary_final": {
                "enrich_time_elapsed": enrich_time_elapsed,
                "enrich_sections_total": enrich_sections_total,
                "enrich_sections_succeed": enrich_sections_succeeded,
                "enrich_sections_failed": enrich_sections_failed,
                "enrich_sections_detail": enrich_sections_detail,
                "enrich_rows_input": enrich_rows_input,
                "enrich_rows_output": enrich_rows_output,
            },
        }    
    return enrich_results_final

# 2. ENRICH BUDGET ALLOCATION FROM STAGING PHASE

# 2.1. Enrich budget allocation from staging phase
def enrich_budget_fields(enrich_df_input: pd.DataFrame, enrich_table_id: str) -> pd.DataFrame:
    print(f"🚀 [ENRICH] Starting to enrich staging Budget Allocation for {len(enrich_df_input)} row(s)...")
    logging.info(f"🚀 [ENRICH] Starting to enrich staging Budget Allocation for {len(enrich_df_input)} row(s)...")

    # 2.1.1. Start timing the staging Budget Allocation enrichment
    enrich_time_start = time.time()   
    enrich_sections_status = {}
    enrich_sections_time = {}
    enrich_df_table = pd.DataFrame()
    enrich_df_other = pd.DataFrame()
    print(f"🔍 [ENRICH] Proceeding to enrich staging Budget Allocation for {len(enrich_df_input)} row(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [ENRICH] Proceeding to enrich staging Budget Allocation for {len(enrich_df_input)} row(s) at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    # 2.1.2. Validate input for the staging Budget Allocation enrichment
    enrich_section_name = "[ENRICH] Validate input for the staging Budget Allocation enrichment"
    enrich_section_start = time.time()    
    try:
        if enrich_df_input.empty:
            enrich_sections_status[enrich_section_name] = "failed"
            print("⚠️ [ENRICH] Empty staging Budget Allocation provided then enrichment is suspended.")
            logging.warning("⚠️ [ENRICH] Empty staging Budget Allocation provided then enrichment is suspended.")
        else:
            enrich_sections_status[enrich_section_name] = "succeed"
            print("✅ [ENRICH] Successfully validated input for staging Budget Allocation enrichment.")
            logging.info("✅ [ENRICH] Successfully validated input for staging Budget Allocation enrichment.")
    finally:
        enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)

    try:

    # 2.1.3. Enrich table field(s) for staging Budget Allocation
        enrich_section_name = "[ENRICH] Enrich table field(s) for staging Budget Allocation"
        enrich_section_start = time.time()            
        try: 
            print(f"🔍 [ENRICH] Enriching table field(s) for staging Budget Allocation with {len(enrich_df_input)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enriching table field(s) for staging Budget Allocation with {len(enrich_df_input)} row(s)...")
            enrich_df_table = enrich_df_input.copy()
            enrich_df_table = enrich_df_table.assign(
                spend=lambda df: pd.to_numeric(df["spend"], errors="coerce").fillna(0)            )
            
            enrich_table_name = enrich_table_id.split(".")[-1]
            match = re.search(
                r"^(?P<company>\w+)_table_(?P<platform>\w+)_(?P<department>\w+)_(?P<account>\w+)_allocation_m\d{6}$",
                enrich_table_name
            )            
            enrich_df_table = enrich_df_table.assign(
                enrich_account_platform=match.group("platform") if match else "unknown",
                enrich_account_department=match.group("department") if match else "unknown",
                enrich_account_name=match.group("account") if match else "unknown"
            )            
            print(f"✅ [ENRICH] Successfully enriched table field(s) for staging Budget Allocation with {len(enrich_df_table)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched table field(s) for staging Budget Allocation with {len(enrich_df_table)} row(s).")
            enrich_sections_status[enrich_section_name] = "succeed"        
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to enrich table field(s) for staging TikTok Ads campaign insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich table field(s) for staging TikTok Ads campaign insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)

    # 2.1.4. Enrich internal field(s) for staging Budget Allocation
        enrich_section_name = "[ENRICH] Enrich internal field(s) for staging Budget Allocation"
        enrich_section_start = time.time()            
        try:
            print(f"🔍 [ENRICH] Enriching internal field(s) for staging TikTok Ads campaign insights with {len(enrich_df_table)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enriching internal field(s) for staging TikTok Ads campaign insights with {len(enrich_df_table)} row(s)...")
            enrich_df_internal = enrich_df_table.copy()
            enrich_df_internal["nen_tang"] = enrich_df_internal["nen_tang"].astype(str).str.strip().str.lower()        
            enrich_df_internal["nen_tang"] = enrich_df_internal["nen_tang"].astype(str).str.strip().str.lower()
            enrich_df_internal["chuong_trinh"] = enrich_df_internal["chuong_trinh"].astype(str).str.strip().str.upper()
            enrich_df_internal["noi_dung"] = enrich_df_internal["noi_dung"].astype(str).str.strip().str.upper()
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to enrich table field(s) for staging TikTok Ads campaign insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich table field(s) for staging TikTok Ads campaign insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)

    # 2.1.5. Enrich other field(s) for staging Budget Allocation
        enrich_section_name = "[ENRICH] Enrich other field(s) for staging Budget Allocation"
        enrich_section_start = time.time()            
        try:
            print(f"🔍 [ENRICH] Enriching other field(s) for staging Budget Allocation with {len(enrich_df_internal)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enriching other field(s) for staging Budget Allocation with {len(enrich_df_internal)} row(s)...")
            enrich_df_other = enrich_df_internal.copy()
            enrich_df_other = enrich_df_other.assign(
                last_updated_at=lambda _: datetime.utcnow().replace(tzinfo=pytz.UTC),
            )
            print(f"✅ [ENRICH] Successfully enriched other field(s) for staging Budget Allocation with {len(enrich_df_other)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched other field(s) for staging Budget Allocation with {len(enrich_df_other)} row(s).")
            enrich_sections_status[enrich_section_name] = "succeed"
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to enrich other field(s) for staging Budget Allocation due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich other field(s) for staging Budget Allocation due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2) 

    # 2.1.6. Summarize enrichment result(s) for staging Budget Allocation
    finally:
        enrich_time_elapsed = round(time.time() - enrich_time_start, 2)
        enrich_df_final = enrich_df_other.copy() if not enrich_df_other.empty else pd.DataFrame()
        enrich_sections_total = len(enrich_sections_status)
        enrich_sections_failed = [k for k, v in enrich_sections_status.items() if v == "failed"]
        enrich_sections_succeeded = [k for k, v in enrich_sections_status.items() if v == "succeed"]
        enrich_rows_input = len(enrich_df_input)
        enrich_rows_output = len(enrich_df_final)
        enrich_sections_summary = list(dict.fromkeys(
            list(enrich_sections_status.keys()) +
            list(enrich_sections_time.keys())
        ))
        enrich_sections_detail = {
            enrich_section_summary: {
                "status": enrich_sections_status.get(enrich_section_summary, "unknown"),
                "time": enrich_sections_time.get(enrich_section_summary, None),
            }
            for enrich_section_summary in enrich_sections_summary
        }        
        if any(v == "failed" for v in enrich_sections_status.values()):
            print(f"❌ [ENRICH] Failed to complete staging Budget Allocation enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) due to section(s) {', '.join(enrich_sections_failed)} in {enrich_time_elapsed}s.")
            logging.error(f"❌ [ENRICH] Failed to complete staging Budget Allocation enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) due to section(s) {', '.join(enrich_sections_failed)} in {enrich_time_elapsed}s.")
            enrich_status_final = "enrich_failed_all"        
        else:
            print(f"🏆 [ENRICH] Successfully completed staging Budget Allocation enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) output in {enrich_time_elapsed}s.")
            logging.info(f"🏆 [ENRICH] Successfully completed staging Budget Allocation enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) output in {enrich_time_elapsed}s.")
            enrich_status_final = "enrich_succeed_all"                
        enrich_results_final = {
            "enrich_df_final": enrich_df_final,
            "enrich_status_final": enrich_status_final,
            "enrich_summary_final": {
                "enrich_time_elapsed": enrich_time_elapsed,
                "enrich_sections_total": enrich_sections_total,
                "enrich_sections_succeed": enrich_sections_succeeded,
                "enrich_sections_failed": enrich_sections_failed,
                "enrich_sections_detail": enrich_sections_detail,
                "enrich_rows_input": enrich_rows_input,
                "enrich_rows_output": enrich_rows_output,
            },
        }
    return enrich_results_final