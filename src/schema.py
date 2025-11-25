"""
===================================================================
BUDTE SCHEMA MODULE
-------------------------------------------------------------------
This module provides a centralized definition and management of  
schema structures used throughout the Budget data pipeline.  
It shares a consistent structure and data type alignment.  

Its main purpose is to validate, enforce, and standardize field 
structures across every pipeline stage to support reliable
execution and seamless data integration.

✔️ Define and store expected field names and data types
✔️ Validate schema integrity before ingestion or transformation  
✔️ Enforce data type consistency across different processing layers  
✔️ Automatically handle missing or mismatched columns  
✔️ Provide schema utilities for debugging and audit logging  

⚠️ This module does not perform data fetching or transformation.  
It serves purely as a utility layer to support schema consistency  
throughout the Budget ETL process.
===================================================================
"""

# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add Python logging ultilities for integration
import logging

# Add Python time ultilities for integration
import time

# Add Python Pandas libraries for integration
import pandas as pd

# 1. ENFORCE SCHEMA FOR GIVEN PYTHON DATAFRAME

# 1.1. Enforce that the given DataFrame contains all required columns with correct datatypes
def enforce_table_schema(schema_df_input: pd.DataFrame, schema_type_mapping: str) -> pd.DataFrame:
    print(f"🚀 [SCHEMA] Starting to enforce schema {schema_type_mapping} on Python DataFrame with {schema_df_input.shape[1]} column(s)...")
    logging.info(f"🚀 [SCHEMA] Starting to enforce schema {schema_type_mapping} on Python DataFrame with {schema_df_input.shape[1]} column(s)...")
    
    # 1.1.1. Start timing the Budget Allocation enrichment
    schema_time_start = time.time()
    schema_sections_status = {}
    schema_sections_time = {}
    print(f"🔍 [SCHEMA] Proceeding to enforce schema for Budget Allocation with {len(schema_df_input)} given row(s) for mapping type {schema_type_mapping} at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [SCHEMA] Proceeding to enforce schema for Budget Allocation with {len(schema_df_input)} given row(s) for mapping type {schema_type_mapping} at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    # 1.1.2. Define schema mapping for Budget Allocation data type
    schema_section_name = "[SCHEMA] Define schema mapping for Budget Allocation data type"
    schema_section_start = time.time()        
    schema_types_mapping = {
        "fetch_budget_allocation": {
            "raw_budget_group": str,
            "raw_budget_type": str,
            "raw_budget_region": str,
            "raw_category_group": str,
            "raw_budget_detail": str,
            "raw_program_track": str,
            "raw_program_group": str,
            "raw_program_type": str,
            "raw_date_month": str,
            "raw_date_start": str,
            "raw_date_end": str,
            "raw_budget_platform": str,
            "raw_budget_objective": str,
            "raw_budget_initial": int,
            "raw_budget_adjusted": int,
            "raw_budget_additional": int,
        },
        "ingest_budget_allocation": {
            "raw_budget_group": str,
            "raw_budget_type": str,
            "raw_budget_region": str,
            "raw_category_group": str,
            "raw_budget_detail": str,
            "raw_program_track": str,
            "raw_program_group": str,
            "raw_program_type": str,
            "raw_date_month": str,
            "raw_date_start": str,
            "raw_date_end": str,
            "raw_budget_platform": str,
            "raw_budget_objective": str,
            "raw_budget_initial": int,
            "raw_budget_adjusted": int,
            "raw_budget_additional": int,
        },
        "staging_budget_allocation": {
            "raw_budget_group": str,
            "raw_budget_type": str,
            "raw_budget_region": str,
            "raw_category_group": str,
            "raw_budget_detail": str,
            "raw_program_track": str,
            "raw_program_group": str,
            "raw_program_type": str,
            "raw_date_month": str,
            "raw_date_start": "datetime64[ns]",
            "raw_date_end": "datetime64[ns]",
            "raw_budget_platform": str,
            "raw_budget_objective": str,
            "raw_budget_initial": int,
            "raw_budget_adjusted": int,
            "raw_budget_additional": int,
            
            # Enriched dimensions and specific to budget classfication
            "enrich_budget_actual": int,
            "enrich_budget_marketing": int,
            "enrich_budget_supplier": int,
            "enrich_budget_retail": int,
            "enrich_budget_customer": int,
            "enrich_budget_recruitment": int,

            # Enriched dimensions from table_id and specific to internal company structure
            "enrich_account_platform": str,
            "enrich_account_department": str,
            "enrich_account_name": str,

            # Standardized time columns
            "enrich_time_total": int,
            "enrich_time_passed": int,            
            "last_updated_at": "datetime64[ns, UTC]",           
        }
    }

    schema_sections_status[schema_section_name] = "succeed"
    schema_sections_time[schema_section_name] = round(time.time() - schema_section_start, 2)
    
    try:

    # 1.1.3. Validate that the given schema_type_mapping exists
        schema_section_name = "[SCHEMA] Validate that the given schema_type_mapping exists"
        schema_section_start = time.time()            
        try:
            if schema_type_mapping not in schema_types_mapping:
                schema_sections_status[schema_section_name] = "failed"
                print(f"❌ [SCHEMA] Failed to validate schema type {schema_type_mapping} for Budget Allocation then enforcement will be suspended.")
                logging.error(f"❌ [SCHEMA] Failed to validate schema type {schema_type_mapping} for Budget Allocation then enforcement will be suspended.")
            else:
                schema_columns_expected = schema_types_mapping[schema_type_mapping]
                schema_sections_status[schema_section_name] = "succeed"
                print(f"✅ [SCHEMA] Successfully validated schema type {schema_type_mapping} for Budget Allocation.")
                logging.info(f"✅ [SCHEMA] Successfully validated schema type {schema_type_mapping} for Budget Allocation.")
        finally:
            schema_sections_time[schema_section_name] = round(time.time() - schema_section_start, 2)

    # 1.1.4. Enforce schema columns for Budget Allocation
        schema_section_name = "[SCHEMA] Enforce schema columns for Budget Allocation"
        schema_section_start = time.time()              
        try:
            print(f"🔄 [SCHEMA] Enforcing schema for Budget Allocation with schema type {schema_type_mapping}...")
            logging.info(f"🔄 [SCHEMA] Enforcing schema for Budget Allocation with schema type {schema_type_mapping}...")
            schema_df_enforced = schema_df_input.copy()            
            for schema_column_expected, schema_data_type in schema_columns_expected.items():
                if schema_column_expected not in schema_df_enforced.columns: 
                    schema_df_enforced[schema_column_expected] = pd.NA               
                try:
                    if schema_data_type == int:
                        schema_df_enforced[schema_column_expected] = pd.to_numeric(
                            schema_df_enforced[schema_column_expected].astype(str).str.replace(",", "."), errors="coerce"
                        ).fillna(0).astype(int)
                    elif schema_data_type == float:
                        schema_df_enforced[schema_column_expected] = pd.to_numeric(
                            schema_df_enforced[schema_column_expected].astype(str).str.replace(",", "."), errors="coerce"
                        ).fillna(0.0).astype(float)
                    elif schema_data_type == "datetime64[ns, UTC]":
                        schema_df_enforced[schema_column_expected] = pd.to_datetime(
                            schema_df_enforced[schema_column_expected], errors="coerce", utc=True
                        )
                    elif schema_data_type == str:
                        schema_df_enforced[schema_column_expected] = schema_df_enforced[schema_column_expected].astype(str).fillna("")
                    else:
                        schema_df_enforced[schema_column_expected] = schema_df_enforced[schema_column_expected]
                except Exception as e:
                    print(f"⚠️ [SCHEMA] Failed to coerce column {schema_column_expected} of Budget Allocation to {schema_data_type} due to {e}.")
                    logging.warning(f"⚠️ [SCHEMA] Failed to coerce column {schema_column_expected} of Budget Allocation to {schema_data_type} due to {e}.")
            schema_df_enforced = schema_df_enforced[list(schema_columns_expected.keys())]       
            print(f"✅ [SCHEMA] Successfully enforced schema for Budget Allocation with {len(schema_df_enforced)} row(s) and schema type {schema_type_mapping}.")
            logging.info(f"✅ [SCHEMA] Successfully enforced schema for Budget Allocation with {len(schema_df_enforced)} row(s) and schema type {schema_type_mapping}.")
            schema_sections_status[schema_section_name] = "succeed"        
        except Exception as e:
            schema_sections_status[schema_section_name] = "failed"
            print(f"❌ [SCHEMA] Failed to enforce schema for Budget Allocation with schema type {schema_type_mapping} due to {e}.")
            logging.error(f"❌ [SCHEMA] Failed to enforce schema for Budget Allocation with schema type {schema_type_mapping} due to {e}.")
        finally:
            schema_sections_time[schema_section_name] = round(time.time() - schema_section_start, 2)   

    # 1.1.5. Summarize schema enforcement results for Budget Allocation
    finally:
        schema_time_elapsed = round(time.time() - schema_time_start, 2)
        schema_df_final = schema_df_enforced.copy() if "schema_df_enforced" in locals() and not schema_df_enforced.empty else pd.DataFrame()        
        schema_sections_total = len(schema_sections_status)
        schema_sections_succeed = [k for k, v in schema_sections_status.items() if v == "succeed"]
        schema_sections_failed = [k for k, v in schema_sections_status.items() if v == "failed"]        
        schema_rows_input = len(schema_df_input)
        schema_rows_output = len(schema_df_final)        
        schema_sections_summary = list(dict.fromkeys(
            list(schema_sections_status.keys()) +
            list(schema_sections_time.keys())
        ))
        schema_sections_detail = {
            schema_section_summary: {
                "status": schema_sections_status.get(schema_section_summary, "unknown"),
                "time": round(schema_sections_time.get(schema_section_summary, 0.0), 2),
            }
            for schema_section_summary in schema_sections_summary
        }         
        if any(v == "failed" for v in schema_sections_status.values()):
            print(f"❌ [SCHEMA] Failed to complete schema enforcement for Budget Allocation due to section(s): {', '.join(schema_sections_failed)} in {schema_time_elapsed}s.")
            logging.error(f"❌ [SCHEMA] Failed to complete schema enforcement for Budget Allocation due to section(s): {', '.join(schema_sections_failed)} in {schema_time_elapsed}s.")
            schema_status_final = "schema_failed_all"
        else:
            print(f"🏆 [SCHEMA] Successfully completed schema enforcement for Budget Allocation with {schema_rows_output} enforced row(s) output in {schema_time_elapsed}s.")
            logging.info(f"🏆 [SCHEMA] Successfully completed schema enforcement for Budget Allocation with {schema_rows_output} enforced row(s) output in {schema_time_elapsed}s.")
            schema_status_final = "schema_succeed_all"        
        schema_results_final = {
            "schema_df_final": schema_df_final,
            "schema_status_final": schema_status_final,
            "schema_summary_final": {
                "schema_time_elapsed": schema_time_elapsed,
                "schema_sections_total": schema_sections_total,
                "schema_sections_succeed": schema_sections_succeed,
                "schema_sections_failed": schema_sections_failed,
                "schema_sections_detail": schema_sections_detail,
                "schema_rows_input": schema_rows_input,
                "schema_rows_output": schema_rows_output,
            },
        }    
    return schema_results_final