"""
==================================================================
BUDGET UPDATE MODULE
------------------------------------------------------------------
This module performs incremental updates to Budget Allocation data 
at the raw layer, providing an efficient mechanism for refreshing  
recent or specific datasets without the need for full reloads.

By supporting targeted updates included per day, layer or entity, it
enables faster turnaround for near-real-time dashboards and daily  
data sync jobs while maintaining historical accuracy and integrity.

✔️ Handles incremental data ingestion from the GSpread API
✔️ Supports selective updates for campaign, adset, ad or creative  
✔️ Preserves schema alignment with staging and MART layers  
✔️ Implements error handling and retry logic for partial failures  
✔️ Designed for integration in daily or on-demand sync pipelines  

⚠️ This module is strictly responsible for raw layer updates only.  
It does not perform transformations, enrichment or aggregations.  
Processed data is consumed by the staging and materialization modules.
==================================================================
"""

# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add Python logging ultilities for integration
import logging

# Add Python time ultilities for integration
import time

# Add internal Budget Allocation modules for handling
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

# 1.1. Update Budget Allocation data for a given date range
def update_budget_allocation(update_month_allocation: str) -> None:
    print(f"🚀 [UPDATE] Starting to update Budget Allocation for month {update_month_allocation}...")
    logging.info(f"🚀 [UPDATE] Starting to update Budget Allocation for month {update_month_allocation}...")

    # 1.1.1. Start timing TikTok Ads campaign insights update
    update_time_start = time.time()
    update_sections_status = {}
    update_sections_time = {}
    print(f"🔍 [UPDATE] Proceeding to update Budget Allocation for month {update_month_allocation} at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
    logging.info(f"🔍 [UPDATE] Proceeding to update Budget Allocation for month {update_month_allocation} at {time.strftime('%Y-%m-%d %H:%M:%S')}...")

    try:  

    # 1.1.2. Trigger to ingest Budget Allocation
        update_section_name = "[UPDATE] Trigger to ingest Budget Allocation"
        update_section_start = time.time()
        try:
            print(f"🔄 [UPDATE] Triggering to ingest Budget Allocation for month {update_month_allocation}...")
            logging.info(f"🔄 [UPDATE] Triggering to ingest Budget Allocation for month {update_month_allocation}...")
            ingest_results_allocation = ingest_budget_allocation(ingest_month_allocation=update_month_allocation)
            ingest_df_insights = ingest_results_allocation["ingest_df_final"]
            ingest_status_insights = ingest_results_allocation["ingest_status_final"]
            ingest_summary_insights = ingest_results_allocation["ingest_summary_final"]
            if ingest_status_insights == "ingest_succeed_all":
                print(f"✅ [UPDATE] Successfully triggered Budget Allocation ingestion for month {update_month_allocation} with {ingest_summary_insights['ingest_rows_output']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")
                logging.info(f"✅ [UPDATE] Successfully triggered Budget Allocation ingestion for month {update_month_allocation} with {ingest_summary_insights['ingest_rows_output']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")
                update_sections_status[update_section_name] = "succeed"
            else:
                update_sections_status[update_section_name] = "failed"
                print(f"❌ [UPDATE] Failed to trigger Budget Allocation ingestion for month {update_month_allocation} {ingest_summary_insights['ingest_rows_output']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")
                logging.error(f"❌ [UPDATE] Failed to trigger Budget Allocation ingestion for month {update_month_allocation} {ingest_summary_insights['ingest_rows_output']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")
        finally:
            update_sections_time[update_section_name] = round(time.time() - update_section_start, 2)

    # 1.1.3. Trigger to build staging Budget Allocation
        update_section_name = "[UPDATE] Trigger to build staging Budget Allocation"
        update_section_start = time.time()
        try:
            print("🔄 [UPDATE] Triggering to create or overwrite staging Budget Allocation table...")
            logging.info("🔄 [UPDATE] Triggering to create or overwrite staging Budget Allocation table...")
            staging_results_allocation = staging_budget_allocation()
            staging_status_campaign = staging_results_allocation["staging_status_final"]
            staging_summary_campaign = staging_results_allocation["staging_summary_final"]
            if staging_status_campaign == "staging_succeed_all":
                print(f"✅ [UPDATE] Successfully triggered Budget Allocation staging with {staging_summary_campaign['staging_tables_output']}/{staging_summary_campaign['staging_tables_input']} table(s) on {staging_summary_campaign['staging_tables_input']} queried table(s) and {staging_summary_campaign['staging_rows_output']} uploaded row(s) in {staging_summary_campaign['staging_time_elapsed']}s.")
                logging.info(f"✅ [UPDATE] Successfully triggered Budget Allocation staging with {staging_summary_campaign['staging_tables_output']}/{staging_summary_campaign['staging_tables_input']} table(s) on {staging_summary_campaign['staging_tables_input']} queried table(s) and {staging_summary_campaign['staging_rows_output']} uploaded row(s) in {staging_summary_campaign['staging_time_elapsed']}s.")
                update_sections_status[update_section_name] = "succeed"
            elif staging_status_campaign == "staging_failed_partial":
                print(f"⚠️ [UPDATE] Partially triggered Budget Allocation staging with {staging_summary_campaign['staging_tables_output']}/{staging_summary_campaign['staging_tables_input']} table(s) on {staging_summary_campaign['staging_tables_input']} queried table(s) and {staging_summary_campaign['staging_rows_output']} uploaded row(s) in {staging_summary_campaign['staging_time_elapsed']}s.")
                logging.warning(f"⚠️ [UPDATE] Partially triggered Budget Allocation staging with {staging_summary_campaign['staging_tables_output']}/{staging_summary_campaign['staging_tables_input']} table(s) on {staging_summary_campaign['staging_tables_input']} queried table(s) and {staging_summary_campaign['staging_rows_output']} uploaded row(s) in {staging_summary_campaign['staging_time_elapsed']}s.")
                update_sections_status[update_section_name] = "partial"
            else:
                print(f"❌ [UPDATE] Failed to trigger Budget Allocation staging with {staging_summary_campaign['staging_tables_output']}/{staging_summary_campaign['staging_tables_input']} table(s) on {staging_summary_campaign['staging_tables_input']} queried table(s) and {staging_summary_campaign['staging_rows_output']} uploaded row(s) in {staging_summary_campaign['staging_time_elapsed']}s.")
                logging.error(f"❌ [UPDATE] Failed to trigger Budget Allocation staging with {staging_summary_campaign['staging_tables_output']}/{staging_summary_campaign['staging_tables_input']} table(s) on {staging_summary_campaign['staging_tables_input']} queried table(s) and {staging_summary_campaign['staging_rows_output']} uploaded row(s) in {staging_summary_campaign['staging_time_elapsed']}s.")
                update_sections_status[update_section_name] = "failed"
        finally:
            update_sections_time[update_section_name] = round(time.time() - update_section_start, 2)

    # 1.1.4. Trigger to materialize Budget Allocation
        update_section_name = "[UPDATE] Trigger to materialize Budget Allocation"
        update_section_start = time.time()
        try:
            if staging_status_campaign in ["staging_succeed_all", "staging_failed_partial"]:
                print("🔄 [UPDATE] Triggering to materialize Budget Allocation...")
                logging.info("🔄 [UPDATE] Triggering to materialize Budget Allocation...")
                mart_results_allocation = mart_budget_allocation()
                mart_status_all = mart_results_allocation["mart_status_final"]
                mart_summary_all = mart_results_allocation["mart_summary_final"]                
                if mart_status_all == "mart_succeed_all":
                    print(f"✅ [UPDATE] Successfully completed Budget Allocation materialization in {mart_summary_all['mart_time_elapsed']}s.")
                    logging.info(f"✅ [UPDATE] Successfully completed Budget Allocation materialization in {mart_summary_all['mart_time_elapsed']}s.")
                    update_sections_status[update_section_name] = "succeed"
                elif mart_status_all == "mart_failed_all":
                    print(f"❌ [UPDATE] Failed to complete Budget Allocation materialization due to unsuccessful section(s) of {', '.join(mart_summary_all['mart_sections_failed']) if mart_summary_all['mart_sections_failed'] else 'unknown'}.")
                    logging.error(f"❌ [UPDATE] Failed to complete Budget Allocation materialization due to unsuccessful section(s) of {', '.join(mart_summary_all['mart_sections_failed']) if mart_summary_all['mart_sections_failed'] else 'unknown'}.")
                    update_sections_status[update_section_name] = "failed"
        finally:
            update_sections_time[update_section_name] = round(time.time() - update_section_start, 2)

    # 1.1.5. Summarize update results for Budget Allocation
    finally:
        update_time_total = round(time.time() - update_time_start, 2)
        print("\n📊 [UPDATE] BUDGET ALLOCATION UPDATE SUMMARY")
        print("=" * 120)
        print(f"{'Step':<80} | {'Status':<10} | {'Time (s)'}")
        print("-" * 120)

        # Summarize sections mapping to results dicts
        update_summary_mapping = {
            "[UPDATE] Trigger to ingest Budget Allocation": locals().get("ingest_results_allocation"),
            "[UPDATE] Trigger to build staging Budget Allocation": locals().get("staging_results_allocation"),
            "[UPDATE] Trigger to materialize Budget Allocation": locals().get("mart_results_allocation"),
        }
        for update_section_name, update_section_status in update_sections_status.items():
            update_section_result = update_summary_mapping.get(update_section_name)
            update_summary_nested = None

        # Determine nested summary final
            if isinstance(update_section_result, dict):
                for k, v in update_section_result.items():
                    if isinstance(v, dict) and k.endswith("_summary_final"):
                        update_summary_nested = v
                        break

        # Determine section time
            update_section_time = "-"
            if update_summary_nested:
                for k in ("ingest_time_elapsed", "staging_time_elapsed", "mart_time_elapsed", "time_elapsed"):
                    if k in update_summary_nested and isinstance(update_summary_nested[k], (float, int)):
                        update_section_time = f"{update_summary_nested[k]:>8.2f}"
                        break

        # Print sub-sections detail
            print(f"• {update_section_name:<76} | {update_section_status:<10} | {update_section_time}")
            if update_summary_nested:
                for update_section_detail in ["sections_detail", "mart_sections_detail", "staging_sections_detail", "ingest_sections_detail"]:
                    sub_section_detail = update_summary_nested.get(update_section_detail)
                    if isinstance(sub_section_detail, dict):
                        for idx, (sub_section_name, sub_section_detail) in enumerate(sub_section_detail.items(), start=1):
                            sub_section_status = sub_section_detail.get("status", "-")
                            sub_time_single = sub_section_detail.get("time", 0.0) or 0.0
                            sub_time_loop = sub_section_detail.get("loop_time", 0.0) or 0.0
                            sub_time_total = round(sub_time_single + sub_time_loop, 2)
                            print(f"    {idx:>2}. {sub_section_name:<70} | {sub_section_status:<10} | {sub_time_total:>8.2f}")

        # Print total execution result
        print("-" * 120)
        print(f"{'Total execution time':<80} | {'-':<10} | {update_time_total:>8.2f}s")
        print("=" * 120)