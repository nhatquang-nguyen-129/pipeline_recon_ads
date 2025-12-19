"""
==================================================================
BUDGET MATERIALIZATION MODULE
------------------------------------------------------------------
This module materializes the final layer for Budget Allocation by 
aggregating and transforming data sourced from staging tables 
produced during the raw data ingestion process.

It serves as the final transformation stage, consolidating daily 
performance and cost metrics into analytics-ready BigQuery tables 
optimized for reporting, dashboarding, and business analysis.

✔️ Dynamically identifies all Budget Allocation staging tables  
✔️ Applies data transformation, standardization, and type enforcement  
✔️ Performs daily-level aggregation of campaign performance metrics  
✔️ Creates partitioned and clustered MART tables in Google BigQuery  
✔️ Ensures consistency and traceability across the data pipeline  

⚠️ This module is exclusively responsible for materialized layer  
construction. It does not perform data ingestion, API fetching, 
or enrichment tasks.
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

# Add Python time ultilities for integration
import time

# Add Python IANA time zone ultilities for integration
from zoneinfo import ZoneInfo

# Add Google API Core modules for integration
from google.api_core.exceptions import NotFound

# Add Google Cloud modules for integration
from google.cloud import bigquery

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

# 1. BUILD MONTHLY MATERIALIZED TABLE FOR BUDGET ALLOCATION

# 1.1. Build materialized table for Budget Allocation by union all staging tables
def mart_budget_allocation() -> dict:
    print(f"🚀 [MART] Starting to build materialized table for Budget Allocation...")
    logging.info(f"🚀 [MART] Starting to build materialized table for Budget Allocation...")

    # 1.1.1. Start timing the Budget Allocation materialization
    ICT = ZoneInfo("Asia/Ho_Chi_Minh")    
    mart_time_start = time.time()
    mart_sections_status = {}
    mart_sections_time = {}
    print(f"🔍 [MART] Proceeding to build materialized table for Budget Allocation at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")
    logging.info(f"🔍 [MART] Proceeding to build materialized table for Budget Allocation at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")

    try:
    
    # 1.1.2. Prepare Google BigQuery table_id for materialization
        mart_section_name = "[MART] Prepare Google BigQuery table_id for materialization"
        mart_section_start = time.time()        
        try: 
            staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
            staging_table_budget = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_allocation_monthly"
            print(f"🔍 [MART] Using staging table {staging_table_budget} to build materialized table for Budget Allocation...")
            logging.info(f"🔍 [MART] Using staging table {staging_table_budget} to build materialized table for Budget Allocation...")
            mart_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_mart"
            mart_table_all = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{PLATFORM}_all_all_allocation_monthly"
            print(f"🔍 [MART] Preparing to build materialized table {mart_table_all} for Budget Allocation...")
            logging.info(f"🔍 [MART] Preparing to build materialized table {mart_table_all} for Budget Allocation...")
            mart_sections_status[mart_section_name] = "succeed"    
        finally:
            mart_sections_time[mart_section_name] = round(time.time() - mart_section_start, 2)           

    # 1.1.3. Initialize Google BigQuery client
        mart_section_name = "[MART] Initialize Google BigQuery client"
        mart_section_start = time.time()
        try:
            print(f"🔍 [MART] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [MART] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            google_bigquery_client = bigquery.Client(project=PROJECT)
            mart_sections_status[mart_section_name] = "succeed"
            print(f"✅ [MART] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [MART] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")            
        except Exception as e:
            mart_sections_status[mart_section_name] = "failed"
            print(f"❌ [MART] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [MART] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        finally:
            mart_sections_time[mart_section_name] = round(time.time() - mart_section_start, 2)

    # 1.1.4. Query all staging Budget Allocation tables
        mart_section_name = "[MART] Query all staging Budget Allocation tables"
        mart_section_start = time.time()    
        try:
            print(f"🔄 [MART] Querying staging Budget Allocation table {staging_table_budget} to create or replace materialized table...")
            logging.info(f"🔄 [MART] Querying staging Budget Allocation table {staging_table_budget} to create or replace materialized table...")            
            query_replace_config = f"""
                CREATE OR REPLACE TABLE `{mart_table_all}`
                CLUSTER BY thang, ma_ngan_sach_cap_1, hang_muc AS
                SELECT
                    SAFE_CAST(enrich_account_name AS STRING) AS tai_khoan,
                    SAFE_CAST(enrich_account_department AS STRING) AS phong_ban,
                    SAFE_CAST(enrich_account_platform AS STRING) AS nen_tang,
                    SAFE_CAST(raw_budget_group AS STRING) AS ma_ngan_sach_cap_1,
                    SAFE_CAST(raw_budget_type AS STRING) AS ma_ngan_sach_cap_2,
                    SAFE_CAST(raw_program_track AS STRING) AS hang_muc,
                    SAFE_CAST(raw_program_group AS STRING) AS chuong_trinh,
                    SAFE_CAST(raw_program_type AS STRING) AS noi_dung,
                    SAFE_CAST(raw_budget_platform AS STRING) AS kenh,
                    SAFE_CAST(raw_budget_objective AS STRING) AS hinh_thuc,
                    SAFE_CAST(raw_date_month AS STRING) AS thang,
                    SAFE_CAST(raw_date_start AS TIMESTAMP) AS thoi_gian_bat_dau,
                    SAFE_CAST(raw_date_end AS TIMESTAMP) AS thoi_gian_ket_thuc,
                    SAFE_CAST(enrich_time_total AS INT) AS tong_so_ngay_thuc_chay,
                    SAFE_CAST(enrich_time_passed AS INT) AS tong_so_ngay_da_qua,
                    SAFE_CAST(raw_budget_initial AS INT) AS ngan_sach_ban_dau,
                    SAFE_CAST(raw_budget_adjusted AS INT) AS ngan_sach_dieu_chinh,
                    SAFE_CAST(raw_budget_additional AS INT) AS ngan_sach_bo_sung,
                    SAFE_CAST(enrich_budget_actual AS INT) AS ngan_sach_thuc_chi,
                    SAFE_CAST(enrich_budget_marketing AS INT) AS ngan_sach_he_thong,
                    SAFE_CAST(enrich_budget_supplier AS INT) AS ngan_sach_nha_cung_cap,
                    SAFE_CAST(enrich_budget_retail AS INT) AS ngan_sach_kinh_doanh,
                    SAFE_CAST(enrich_budget_customer AS INT) AS ngan_sach_tien_san,
                    SAFE_CAST(enrich_budget_recruitment AS INT) AS ngan_sach_tuyen_dung,
                FROM `{staging_table_budget}`
            """
            query_replace_load = google_bigquery_client.query(query_replace_config)
            query_replace_result = query_replace_load.result()
            query_count_config = f"SELECT COUNT(1) AS mart_rows_count FROM `{mart_table_all}`"
            query_count_load = google_bigquery_client.query(query_count_config)
            query_count_result = query_count_load.result()
            mart_rows_uploaded = list(query_count_result)[0]["mart_rows_count"]
            mart_sections_status[mart_section_name] = "succeed"
            print(f"✅ [MART] Successfully created or replace materialized table {mart_table_all} for Budget Allocation with {mart_rows_uploaded} row(s).")
            logging.info(f"✅ [MART] Successfully created or replace materialized table {mart_table_all} for Budget Allocation with {mart_rows_uploaded} row(s).")
        except Exception as e:
            mart_sections_status[mart_section_name] = "failed"
            print(f"❌ [MART] Failed to create or replace materialized table for Budget Allocation due to {e}.")
            logging.error(f"❌ [MART] Failed to create or replace materialized table for Budget Allocation due to {e}.")
        finally:
            mart_sections_time[mart_section_name] = round(time.time() - mart_section_start, 2)

    # 1.1.5. Summarize materialization results for Budget Allocation
    finally:
        mart_time_elapsed = round(time.time() - mart_time_start, 2)
        mart_sections_total = len(mart_sections_status) 
        mart_sections_failed = [k for k, v in mart_sections_status.items() if v == "failed"] 
        mart_sections_succeeded = [k for k, v in mart_sections_status.items() if v == "succeed"]
        mart_rows_output = mart_rows_uploaded
        mart_sections_summary = list(dict.fromkeys(
            list(mart_sections_status.keys()) +
            list(mart_sections_time.keys())
        ))
        mart_sections_detail = {
            mart_section_summary: {
                "status": mart_sections_status.get(mart_section_summary, "unknown"),
                "time": round(mart_sections_time.get(mart_section_summary, 0.0), 2),
            }
            for mart_section_summary in mart_sections_summary
        }       
        if mart_sections_failed:
            mart_status_final = "mart_failed_all"
            print(f"❌ [MART] Failed to complete Budget Allocation materialization with {mart_rows_output} materialized row(s) due to {', '.join(mart_sections_failed)} failed section(s) in {mart_time_elapsed}s.")
            logging.error(f"❌ [MART] Failed to complete Budget Allocation materialization with {mart_rows_output} materialized row(s) due to {', '.join(mart_sections_failed)} failed section(s) in {mart_time_elapsed}s.")
        else:
            mart_status_final = "mart_succeed_all"
            print(f"🏆 [MART] Successfully completed Budget Allocatione materialization with {mart_rows_output} materialized row(s) in {mart_time_elapsed}s.")
            logging.info(f"🏆 [MART] Successfully completed Budget Allocation materialization with {mart_rows_output} materialized row(s) in {mart_time_elapsed}s.")
        mart_results_final = {
            "mart_df_final": None,
            "mart_status_final": mart_status_final,
            "mart_summary_final": {
                "mart_time_elapsed": mart_time_elapsed,
                "mart_sections_total": mart_sections_total,
                "mart_sections_succeed": mart_sections_succeeded,
                "mart_sections_failed": mart_sections_failed,
                "mart_sections_detail": mart_sections_detail,
                "mart_rows_output": mart_rows_output,
            },
        }
    return mart_results_final

# 2. BUILD SPENDING AGGREGATION TABLE FROM MULTIPLE ADVERTISING PLATFORMS

# 2.1. Build spending aggregation table from multiple advertising platforms
def mart_aggregate_all():
    print("🚀 [MART] Starting to materialize Spending Aggregation...")
    logging.info("🚀 [MART] Starting to materialize Spending Aggregation...")

    # 2.1.1. Start timing the Spending Aggregation
    ICT = ZoneInfo("Asia/Ho_Chi_Minh")      
    mart_time_start = time.time()
    mart_sections_status = {}
    mart_sections_time = {}
    print(f"🔍 [MART] Proceeding to build materialized table for Spending Aggregation at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")
    logging.info(f"🔍 [MART] Proceeding to build materialized table for Spending Aggregation at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")

    try: 
    
    # 2.1.2. Initialize Google BigQuery client
        mart_section_name = "[MART] Initialize Google BigQuery client"
        mart_section_start = time.time()
        try:
            print(f"🔍 [MART] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [MART] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            google_bigquery_client = bigquery.Client(project=PROJECT)
            mart_sections_status[mart_section_name] = "succeed"
            print(f"✅ [MART] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [MART] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")            
        except Exception as e:
            mart_sections_status[mart_section_name] = "failed"
            print(f"❌ [MART] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [MART] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        finally:
            mart_sections_time[mart_section_name] = round(time.time() - mart_section_start, 2)

    # 2.1.3. Scan existing campaign performance tables
        mart_section_name = "[MART] Scan existing campaign performance tables"
        mart_section_start = time.time()
        mart_tables_campaign = []
        mart_dataset_prefix = f"{COMPANY}_dataset_"
        mart_suffixes_excluded = ["budget", "recon"]
        mart_datasets_all = google_bigquery_client.list_datasets(project=PROJECT)
        try:        
            for mart_dataset_all in mart_datasets_all:
                mart_dataset_id = mart_dataset_all.dataset_id
                if not mart_dataset_id.startswith(mart_dataset_prefix):
                    continue
                if any(mart_suffix_excluded in mart_dataset_id for mart_suffix_excluded in mart_suffixes_excluded):
                    continue
                mart_tables_included = google_bigquery_client.list_tables(f"{PROJECT}.{mart_dataset_id}")
                for mart_table_included in mart_tables_included:
                    mart_table_id = mart_table_included.table_id
                    if "campaign_performance" not in mart_table_id:
                        continue
                    mart_table_campaign = f"{PROJECT}.{mart_dataset_id}.{mart_table_id}"
                    try:
                        print(f"🔍 [MART] Scanning for existing materialized campaign performance table in Google BigQuery dataset {mart_dataset_id}...")
                        logging.info(f"🔍 [MART] Scanning for existing materialized campaign performance table in Google BigQuery dataset {mart_dataset_id}...")
                        google_bigquery_client.get_table(mart_table_campaign)
                        mart_tables_campaign.append(mart_table_campaign)
                        print(f"✅ [MART] Successfully found existing materialized campaign performance table {mart_table_campaign}.")
                        logging.info(f"✅ [MART] Successfully found existing materialized campaign performance table {mart_table_campaign}.")
                    except NotFound:
                        print(f"⚠️ [MART] No existing materialized campaign performance table found in Google BigQuery dataset {mart_dataset_id}.")
                        logging.warning(f"⚠️ [MART] No existing materialized campaign performance table found in Google BigQuery dataset {mart_dataset_id}.")
                        continue
        except Exception as e:
            mart_sections_status[mart_section_name] = "failed"
            print(f"❌ [MART] Failed to scan existing materialized campaign performance tables due to {e}.")
            logging.error(f"❌ [MART] Failed to scan existing materialized campaign performance tables due to {e}.")
        else:
            mart_sections_status[mart_section_name] = "succeed"
        finally:
            mart_sections_time[mart_section_name] = round(time.time() - mart_section_start, 2)
      
    # 2.1.4. Query all materialized campaign performance tables
        mart_section_name = "[MART] Query all staging Budget Allocation table(s)"
        mart_section_start = time.time()          
        try:
            if mart_tables_campaign:
                try:
                    mart_table_spend = f"{PROJECT}.{COMPANY}_dataset_recon_api_mart.{COMPANY}_table_recon_all_all_campaign_spend"
                    query_replace_config = f"""
                        CREATE OR REPLACE TABLE `{mart_table_spend}`
                        PARTITION BY ngay
                        CLUSTER BY ma_ngan_sach_cap_1, chuong_trinh, nhan_su, thang
                        AS
                        {'\nUNION ALL\n'.join([
                            f'''
                            SELECT
                                INITCAP(nen_tang) AS nen_tang,
                                phong_ban,
                                tai_khoan,
                                ma_ngan_sach_cap_1,
                                ma_ngan_sach_cap_2,
                                hang_muc,
                                chuong_trinh,
                                noi_dung,
                                hinh_thuc,
                                khu_vuc,
                                nhan_su,
                                nganh_hang,
                                campaign_name AS chien_dich,
                                ngay,
                                thang,
                                nam,
                                spend AS chi_tieu,
                                result AS ket_qua,
                                result_type AS loai_ket_qua,
                                trang_thai,
                            FROM `{mart_table_campaign}`
                            WHERE spend > 0
                            ''' for mart_table_campaign in mart_tables_campaign
                        ])}
                    """
                    print(f"🔄 [MART] Querying all campaign performance tables to create or replace materialized table {mart_table_spend} for Spending Aggregation...")
                    logging.info(f"🔄 [MART] Querying all campaign performance tables to create or replace materialized table {mart_table_spend} for Spending Aggregation...")
                    query_replace_load = google_bigquery_client.query(query_replace_config)
                    query_replace_result = query_replace_load.result()
                    query_count_config = f"SELECT COUNT(1) AS mart_rows_count FROM `{mart_table_spend}`"
                    query_count_load = google_bigquery_client.query(query_count_config)
                    query_count_result = query_count_load.result()
                    mart_rows_uploaded = list(query_count_result)[0]["mart_rows_count"]
                    mart_sections_status[mart_section_name] = "succeed"
                    print(f"✅ [MART] Successfully created or replace materialized table {mart_table_spend} for Spending Aggregation with {mart_rows_uploaded} row(s).")
                    logging.info(f"✅ [MART] Successfully created or replace materialized table {mart_table_spend} for Spending Aggregation with {mart_rows_uploaded} row(s).")            
                except Exception as e:
                    mart_sections_status[mart_section_name] = "failed"
                    print(f"❌ [MART] Failed to create or replace materialized table for Spending Aggregation due to {e}.")
                    logging.error(f"❌ [MART] Failed to create or replace materialized table for Spending Aggregation due to {e}.")
        finally:
            mart_sections_time[mart_section_name] = round(time.time() - mart_section_start, 2)

    # 2.1.5. Summarize materialization results for Spending Aggregation
    finally:
        mart_time_elapsed = round(time.time() - mart_time_start, 2)
        mart_sections_total = len(mart_sections_status) 
        mart_sections_failed = [k for k, v in mart_sections_status.items() if v == "failed"] 
        mart_sections_succeeded = [k for k, v in mart_sections_status.items() if v == "succeed"]
        mart_rows_output = mart_rows_uploaded
        mart_sections_summary = list(dict.fromkeys(
            list(mart_sections_status.keys()) +
            list(mart_sections_time.keys())
        ))
        mart_sections_detail = {
            mart_section_summary: {
                "status": mart_sections_status.get(mart_section_summary, "unknown"),
                "time": round(mart_sections_time.get(mart_section_summary, 0.0), 2),
            }
            for mart_section_summary in mart_sections_summary
        }       
        if mart_sections_failed:
            mart_status_final = "mart_failed_all"
            print(f"❌ [MART] Failed to complete Spending Aggregation materialization with {mart_rows_output} materialized row(s) due to {', '.join(mart_sections_failed)} failed section(s) in {mart_time_elapsed}s.")
            logging.error(f"❌ [MART] Failed to complete Spending Aggregation materialization with {mart_rows_output} materialized row(s) due to {', '.join(mart_sections_failed)} failed section(s) in {mart_time_elapsed}s.")
        else:
            mart_status_final = "mart_succeed_all"
            print(f"🏆 [MART] Successfully completed Spending Aggregation materialization with {mart_rows_output} materialized row(s) in {mart_time_elapsed}s.")
            logging.info(f"🏆 [MART] Successfully completed Spending Aggregation materialization with {mart_rows_output} materialized row(s) in {mart_time_elapsed}s.")
        mart_results_final = {
            "mart_df_final": None,
            "mart_status_final": mart_status_final,
            "mart_summary_final": {
                "mart_time_elapsed": mart_time_elapsed,
                "mart_sections_total": mart_sections_total,
                "mart_sections_succeed": mart_sections_succeeded,
                "mart_sections_failed": mart_sections_failed,
                "mart_sections_detail": mart_sections_detail,
                "mart_rows_output": mart_rows_output,
            },
        }
    return mart_results_final

# 3. BUILD MATERIALIZED TABLE FOR BUDGET ALLOCATION AND ADVERTISING SPEND RECONCILIATION

# 3.1 Build materialized table for monthly budget allocation and advertising spend reconciliation
def mart_recon_all():
    print("🚀 [MART] Starting to build materialized table for Monthly Reconciliation...")
    logging.info("🚀 [MART] Starting to build materialized table for Monthly Reconciliation...")

    # 3.1.1. Start timing the Monthly Reconciliation
    ICT = ZoneInfo("Asia/Ho_Chi_Minh")        
    mart_time_start = time.time()
    mart_sections_status = {}
    mart_sections_time = {}
    print(f"🔍 [MART] Proceeding to build materialized table for Monthly Reconciliation at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")
    logging.info(f"🔍 [MART] Proceeding to build materialized table for Monthly Reconciliation at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")

    try:

    # 3.1.2. Prepare Google BigQuery table_id for materialization
        mart_section_name = "[MART] Prepare Google BigQuery table_id for materialization"
        mart_section_start = time.time()
        try: 
            mart_dataset_budget = f"{COMPANY}_dataset_budget_api_mart"
            mart_table_budget = f"{PROJECT}.{mart_dataset_budget}.{COMPANY}_table_budget_all_all_allocation_monthly"
            mart_dataset_recon = f"{COMPANY}_dataset_recon_api_mart"
            mart_table_spend = f"{mart_dataset_recon}.{COMPANY}_table_recon_all_all_campaign_spend"
            mart_table_recon = f"{mart_dataset_recon}.{COMPANY}_table_recon_all_all_recon_spend"            
            print(f"🔍 [MART] Preparing to build materialized table {mart_table_recon} for Monthly Reconciliation...")
            logging.info(f"🔍 [MART] Preparing to build materialized table {mart_table_recon} for Monthly Reconciliation...")
            mart_sections_status[mart_section_name] = "succeed"    
            mart_sections_time[mart_section_name] = round(time.time() - mart_section_start, 2)
        finally:
            mart_sections_time[mart_section_name] = round(time.time() - mart_section_start, 2)      

    # 3.1.3. Initialize Google BigQuery client
        mart_section_name = "[MART] Initialize Google BigQuery client"
        mart_section_start = time.time()
        try:
            print(f"🔍 [MART] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [MART] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            google_bigquery_client = bigquery.Client(project=PROJECT)
            mart_sections_status[mart_section_name] = "succeed"
            print(f"✅ [MART] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [MART] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")            
        except Exception as e:
            mart_sections_status[mart_section_name] = "failed"
            print(f"❌ [MART] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [MART] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        finally:
            mart_sections_time[mart_section_name] = round(time.time() - mart_section_start, 2)

    # 3.1.4. Query budget and spend to build monthly reconciliation
        mart_section_name = "[MART] Query budget and spend to build monthly reconciliation"
        mart_section_start = time.time()       
        try:
            query_replace_config = f"""
                CREATE OR REPLACE TABLE `{mart_table_recon}`
                CLUSTER BY ma_ngan_sach_cap_1, chuong_trinh, nhan_su, thang AS
                WITH mart_cost_monthly AS (
                    SELECT
                        nen_tang,
                        ma_ngan_sach_cap_1,
                        hang_muc,
                        chuong_trinh,
                        noi_dung,
                        hinh_thuc,
                        nhan_su,                        
                        thang,
                        SUM(ket_qua) AS ket_qua,
                        SUM(chi_tieu) AS chi_tieu,
                        CASE
                            WHEN COUNTIF(LOWER(trang_thai) = '🟢') > 0 THEN 'active'
                            ELSE 'inactive'
                        END AS trang_thai
                    FROM `{mart_table_spend}`
                    GROUP BY
                        nen_tang,
                        ma_ngan_sach_cap_1,
                        chuong_trinh,
                        noi_dung,
                        hinh_thuc,
                        nhan_su,
                        hang_muc,
                        thang
                ),
                budget AS (
                    SELECT * FROM `{mart_table_budget}`
                )

                SELECT
                    COALESCE(b.ma_ngan_sach_cap_1, c.ma_ngan_sach_cap_1) AS ma_ngan_sach_cap_1,
                    COALESCE(b.hang_muc, c.hang_muc) AS hang_muc,
                    COALESCE(b.chuong_trinh, c.chuong_trinh) AS chuong_trinh,
                    COALESCE(b.noi_dung, c.noi_dung) AS noi_dung,
                    COALESCE(b.kenh, c.nen_tang, "other") AS nen_tang,
                    COALESCE(b.hinh_thuc, c.hinh_thuc, "other") AS hinh_thuc,
                    COALESCE(b.thang, c.thang) AS thang,
                    COALESCE(c.nhan_su, "other") AS nhan_su,
                    b.ngan_sach_ban_dau,
                    b.ngan_sach_dieu_chinh,
                    b.ngan_sach_bo_sung,
                    b.ngan_sach_thuc_chi,
                    b.ngan_sach_he_thong,
                    b.ngan_sach_nha_cung_cap,
                    b.ngan_sach_kinh_doanh,
                    b.ngan_sach_tien_san,
                    b.ngan_sach_tuyen_dung,
                    b.thoi_gian_bat_dau,
                    b.thoi_gian_ket_thuc,
                    c.chi_tieu AS so_tien_thuc_tieu,
                    c.trang_thai AS trang_thai,

                    CASE
                        -- Status: Spend without Budget
                        WHEN (c.chi_tieu IS NOT NULL AND c.chi_tieu > 0)
                            AND COALESCE(b.ngan_sach_thuc_chi, 0) = 0
                            AND LOWER(COALESCE(c.trang_thai, '')) = 'active'
                            THEN "🔴 Spend without Budget (On)"
                        WHEN (c.chi_tieu IS NOT NULL AND c.chi_tieu > 0)
                            AND COALESCE(b.ngan_sach_thuc_chi, 0) = 0
                            AND LOWER(COALESCE(c.trang_thai, '')) != 'active'
                            THEN "⚪ Spend without Budget (Off)"

                        -- Status:  No Budget
                        WHEN COALESCE(b.ngan_sach_thuc_chi, 0) = 0
                            THEN "🚫 No Budget"

                        -- Status:  Not Yet Started
                        WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                            AND CURRENT_DATE() < DATE(b.thoi_gian_bat_dau)
                            THEN "🕓 Not Yet Started"

                        -- Not Set
                        WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                            AND CURRENT_DATE() >= DATE(b.thoi_gian_bat_dau)
                            AND (c.chi_tieu IS NULL OR c.chi_tieu = 0)
                            AND (c.trang_thai IS NULL OR TRIM(c.trang_thai) = '')
                            AND DATE_DIFF(CURRENT_DATE(), DATE(b.thoi_gian_bat_dau), DAY) <= 3
                            THEN "⚪ Not Set"

                        -- Status:  Delayed (>3 days, no spend, not ended)
                        WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                            AND CURRENT_DATE() >= DATE(b.thoi_gian_bat_dau)
                            AND CURRENT_DATE() <= DATE(b.thoi_gian_ket_thuc)
                            AND (c.chi_tieu IS NULL OR c.chi_tieu = 0)
                            AND (c.trang_thai IS NULL OR TRIM(c.trang_thai) = '')
                            AND DATE_DIFF(CURRENT_DATE(), DATE(b.thoi_gian_bat_dau), DAY) > 3
                            THEN "⚠️ Delayed"

                        -- Status:  Ended without Spend
                        WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                            AND CURRENT_DATE() > DATE(b.thoi_gian_ket_thuc)
                            AND (c.chi_tieu IS NULL OR c.chi_tieu = 0)
                            THEN "🔒 Ended without Spend"

                        -- Status: Low Spend (active)
                        WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                            AND LOWER(COALESCE(c.trang_thai, '')) = 'active'
                            AND SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), b.ngan_sach_thuc_chi) < 0.95
                            AND DATE_DIFF(DATE(b.thoi_gian_ket_thuc), DATE(b.thoi_gian_bat_dau), DAY) > 0
                            AND SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), b.ngan_sach_thuc_chi)
                                < SAFE_DIVIDE(
                                    DATE_DIFF(CURRENT_DATE(), DATE(b.thoi_gian_bat_dau), DAY),
                                    DATE_DIFF(DATE(b.thoi_gian_ket_thuc), DATE(b.thoi_gian_bat_dau), DAY)
                                ) - 0.3
                            THEN "📉 Low Spend"

                        -- Status: High Spend (active)
                        WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                            AND LOWER(COALESCE(c.trang_thai, '')) = 'active'
                            AND SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), b.ngan_sach_thuc_chi) < 0.95
                            AND DATE_DIFF(DATE(b.thoi_gian_ket_thuc), DATE(b.thoi_gian_bat_dau), DAY) > 0
                            AND SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), b.ngan_sach_thuc_chi)
                                > SAFE_DIVIDE(
                                    DATE_DIFF(CURRENT_DATE(), DATE(b.thoi_gian_bat_dau), DAY),
                                    DATE_DIFF(DATE(b.thoi_gian_ket_thuc), DATE(b.thoi_gian_bat_dau), DAY)
                                ) + 0.3
                            THEN "📈 High Spend"

                        -- Status: Near Completion (active)
                        WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                            AND LOWER(COALESCE(c.trang_thai, '')) = 'active'
                            AND SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), b.ngan_sach_thuc_chi) BETWEEN 0.95 AND 0.99
                            THEN "🟢 Near Completion"

                        -- Status: Off (paused, not ended, not overspend)
                        WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                            AND (c.chi_tieu IS NOT NULL AND c.chi_tieu > 0)
                            AND LOWER(COALESCE(c.trang_thai, '')) != 'active'
                            AND COALESCE(c.chi_tieu, 0) < b.ngan_sach_thuc_chi * 0.99
                            THEN "⚪ Off (Early Stopped)"

                        -- Status: Completed
                        WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                            AND SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), b.ngan_sach_thuc_chi) > 0.99
                            AND COALESCE(c.chi_tieu, 0) < b.ngan_sach_thuc_chi * 1.01
                            THEN "🔵 Completed"

                        -- Status: Over Budget
                        WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                            AND COALESCE(c.chi_tieu, 0) >= b.ngan_sach_thuc_chi * 1.01
                            AND LOWER(COALESCE(c.trang_thai, '')) = 'active'
                            THEN "🔴 Over Budget (Still Running)"
                        WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                            AND COALESCE(c.chi_tieu, 0) >= b.ngan_sach_thuc_chi * 1.01
                            AND LOWER(COALESCE(c.trang_thai, '')) != 'active'
                            THEN "⚪ Over Budget (Stopped)"

                        -- Status: In Progress
                        WHEN COALESCE(b.ngan_sach_thuc_chi, 0) > 0
                            AND (c.chi_tieu IS NOT NULL AND c.chi_tieu > 0)
                            AND LOWER(COALESCE(c.trang_thai, '')) = 'active'
                            THEN "🟢 In Progress"

                        ELSE "❓ Not Recognized"
                    END AS trang_thai_chien_dich,

                    SAFE_DIVIDE(COALESCE(c.chi_tieu, 0), COALESCE(b.ngan_sach_thuc_chi, 0)) AS spending_ratio

                FROM budget b
                FULL OUTER JOIN mart_cost_monthly c
                ON b.ma_ngan_sach_cap_1 = c.ma_ngan_sach_cap_1
                AND b.hang_muc = c.hang_muc
                AND b.chuong_trinh = c.chuong_trinh
                AND b.noi_dung = c.noi_dung
                AND b.kenh = c.nen_tang
                AND b.hinh_thuc = c.hinh_thuc
                AND b.thang = c.thang;
            """                 
            print(f"🔁 [MART] Querying budget and spending tables to create or replace materialized table {mart_table_recon} for Monthly Reconciliation...")
            logging.info(f"🔁 [MART] Querying budget and spending tables to create or replace materialized table {mart_table_recon} for Monthly Reconciliation...")
            query_replace_load = google_bigquery_client.query(query_replace_config)
            query_replace_result = query_replace_load.result()
            query_count_config = f"SELECT COUNT(1) AS mart_rows_count FROM `{mart_table_recon}`"
            query_count_load = google_bigquery_client.query(query_count_config)
            query_count_result = query_count_load.result()
            mart_rows_uploaded = list(query_count_result)[0]["mart_rows_count"]
            mart_sections_status[mart_section_name] = "succeed"
            print(f"✅ [MART] Successfully created or replace materialized table {mart_table_recon} for Monthly Reconciliation with {mart_rows_uploaded} row(s).")
            logging.info(f"✅ [MART] Successfully created or replace materialized table {mart_table_recon} for Monthly Reconciliation with {mart_rows_uploaded} row(s).")
        except Exception as e:
            mart_sections_status[mart_section_name] = "failed"
            print(f"❌ [MART] Failed to create or replace materialized table for Monthly Reconciliation due to {e}.")
            logging.error(f"❌ [MART] Failed to create or replace materialized table for Monthly Reconciliation due to {e}.")
        finally:
            mart_sections_time[mart_section_name] = round(time.time() - mart_section_start, 2)

    # 3.1.5. Summarize materialization results for Monthly Reconciliation
    finally:
        mart_time_elapsed = round(time.time() - mart_time_start, 2)
        mart_sections_total = len(mart_sections_status) 
        mart_sections_failed = [k for k, v in mart_sections_status.items() if v == "failed"] 
        mart_sections_succeeded = [k for k, v in mart_sections_status.items() if v == "succeed"]
        mart_rows_output = mart_rows_uploaded
        mart_sections_summary = list(dict.fromkeys(
            list(mart_sections_status.keys()) +
            list(mart_sections_time.keys())
        ))
        mart_sections_detail = {
            mart_section_summary: {
                "status": mart_sections_status.get(mart_section_summary, "unknown"),
                "time": round(mart_sections_time.get(mart_section_summary, 0.0), 2),
            }
            for mart_section_summary in mart_sections_summary
        }       
        if mart_sections_failed:
            mart_status_final = "mart_failed_all"
            print(f"❌ [MART] Failed to complete Monthly Reconciliation materialization with {mart_rows_output} materialized row(s) due to {', '.join(mart_sections_failed)} failed section(s) in {mart_time_elapsed}s.")
            logging.error(f"❌ [MART] Failed to complete Monthly Reconciliation materialization with {mart_rows_output} materialized row(s) due to {', '.join(mart_sections_failed)} failed section(s) in {mart_time_elapsed}s.")
        else:
            mart_status_final = "mart_succeed_all"
            print(f"🏆 [MART] Successfully completed Monthly Reconciliation materialization with {mart_rows_output} materialized row(s) in {mart_time_elapsed}s.")
            logging.info(f"🏆 [MART] Successfully completed Monthly Reconciliation materialization with {mart_rows_output} materialized row(s) in {mart_time_elapsed}s.")
        mart_results_final = {
            "mart_df_final": None,
            "mart_status_final": mart_status_final,
            "mart_summary_final": {
                "mart_time_elapsed": mart_time_elapsed,
                "mart_sections_total": mart_sections_total,
                "mart_sections_succeed": mart_sections_succeeded,
                "mart_sections_failed": mart_sections_failed,
                "mart_sections_detail": mart_sections_detail,
                "mart_rows_output": mart_rows_output,
            },
        }
    return mart_results_final