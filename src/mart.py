"""
===========================================================================
RECON MATERIALIZATION MODULE
---------------------------------------------------------------------------
This function aggregates daily advertising costs across all supported
platforms (Facebook, TikTok, Google) into a unified summary table.

It dynamically checks the existence of individual platform-level spend 
tables in BigQuery, constructs UNION queries for available sources, and 
writes the result to a centralized `cost_all_daily` table, partitioned 
and clustered for efficient querying.

✔️ Dynamically supports multi-platform integration  
✔️ Skips platforms with missing tables (non-blocking)  
✔️ Aggregates and groups spend data by date, campaign info, and personnel  
✔️ Outputs a clean mart table for downstream dashboards or analysis  

⚠️ This module assumes platform-specific mart tables already exist.  
It does not trigger or depend on raw data ingestion or staging logic.
===========================================================================
"""
# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))  

# Add utilities for logging and error tracking
import logging

# Add Python 'time' library for tracking execution time and implementing delays
import time

# Add Google API Core libraries for integration
from google.api_core.exceptions import NotFound

# Add Google Authentication libraries for integration
from google.auth.exceptions import DefaultCredentialsError

# Add Google Cloud libraries for integration
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

# 1.1. BUILD UNIFIED COST AGGREGATION TABLE FROM ADVERTISING PLATFORM(s)

# 1.1. Build unified daily cost aggregation table from multiple advertising platforms
def mart_spend_all():
    print(f"🚀 [MART] Starting unified daily advertising spend aggregation for {COMPANY} company...")
    logging.info(f"🚀 [MART] Starting unified daily advertising spend aggregation for {COMPANY} company...")

    # 1.1.1. Initialize BigQuery client
    try:
        bigquery_client = bigquery.Client(project=PROJECT)
    except DefaultCredentialsError as e:
        raise RuntimeError("❌ [MART] Failed to initialize Google BigQuery client due to credentials error.") from e

    # 1.1.2. Define advertising networks
    networks = ["facebook", "tiktok", "google"]

    # 1.1.3. Prepare valid source tables lists
    valid_source_tables_all = []        # dùng để build bảng all_all
    valid_source_tables_specific = []   # dùng để build bảng theo DEPARTMENT/ACCOUNT

    # 1.1.4. Scan source tables for each network
    for network in networks:
        try:
            print(f"🔍 [MART] Scanning for {network} materialized dataset for advertising spend unification...")
            logging.info(f"🔍 [MART] Scanning for {network} materialized dataset for advertising spend unification...")
            mart_dataset = f"{COMPANY}_dataset_{network}_api_mart"
            print(f"✅ [MART] Successfully retrieved {network} materialized dataset for advertising spend unification.")
            logging.info(f"✅ [MART] Successfully retrieved {network} materialized dataset for advertising spend unification.")
        except KeyError:
            print(f"⚠️ [MART] Materialized dataset for {network} not found then unification is skipped.")
            logging.warning(f"⚠️ [MART] Materialized dataset for {network} not found then unification is skipped.")
            continue

        # 1.1.4.1. Check all_all source table
        source_table_all = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{network}_all_all_campaign_spend"
        try:
            bigquery_client.get_table(source_table_all)
            valid_source_tables_all.append(source_table_all)
            print(f"✅ [MART] Successfully retrieved {source_table_all} materialized table for all_all spend unification.")
            logging.info(f"✅ [MART] Successfully retrieved {source_table_all} materialized table for all_all spend unification.")
        except NotFound:
            print(f"⚠️ [MART] Source table {source_table_all} not found then all_all unification is skipped.")
            logging.warning(f"⚠️ [MART] Source table {source_table_all} not found then all_all unification is skipped.")

        # 1.1.4.2. Check specific source table (DEPARTMENT/ACCOUNT)
        source_table_specific = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{network}_{DEPARTMENT}_{ACCOUNT}_campaign_spend"
        try:
            bigquery_client.get_table(source_table_specific)
            valid_source_tables_specific.append(source_table_specific)
            print(f"✅ [MART] Successfully retrieved {source_table_specific} materialized table for pair-key spend unification.")
            logging.info(f"✅ [MART] Successfully retrieved {source_table_specific} materialized table for pair-key spend unification.")
        except NotFound:
            print(f"⚠️ [MART] Source table {source_table_specific} not found then pair-key unification is skipped.")
            logging.warning(f"⚠️ [MART] Source table {source_table_specific} not found then pair-key unification is skipped.")

    # 1.1.5. Validate at least some sources exist
    if not valid_source_tables_all and not valid_source_tables_specific:
        print("❌ [MART] Failed to unify advertising spend due to no valid source tables found.")
        logging.error("❌ [MART] Failed to unify advertising spend due to no valid source tables found.")
        return

    # 1.2. Always build all_all unified advertising spend table
    if valid_source_tables_all:
        output_table_all = f"{PROJECT}.{COMPANY}_dataset_{PLATFORM}_api_mart.{COMPANY}_table_{PLATFORM}_all_all_campaign_spend"
        union_sql_all = "\nUNION ALL\n".join([
            f"""
            SELECT
                nen_tang,
                ma_ngan_sach_cap_1,
                chuong_trinh,
                noi_dung,
                hinh_thuc,
                thang,
                trang_thai,
                ngay,
                nhan_su,
                chi_tieu
            FROM `{tbl}`
            """ for tbl in valid_source_tables_all
        ])
        query_all = f"""
            CREATE OR REPLACE TABLE `{output_table_all}`
            PARTITION BY ngay
            CLUSTER BY ma_ngan_sach_cap_1, chuong_trinh, nhan_su, thang
            AS
            {union_sql_all}
        """
        bigquery_client.query(query_all).result()
        print(f"✅ [MART] Successfully created materialized table {output_table_all} (all_all).")
        logging.info(f"✅ [MART] Successfully created materialized table {output_table_all} (all_all).")

    # 1.3. If DEPARTMENT/ACCOUNT != all then build specific table
    if not (DEPARTMENT == "all" and ACCOUNT == "all") and valid_source_tables_specific:
        output_table_specific = f"{PROJECT}.{COMPANY}_dataset_{PLATFORM}_api_mart.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_spend"
        union_sql_specific = "\nUNION ALL\n".join([
            f"""
            SELECT
                nen_tang,
                ma_ngan_sach_cap_1,
                chuong_trinh,
                noi_dung,
                hinh_thuc,
                thang,
                trang_thai,
                ngay,
                nhan_su,
                chi_tieu
            FROM `{tbl}`
            """ for tbl in valid_source_tables_specific
        ])
        query_specific = f"""
            CREATE OR REPLACE TABLE `{output_table_specific}`
            PARTITION BY ngay
            CLUSTER BY ma_ngan_sach_cap_1, chuong_trinh, nhan_su, thang
            AS
            {union_sql_specific}
        """
        bigquery_client.query(query_specific).result()
        print(f"✅ [MART] Successfully created materialized table {output_table_specific} (pair key).")
        logging.info(f"✅ [MART] Successfully created materialized table {output_table_specific} (pair key).")

def mart_spend_monthly():
    print(f"🚀 [MART] Starting unified monthly advertising spend aggregation for {COMPANY} company...")
    logging.info(f"🚀 [MART] Starting unified monthly advertising spend aggregation for {COMPANY} company...")

    try:
        bigquery_client = bigquery.Client(project=PROJECT)
    except DefaultCredentialsError as e:
        raise RuntimeError("❌ [MART] Failed to initialize Google BigQuery client due to credentials error.") from e

    networks = ["facebook", "tiktok", "google"]

    valid_source_tables_all = []
    valid_source_tables_specific = []

    # 1. Scan daily source tables
    for network in networks:
        mart_dataset = f"{COMPANY}_dataset_{network}_api_mart"

        source_table_all = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{network}_all_all_campaign_spend"
        try:
            bigquery_client.get_table(source_table_all)
            valid_source_tables_all.append(source_table_all)
        except NotFound:
            continue

        source_table_specific = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{network}_{DEPARTMENT}_{ACCOUNT}_campaign_spend"
        try:
            bigquery_client.get_table(source_table_specific)
            valid_source_tables_specific.append(source_table_specific)
        except NotFound:
            continue

    if not valid_source_tables_all and not valid_source_tables_specific:
        print("❌ [MART] No valid daily source tables found for monthly aggregation.")
        logging.error("❌ [MART] No valid daily source tables found for monthly aggregation.")
        return

    # 2. Build all_all monthly spend
    if valid_source_tables_all:
        output_table_all = f"{PROJECT}.{COMPANY}_dataset_{PLATFORM}_api_mart.{COMPANY}_table_{PLATFORM}_all_all_campaign_spend_monthly"
        # Drop trước để tránh conflict partition spec
        bigquery_client.query(f"DROP TABLE IF EXISTS `{output_table_all}`").result()
        union_sql_all = "\nUNION ALL\n".join([
            f"""
            SELECT
                nen_tang,
                ma_ngan_sach_cap_1,
                chuong_trinh,
                noi_dung,
                hinh_thuc,
                thang,
                nhan_su,
                LOWER(trang_thai) AS trang_thai,
                SUM(chi_tieu) AS chi_tieu
            FROM `{tbl}`
            GROUP BY nen_tang, ma_ngan_sach_cap_1, chuong_trinh, noi_dung, hinh_thuc, thang, nhan_su, trang_thai
            """ for tbl in valid_source_tables_all
        ])

        query_all = f"""
            CREATE TABLE `{output_table_all}`
            CLUSTER BY ma_ngan_sach_cap_1, chuong_trinh, nhan_su, thang
            AS
            {union_sql_all}
        """
        bigquery_client.query(query_all).result()
        print(f"✅ [MART] Successfully created monthly table {output_table_all} (all_all).")

    # 3. Build specific monthly spend
    if not (DEPARTMENT == "all" and ACCOUNT == "all") and valid_source_tables_specific:
        output_table_specific = f"{PROJECT}.{COMPANY}_dataset_{PLATFORM}_api_mart.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_spend_monthly"

        bigquery_client.query(f"DROP TABLE IF EXISTS `{output_table_specific}`").result()

        union_sql_specific = "\nUNION ALL\n".join([
            f"""
            SELECT
                nen_tang,
                ma_ngan_sach_cap_1,
                chuong_trinh,
                noi_dung,
                hinh_thuc,
                thang,
                nhan_su,
                LOWER(trang_thai) AS trang_thai,
                SUM(chi_tieu) AS chi_tieu
            FROM `{tbl}`
            GROUP BY nen_tang, ma_ngan_sach_cap_1, chuong_trinh, noi_dung, hinh_thuc, thang, nhan_su, trang_thai
            """ for tbl in valid_source_tables_specific
        ])

        query_specific = f"""
            CREATE TABLE `{output_table_specific}`
            CLUSTER BY ma_ngan_sach_cap_1, chuong_trinh, nhan_su, thang
            AS
            {union_sql_specific}
        """
        bigquery_client.query(query_specific).result()
        print(f"✅ [MART] Successfully created monthly table {output_table_specific} (pair key).")

# 2. BUILD MATERIALIZED TABLE FOR BUDGET ALLOCATION AND ADVERTISING SPEND RECONCILIATION

# 2.1 Build materialized table for monthly budget allocation and advertising spend reconciliation
def mart_recon_all():
    print("🚀 [MART] Starting monthly budget allocation and advertising reconciliation...")
    logging.info("🚀 [MART] Starting monthly budget allocation and advertising reconciliation...")

    # 2.1.1. Prepare full table_id for upper layer(s)
    try:
        bigquery_client = bigquery.Client(project=PROJECT)
    except DefaultCredentialsError as e:
        raise RuntimeError(" ❌ [MART] Failed to initialize Google BigQuery client due to credentials error.") from e
    networks = ["facebook",
                "tiktok",
                "google"]
    valid_source_tables = []
    for network in networks:
        try:
            input_dataset_spend = f"{COMPANY}_dataset_{network}_api_mart"
            print(f"🔍 [MART] Scanning for {network} materialized dataset {input_dataset_spend} for advertising spend reconciliation...")
            logging.warning(f"🔍 [MART] Scanning for {network} materialized dataset {input_dataset_spend} for advertising spend reconciliation...")
        except KeyError:
            print(f"⚠️ [MART] Materialized dataset for {network} not found then reconciliation is skipped.")
            logging.warning(f"⚠️ [MART] Materialized dataset for {network} not found then reconciliation is skipped.")
            continue
        input_table = f"{PROJECT}.{input_dataset_spend}.{COMPANY}_table_{network}_all_all_campaign_spend"
        try:
            print(f"🔍 [MART] Scanning for {network} materialized advertising spend table for advertising spend reconciliation...")
            logging.warning(f"🔍 [MART] Scanning for {network} materialized advertising spend table for advertising spend reconciliation...")
            bigquery_client.get_table(input_table)
            valid_source_tables.append(input_table)
            print(f"✅ [MART] Successfully retrieved {input_table} materialized table for advertising spend unification.")
            logging.warning(f"✅ [MART] Successfully retrieved {input_table} materialized table for advertising spend unification.")
        except NotFound:
            print(f"⚠️ [MART] Source table {input_table} not found then reconciliation is skipped.")
            logging.warning(f"⚠️ [MART] Source table {input_table} not found then reconciliation is skipped.")
    if not valid_source_tables:
        print("❌ [MART] Failed to reconcile advertising spend due to no valid input tables found.")
        logging.error("❌ [MART] Failed to reconcile advertising spend due to no valid input tables found.")
        return

    # 2.1.2. Update existing monthly advertising spend table or create new table if it not exist
    output_table_spend_all = f"{PROJECT}.{COMPANY}_dataset_{PLATFORM}_api_mart.{COMPANY}_table_{PLATFORM}_all_all_campaign_spend"
    print(f"🔍 [MART] Preparing to build {output_table_spend_all} monthly advertising spend table...")
    logging.info(f"🔍 [MART] Preparing to build {output_table_spend_all} monthly advertising spend table...")

    # 2.1.3. Query all input table(s) to build materialized table for monthly advertising spend
    union_sql = " UNION ALL ".join([
        f"""
        SELECT 
            nen_tang, 
            ma_ngan_sach_cap_1, 
            chuong_trinh, 
            noi_dung, 
            hinh_thuc, 
            thang, 
            nhan_su, 
            chi_tieu,
            LOWER(trang_thai) AS trang_thai
        FROM `{tbl}`
        WHERE chi_tieu > 0
        """ for tbl in valid_source_tables
    ])

    query_monthly_all = f"""
        CREATE OR REPLACE TABLE `{output_table_spend_all}`
        CLUSTER BY ma_ngan_sach_cap_1, chuong_trinh, nhan_su, thang AS
        SELECT 
            nen_tang, 
            ma_ngan_sach_cap_1, 
            chuong_trinh, 
            noi_dung, 
            hinh_thuc, 
            thang, 
            nhan_su, 
            CASE 
                WHEN MAX(CASE WHEN trang_thai = 'active' THEN 1 ELSE 0 END) = 1 
                    THEN 'active'
                ELSE 'paused'
            END AS trang_thai,
            SUM(chi_tieu) AS chi_tieu
        FROM ({union_sql})
        GROUP BY nen_tang, ma_ngan_sach_cap_1, chuong_trinh, noi_dung, hinh_thuc, thang, nhan_su;
    """
    bigquery_client.query(query_monthly_all).result()

    # 2.1.4. Build and execute reconciliation query for all_all
    input_dataset_budget = f"{COMPANY}_dataset_budget_api_mart"
    input_table_budget_all = f"{PROJECT}.{input_dataset_budget}.{COMPANY}_table_budget_all_all_allocation_monthly"
    output_dataset_recon = f"{COMPANY}_dataset_{PLATFORM}_api_mart"
    output_table_recon_all = f"{PROJECT}.{output_dataset_recon}.{COMPANY}_table_{PLATFORM}_all_all_reconciliation_all"
    query_recon_all = f"""
        CREATE OR REPLACE TABLE `{output_table_recon_all}`
        CLUSTER BY ma_ngan_sach_cap_1, chuong_trinh, nhan_su, thang AS
        WITH budget AS (
            SELECT * FROM `{input_table_budget_all}`
        ),
        cost AS (
            SELECT * FROM `{output_table_spend_all}`
        )
        SELECT * FROM budget FULL OUTER JOIN cost
        USING (ma_ngan_sach_cap_1, chuong_trinh, noi_dung, nen_tang, hinh_thuc, thang)
    """
    try:
        print(f"🔁 [MART] Executing aggregation query to create {output_table_recon_all} reconciled advertising spend table...")
        logging.info(f"🔁 [MART] Executing aggregation query to create {output_table_recon_all} reconciled advertising spend table...")
        bigquery_client.query(query_recon_all).result()
        logging.info(f"✅ [MART] Successfully created materialized table {output_table_recon_all} for reconciled advertising spend.")
        print(f"✅ [MART] Successfully created materialized table {output_table_recon_all} for reconciled advertising spend.")
    except Exception as e:
        logging.error(f"❌ [MART] Failed to excute aggregation query to build reconciled advertising spend table due to {e}.")
        print(f"❌ [MART] Failed to excute aggregation query to build reconciled advertising spend table due to {e}.")

    # 2.1.5. If DEPARTMENT/ACCOUNT != all then build specific tables
    if not (DEPARTMENT == "all" and ACCOUNT == "all"):
        output_table_spend_specific = f"{PROJECT}.{COMPANY}_dataset_{PLATFORM}_api_mart.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_spend"
        query_monthly_specific = f"""
            CREATE OR REPLACE TABLE `{output_table_spend_specific}`
            CLUSTER BY ma_ngan_sach_cap_1, chuong_trinh, nhan_su, thang AS
            SELECT * FROM `{output_table_spend_all}`
        """
        bigquery_client.query(query_monthly_specific).result()

        input_table_budget_specific = f"{PROJECT}.{input_dataset_budget}.{COMPANY}_table_budget_{DEPARTMENT}_{ACCOUNT}_allocation_monthly"
        output_table_recon_specific = f"{PROJECT}.{output_dataset_recon}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_reconciliation_all"
        query_recon_specific = f"""
            CREATE OR REPLACE TABLE `{output_table_recon_specific}`
            CLUSTER BY ma_ngan_sach_cap_1, chuong_trinh, nhan_su, thang AS
            WITH budget AS (
                SELECT * FROM `{input_table_budget_specific}`
            ),
            cost AS (
                SELECT * FROM `{output_table_spend_specific}`
            )
            SELECT * FROM budget FULL OUTER JOIN cost
            USING (ma_ngan_sach_cap_1, chuong_trinh, noi_dung, nen_tang, hinh_thuc, thang)
        """    
        try:
            print(f"🔁 [MART] Executing aggregation query to create {output_table_recon_specific} reconciled advertising spend table...")
            logging.info(f"🔁 [MART] Executing aggregation query to create {output_table_recon_specific} reconciled advertising spend table...")
            bigquery_client.query(query_recon_specific).result()
            logging.info(f"✅ [MART] Successfully created materialized table {output_table_recon_specific} for reconciled advertising spend.")
            print(f"✅ [MART] Successfully created materialized table {output_table_recon_specific} for reconciled advertising spend.")
        except Exception as e:
            logging.error(f"❌ [MART] Failed to excute aggregation query to build reconciled advertising spend table due to {e}.")
            print(f"❌ [MART] Failed to excute aggregation query to build reconciled advertising spend table due to {e}.")

if __name__ == "__main__":
    mart_spend_all()
    mart_recon_all()