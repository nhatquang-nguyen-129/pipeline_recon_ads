"""
==================================================================
RECON UPDATE MODULE
------------------------------------------------------------------
This module performs **incremental updates** of advertising spend  
data at the raw layer, consolidating delivery and cost information  
from all ad networks and reconciling it against planned budgets.

It is designed to support scheduled refreshes, multi-platform cost  
aggregation, and budget vs. actual spend tracking as the foundation  
for downstream staging and MART layers.

✔️ Supports multi-platform ingestion (Facebook, Google Ads, TikTok, etc.)  
✔️ Consolidates spend data across networks into a unified structure  
✔️ Performs reconciliation with budget allocation at campaign/program level  
✔️ Loads data incrementally to minimize latency and ensure freshness  

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

# Add internal Ads module for data handling

from src.mart import (
    mart_spend_all,
    mart_aggregate_all,
    mart_recon_all
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

# 1. UPDATE BUDGET ALLOCATION AND ADVERTISING SPEND RECONCILIATION

# 1.1. Update materialized table for monthly budget allocation and advertising spend reconciliation
def update_recon_all():
    print(f"🚀 [UPDATE] Starting monthly budget allocation and advertising reconciliation for {COMPANY} company with {DEPARTMENT} department and {ACCOUNT} account....")
    logging.info(f"🚀 [UPDATE] Starting monthly budget allocation and advertising reconciliation for {COMPANY} company with {DEPARTMENT} department and {ACCOUNT} account....")
    
    result = None
    try:

    # 1.1.1. Rebuild materialized table for unified advertising spend across multiple networks
        print(f"🔄 [UPDATE] Triggering to rebuild materialized table for unified advertising spend across multiple networks for {COMPANY} company with {DEPARTMENT} department and {ACCOUNT} account...")
        logging.info(f"🔄 [UPDATE] Triggering to rebuild materialized table for unified advertising spend across multiple networks for {COMPANY} company with {DEPARTMENT} department and {ACCOUNT} account....")
        try:
            result = mart_spend_all()
        except Exception as e:
            print(f"❌ [UPDATE] Failed to trigger materialized table rebuild for unified advertising spend across multiple networks for {COMPANY} company with {DEPARTMENT} department and {ACCOUNT} account due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to trigger materialized table rebuild for unified advertising spend across multiple networks for {COMPANY} company with {DEPARTMENT} department and {ACCOUNT} account due to {e}.")
    
    # 1.1.2. Rebuild materialized table for aggregated advertising spend across multiple networks
        print(f"🔄 [UPDATE] Triggering to rebuild materialized table for unified monthly advertising spend across multiple networks for {COMPANY} company with {DEPARTMENT} department and {ACCOUNT} account....")
        logging.info(f"🔄 [UPDATE] Triggering to rebuild materialized table for unified monthly advertising spend across multiple networks for {COMPANY} company with {DEPARTMENT} department and {ACCOUNT} account....")
        try:
            result = mart_aggregate_all()
        except Exception as e:
            print(f"❌ [UPDATE] Failed to trigger materialized table rebuild for unified advertising spend across multiple networks for {COMPANY} company with {DEPARTMENT} department and {ACCOUNT} account due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to trigger materialized table rebuild for unified advertising spend across multiple networks for {COMPANY} company with {DEPARTMENT} department and {ACCOUNT} account due to {e}.")

    # 2.1.1. Update materialized table for unified advertising spend across multiple networks
        try:
            print(f"🔄 [UPDATE] Triggering to rebuild materialized table for monthly budget allocation and advertising reconciliation for {COMPANY} company with {DEPARTMENT} department and {ACCOUNT} account...")
            logging.info(f"🔄 [UPDATE] Triggering to rebuild materialized table for monthly budget allocation and advertising reconciliation for for {COMPANY} company with {DEPARTMENT} department and {ACCOUNT} account...")
            result = mart_recon_all()
        except Exception as e:
            print(f"❌ [UPDATE] Failed to trigger materialized table rebuild for monthly budget allocation and advertising reconciliation for {COMPANY} company with {DEPARTMENT} department and {ACCOUNT} account due to {e}.")
            logging.error(f"❌ [UPDATE] Failed to trigger materialized table rebuild for monthly budget allocation and advertising reconciliation for {COMPANY} company with {DEPARTMENT} department and {ACCOUNT} account due to {e}.")

    except Exception as e:
        print(f"❌ [UPDATE] Failed to update materialized reconciliation table for {COMPANY} company with {DEPARTMENT} department and {ACCOUNT} account due to {e}.")
        logging.error(f"❌ [UPDATE] Failed to update materialized reconciliation table for {COMPANY} company with {DEPARTMENT} department and {ACCOUNT} account due to {e}.")
        raise
    print(f"✅ [UPDATE] Successfully completed materialized reconciliation update for {COMPANY} company with {DEPARTMENT} department and {ACCOUNT} account.")
    logging.info(f"✅ [UPDATE] Successfully completed materialized reconciliation update for {COMPANY} company with {DEPARTMENT} department and {ACCOUNT} account.") 
    return result