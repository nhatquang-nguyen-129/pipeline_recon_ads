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

from services.ads.mart import (
    mart_recon_all,
    mart_spend_all,
)

# Get Google Cloud Project ID environment variable
PROJECT = os.getenv("GCP_PROJECT_ID")

# Get Facebook service environment variable for Brand
COMPANY = os.getenv("COMPANY") 

# Get Facebook service environment variable for Platform
PLATFORM = os.getenv("PLATFORM")

# Get Facebook service environment variable for Account
ACCOUNT = os.getenv("ACCOUNT")

# Get Facebook service environment variable for Layer
LAYER = os.getenv("LAYER")

# 1. UPDATE UNIFIED ADVERTISING SPEND ACROSS MULTIPLE NETWORKS

# 1.1. Update unified advertising spend across multiple networks
def update_spend_all():
    print(f"🚀 [UPDATE] Starting unified daily advertising spend aggregation for {COMPANY} company...")
    logging.info(f"🚀 [UPDATE] Starting unified daily advertising spend aggregation for {COMPANY} company...")
    
    # 1.1.1. Rebuild materialized table for unified advertising spend across multiple networks
    print(f"🔄 [UPDATE] Triggering to rebuild materialized table for unified advertising spend across multiple networks for {COMPANY} company...")
    logging.info(f"🔄 [UPDATE] Triggering to rebuild materialized table for unified advertising spend across multiple networks for {COMPANY} company...")
    try:
        result = mart_spend_all()
        print(f"✅ [UPDATE] Successfully rebuilt materialized table for unified advertising spend across multiple networks for {COMPANY} company.")
        logging.info(f"✅ [UPDATE] Successfully rebuilt materialized table for unified advertising spend across multiple networks {COMPANY} company.")
        return result
    except Exception as e:
        print(f"❌ [UPDATE] Failed to rebuild materialized table for unified advertising spend across multiple networks for {COMPANY} company due to {e}.")
        logging.error(f"❌ [UPDATE] Failed to rebuild materialized table for unified advertising spend across multiple networks for {COMPANY} company due to {e}.")
        raise

# 2. UPDATE BUDGET ALLOCATION AND ADVERTISING SPEND RECONCILIATION

# 2.1. Update materialized table for monthly budget allocation and advertising spend reconciliation
def update_recon_all():
    print(f"🚀 [UPDATE] Starting monthly budget allocation and advertising reconciliation for {COMPANY} company...")
    logging.info(f"🚀 [UPDATE] Starting monthly budget allocation and advertising reconciliation for {COMPANY} company...")
    try:
        result = mart_recon_all()
        print(f"✅ [UPDATE] Successfully rebuilt materialized table for monthly budget allocation and advertising reconciliation for {COMPANY} company.")
        logging.info(f"✅ [UPDATE] Successfully rebuilt materialized table for monthly budget allocation and advertising reconciliation for {COMPANY} company.")
        return result
    except Exception as e:
        print(f"❌ [UPDATE] Failed to rebuild materialized table for monthly budget allocation and advertising reconciliation for {COMPANY} company due to {e}.")
        logging.error(f"❌ [UPDATE] Failed to rebuild materialized table for monthly budget allocation and advertising reconciliation for {COMPANY} company due to {e}.")
        raise