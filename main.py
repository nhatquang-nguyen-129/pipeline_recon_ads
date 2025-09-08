"""
==================================================================
MAIN ENTRYPOINT FOR PLATFORM-AGNOSTIC DATA UPDATES
------------------------------------------------------------------
This script serves as the unified CLI **controller** for triggering  
ads data updates across multiple platforms (e.g., Facebook, Google),  
based on command-line arguments and environment variables.

It supports **incremental daily ingestion** for selected data layers  
(e.g., campaign, ad) and allows flexible control over date ranges.

✔️ Dynamic routing to the correct update module based on PLATFORM  
✔️ CLI flags to select data layers and date range mode  
✔️ Shared logging and error handling across update jobs  
✔️ Supports scheduled jobs or manual on-demand executions  

⚠️ This script does *not* contain data processing logic itself.  
It simply delegates update tasks to platform-specific modules  
(e.g., services.facebook.update, services.budget.update).
==================================================================
"""
# Add project root to sys.path to enable absolute imports
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add argparse to parse comman-line arguments
import argparse

# Add dynamic platform-specific modules
import importlib

# Import datetime to calculate time
from datetime import datetime, timedelta

# Add logging capability for tracking process execution and errors
import logging

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

# Get validated environment variables
if not all([COMPANY, PLATFORM, ACCOUNT, LAYER, MODE]):
    raise EnvironmentError("❌ [MAIN] Missing required environment variables COMPANY/PLATFORM/ACCOUNT/LAYER/MODE.")

# 1. DYNAMIC IMPORT MODULE BASED ON PLATFORM
try:
    update_module = importlib.import_module(f"src.update")
except ModuleNotFoundError:
    raise ImportError(f"❌ [MAIN] Platform '{PLATFORM}' is not supported so please ensure services/{PLATFORM}/update.py exists.")

# 1.2. Main entrypoint function
def main():
    today = datetime.today()

    # 1.2.3. PLATFORM = recon
    if PLATFORM == "recon":
        try:
            update_spend = update_module.update_spend_all
            update_recon = update_module.update_recon_all
            update_aggregate = update_module.update_aggregate_all
        except AttributeError:
            raise ImportError(
                f"❌ [MAIN] Ads update module must define 'mart_spend_all', 'mart_recon_all', and 'mart_aggregate_all'."
            )

        layers = [layer.strip() for layer in LAYER.split(",") if layer.strip()]
        if len(layers) != 1:
            raise ValueError("⚠️ [MAIN] Ads only supports one LAYER per execution (spend, recon, or aggregate).")
        if MODE != "all":
            raise ValueError("⚠️ [MAIN] Ads only supports MODE=all.")

        layer = layers[0]
        if layer == "spend":
            print(f"🚀 [MAIN] Starting to build unified ads spend mart for {COMPANY}...")
            logging.info(f"🚀 [MAIN] Starting to build unified ads spend mart for {COMPANY}...")
            update_spend()
            print(f"✅ [MAIN] Successfully built unified ads spend mart for {COMPANY}.")
            logging.info(f"✅ [MAIN] Successfully built unified ads spend mart for {COMPANY}.")
        elif layer == "recon":
            print(f"🚀 [MAIN] Starting to build unified ads spend reconciliation mart for {COMPANY}...")
            logging.info(f"🚀 [MAIN] Starting to build unified ads spend reconciliation mart for {COMPANY}...")
            update_recon()
            print(f"✅ [MAIN] Successfully built unified ads spend reconciliation mart for {COMPANY}.")
            logging.info(f"✅ [MAIN] Successfully built unified ads spend reconciliation mart for {COMPANY}.")
        elif layer == "aggregate":
            print(f"🚀 [MAIN] Starting to build unified ads aggregate mart for {COMPANY}...")
            logging.info(f"🚀 [MAIN] Starting to build unified ads aggregate mart for {COMPANY}...")
            update_aggregate()
            print(f"✅ [MAIN] Successfully built unified ads aggregate mart for {COMPANY}.")
            logging.info(f"✅ [MAIN] Successfully built unified ads aggregate mart for {COMPANY}.")
        else:
            raise ValueError(f"⚠️ [MAIN] Unsupported ads LAYER={layer}. Use spend, recon, or aggregate.")

# 1.3. Entrypoint guard to run main() when this script is executed directly
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"❌ Update failed: {e}")
        sys.exit(1)
