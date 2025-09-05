"""
==================================================================
MAIN ENTRYPOINT
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
    raise ImportError(f"❌ [MAIN] Platform '{PLATFORM}' is not supported so please ensure src/update.py exists.")

# 1.2. Main entrypoint function
def main():
    today = datetime.today()

    # 1.2.1. PLATFORM = budget
    if PLATFORM == "budget":
        try:
            update_budget_allocation = update_module.update_budget_allocation
        except AttributeError:
            raise ImportError(f"❌ [MAIN] Budget update module must define 'update_budget_allocation'.")
        if MODE == "thismonth":
            thang = today.strftime("%Y-%m")
        elif MODE == "lastmonth":
            first_day_this_month = today.replace(day=1)
            last_day_last_month = first_day_this_month - timedelta(days=1)
            thang = last_day_last_month.strftime("%Y-%m")
        else:
            raise ValueError(f"⚠️ [MAIN] Unsupported mode {MODE} for budget. Use thismonth or lastmonth.")
        if LAYER != "all":
            raise ValueError("⚠️ [MAIN] Budget only supports LAYER=all.")
        try:
            print(f"🚀 [MAIN] Starting to update {PLATFORM} allocation of {COMPANY} in {MODE} mode and {DEPARTMENT} for {thang}...")
            logging.info(f"🚀 [MAIN] Starting to update {PLATFORM} allocation of {COMPANY} in {MODE} mode and {DEPARTMENT} for {thang}...")
            update_budget_allocation(thang)
        except Exception as e:
            print(f"❌ [MAIN] Failed to trigger update {PLATFORM} allocation of {COMPANY} in {MODE} mode and {DEPARTMENT} for {thang} due to {e}.")
            logging.error(f"❌ [MAIN] Failed to trigger update {PLATFORM} allocation of {COMPANY} in {MODE} mode and {DEPARTMENT} for {thang} due to {e}.")

# 1.3. Entrypoint guard to run main() when this script is executed directly
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"❌ Update failed: {e}")
        sys.exit(1)