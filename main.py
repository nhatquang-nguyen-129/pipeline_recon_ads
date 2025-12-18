"""
==================================================================
BUDGET MAIN ENTRYPOINT
------------------------------------------------------------------
This script serves as the unified CLI controller for triggering  
ads data updates from Budget Allocation based on command-line 
arguments and environment variables.

It supports incremental daily ingestion for selected data layers  
and allows flexible control over date ranges.

✔️ Dynamic routing to the correct update module based on PLATFORM  
✔️ CLI flags to select data layers and date range mode  
✔️ Shared logging and error handling across update jobs  
✔️ Supports scheduled jobs or manual on-demand executions 
✔️ Automatic update summary and execution timing for each step

⚠️ This script does not contain data processing logic itself.  
It simply delegates update tasks to platform-specific modules .
==================================================================
"""

# Add project root to sys.path to enable absolute imports
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add Python datetime ultilities for integration
from datetime import (
    datetime, 
    timedelta
)

# Add Python dynamic import ultilities for integration
import importlib

# Add Python logging ultilities for integration
import logging

# Add Python IANA time zone ultilities for integration
from zoneinfo import ZoneInfo

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
    raise EnvironmentError("❌ [MAIN] Failed to trigger Budget Allocation update due to missing required environment variables COMPANY/PLATFORM/ACCOUNT/LAYER/MODE.")

# 1. MAIN ENTRYPOINT CONTROLLER

# 1.1. Validate input for main entrypoint function
if PLATFORM != "budget":
    raise ValueError(f"❌ [MAIN] Failed to trigger Budget Allocation update due to {PLATFORM} platform is unsupported.")
try:
    update_module_location = importlib.import_module(f"src.update")
except ModuleNotFoundError:
    raise ImportError("❌ [MAIN] Failed to trigger Budget Allocation update due to src/update.py not exist.")

# 1.2. Execution controller
def main():
    ICT = ZoneInfo("Asia/Ho_Chi_Minh")
    main_date_today = datetime.now(ICT)
    if PLATFORM == "budget":
        try:
            update_budget_allocation = update_module_location.update_budget_allocation
        except AttributeError:
            raise ImportError(f"❌ [MAIN] Failed to locate Budget Allocation update module due to update_budget_allocation must be defined.")
        if MODE == "thismonth":
            main_month_updated = main_date_today.strftime("%Y-%m")
        elif MODE == "lastmonth":
            main_day_start = main_date_today.replace(day=1)
            main_day_end = main_day_start - timedelta(days=1)
            main_month_updated = main_day_end.strftime("%Y-%m")
        else:
            raise ValueError(f"⚠️ [MAIN] Failed to trigger Budget Allocation update due to unsupported mode {MODE} for so please use 'thismonth' or 'lastmonth' instead.")
        if LAYER != "all":
            raise ValueError(f"⚠️ [MAIN] Failed to trigger Budget Allocation update due to unsupported layer {LAYER} so please use 'all' only.")
        try:
            print(f"🚀 [MAIN] Starting to update {PLATFORM} allocation of {COMPANY} company in {MODE} mode and {DEPARTMENT} department with {ACCOUNT} account for {main_month_updated} month...")
            logging.info(f"🚀 [MAIN] Starting to update {PLATFORM} allocation of {COMPANY} company in {MODE} mode and {DEPARTMENT} department with {ACCOUNT} account for {main_month_updated} month...")
            update_budget_allocation(update_month_allocation=main_month_updated)
        except Exception as e:
            print(f"❌ [MAIN] Failed to trigger update {PLATFORM} allocation of {COMPANY} company in {MODE} mode and {DEPARTMENT} department with {ACCOUNT} account for {main_month_updated} month due to {e}.")
            logging.error(f"❌ [MAIN] Failed to trigger update {PLATFORM} allocation of {COMPANY} company in {MODE} mode and {DEPARTMENT} department with {ACCOUNT} account for {main_month_updated} month due to {e}.")

# 1.3. Execute main entrypoint function
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"❌ Update failed: {e}")
        sys.exit(1)