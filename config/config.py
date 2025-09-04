"""
===========================================================================
RECONCILIATION CONFIGURATION MODULE
---------------------------------------------------------------------------
This module defines all **cross-platform Ads configurations** used  
for aggregating advertising spend from multiple sources (Facebook,  
TikTok, Google) into unified cost tables.

It promotes centralized, reusable, and environment-agnostic configuration  
handling for cost-related mart logic and downstream reporting.

✔️ Maps BigQuery dataset names for each Ads platform  
✔️ Defines shared output table for unified cost aggregation  
✔️ Keeps data layer references consistent across pipelines  

⚠️ This module is strictly for static or semi-static configurations.  
It does **not** store secrets or perform any authentication logic.
===========================================================================
"""
# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Get Facebook service environment variable for Brand
COMPANY = os.getenv("COMPANY") 

# Global configuration for Ads cost aggregation
MAPPING_ADS_DATASET = {
    "kids": {
        "raw": {
            "facebook": f"{COMPANY}_dataset_facebook_api_raw",
            "tiktok": f"{COMPANY}_dataset_tiktok_api_raw",
            "google": f"{COMPANY}_dataset_google_api_raw",
        },
        "staging": {
            "facebook": f"{COMPANY}_dataset_facebook_api_staging",
            "tiktok": f"{COMPANY}_dataset_tiktok_api_staging",
            "google": f"{COMPANY}_dataset_google_api_staging",
        },
        "mart": {
            "facebook": f"{COMPANY}_dataset_facebook_api_mart",
            "tiktok": f"{COMPANY}_dataset_tiktok_api_mart",
            "google": f"{COMPANY}_dataset_google_api_mart",
        },
    },
}

# List các platform ads hỗ trợ, dùng chung cho các hàm update, query...
MAPPING_ADS_NETWORK = [
    "facebook",
    "tiktok",
    "google",
]