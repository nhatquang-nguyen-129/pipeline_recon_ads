import os
from pathlib import Path
import sys
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[0]
sys.path.append(str(ROOT_FOLDER_LOCATION))

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from google.cloud import secretmanager
from google.api_core.client_options import ClientOptions

from dags.dags_budget_allocation import dags_budget_allocation

COMPANY = os.getenv("COMPANY")
PROJECT = os.getenv("PROJECT")
DEPARTMENT = os.getenv("DEPARTMENT")
ACCOUNT = os.getenv("ACCOUNT")
MODE = os.getenv("MODE")

if not all([
    COMPANY,
    PROJECT,
    DEPARTMENT,
    ACCOUNT,
    MODE
]):
    raise EnvironmentError("❌ [MAIN] Failed to execute Budget Allocation main entrypoint due to missing required environment variables.")

def main():
    """
    Main Budget Allocation entrypoint
    ---------
    Workflow:
        1. Resolve execution time window from MODE
        2. Read & validate OS environment variables
        3. Load secrets from GCP Secret Manager
        4. Resolve worksheet_name and spreadsheet_id
        5. Dispatch execution to DAG orchestrator
    Return:
        None
    """
    
    print(
        "🔄 [MAIN] Triggering to update Budget Allocation for "
        f"{ACCOUNT} account of "
        f"{DEPARTMENT} department in "
        f"{COMPANY} company with "
        f"{MODE} mode to Google Cloud project "
        f"{PROJECT}..."
    )

# Resolve input time range
    ICT = ZoneInfo("Asia/Ho_Chi_Minh")
    today = datetime.now(ICT)
    
    if MODE == "thismonth":
        input_month = today.strftime("%Y-%m")
    elif MODE == "lastmonth":
        start_date = today.replace(day=1)
        end_date = today - timedelta(days=1)
        input_month = end_date.strftime("%Y-%m")
    else:
        raise ValueError(
            "⚠️ [MAIN] Failed to trigger Budget Allocation main entrypoint due to unsupported mode "
            f"{MODE}.")

    year, month = input_month.split("-")
    month = month.zfill(2)
    worksheet_name = f"m{month}{year}"

    print(
        "✅ [MAIN] Successfully resolved "
        f"{MODE} mode to month "
        f"{month} and year "
        f"{year} in worksheet_name "
        f"{worksheet_name}."
    )

# Initialize Google Secret Manager
    try:
        print("🔍 [MAIN] Initialize Google Secret Manager client...")

        google_secret_client = secretmanager.SecretManagerServiceClient(
            client_options=ClientOptions(
                api_endpoint="secretmanager.googleapis.com"
            )
        )

        print("✅ [MAIN] Successfully initialized Google Secret Manager client.")
    
    except Exception as e:
        raise RuntimeError(
            "❌ [MAIN] Failed to initialize Google Secret Manager client due to."
            f"{e}."
        )
        
# Resolve spreadsheet_id from Google Secret Manager
    try:
        secret_account_id = (
            f"{COMPANY}_secret_{DEPARTMENT}_budget_account_id_{ACCOUNT}"
        )
        secret_account_name = (
            f"projects/{PROJECT}/secrets/{secret_account_id}/versions/latest"
        )
        
        print(
            "🔍 [MAIN] Retrieving Budget Allocation secret_account_id "
            f"{secret_account_name} from Google Secret Manager..."
        )

        secret_account_response = google_secret_client.access_secret_version(
            name=secret_account_name,
            timeout=10.0,
        )
        spreadsheet_id = secret_account_response.payload.data.decode("utf-8")
        
        print(
            "✅ [MAIN] Successfully retrieved Budget Allocation spreadsheet_id "
            f"{spreadsheet_id} from Google Secret Manager."
        )
    
    except Exception as e:
        raise RuntimeError(
            "❌ [MAIN] Failed to retrieve Budget Allocation spreadsheet_id from Google Secret Manager due to "
            f"{e}."
        )     

# Execute DAGS
    dags_budget_allocation(
        worksheet_name=worksheet_name,
        spreadsheet_id=spreadsheet_id
    )

# Entrypoint
if __name__ == "__main__":
    try:
        main()
    except Exception:
        sys.exit(1)