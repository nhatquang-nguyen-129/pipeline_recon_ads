import os
import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[0]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import argparse
from datetime import datetime
import traceback

from google.cloud import secretmanager
from google.api_core.client_options import ClientOptions

from dags.dags_budget_reconcile import dags_budget_reconcile

COMPANY    = os.getenv("COMPANY")
PROJECT    = os.getenv("PROJECT")
DEPARTMENT = os.getenv("DEPARTMENT")
ACCOUNT    = os.getenv("ACCOUNT")

if not all([
    COMPANY,
    PROJECT,
    DEPARTMENT,
    ACCOUNT
]):
    
    raise EnvironmentError(
        "❌ [BACKFILL] Failed to execute Budget Reconciliation backfill due to missing required environment variables."
    )

def backfill():
    """
    Backfill Budget Reconciliation
    ---
    Principles:
        1. Resolve execution time window form CLI argument --input_month
        2. Validate OS environment variables
        3. Load secrets from GCP Secret Manager
        4. Resolve worksheet_name and spreadsheet_id
        5. Dispatch execution to DAG orchestrator
    ---
    Returns:
        None
    """

    # CLI arguments parser for manual input_month
    parser = argparse.ArgumentParser(
        description="Manual Budget Reconciliation ETL Executor"
    )

    parser.add_argument(
        "--input_month",
        required=True,
        help="Input month in YYYY-MM format"
    )

    args = parser.parse_args()

    try:
        
        input_month = datetime.strptime(
            args.input_month, "%Y-%m"
        ).strftime("%Y-%m")
    
    except ValueError:
    
        raise ValueError(
            "❌ [BACKFILL] Failed to execute Budget Reconciliation backfill due to input_month must be in YYYY-MM format."
        )

    year, month = input_month.split("-")
    
    month = month.zfill(2)

    worksheet_name = f"m{month}{year}"

    print(
        "🔄 [BACKFILL] Triggering to execute Budget Reconciliation backfill for "
        f"{ACCOUNT} account of "
        f"{DEPARTMENT} department in "
        f"{COMPANY} company for month "
        f"{input_month} with worksheet_name "
        f"{worksheet_name} on Google Cloud project "
        f"{PROJECT}..."
    )

    # Initialize Google Secret Manager
    try:
        
        print(
            "🔍 [BACKFILL] Initialize Google Secret Manager client..."
        )
        
        google_secret_client = secretmanager.SecretManagerServiceClient(
            client_options=ClientOptions(
                api_endpoint="secretmanager.googleapis.com"
            )
        )

        print(
            "✅ [BACKFILL] Successfully initialized Google Secret Manager client."
        )
    
    except Exception as e:
        
        raise RuntimeError(
            "❌ [BACKFILL] Failed to initialize Google Secret Manager client due to."
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
            "🔍 [BACKFILL] Retrieving Budget Allocation secret_spreadsheet_id "
            f"{secret_account_name} from Google Secret Manager..."
        )

        secret_account_response = google_secret_client.access_secret_version(
            name=secret_account_name,
            timeout=10.0,
        )

        spreadsheet_id = secret_account_response.payload.data.decode("utf-8")

        print(
            "✅ [BACKFILL] Successfully retrieved Budget Allocation spreadsheet_id "
            f"{spreadsheet_id} from Google Secret Manager."
        )

    except Exception as e:
        
        raise RuntimeError(
            "❌ [BACKFILL] Failed to retrieve Budget Allocation spreadsheet_id from Google Secret Manager due to "
            f"{e}."
        )

    # Execute DAGS
    dags_budget_reconcile(
        worksheet_name=worksheet_name,
        spreadsheet_id=spreadsheet_id
    )

    # Entrypoint
if __name__ == "__main__":

    try:

        backfill()

    except Exception:

        print(
            "❌ [BACKFILL] Failed to execute Budget Reconciliation backfill due to..."
        )

        traceback.print_exc()

        sys.exit(1)