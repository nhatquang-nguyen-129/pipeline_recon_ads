import os
import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import time

from etl.extract_budget_allocation import extract_budget_allocation
from etl.transform_budget_allocation import transform_budget_allocation
from etl.load_budget_allocation import load_budget_allocation

from dbt.run import dbt_recon_spend

COMPANY     = os.getenv("COMPANY")
PROJECT     = os.getenv("PROJECT")
DEPARTMENT  = os.getenv("DEPARTMENT")
ACCOUNT     = os.getenv("ACCOUNT")
MODE        = os.getenv("MODE")

def dags_budget_reconciliation(
    *,
    worksheet_name: str,
    spreadsheet_id: str,
):
    print(
        "🔄 [DAGS] Trigger Advertising Reconciliation with Budget Allocation worksheet_name " 
        f"{worksheet_name} from spreadsheet_id "
        f"{spreadsheet_id}..."
    )

# ETL for Budget Allocation
    DAGS_BUDGET_ATTEMPTS = 3

    for attempt in range(1, DAGS_BUDGET_ATTEMPTS + 1):
    
    # Extract       
        try:
            print(
                "🔄 [DAGS] Triggering to extract Budget Allocation worksheet_name "
                f"{worksheet_name} from spreadsheet_id "
                f"{spreadsheet_id} in "
                f"{attempt}/{DAGS_BUDGET_ATTEMPTS} attempt(s)..."
            )

            df = extract_budget_allocation(
                spreadsheet_id=spreadsheet_id,
                worksheet_name=worksheet_name,
            )

            if df.empty:
                print(
                    "⚠️ [DAGS] Budget Allocation extract returned empty dataframe. "
                    "DAG execution suspended."
                )
                return

            break

        except Exception as e:
            retryable = getattr(e, "retryable", False)

            print(
                "⚠️ [DAGS] Failed to extract Budget Allocation "
                f"{attempt}/{DAGS_BUDGET_ATTEMPTS} due to {e}"
            )

            if not retryable:
                raise RuntimeError(
                    "❌ [DAGS] Non-retryable error occurred while extracting "
                    "Budget Allocation, DAG execution aborted."
                ) from e

            if attempt == DAGS_BUDGET_ATTEMPTS:
                raise RuntimeError(
                    "❌ [DAGS] Exceeded retry attempts while extracting "
                    "Budget Allocation, DAG execution aborted."
                ) from e

            wait_to_retry = 60 + (attempt - 1) * 30
            print(
                "🔄 [DAGS] Waiting "
                f"{wait_to_retry} second(s) before retrying extract..."
            )
            time.sleep(wait_to_retry)

    # Transform
    print(
        "🔄 [DAGS] Transforming Budget Allocation with "
        f"{len(df)} row(s)..."
    )

    df = transform_budget_allocation(df)

    # Load
    _budget_allocation_direction = (
        f"{PROJECT}."
        f"{COMPANY}_dataset_recon_api_raw."
        f"{COMPANY}_table_budget_{DEPARTMENT}_{ACCOUNT}_allocation_{worksheet_name}"
    )

    print(
        "🔄 [DAGS] Loading Budget Allocation to "
        f"{_budget_allocation_direction}..."
    )

    load_budget_allocation(
        df=df,
        direction=_budget_allocation_direction,
    )

# Materialization with dbt
    print("🔄 [DAGS] Trigger to materialize Budget Allocation with dbt...")

    dbt_recon_spend(
        google_cloud_project=PROJECT,
        select="tag:mart,tag:recon",
    )