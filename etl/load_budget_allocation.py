import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import pandas as pd

from plugins.google_bigquery import internalGoogleBigqueryLoader

def load_budget_allocation(
    *,
    df: pd.DataFrame,
    direction: str,
) -> None:
    """
    Load Budget Allocation to Google BigQuery
    ----------------------
    Workflow:
        1. Validate input DataFrame
        2. Validate output direction for Google BigQuery
        3. Set primary key(s) to date
        4. Use UPSERT mode with parameterized query for deduplication
        5. Make internalGoogleBigQueryLoader API call
    ---------
    Returns:
        None
    """    

    if df.empty:
        print("⚠️ [LOADER] Completely loaded Budget Allocation but empty DataFrame returned.")
        return

    print(
        "🔄 [LOADER] Triggering to load "
        f"{len(df)} row(s) of Budget Allocation to Google BigQuery table "
        f"{direction}..."
        )

    loader = internalGoogleBigqueryLoader()

    loader.load(
        df=df,
        direction=direction,
        mode="upsert",
        keys=["month"],
        partition=None,
        cluster=[
            "month"
        ],
    )