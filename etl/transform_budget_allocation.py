import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import pandas as pd

def transform_budget_allocation(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Transform Budget Allocation
    ---------
    Workflow:
        1. Validate input
        2. Enforce required schema
        3. Compute derived budget metrics
        4. Normalize date dimension
        5. Enforce numeric types
    ---------
    Returns:
        1. DataFrame:
            Enforced budget allocation records
    """

    print(
        "🔄 [TRANSFORM] Transforming "
        f"{len(df)} row(s) of Budget Allocation..."
    )

    # Validate input
    if df.empty:
        print("⚠️ [TRANSFORM] Empty Budget Allocation input then transformation will be suspended.")
        return df

    required_cols = {
        "budget_group_1",
        "budget_group_2",
        "region",
        "category_level_1",
        "track_group",
        "pillar_group",
        "content_group",
        "month",
        "start_date",
        "end_date",
        "platform",
        "objective",        
        "initial_budget",
        "adjusted_budget",
        "additional_budget",

    }

    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(
            "❌ [TRANSFORM] Failed to transform Budget Allocation due to missing columns "
            f"{missing} then transformation will be suspended."
        )

    # Normalize numeric metrics
    for col in [
        "initial_budget",
        "adjusted_budget",
        "additional_budget",
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Compute derived budget metrics
    df["actual_budget"] = (
        df["initial_budget"]
        + df["adjusted_budget"]
        + df["additional_budget"]
    )

    df["grouped_marketing_budget"] = (
        df["budget_group_1"] == "KP"
    ) * df["actual_budget"]

    df["grouped_supplier_budget"] = (
        df["budget_group_1"] == "NC"
    ) * df["actual_budget"]

    df["grouped_store_retail"] = (
        df["budget_group_1"] == "KD"
    ) * df["actual_budget"]

    df["grouped_customer_budget"] = (
        df["budget_group_1"] == "CS"
    ) * df["actual_budget"]

    df["grouped_recruitment_budget"] = (
        df["budget_group_1"] == "HC"
    ) * df["actual_budget"]

    # 4. Normalize date dimension
    df["start_date"] = pd.to_datetime(
        df["start_date"],
        errors="coerce",
    )

    df["end_date"] = pd.to_datetime(
        df["end_date"],
        errors="coerce",
    )

    df["year"] = pd.to_datetime(
        df["month"] + "-01",
        format="%Y-%m-%d",
        errors="coerce"
    ).dt.year

    df["total_effective_time"] = (
        df["date_end"] - df["date_start"]
    ).dt.days

    today = pd.Timestamp.utcnow().normalize()
    df["total_passed_time"] = (
        today - df["date_start"]
    ).dt.days.clip(lower=0)

    print(
        "✅ [TRANSFORM] Successfully transformed "
        f"{len(df)} row(s) of Budget Allocation."
    )

    return df
