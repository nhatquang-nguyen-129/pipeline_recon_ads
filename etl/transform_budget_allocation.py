import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import pandas as pd
from zoneinfo import ZoneInfo
import traceback

def transform_budget_allocation(
    df: pd.DataFrame
) -> pd.DataFrame:

    print(
        "🔄 [TRANSFORM] Transforming "
        f"{len(df)} row(s) of Budget Allocation..."
    )

    try:
        
        # Validate input
        if df.empty:
            print("⚠️ [TRANSFORM] Empty Budget Allocation input then transformation will be suspended.")
            return df

        print("DEBUG 1: after validation")

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
            raise ValueError(f"Missing required columns: {missing}")

        # Safe cast numeric columns
        for col in [
            "initial_budget",
            "adjusted_budget",
            "additional_budget",
        ]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # Transform derived columns
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

        # Transform time columns
        df["month"] = df["month"].astype(str).str.strip()

        df["year"] = pd.to_datetime(
            df["month"] + "-01",
            errors="coerce"
        ).dt.year        

        df["start_date"] = pd.to_datetime(
            df["start_date"], errors="coerce"
        ).dt.tz_localize(ZoneInfo("Asia/Ho_Chi_Minh"))

        df["end_date"] = pd.to_datetime(
            df["end_date"], errors="coerce"
        ).dt.tz_localize(ZoneInfo("Asia/Ho_Chi_Minh"))       

        df["total_effective_time"] = (
            df["end_date"] - df["start_date"]
        ).dt.days

        df["total_passed_time"] = (
            pd.Timestamp.now(tz=ZoneInfo("Asia/Ho_Chi_Minh")).normalize() - df["start_date"]
        ).dt.days

        print(
            "✅ [TRANSFORM] Successfully transformed "
            f"{len(df)} row(s) of Budget Allocation."
        )

        return df

    except Exception as e:
        print("❌ [TRANSFORM][ERROR] Exception occurred during transform")
        print("🔎 Error type:", type(e).__name__)
        print("🔎 Error message:", str(e))

        print("🔎 Traceback:")
        print(traceback.format_exc())

        # QUAN TRỌNG: re-raise để DAG fail đúng
        raise