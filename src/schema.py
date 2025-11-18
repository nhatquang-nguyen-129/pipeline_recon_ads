"""
===================================================================
BUDGET SCHEMA MODULE
-------------------------------------------------------------------
This module defines and manages **schema-related logic** for the
Budget data pipeline, acting as a single source of truth for all
required fields across different layers.

It ensures consistency and prevents data mismatch when ingesting
budget allocation data from Google Sheets → staging → BigQuery.

✔️ Declares expected column names and data types for each schema type
✔️ Supports schema enforcement by validating and coercing DataFrame
✔️ Automatically fills in missing columns with appropriate types

⚠️ This module does *not* fetch or transform business data.
It only provides schema utilities to support other pipeline components.
===================================================================
"""
# Add logging ultilities for integration
import logging

# Add Python Pandas libraries for integration
import pandas as pd

# Add Python NumPy libraries for integration
import numpy as np

# 1. ENSURE SCHEMA FOR GIVEN PYTHON DATAFRAME IN BUDGET ALLOCAITON

# 1.1. Ensure that the given DataFrame contains all required columns with correct datatypes for the specified schema type
def enforce_table_schema(df: pd.DataFrame, schema_type: str) -> pd.DataFrame:
    print(f"🔄 [SCHEMA] Enforce schema {schema_type} on Python DataFrame with {df.shape[1]} column(s)...")
    logging.info(f"🔄 [SCHEMA] Enforce schema {schema_type} on Python DataFrame with {df.shape[1]} column(s)...")
    
    mapping_budget_schema = {
        "fetch_budget_allocation": {
            "ma_ngan_sach_cap_1": str,
            "ma_ngan_sach_cap_2": str,
            "khu_vuc": str,
            "nganh_hang": str,
            "chi_tiet": str,
            "chuong_trinh": str,
            "noi_dung": str,
            "thang": str,
            "thoi_gian_bat_dau": str,
            "thoi_gian_ket_thuc": str,
            "nen_tang": str,
            "hinh_thuc": str,
            "ngan_sach_ban_dau": int,
            "ngan_sach_dieu_chinh": int,
            "ngan_sach_bo_sung": int,
        },
        "ingest_budget_allocation": {
            "ma_ngan_sach_cap_1": str,
            "ma_ngan_sach_cap_2": str,
            "khu_vuc": str,
            "nganh_hang": str,
            "chi_tiet": str,
            "chuong_trinh": str,
            "noi_dung": str,
            "thang": str,
            "thoi_gian_bat_dau": str,
            "thoi_gian_ket_thuc": str,
            "nen_tang": str,
            "hinh_thuc": str,
            "ngan_sach_ban_dau": int,
            "ngan_sach_dieu_chinh": int,
            "ngan_sach_bo_sung": int,
            "last_updated_at": "datetime64[ns, UTC]"
        },
        "staging_budget_allocation": {
            "ma_ngan_sach_cap_1": str,
            "ma_ngan_sach_cap_2": str,
            "khu_vuc": str,
            "nganh_hang": str,
            "chi_tiet": str,
            "chuong_trinh": str,
            "noi_dung": str,
            "thang": str,
            "thoi_gian_bat_dau": "datetime64[ns]",
            "thoi_gian_ket_thuc": "datetime64[ns]",
            "nen_tang": str,
            "hinh_thuc": str,
            "ngan_sach_ban_dau": int,
            "ngan_sach_dieu_chinh": int,
            "ngan_sach_bo_sung": int,
            "ngan_sach_thuc_chi": int,
            "tong_so_ngay_thuc_chay": int,
            "tong_so_ngay_da_qua": int,
            "department": str,
            "account": str,
            "worksheet": str,
            "ngan_sach_he_thong": int,
            "ngan_sach_nha_cung_cap": int,
            "ngan_sach_kinh_doanh": int,
            "ngan_sach_tien_san": int,
            "ngan_sach_tuyen_dung": int,
            "ngan_sach_khac": int,
        }
    }

    if schema_type not in mapping_budget_schema:
        raise ValueError(f"❌ Unknown schema_type: {schema_type}")
    expected_columns = mapping_budget_schema[schema_type]
    for col, dtype in expected_columns.items():
        if col not in df.columns:
            df[col] = pd.NA
        try:
            if dtype in [int, float]:
                df[col] = df[col].apply(
                    lambda x: x if isinstance(x, (int, float, np.number, type(None))) else np.nan
                )
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(dtype)
            elif dtype == "datetime64[ns, UTC]":
                df[col] = pd.to_datetime(df[col], errors="coerce")
                if df[col].dt.tz is None:
                    df[col] = df[col].dt.tz_localize("UTC")
                else:
                    df[col] = df[col].dt.tz_convert("UTC")
            else:
                df[col] = df[col].astype(dtype, errors="ignore")
        except Exception as e:
            logging.warning(f"⚠️ Column '{col}' cannot be coerced to {dtype}: {e}")
    df = df[[col for col in expected_columns]]
    print(f"✅ [SCHEMA] Successfully enforced schema {schema_type} on Python DataFrame with {df.shape[1]} column(s).")
    logging.info(f"✅ [SCHEMA] Successfully enforced schema {schema_type} on Python DataFrame with {df.shape[1]} column(s).")
    return df