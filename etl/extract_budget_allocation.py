import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

from google.auth import default
from google.auth.transport.requests import AuthorizedSession
from google.cloud import secretmanager
import gspread

def extract_budget_allocation(
    worksheet_name,
    spreadsheet_id,
) -> pd.DataFrame:
    """
    Extract Budget Allocation from Google Sheets
    ---------
    Workflow:
        1. Convert YYYY-MM to worksheet name mMMYYYY
        2. Retrieve spreadsheet_id from Google Secret Manager
        3. Initialize gspread client
        4. Fetch worksheet records
        5. Return flattened DataFrame
    ---------
    Returns:
        pd.DataFrame
    """

    start_time = time.time()
    ICT = ZoneInfo("Asia/Ho_Chi_Minh")

    COMPANY = os.getenv("COMPANY")
    PROJECT = os.getenv("PROJECT")
    PLATFORM = os.getenv("PLATFORM")
    DEPARTMENT = os.getenv("DEPARTMENT")
    ACCOUNT = os.getenv("ACCOUNT")

    try:
        print(
            "🔍 [EXTRACT] Extracting Budget Allocation for month "
            f"{fetch_month_allocation} at "
            f"{datetime.now(ICT).strftime('%Y-%m-%d %H:%M:%S')}..."
        )

        # 1. Convert YYYY-MM → mMMYYYY
        try:
            year, month = fetch_month_allocation.split("-")
            worksheet_name = f"m{month.zfill(2)}{year}"
        except Exception:
            raise RuntimeError(
                "❌ [EXTRACT] Invalid fetch_month_allocation format. "
                "Expected YYYY-MM."
            )

        # 2. Retrieve spreadsheet_id from Secret Manager
        secret_client = secretmanager.SecretManagerServiceClient()

        secret_id = f"{COMPANY}_secret_{DEPARTMENT}_{PLATFORM}_sheet_id_{ACCOUNT}"
        secret_name = f"projects/{PROJECT}/secrets/{secret_id}/versions/latest"

        try:
            secret_response = secret_client.access_secret_version(
                request={"name": secret_name}
            )
            spreadsheet_id = secret_response.payload.data.decode("utf-8")
        except Exception as e:
            raise RuntimeError(
                "❌ [EXTRACT] Failed to retrieve Budget Allocation spreadsheet_id "
                "from Google Secret Manager."
            ) from e

        # 3. Initialize gspread client
        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        creds, _ = default(scopes=scopes)

        gspread_client = gspread.Client(auth=creds)
        gspread_client.session = AuthorizedSession(creds)

        # 4. Fetch worksheet data
        try:
            sheet = gspread_client.open_by_key(spreadsheet_id)
            worksheet = sheet.worksheet(worksheet_name)
            records = worksheet.get_all_records()
        except gspread.exceptions.WorksheetNotFound as e:
            raise RuntimeError(
                "❌ [EXTRACT] Worksheet "
                f"{worksheet_name} not found in spreadsheet {spreadsheet_id}."
            ) from e
        except Exception as e:
            raise RuntimeError(
                "❌ [EXTRACT] Failed to fetch Budget Allocation data "
                "from Google Sheets."
            ) from e

        if not records:
            print(
                "⚠️ [EXTRACT] No Budget Allocation data found in worksheet "
                f"{worksheet_name}. Returning empty DataFrame."
            )
            return pd.DataFrame()

        df = pd.DataFrame(records).replace("", None)

        df.attrs["retryable"] = False
        df.attrs["rows_output"] = len(df)
        df.attrs["time_elapsed"] = round(time.time() - start_time, 2)

        print(
            "✅ [EXTRACT] Successfully extracted Budget Allocation for month "
            f"{fetch_month_allocation} with "
            f"{len(df)} row(s) in "
            f"{df.attrs['time_elapsed']}s."
        )

        return df

    except Exception as e:
        raise RuntimeError(
            "❌ [EXTRACT] Failed to extract Budget Allocation for month "
            f"{fetch_month_allocation} due to {e}."
        ) from e
