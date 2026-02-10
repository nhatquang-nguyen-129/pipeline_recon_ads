import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import time
import requests

import pandas as pd

from google.auth import default
from google.auth.exceptions import RefreshError

import gspread
from gspread.exceptions import APIError, WorksheetNotFound

def extract_budget_allocation(
    worksheet_name,
    spreadsheet_id,
) -> pd.DataFrame:
    """
    Extract Budget Allocation from Google Spreadsheets
    ---------
    Workflow:
        1. Validate input worksheet_name
        2. Validate input spreadsheet_id
        3. Make API call for spreadsheets.readonly scope
        4. Append extract tabular data
        5. Enforce to DataFrame
    ---------
    Returns:
        1. DataFrame:
            Flattened budget allocation records
    """

    start_time = time.time()

    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds, _ = default(scopes=scopes)

    # Initialize gspread client
    try:
        print(
            "🔍 [EXTRACT] Initializing Google Gspread client with scopes "
            f"{scopes}..."
        )
        
        google_gspread_client = gspread.authorize(creds)

        print(
            "✅ [EXTRACT] Successfully initialized Google Gspread client with scopes "
            f"{scopes} for Budget Allocation extraction."
        )

    except Exception as e:
        raise RuntimeError(
            "❌ [EXTRACT] Failed to initialize Google Gspread client due to "
            f"{e}."
        ) from e       

    # Make Gspread API call for budget allocation
    try:
        try:
            print(
                "🔍 [EXTRACT][STEP 1] Opening spreadsheet by key "
                f"{spreadsheet_id}..."
            )
            sheet = google_gspread_client.open_by_key(spreadsheet_id)
            print(
                "✅ [EXTRACT][STEP 1] Successfully opened spreadsheet."
            )

            print(
                "🔍 [EXTRACT][STEP 2] Retrieving worksheet "
                f"{worksheet_name}..."
            )
            worksheet = sheet.worksheet(worksheet_name)
            print(
                "✅ [EXTRACT][STEP 2] Successfully retrieved worksheet "
                f"{worksheet_name}."
            )

            print(
                "🔍 [EXTRACT][STEP 3] Fetching all records from worksheet "
                f"{worksheet_name}..."
            )
            records = worksheet.get_all_records()
            print(
                "✅ [EXTRACT][STEP 3] Successfully fetched "
                f"{len(records)} record(s) from worksheet "
                f"{worksheet_name}."
            )

        except Exception as e:
            import traceback
            tb = traceback.format_exc()

            print(
                "❌ [EXTRACT][DEBUG] Exception occurred during Google Sheet extraction.\n"
                f"Spreadsheet ID : {spreadsheet_id}\n"
                f"Worksheet name : {worksheet_name}\n"
                f"Exception type : {type(e)}\n"
                f"Exception repr : {repr(e)}\n"
                f"Traceback:\n{tb}"
            )

            raise RuntimeError(
                "❌ [EXTRACT] Failed during Google Sheet extraction. "
                "See detailed debug logs above."
            ) from e

        if not records:
            print(
                "⚠️ [EXTRACT] Completely extracted Budget Allocation from worksheet_name "
                f"{worksheet_name} but empty DataFrame returned."
            )

            df = pd.DataFrame()
            df.attrs.update({
                "success": True,
                "retryable": False,
                "time_elapsed": round(time.time() - start_time, 2),
                "rows_input": None,
                "rows_output": 0,
            })
            return df

        df = pd.DataFrame(records)
        df.attrs.update({
            "success": True,
            "retryable": False,
            "time_elapsed": round(time.time() - start_time, 2),
            "rows_input": None,
            "rows_output": len(df),
        })
        
        print(
            "✅ [EXTRACT] Successfully extracted Budget Allocation from worksheet_name "
            f"{worksheet_name} with "
            f"{len(df)} row(s) in "
            f"{df.attrs['time_elapsed']}s."
        )

        return df

    except WorksheetNotFound as e:
        df = pd.DataFrame()
        df.attrs.update({
            "success": False,
            "retryable": False,
            "error_message": (f"Worksheet {worksheet_name} does not exist in spreadsheet {spreadsheet_id}."),
            "time_elapsed": round(time.time() - start_time, 2),
            "rows_input": None,
            "rows_output": 0,
        })
        
        raise RuntimeError(
            "❌ [EXTRACT] Failed to extract Budget Allocation due to worksheet "
            f"{worksheet_name} does not exist in spreadsheet "
            f"{spreadsheet_id}."
        ) from e

    # Unauthorized credentials
    except RefreshError as e:
        df = pd.DataFrame()
        df.attrs.update({
            "success": False,
            "retryable": False,
            "error_message": ("Unauthorized Google credentials. Manual re-authentication required."),
            "time_elapsed": round(time.time() - start_time, 2),
            "rows_input": None,
            "rows_output": 0,
        })
        
        raise RuntimeError(
            "❌ [EXTRACT] Failed to extract Budget Allocation due to "
            "unauthorized Google credentials. Manual re-authentication required."
        ) from e

    except APIError as e:
        status = e.response.status_code if e.response else None

    # Unexpected retryable API error
        if status in {
            408, 
            429, 
            500, 
            502, 
            503, 
            504
        }:
            
            df = pd.DataFrame()
            df.attrs.update({
                "success": False,
                "retryable": True,
                "error_message": (
                    f"Google API error {status}: {e}"
                ),
                "time_elapsed": round(time.time() - start_time, 2),
                "rows_input": None,
                "rows_output": 0,
            })
            
            raise RuntimeError(
                "⚠️ [EXTRACT] Failed to extract Budget Allocation for worksheet_name "
                f"{worksheet_name} due to API error "
                f"{e} with HTTP request status "
                f"{status} then this request is eligible to retry."
            ) from e

    # Unauthorized non-retryable access error
        if status in {
            401, 
            403
        }:
            
            df = pd.DataFrame()
            df.attrs.update({
                "success": False,
                "retryable": False,
                "error_message": (
                    f"Google API error {status}: {e}"
                ),
                "time_elapsed": round(time.time() - start_time, 2),
                "rows_input": None,
                "rows_output": 0,
            })
            
            raise RuntimeError(               
                "❌ [EXTRACT] Failed to extract Budget Allocation for worksheet_name "
                f"{worksheet_name} due to unauthorized access "
                f"{e} then this request is not eligible to retry."
            ) from e

    # Unexpected non-retryable API error
        df = pd.DataFrame()
        df.attrs.update({
            "success": False,
            "retryable": False,
            "error_message": ("Unexpected non-retryable error"),
            "time_elapsed": round(time.time() - start_time, 2),
            "rows_input": None,
            "rows_output": 0,
        })
        raise RuntimeError(
            "❌ [EXTRACT] Failed to extract Budget Allocation for worksheet_name "
            f"{worksheet_name} due to API error "
            f"{e} with HTTP request status "
            f"{status} then this request is not eligible to retry."
        ) from e

    # Unexpected retryable request timeout error
    except requests.exceptions.Timeout as e:
        
        df = pd.DataFrame()
        df.attrs.update({
            "success": False,
            "retryable": True,
            "error_message": f"Request timeout: {e}",
            "time_elapsed": round(time.time() - start_time, 2),
            "rows_input": None,
            "rows_output": 0,
        })
        
        raise RuntimeError(
            "⚠️ [EXTRACT] Failed to extract Budget Allocation for worksheet_name"
            f"{worksheet_name} due to request timeout error then this request is eligible to retry."
        ) from e

    # Unexpected retryable request connection error
    except requests.exceptions.ConnectionError as e:
        
        df = pd.DataFrame()
        df.attrs.update({
            "success": False,
            "retryable": True,
            "error_message": f"Connection error: {e}",
            "time_elapsed": round(time.time() - start_time, 2),
            "rows_input": None,
            "rows_output": 0,
        })

        raise RuntimeError(
            "⚠️ [EXTRACT] Failed to extract Budget Allocation for worksheet_name "
            f"{worksheet_name} due to request connection error hen this request is eligible to retry."
        ) from e

    # Unknown non-retryable error 
    except Exception as e:
        
        df = pd.DataFrame()
        df.attrs.update({
            "success": False,
            "retryable": False,
            "error_message": f"Unexpected error: {e}",
            "time_elapsed": round(time.time() - start_time, 2),
            "rows_input": None,
            "rows_output": 0,
        })

        raise RuntimeError(
            "❌ [EXTRACT] Failed to extract Budget Allocation for worksheet_name "
            f"{worksheet_name} due to "
            f"{e}."
        ) from e