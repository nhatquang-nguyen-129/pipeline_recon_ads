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
        error = RuntimeError(
            "❌ [EXTRACT] Failed to initialize Google Gspread client due to "
            f"{e}."
        )
        error.retryable = False
        raise error from e   

    # Make Gspread API call for budget allocation
    try:
        print(
            "🔍 [EXTRACT] Extracting Budget Allocation in worksheet_name "
            f"{worksheet_name} from spreadsheet_id "
            f"{spreadsheet_id}..."
        )
        
        sheet = google_gspread_client.open_by_key(spreadsheet_id)
        worksheet = sheet.worksheet(worksheet_name)
        records = worksheet.get_all_records()
        
        print(
            "✅ [EXTRACT] Successfully extracted "
            f"{len(records)} record(s) in worksheet_name "
            f"{worksheet_name} from spreadsheet_id "
            f"{spreadsheet_id}."
        )

        if not records:
            print(
                "⚠️ [EXTRACT] Completely extracted Budget Allocation from worksheet_name "
                f"{worksheet_name} but empty DataFrame returned."
            )

            df = pd.DataFrame()
            
            return df

        df = pd.DataFrame(records)
        
        print(
            "✅ [EXTRACT] Successfully extracted Budget Allocation from worksheet_name "
            f"{worksheet_name} with "
            f"{len(df)} row(s). "
        )

        return df

    except WorksheetNotFound as e:
        error = RuntimeError(
            "❌ [EXTRACT] Failed to extract Budget Allocation due to worksheet "
            f"{worksheet_name} does not exist in spreadsheet "
            f"{spreadsheet_id}."
        )
        error.retryable = False
        raise error from e

    # Unauthorized credentials
    except RefreshError as e:
        error = RuntimeError("❌ [EXTRACT] Failed to extract Budget Allocation due to unauthorized Google credentials then manual re-authentication is required.")
        error.retryable = False
        raise error from e

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
            
            error = RuntimeError(
                "⚠️ [EXTRACT] Failed to extract Budget Allocation for worksheet_name "
                f"{worksheet_name} due to API error "
                f"{e} with HTTP request status "
                f"{status} then this request is eligible to retry."
            )
            error.retryable = True
            raise error from e

    # Unauthorized non-retryable access error
        if status in {
            401, 
            403
        }:

            error = RuntimeError(               
                "❌ [EXTRACT] Failed to extract Budget Allocation for worksheet_name "
                f"{worksheet_name} due to unauthorized access "
                f"{e} then this request is not eligible to retry."
            )
            error.retryable = False
            raise error from e

    # Unexpected non-retryable API error
        error = RuntimeError(
            "❌ [EXTRACT] Failed to extract Budget Allocation for worksheet_name "
            f"{worksheet_name} due to API error "
            f"{e} with HTTP request status "
            f"{status} then this request is not eligible to retry."
        )
        error.retryable = False
        raise error from e        

    # Unexpected retryable request timeout error
    except requests.exceptions.Timeout as e:
        error = RuntimeError(
            "⚠️ [EXTRACT] Failed to extract Budget Allocation for worksheet_name"
            f"{worksheet_name} due to request timeout error then this request is eligible to retry."
        )
        error.retryable = True
        raise error from e               

    # Unexpected retryable request connection error
    except requests.exceptions.ConnectionError as e:
        error = RuntimeError(
            "⚠️ [EXTRACT] Failed to extract Budget Allocation for worksheet_name "
            f"{worksheet_name} due to request connection error hen this request is eligible to retry."
        )
        error.retryable = True
        raise error from e       

    # Unknown non-retryable error 
    except Exception as e:
        error = RuntimeError(
            "❌ [EXTRACT] Failed to extract Budget Allocation for worksheet_name "
            f"{worksheet_name} due to "
            f"{e}."
        )
        error.retryable = False
        raise error from e          