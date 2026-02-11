import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import time

from dags._dags_budget_reconciliation import dags_budget_reconciliation

def dags_budget_reconciliation_1(
    *,
    worksheet_name: str,
    spreadsheet_id: str,
):
    tasks = {
        "dags_budget_reconciliation": dags_budget_reconciliation,
    }

    results = {}

    for name, fn in tasks.items():
        start_ts = time.time()

        try:
            fn(
                worksheet_name=worksheet_name,
                spreadsheet_id=spreadsheet_id,
            )

            results[name] = {
                "status": "SUCCESS",
                "duration": round(time.time() - start_ts, 2),
                "detail": "",
            }

        except Exception as e:
            results[name] = {
                "status": "FAILED",
                "duration": round(time.time() - start_ts, 2),
                "detail": str(e),
            }