# Backfill for Budget Reconciliation

## Purpose

- Execute Budget Reconciliation DAGs for **historical data** outside of default runtime schedule

- Allow **manual override** of time range instead of relying on `main.py` predefined `MODE` mappings

- Require standard environment variables `COMPANY`, `PROJECT`, `DEPARTMENT`, `ACCOUNT` to be provided

- Primarily designed to run in **local environment** for controlled historical reprocessing

- Accept `--input_month` via argparse for flexible time-based backfill execution

---

## Execution

### Windows

- Ensure Google Cloud Platform's Application Default Credentials already configured

- Run Backfill for Budget Reconciliation with specific month using CLI
```bash
$env:PROJECT="your-gcp-project"
$env:COMPANY="your-company-in-short"
$env:DEPARTMENT="your-department"
$env:ACCOUNT="your-account"

python -m backfill.backfill_budget_reconcile --input_month=YYYY-MM
```

### MacOS

- Ensure Google Cloud Platform's Application Default Credentials already configured

- Run Backfill for Budget Reconciliation with specific month using CLI
```bash
export PROJECT="your-gcp-project"
export COMPANY="your-company-in-short"
export DEPARTMENT="your-department"
export ACCOUNT="your-account"

python -m backfill.backfill_budget_reconcile --input_month=YYYY-MM
```