# Data Build Tool for Budget Reconcilication

## Purpose

- Use **dbt** adapter to build Budget analytics-ready materialized tables in **Google BigQuery**

- Used **dbt** only for SQL transformations and all ELT processes are handled upstream

- Join Budget Reconciliation with multiple facts table separated by **month**

- Join Budget Reconciliation materialized table with advertising spend

- Define final analytical grain and manage model dependencies using `ref()`

---

## Install for Windows

### Activate Python venv

- Create Python virtual environment with Python 3.13 interpreter if `venv\` folder not exists
```bash
& "C:\Users\ADMIN\AppData\Local\Programs\Python\Python313\python.exe" -m venv venv
```

- Activate Python virtual environment and check `(venv)` in the terminal
```bash
venv/scripts/activate
```

---

### Install dbt adapter for Google BigQuery

- Install dbt adapter for Google BigQuery using the terminal
```bash
pip install dbt-core dbt-bigquery
```

- Verify installation and check installed dbt version
```bash
dbt --version
```

---

## Install for MacOS

### Activate Python virtual environment

- Create Python virtual environment if `venv\` folder not exists
```bash
python3.13 -m venv venv
```

- Activate Python virtual environment and check `(venv)` in the terminal
```bash
source venv/bin/activate
```

---

### Install dbt adapter for Google BigQuery

- Install dbt adapter for Google BigQuery using the terminal
```bash
pip install dbt-core dbt-bigquery
```

- Verify installation and check installed dbt version
```bash
dbt --version
```

---

## Structure

### Models folder

- `models` is root folder for all dbt models and all logical separation by transformation stage

- `models/stg` is the staging layer providing a clean abstraction over ETL output tables and materialized as `ephemeral` with example:
```bash
{{ config(
    materialized='ephemeral',
    tags=['stg', 'recon', 'spend']
) }}
```

- `models/int` is the intermediate layer with the responsibilty to combine staging models then join with dimensions and materialized as `ephemeral` with example:
```bash
{{ config(
    materialized='ephemeral',
    tags=['int', 'recon', 'spend']
) }}
```

- `models/mart` is the final materialization layer and materialized as physical `table` with example:
```bash
{{ config(
    materialized='table',
    tags=['mart', 'recon', 'spend']
) }}
```

---

### Config file

- `dbt_project.yml` is a required file for all dbt project which contains project operation instructions

- `profiles.yml` is a required file which contains the connection details for the data warehouse

---

## Deployment

### BigQuery Metadata Permission

- Budget Reconciliation models need to join spend data from multiple datasets using regional `INFORMATION_SCHEMA` views

- Budget Reconciliation models specifically require the `BigQuery Metadata Viewer` role in the IAM
```text
roles/bigquery.metadataViewer
```

- Without this permission, dbt may fail to read metadata from Google BigQuery datasets
```text
User does not have permission to query table
```

---

### Manual Deployment for Windows

- Complie only with no execution
```bash
dbt compile
```

- Run all models
```bash
dbt build
```

- Run only budget reconciliation
```bash
$env:PROJECT="your-gcp-project"
$env:COMPANY="your-company-in-short"
$env:DEPARTMENT="your-department"
$env:ACCOUNT="your-account"

dbt build `
  --project-dir dbt `
  --profiles-dir dbt `
  --select tag:mart
```

---

### Manual Deployment for MacOS

- Complie only with no execution
```bash
dbt compile
```

- Run all models
```bash
dbt build
```

- Run only budget reconciliation
```bash
export PROJECT="your-gcp-project"
export COMPANY="your-company-in-short"
export DEPARTMENT="your-department"
export ACCOUNT="your-account"

dbt build `
  --project-dir dbt `
  --profiles-dir dbt `
  --select tag:mart
```

---

### Deployment with DAGs

- Using Python `subprocess` to call dbt for each stream
```bash
dbt_budget_reconcilie(
    google_cloud_project=PROJECT,
    select="tag:mart",
)
```