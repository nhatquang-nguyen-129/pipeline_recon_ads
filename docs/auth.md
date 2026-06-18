# Authentication for Budget Reconciliation

## Purpose

- Authenticate **Google Cloud Platform** services used in this pipeline

- Authenticate **Google Sheets API** for reading budget allocation

- Use manual login with **Application Default Credentials** for local environment

- Use **Service Account** authentication to manage permissions in cloud environments

- Use centralized Google Cloud Project with required APIs enabled for cloud deployment

---

## Local setup

### Customize OAuth Client ID to login with Google Drive scopes

-  Attempting to authenticate with these scopes using the default Cloud SDK client may result in the following error:
```text
This app is blocked

This app tried to access sensitive info in your Google Account. To keep your account safe, Google blocked this access.
```

- In Google Cloud Platform, navigate to **APIs & Services** then **Library**, enable the following APIs including Google Sheets API and Google Drive API

- Create a dedicated OAuth 2.0 Desktop App for local development and keep the OAuth Consent Screen in Production mode.

- **Do not** add the following scopes to your OAuth2 Client Desktop App because Google classifies both `spreadsheets` and `drive.readonly` as sensitive OAuth scopes then it may trigger additional verification requirements for the in-production application
```bash
https://www.googleapis.com/auth/spreadsheets
https://www.googleapis.com/auth/drive.readonly
https://www.googleapis.com/auth/cloud-platform
```

- Download the OAuth Client JSON then store this `oauth2_desktop_client.json` file securely and do not commit to Git

---

### Local setup for Windows

- Download and install Google Cloud SDK from official source
```bash
https://cloud.google.com/sdk
```

- Verify installed Google Cloud SDK version
```bash
gcloud --version
```

- Login to Google Cloud on your Windows local environment
```bash
gcloud auth login
```

- Allow applications using Application Default Credentials to access Google Sheets files stored in Google Drive by authenticating with the required OAuth scopes then accept the "Google hasn't verified this app" warning
```bash
gcloud auth application-default login `
  --client-id-file=oauth2_desktop_client.json `
  --scopes="https://www.googleapis.com/auth/spreadsheets,https://www.googleapis.com/auth/drive.readonly,https://www.googleapis.com/auth/cloud-platform"
```

- Verify authenticated Google accounts
```bash
gcloud auth list
```

- Check all accessible Google Cloud projects attached to the current ADC
```bashß
gcloud projects list
```

- Set default Google Cloud project for Google BigQuery and quota billing
```bash
gcloud auth application-default set-quota-project YOUR_GOOGLE_CLOUD_PROJECT_ID
```

- Check Google Cloud quota project attached to ADC
```bash
gcloud config get-value project
```

- Verify ADC is working
```bash
gcloud auth application-default print-access-token
```

---

### Local setup for MacOS

- Install **Homebrew** from official source
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

- Add **Homebrew** to your system path once the installation finishes if you're using an **Apple Silicon Mac with M chip**
```bash
echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> ~/.zprofile
eval "$(/opt/homebrew/bin/brew shellenv)"
```

- Verify Homebrew version
```bash
brew --version
```

- Download and install Google Cloud SDK from official source
```bash
brew install --cask google-cloud-sdk
```

- Verify installed Google Cloud SDK version
```bash
gcloud --version
```

- Allow applications using Application Default Credentials to access Google Sheets files stored in Google Drive by authenticating with the required OAuth scopes then accept the "Google hasn't verified this app" warning
```bash
gcloud auth application-default login \
  --client-id-file=oauth2_desktop_client.json \
  --scopes="https://www.googleapis.com/auth/spreadsheets,https://www.googleapis.com/auth/drive.readonly,https://www.googleapis.com/auth/cloud-platform"
```

- Verify authenticated Google accounts
```bash
gcloud auth list
```

- Check all accessible Google Cloud projects attached to the current ADC
```bash
gcloud projects list
```

- Set default Google Cloud project for Google BigQuery and quota billing
```bash
gcloud auth application-default set-quota-project YOUR_GOOGLE_CLOUD_PROJECT_ID
```

- Check Google Cloud quota project attached to ADC
```bash
gcloud config get-value project
```

- Verify ADC is working
```bash
gcloud auth application-default print-access-token
```

## Cloud Run setup

### Enable minimum required APIs and services

- Enable **Cloud Run API** for container execution in the target Google Cloud project

- Enable **Cloud Run API** for container execution in the target Google Cloud project

- Enable **Google BigQuery API** for data warehouse access in the target Google Cloud project

---

### Enable Service Account

- Create a dedicated Google Cloud Platform's **Service Account** for pipeline_recon_ads

- Grant **Cloud Run Admin permissions** for required IAM Roles

- Grant **BigQuery Data Editor** and **BigQuery Job User** for required IAM Roles

---

### Share Google Sheets access

- Open the Google Sheet containing budget allocation

- Click Share button on the top right

- Add Service Account email `etl-budget-reconcile@YOUR_GCP_PROJECT_ID.iam.gserviceaccount.com`

- Grant `Viewer` if read-only or `Editor` permission if writing back reconciliation results