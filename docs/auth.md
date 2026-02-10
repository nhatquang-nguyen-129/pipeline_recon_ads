# Nothing to read

- Login Google Sheet
```bash
gcloud auth application-default login `
  --scopes="https://www.googleapis.com/auth/spreadsheets,https://www.googleapis.com/auth/drive.readonly,https://www.googleapis.com/auth/cloud-platform"
```
# Check quota project trong ADC
gcloud config get-value project

gcloud auth application-default login


(venv) PS D:\data-pipeline\pipeline_recon_ads> gcloud config list
>>
[accessibility]
screen_reader = False
[core]
account = nhatquang.nguyen.129@gmail.com
disable_usage_reporting = True
project = seer-retail-sales

Your active configuration is: [default]


gcloud config set project seer-digital-ads

gcloud auth application-default show-quota-project

