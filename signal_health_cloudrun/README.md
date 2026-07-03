# Signal Health → Cloud Run Jobs

## What this is
A Cloud Run Jobs-compatible refactor of your Colab notebook so it can run on schedule and:
- Export one CSV per DataFrame to GCS (optional)
- Load each DataFrame to BigQuery (optional)

## Required env vars
- META_ACCESS_TOKEN   (store in Secret Manager; do not hardcode)
- BUSINESS_ID

## Optional env vars
- DAYS_BACK (default 7)
- BQ_PROJECT_ID
- BQ_DATASET_ID
- BQ_WRITE_MODE (append|replace) default append
- GCS_BUCKET
- GCS_PREFIX (default signal_health_outputs)

## Deploy (Cloud Run Jobs)
Example (build from source):
gcloud run jobs deploy signal-health-job \
  --source . \
  --region us-central1 \
  --set-env-vars BUSINESS_ID=YOUR_BM_ID,DAYS_BACK=7,BQ_PROJECT_ID=YOUR_PROJECT,BQ_DATASET_ID=meta_signals,GCS_BUCKET=YOUR_BUCKET \
  --set-secrets META_ACCESS_TOKEN=meta_access_token:latest

Then run:
gcloud run jobs execute signal-health-job --region us-central1

## Schedule (Cloud Scheduler)
Create a Scheduler job that executes the Cloud Run Job (recommended via gcloud):
gcloud scheduler jobs create http signal-health-schedule \
  --schedule="0 7 * * *" \
  --uri="https://run.googleapis.com/apis/run.googleapis.com/v1/namespaces/YOUR_PROJECT/jobs/signal-health-job:run" \
  --http-method=POST \
  --oauth-service-account-email=SCHEDULER_SA@YOUR_PROJECT.iam.gserviceaccount.com \
  --location=us-central1

Grant SCHEDULER_SA permission:
roles/run.developer + roles/iam.serviceAccountUser (or narrower per your org policy)

## Notes
- This repo includes the business-level workflow + daily spend.
- Your notebook’s full pixel-level dataset quality / event volume sections can be transplanted into src/signal_health.py as well.
