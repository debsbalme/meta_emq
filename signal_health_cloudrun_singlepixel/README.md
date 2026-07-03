# Signal Health → Cloud Run Jobs (Full Pixel + Business)

This package is a Cloud Run Jobs-compatible refactor of your Colab notebook so it can run on a schedule and:
- Pull business-level assets (pixels, ad accounts)
- Run pixel-level extraction (settings, event volume, dataset quality) per your notebook logic
- Export one CSV per output DataFrame to GCS (optional)
- Load each output DataFrame to BigQuery (optional)

## Required env vars
- META_ACCESS_TOKEN   (store in Secret Manager)
- BUSINESS_ID

## Optional env vars
- DAYS_BACK (default 7) used for daily spend range
- BQ_PROJECT_ID
- BQ_DATASET_ID
- BQ_WRITE_MODE (append|replace) default append
- GCS_BUCKET
- GCS_PREFIX (default signal_health_outputs)
- GRAPH_VERSION (default v24.0)

## Deploy (Cloud Run Jobs)
gcloud run jobs deploy signal-health-job \
  --source . \
  --region us-central1 \
  --set-env-vars BUSINESS_ID=YOUR_BM_ID,DAYS_BACK=7,BQ_PROJECT_ID=YOUR_PROJECT,BQ_DATASET_ID=meta_signals,GCS_BUCKET=YOUR_BUCKET \
  --set-secrets META_ACCESS_TOKEN=meta_access_token:latest

Run:
gcloud run jobs execute signal-health-job --region us-central1

## Enabling full pixel extraction at business level
The notebook includes `analyze_signal_health_for_pixel(...)`. The business-level wrapper can either:
- Run pixel settings only, or
- Loop through pixels and concatenate event volume + dataset quality.

If your `analyze_signal_health_for_business(...)` currently returns only pixels + pixel_settings + spend, you can switch on
the pixel loop by updating it to concatenate the per-pixel outputs (see below).

Recommended pattern inside `analyze_signal_health_for_business`:

- For each pixel_id:
  - pixel_res = analyze_signal_health_for_pixel(pixel_id, start_time=...)
  - append pixel_res['event_volume'] to a list
  - append pixel_res['dataset_quality'] to a list
- Concatenate at end and add to results dict (keys: pixel_event_volume, pixel_dataset_quality)

Your existing `load_signal_health_to_bq` will automatically create/load tables for any keys present in results.


## Pixel extraction controls
- RUN_PIXEL_WORKFLOW (default true)
- PIXEL_START_TIME (optional; passed into pixel stats endpoint logic)
