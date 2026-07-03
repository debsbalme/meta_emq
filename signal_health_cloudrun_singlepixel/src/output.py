import os
from datetime import datetime, timezone
from typing import Dict
import pandas as pd
from google.cloud import storage

def write_results_to_gcs_csv(results: Dict[str, pd.DataFrame], bucket_name: str, prefix: str = "") -> str:
    """
    Writes each DataFrame in results to GCS as CSV.
    Returns the run folder (gs://bucket/prefix/run_ts/).
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    base_prefix = "/".join([p for p in [prefix.strip("/"), run_ts] if p])

    for name, df in results.items():
        if df is None or df.empty:
            continue
        # write to /tmp then upload
        local_path = f"/tmp/{name}.csv"
        df.to_csv(local_path, index=False)

        blob_path = f"{base_prefix}/{name}.csv"
        blob = bucket.blob(blob_path)
        blob.upload_from_filename(local_path, content_type="text/csv")
        print(f"[GCS] Uploaded gs://{bucket_name}/{blob_path}")

    return f"gs://{bucket_name}/{base_prefix}/"
