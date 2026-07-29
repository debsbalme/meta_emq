import os
from src.signal_health import analyze_signal_health_for_business, load_signal_health_to_bq
from src.output import write_results_to_gcs_csv
from src.post_load_sql import run_post_load_sql


def main() -> None:
    business_id = os.getenv("BUSINESS_ID", "")
    if not business_id:
        raise RuntimeError("BUSINESS_ID env var is required.")

    days_back = int(os.getenv("DAYS_BACK", "7"))
    bq_project = os.getenv("BQ_PROJECT_ID", "")
    bq_dataset = os.getenv("BQ_DATASET_ID", "")
    bq_mode = os.getenv("BQ_WRITE_MODE", "append")

    lookup_project = os.getenv("LOOKUP_PROJECT_ID", "pj-scoe-wzccxo")
    lookup_dataset = os.getenv("LOOKUP_DATASET_ID", "adverity_manual")

    gcs_bucket = os.getenv("GCS_BUCKET", "")
    gcs_prefix = os.getenv("GCS_PREFIX", "signal_health_outputs")

    run_post_load = os.getenv("RUN_POST_LOAD_SQL", "true").lower() == "true"

    print(f"[INFO] Running business analysis for BUSINESS_ID={business_id} days_back={days_back}")
    results = analyze_signal_health_for_business(
        business_id=business_id,
        days_back=days_back
    )

    if gcs_bucket:
        folder = write_results_to_gcs_csv(
            results,
            bucket_name=gcs_bucket,
            prefix=gcs_prefix
        )
        print(f"[INFO] CSV outputs written under {folder}")
    else:
        print("[INFO] GCS_BUCKET not set; skipping CSV export.")

    if bq_project and bq_dataset:
        print(f"[INFO] Loading results to BigQuery {bq_project}.{bq_dataset} mode={bq_mode}")
        load_signal_health_to_bq(
            results,
            project_id=bq_project,
            dataset_id=bq_dataset,
            if_exists=bq_mode
        )

        if run_post_load:
            run_post_load_sql(
                project_id=bq_project,
                dataset_id=bq_dataset,
                lookup_project=lookup_project,
                lookup_dataset=lookup_dataset,
            )
    else:
        print("[INFO] BQ_PROJECT_ID/BQ_DATASET_ID not set; skipping BigQuery load.")

    print("[DONE] Job complete.")


if __name__ == "__main__":
    main()