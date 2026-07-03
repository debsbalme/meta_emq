# src/post_load_sql.py

from pathlib import Path
from google.cloud import bigquery


def run_post_load_sql(
    project_id: str,
    dataset_id: str,
    *,
    lookup_project: str = "pj-scoe-wzccxo",
    lookup_dataset: str = "adverity_manual",
    sql_path: str = "sql/bq_meta_post.sql",
) -> None:
    client = bigquery.Client(project=project_id)

    sql = Path(sql_path).read_text()

    sql = sql.format(
        bq_project=project_id,
        bq_dataset=dataset_id,
        lookup_project=lookup_project,
        lookup_dataset=lookup_dataset,
    )

    print(f"[INFO] Running post-load SQL from {sql_path}")
    job = client.query(sql)
    job.result()

    print("[INFO] Post-load SQL complete.")