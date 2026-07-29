import os
import time
import random
import json
from collections import deque
from datetime import date, datetime, timedelta, UTC
from typing import Any, Dict, List, Optional, Union, Tuple

import pandas as pd
import requests
from google.cloud import bigquery

# ================================================================
# CONFIG (env-driven for Cloud Run)
# ================================================================

GRAPH_VERSION = os.getenv("GRAPH_VERSION", "v24.0")
GRAPH_URL = f"https://graph.facebook.com/{GRAPH_VERSION}"

ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN", "")
if not ACCESS_TOKEN:
    raise RuntimeError("META_ACCESS_TOKEN env var is required (do not hardcode tokens in code).")

# Client-side throttling / backoff
RATE_LIMIT_MAX_CALLS = int(os.getenv("RATE_LIMIT_MAX_CALLS", "80"))
RATE_LIMIT_WINDOW_SEC = int(os.getenv("RATE_LIMIT_WINDOW_SEC", "60"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
BASE_BACKOFF_SEC = float(os.getenv("BASE_BACKOFF_SEC", "1.0"))

_REQUEST_TIMES = deque()

# ================================================================
# Helpers
# ================================================================

def get_dynamic_date_range(days_back: int, *, end_date: Optional[date] = None) -> Tuple[str, str]:
    """
    Returns (since, until) as ISO date strings in UTC, inclusive.
    """
    if days_back < 0:
        raise ValueError("days_back must be >= 0")
    until_dt = end_date or datetime.now(UTC).date()
    since_dt = until_dt - timedelta(days=days_back)
    return since_dt.isoformat(), until_dt.isoformat()

def to_unix(ts: Optional[Union[str, int, float]]) -> Optional[int]:
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        return int(ts)
    return int(datetime.fromisoformat(ts).timestamp())

def _respect_client_rate_limit() -> None:
    now = time.time()
    while _REQUEST_TIMES and now - _REQUEST_TIMES[0] > RATE_LIMIT_WINDOW_SEC:
        _REQUEST_TIMES.popleft()

    if len(_REQUEST_TIMES) >= RATE_LIMIT_MAX_CALLS:
        sleep_for = RATE_LIMIT_WINDOW_SEC - (now - _REQUEST_TIMES[0]) + 0.1
        if sleep_for > 0:
            print(f"[RATE LIMIT] Sleeping {sleep_for:.2f}s...")
            time.sleep(sleep_for)

class FacebookAPIError(Exception):
    pass

def fb_request(
    method: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    paginate: bool = False,
    max_pages: int = 50,
    timeout: int = 30,
) -> List[Dict[str, Any]]:
    """
    Facebook Graph API helper with retry/backoff and optional pagination.
    Permission/auth errors return [] (skip behavior).
    """
    if params is None:
        params = {}
    params = params.copy()
    params["access_token"] = ACCESS_TOKEN

    url = f"{GRAPH_URL}{path}"
    all_rows: List[Dict[str, Any]] = []
    pages = 0

    while True:
        attempt = 0
        while True:
            _respect_client_rate_limit()
            try:
                resp = requests.request(method, url, params=params, timeout=timeout)
            except requests.RequestException as e:
                if attempt < MAX_RETRIES:
                    attempt += 1
                    backoff = BASE_BACKOFF_SEC * (2 ** (attempt - 1)) * random.uniform(0.7, 1.3)
                    print(f"[RETRY] {path}: transport error ({e}); retry {attempt}/{MAX_RETRIES} in {backoff:.2f}s...")
                    time.sleep(backoff)
                    continue
                raise FacebookAPIError(f"Transport error on {path}: {e}") from e

            _REQUEST_TIMES.append(time.time())

            if resp.status_code == 200:
                break

            err = {}
            try:
                err = resp.json().get("error", {}) or {}
            except Exception:
                err = {}

            code = err.get("code")
            subcode = err.get("error_subcode")
            err_type = err.get("type")
            msg = err.get("message") or resp.text

            # Permission/auth → skip resource
            if err_type == "OAuthException" or code in {10, 190, 200}:
                print(f"[WARN] Permission/auth error {path} (code={code}, subcode={subcode}): {msg}")
                return []

            is_rate = code in {4, 17, 32, 613} or subcode in {99, 2446079, 2446078}
            is_5xx = 500 <= resp.status_code < 600
            retry_after_hdr = resp.headers.get("Retry-After")

            if (is_rate or is_5xx) and attempt < MAX_RETRIES:
                attempt += 1
                if retry_after_hdr and retry_after_hdr.isdigit():
                    backoff = float(retry_after_hdr)
                else:
                    backoff = BASE_BACKOFF_SEC * (2 ** (attempt - 1)) * random.uniform(0.7, 1.3)
                print(
                    f"[RETRY] {path}: status={resp.status_code} code={code} subcode={subcode} "
                    f"retry {attempt}/{MAX_RETRIES} in {backoff:.2f}s..."
                )
                time.sleep(backoff)
                continue

            raise FacebookAPIError(
                f"API error on {path}: status={resp.status_code} code={code} subcode={subcode} type={err_type} msg={msg}"
            )

        data = resp.json()
        if not (isinstance(data, dict) and "data" in data):
            return [data] if isinstance(data, dict) else data

        rows = data.get("data") or []
        all_rows.extend(rows)

        if not paginate:
            break

        paging = data.get("paging") or {}
        next_url = paging.get("next")
        if not next_url:
            break

        pages += 1
        if pages > max_pages:
            break

        url = next_url
        params = {}

    return all_rows

# ================================================================
# Daily Spend
# ================================================================

def get_business_adaccounts(business_id: str, limit: int = 200) -> List[Dict[str, Any]]:
    path = f"/{business_id}/owned_ad_accounts"
    params = {
        "fields": "id,name,account_id,account_status,currency,timezone_name",
        "limit": limit,
    }
    return fb_request("GET", path, params=params, paginate=True)

def get_daily_spend_for_business_adaccounts(
    business_id: str,
    since: Optional[Union[str, date]] = None,
    until: Optional[Union[str, date]] = None,
    *,
    days_back: Optional[int] = None,
    include_zero_rows: bool = False,
    max_pages: int = 50,
) -> pd.DataFrame:
    if days_back is not None:
        since_str, until_str = get_dynamic_date_range(days_back)
    else:
        if since is None:
            raise ValueError("Provide either days_back or since (and optionally until).")
        since_str = since if isinstance(since, str) else since.isoformat()
        until_str = since_str if until is None else (until if isinstance(until, str) else until.isoformat())

    accounts = get_business_adaccounts(business_id)
    out_rows: List[Dict[str, Any]] = []

    for acct in accounts:
        acct_id = acct.get("id")  # "act_<num>"
        if not acct_id:
            continue

        insights_path = f"/{acct_id}/insights"
        insights_params: Dict[str, Any] = {
            "fields": "spend,account_id,account_name,date_start,date_stop",
            "level": "account",
            "time_increment": 1,
            "time_range": json.dumps({"since": since_str, "until": until_str}, separators=(",", ":")),
        }
        if include_zero_rows:
            insights_params["include_zero_impressions"] = "true"

        rows = fb_request("GET", insights_path, params=insights_params, paginate=True, max_pages=max_pages)

        for r in rows:
            out_rows.append(
                {
                    "business_id": business_id,
                    "ad_account_id": acct_id,
                    "ad_account_name": acct.get("name"),
                    "timezone_name": acct.get("timezone_name"),
                    "date_start": r.get("date_start"),
                    "date_stop": r.get("date_stop"),
                    "spend": float(r.get("spend") or 0.0),
                }
            )

    df = pd.DataFrame(out_rows)
    if not df.empty:
        df = df.sort_values(["date_start", "ad_account_id"]).reset_index(drop=True)
    return df

# ================================================================
# Business Pixels
# ================================================================

def get_business_pixels(business_id: str) -> pd.DataFrame:
    edges = [
        ("owned_pixels", f"/{business_id}/owned_pixels"),
        ("adspixels", f"/{business_id}/adspixels"),
    ]

    pixels: Dict[str, Dict[str, Any]] = {}
    fields = ["id", "name", "owner_ad_account", "owner_business", "creation_time", "last_fired_time"]
    fields_str = ",".join(fields)

    for source_label, path in edges:
        rows = fb_request("GET", path, params={"fields": fields_str}, paginate=True)
        for p in rows:
            pid = p.get("id")
            if not pid:
                continue

            ad_account = p.get("owner_ad_account") or {}
            biz_owner = p.get("owner_business") or {}

            row = {
                "business_id": business_id,
                "pixel_id": pid,
                "pixel_name": p.get("name"),
                "pixel_creation_time": p.get("creation_time"),
                "last_fired_time": p.get("last_fired_time"),
                "owner_ad_account_id": ad_account.get("id"),
                "owner_ad_account_name": ad_account.get("name"),
                "owner_business_id": biz_owner.get("id"),
                "owner_business_name": biz_owner.get("name"),
                "source_edge": source_label,
            }

            if pid not in pixels:
                pixels[pid] = row
            else:
                existing = pixels[pid]["source_edge"]
                if source_label not in existing.split(";"):
                    pixels[pid]["source_edge"] = existing + ";" + source_label

    return pd.DataFrame(list(pixels.values())) if pixels else pd.DataFrame()

# ================================================================
# Pixel-level (placeholders)
# NOTE: Your notebook includes full pixel settings/event volume/dataset quality.
# If you want the full fidelity here, we can transplant those sections as well.
# ================================================================

def analyze_signal_health_for_business(
    business_id: str,
    since: Optional[Union[str, date]] = None,
    until: Optional[Union[str, date]] = None,
    *,
    days_back: Optional[int] = 7,
    include_zero_rows: bool = False,
    max_pages: int = 50,
) -> Dict[str, pd.DataFrame]:
    """
    Business-level workflow + daily spend.
    """
    pixels_df = get_business_pixels(business_id)

    results: Dict[str, pd.DataFrame] = {
        "pixels": pixels_df,
        "pixel_settings": pd.DataFrame(),  # populated in your full notebook version
        "ad_account_daily_spend": pd.DataFrame(),
    }

    # Daily spend across BM ad accounts
    adspend_df = get_daily_spend_for_business_adaccounts(
        business_id=business_id,
        since=since,
        until=until,
        days_back=days_back,
        include_zero_rows=include_zero_rows,
        max_pages=max_pages,
    )
    results["ad_account_daily_spend"] = adspend_df

    return results

# ================================================================
# BigQuery loader (generic)
# ================================================================

def load_df_to_bq(df: pd.DataFrame, project_id: str, dataset_id: str, table_id: str, if_exists: str = "append") -> None:
    if df is None or df.empty:
        print(f"[SKIP] {table_id} empty")
        return

    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = (
        bigquery.WriteDisposition.WRITE_APPEND if if_exists == "append"
        else bigquery.WriteDisposition.WRITE_TRUNCATE if if_exists == "replace"
        else bigquery.WriteDisposition.WRITE_EMPTY
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"Loaded → {project_id}.{dataset_id}.{table_id}")

def load_signal_health_to_bq(results: Dict[str, pd.DataFrame], project_id: str, dataset_id: str, if_exists: str = "append") -> None:
    for name, df in results.items():
        if df is None or df.empty:
            print(f"[SKIP] {name} empty")
            continue
        table = name.replace("-", "_")
        load_df_to_bq(df, project_id, dataset_id, table, if_exists)
