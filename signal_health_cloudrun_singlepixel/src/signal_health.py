import os
import time
import random
import pandas as pd
import requests
import json

from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Union, Tuple
from collections import deque
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from google.cloud.bigquery import TimePartitioning, TimePartitioningType
from datetime import datetime, timezone

# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------
# -------------------------------------------------------------------
# RATE LIMIT SETTINGS
# -------------------------------------------------------------------

# Simple global client-side rate limiting
RATE_LIMIT_MAX_CALLS = 80           # max calls per window
RATE_LIMIT_WINDOW_SEC = 60          # rolling window in seconds

# Retry/backoff settings for rate-limit / transient errors
MAX_RETRIES = 5
BASE_BACKOFF_SEC = 1.0

# Internal queue of request timestamps
_REQUEST_TIMES = deque()

# -------------------------------------------------------------------
# FACEBOOK SETTINGS
# -------------------------------------------------------------------


GRAPH_VERSION = os.getenv("GRAPH_VERSION", "v24.0")

# TEMP: keep single pixel workflow
HARDCODED_PIXEL_ID = "607899633095051"
GRAPH_URL = f"https://graph.facebook.com/{GRAPH_VERSION}"


#USER_TOKEN - GRAPH API EXPLORER
#ACCESS_TOKEN = "EAARab7nGV9ABQflXOi2FU1IYwZAfXsMQo8M5lm6kHZC2p7mKXmZAsq7kvZC32M5JCSsasfbgZCPMgDSrRakCzixBSPcqZCBn8BA8uZCQACzwQWXguX6PN2rMFkkmlFWPysNKoC7yzW9ulND3neZAnvftcs297UrQ6Qzgsie9YGJBCoE55Eu5tuuXpt4CQMd4vD68kVxCILgKsLS0bzdsf6ZC29ldDZBKsSBfZCr0Kx0"

#SYSTEM_USER_TOKEN
ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN", "")
if not ACCESS_TOKEN:
    raise RuntimeError("META_ACCESS_TOKEN env var is required (store in Secret Manager; do not hardcode).")


from datetime import datetime, timedelta, UTC

def get_dynamic_date_range(days_back=7):
    run_date = datetime.now(UTC).date()
    since = (run_date - timedelta(days=days_back)).isoformat()

    return since

# ================================================================
#  SIGNAL HEALTH MODULE
#  ---------------------------------------------------------------
#  - Single Business Analysis
#  - Single Pixel Analysis
#  - Flattened BQ-ready DataFrames
#  - Dataset Quality API v24 compliant
#  - Rate-limited + retry logic
# ================================================================


def to_unix(ts):
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        return int(ts)
    # Convert YYYY-MM-DD → Unix timestamp
    return int(datetime.fromisoformat(ts).timestamp())




def _respect_client_rate_limit():
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


# ================================================================
# FACEBOOK API REQUEST WRAPPER
# ================================================================
from typing import Any, Dict, List, Optional
import random
import time
import requests

def fb_request(
    method: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    paginate: bool = False,
    max_pages: int = 50,
    timeout: int = 30,
) -> List[Dict[str, Any]]:
    """
    Facebook Graph API request helper with retry/backoff and optional pagination.

    Notes:
      - max_pages controls the number of pagination "next" hops (not counting the first page).
      - Permission/auth errors return [] (skip behavior).
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
                # Network/transient transport failure
                if attempt < MAX_RETRIES:
                    attempt += 1
                    backoff = BASE_BACKOFF_SEC * (2 ** (attempt - 1)) * random.uniform(0.7, 1.3)
                    print(f"[RETRY] {path}: transport error ({e}); retry {attempt}/{MAX_RETRIES} in {backoff:.2f}s...")
                    time.sleep(backoff)
                    continue
                raise FacebookAPIError(f"Transport error on {path}: {e}") from e

            _REQUEST_TIMES.append(time.time())

            # Success
            if resp.status_code == 200:
                break

            # Parse error payload (best effort)
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

            # Rate limit / transient
            is_rate = code in {4, 17, 32, 613} or subcode in {99, 2446079, 2446078}
            is_5xx = 500 <= resp.status_code < 600
            retry_after_hdr = resp.headers.get("Retry-After")

            if (is_rate or is_5xx) and attempt < MAX_RETRIES:
                attempt += 1

                if retry_after_hdr and retry_after_hdr.isdigit():
                    backoff = float(retry_after_hdr)
                else:
                    backoff = BASE_BACKOFF_SEC * (2 ** (attempt - 1))
                    backoff *= random.uniform(0.7, 1.3)

                print(
                    f"[RETRY] {path}: status={resp.status_code} code={code} subcode={subcode} "
                    f"retry {attempt}/{MAX_RETRIES} in {backoff:.2f}s..."
                )
                time.sleep(backoff)
                continue

            raise FacebookAPIError(
                f"API error on {path}: status={resp.status_code} code={code} subcode={subcode} type={err_type} msg={msg}"
            )

        # ----------------
        # SUCCESS PAYLOAD
        # ----------------
        data = resp.json()

        # If it isn't a standard list response, just return the object
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

        # When following 'next', it already includes query params (including token in most cases).
        # Use next_url directly to avoid param collisions.
        url = next_url
        params = {}

    return all_rows




# ================================================================
# GET DAILY AD ACCOUNT SPEND
# ================================================================
def get_business_adaccounts(
    business_id: str,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    """
    Returns ad accounts connected to a Business Manager.

    Graph edge:
      GET /{business_id}/owned_ad_accounts  (or /adaccounts depending on your setup)

    Notes:
      - 'owned_ad_accounts' is typically the most consistent for BM-owned accounts.
      - If you need all accessible accounts (owned + client), you may want /{business_id}/adaccounts.
    """
    path = f"/{business_id}/owned_ad_accounts"
    params = {
        "fields": "id,name,account_id,account_status,currency,timezone_name",
        "limit": limit,
    }
    return fb_request("GET", path, params=params, paginate=True)

def get_dynamic_date_range(days_back: int, *, end_date: Optional[date] = None) -> Tuple[str, str]:
    """
    Returns (since, until) as ISO date strings in UTC, inclusive.

    Example:
      since, until = get_dynamic_date_range(30)
    """
    if days_back < 0:
        raise ValueError("days_back must be >= 0")

    until_dt = end_date or datetime.now(UTC).date()
    since_dt = until_dt - timedelta(days=days_back)
    return since_dt.isoformat(), until_dt.isoformat()


def get_daily_spend_for_business_adaccounts(
    business_id: str,
    since: Optional[Union[str, date]] = None,
    until: Optional[Union[str, date]] = None,
    *,
    days_back: Optional[int] = None,
    include_zero_rows: bool = False,
    max_pages: int = 50,
) -> pd.DataFrame:
    # ---- Resolve date range ----
    if days_back is not None:
        since_str, until_str = get_dynamic_date_range(days_back)
    else:
        if since is None:
            raise ValueError("Provide either days_back or since (and optionally until).")

        since_str = since if isinstance(since, str) else since.isoformat()
        until_str = since_str if until is None else (until if isinstance(until, str) else until.isoformat())

    # ---- Fetch accounts ----
    accounts = get_business_adaccounts(business_id)

    out_rows: List[Dict[str, Any]] = []

    for acct in accounts:
        acct_id = acct.get("id")  # typically "act_<num>"
        if not acct_id:
            continue

        insights_path = f"/{acct_id}/insights"

        # IMPORTANT: time_range must be JSON string for Graph API query params
        insights_params: Dict[str, Any] = {
            "fields": "spend,account_id,account_name,date_start,date_stop",
            "level": "account",
            "time_increment": 1,
            "time_range": json.dumps({"since": since_str, "until": until_str}, separators=(",", ":")),
        }

        if include_zero_rows:
            insights_params["include_zero_impressions"] = "true"

        rows = fb_request(
            "GET",
            insights_path,
            params=insights_params,
            paginate=True,
            max_pages=max_pages,
        )

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

    adspend = pd.DataFrame(out_rows)
    if not adspend.empty:
        adspend = adspend.sort_values(["date_start", "ad_account_id"]).reset_index(drop=True)

    return adspend



# ================================================================
# BUSINESS → PIXELS (FLATTENED)
# ================================================================

def get_business_pixels(business_id: str) -> pd.DataFrame:
    edges = [
        ("owned_pixels", f"/{business_id}/owned_pixels"),
        ("adspixels", f"/{business_id}/adspixels"),
    ]

    pixels = {}

    fields = [
        "id",
        "name",
        "owner_ad_account",
        "owner_business",
        "creation_time",
        "last_fired_time",
    ]
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
# PIXEL SETTINGS
# ================================================================

PIXEL_SETTINGS_FIELDS = [
    "id",
    "name",
    "last_fired_time",
    "first_party_cookie_status",
    "enable_automatic_matching",
    "automatic_matching_fields",
    "is_crm",
]

def get_pixel_settings(pixel_id: str) -> Dict[str, Any]:
    fields = ",".join(PIXEL_SETTINGS_FIELDS)
    rows = fb_request("GET", f"/{pixel_id}", params={"fields": fields})
    return rows[0] if rows else {}


def pixel_settings_df(pixel_ids: List[str]) -> pd.DataFrame:
    return pd.DataFrame([get_pixel_settings(pid) for pid in pixel_ids if get_pixel_settings(pid)])


# ================================================================
# EVENT VOLUME (PER PIXEL)
# ================================================================

def get_pixel_event_stats_dual_source(pixel_id: str, start_time=None):
    """
    Runs 3 calls to Pixel Stats:
        - event_source=WEB_ONLY
        - event_source=SERVER_ONLY
        - no event_source  (TOTAL)
    Combines results.
    """

    def fetch(es=None):
        qs = []

        if start_time:
            qs.append(f"start_time={start_time}")

        # event_source only if provided
        if es:
            qs.append(f"event_source={es}")

        qs_str = "&".join(qs)
        path = f"/{pixel_id}/stats?{qs_str}"

        # must use params={}
        rows = fb_request("GET", path, params={}, paginate=True)

        # inject event_source label
        for r in rows:
            r["event_source"] = es if es else "TOTAL"

        return rows

    # Run all three
    web_rows = fetch("WEB_ONLY")
    server_rows = fetch("SERVER_ONLY")
    total_rows = fetch(None)

    return web_rows + server_rows + total_rows


def event_volume_df_for_pixel(pixel_id: str, start_time=None):
    """
    Returns a wide-format table with 3 event_count columns:
        event_count_web
        event_count_server
        event_count_total
    """

    # Fetch WEB_ONLY, SERVER_ONLY, TOTAL
    rows = get_pixel_event_stats_dual_source(pixel_id, start_time)
    if not rows:
        return pd.DataFrame()

    long_rows = []

    for r in rows:
        start_t = r.get("start_time")
        src = r.get("event_source")  # WEB_ONLY / SERVER_ONLY / TOTAL

        for ev in r.get("data", []):
            long_rows.append({
                "pixel_id": pixel_id,
                "start_time": start_t,
                "event_name": ev.get("value"),
                "event_source": src,
                "event_count": ev.get("count")
            })

    long_df = pd.DataFrame(long_rows)

    if long_df.empty:
        return long_df

    # ---- Pivot so each event_name has 3 columns for counts ----
    pivot_df = (
        long_df.pivot_table(
            index=["pixel_id", "start_time", "event_name"],
            columns="event_source",
            values="event_count",
            aggfunc="sum",
            fill_value=0
        )
        .reset_index()
    )

    # Normalize column names
    pivot_df = pivot_df.rename(columns={
        "WEB_ONLY": "event_count_web",
        "SERVER_ONLY": "event_count_server",
        "TOTAL": "event_count_total"
    })

    # Ensure all columns exist even if missing
    for col in ["event_count_web", "event_count_server", "event_count_total"]:
        if col not in pivot_df.columns:
            pivot_df[col] = 0

    return pivot_df[
        [
            "pixel_id",
            "start_time",
            "event_name",
            "event_count_web",
            "event_count_server",
            "event_count_total",
        ]
    ]


# ================================================================
# DATASET QUALITY (PIXEL → DATASET)
# ================================================================

def get_pixel_datasets(pixel_id: str) -> List[str]:
    return [pixel_id]   # dataset_id == pixel_id


def get_dataset_quality_for_dataset(dataset_id: str) -> List[Dict[str, Any]]:

    fields = "web{event_name,event_match_quality,event_coverage,event_potential_aly_acr_increase,data_freshness,dedupe_key_feedback,acr}"

    params = {"dataset_id": dataset_id, "fields": fields}
    rows = fb_request("GET", "/dataset_quality", params=params)

    if not rows:
        return []

    root = rows[0]
    web = root.get("web", []) or []

    for w in web:
        w["dataset_id"] = dataset_id

    return web


# ================================================================
# DATASET QUALITY FLATTENERS
# ================================================================

def flatten_dataset_quality(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Flattens dataset quality into a wide BQ-ready table.
    """
    flat = []
    run_date = datetime.utcnow().strftime("%Y-%m-%d")  # <-- ADDED HERE

    for r in rows:
        base = {
            "dataset_id": r.get("dataset_id"),
            "event_name": r.get("event_name"),
            "data_added": run_date,
        }

        # -----------------------------
        # EVENT MATCH QUALITY
        # -----------------------------
        emq = r.get("event_match_quality") or {}
        base["emq_composite_score"] = emq.get("composite_score")

        # match_key_feedback → one col per key
        mkf_list = emq.get("match_key_feedback", [])
        for mk in mkf_list:
            key = mk.get("identifier")
            pct = mk.get("coverage", {}).get("percentage")
            if key:
                col = f"matchkey_{key}_coverage_pct"
                base[col] = pct

        # -----------------------------
        # EVENT COVERAGE (if provided)
        # -----------------------------
        cov = r.get("event_coverage") or {}
        base["coverage_pct"] = cov.get("percentage")
        base["coverage_desc"] = cov.get("description")

        pot_cov = cov.get("potential_aly_acr_increase") or {}
        base["coverage_potential_acr_pct"] = pot_cov.get("percentage")
        base["coverage_potential_acr_desc"] = pot_cov.get("description")

        # -----------------------------
        # EVENT POTENTIAL ACR (non-CAPI events)
        # -----------------------------
        ev_pot = r.get("event_potential_aly_acr_increase") or {}
        base["event_potential_acr_pct"] = ev_pot.get("percentage")
        base["event_potential_acr_desc"] = ev_pot.get("description")

        # -----------------------------
        # DEDUPE KEY FEEDBACK
        # -----------------------------
        dedupe_list = r.get("dedupe_key_feedback", [])
        for d in dedupe_list:
            key = d.get("dedupe_key")
            if not key:
                continue

            prefix = f"dedupe_{key}"

            for sub in [
                "browser_events_with_dedupe_key",
                "server_events_with_dedupe_key",
                "overall_browser_coverage_from_dedupe_key",
            ]:
                sub_obj = d.get(sub, {})
                pct = sub_obj.get("percentage")
                desc = sub_obj.get("description")

                base[f"{prefix}_{sub}_pct"] = pct
                base[f"{prefix}_{sub}_desc"] = desc

        # -----------------------------
        # DATA FRESHNESS
        # -----------------------------
        dfresh = r.get("data_freshness") or {}
        base["freshness_upload_frequency"] = dfresh.get("upload_frequency")
        base["freshness_description"] = dfresh.get("description")

        # -----------------------------
        # ACR (Additional conversions)
        # -----------------------------
        acr = r.get("acr") or {}
        base["acr_pct"] = acr.get("percentage")
        base["acr_desc"] = acr.get("description")

        flat.append(base)

    df = pd.DataFrame(flat)
    return df

def dataset_quality_df_for_pixel(pixel_id: str) -> pd.DataFrame:
    dataset_id = pixel_id  # common mapping

    raw = get_dataset_quality_for_dataset(dataset_id)
    df = flatten_dataset_quality(raw)

    if not df.empty:
        df.insert(0, "pixel_id", pixel_id)

    return df

# ================================================================
# SINGLE PIXEL WORKFLOW
# ================================================================

def analyze_signal_health_for_pixel(
    pixel_id: str,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None
) -> Dict[str, pd.DataFrame]:
    """
    End-to-end signal health extractor for a single pixel.

    Returns dict of:
      - settings: Pixel configuration fields
      - event_volume: Browser / Server / Total counts per event per hour
      - dataset_quality: Flattened EMQ, coverage, ACR, data freshness, dedupe feedback
    """

    results: Dict[str, pd.DataFrame] = {}

    # ------------------------------
    # 1. Pixel Settings
    # ------------------------------
    print(f"[INFO] Fetching settings for pixel {pixel_id}...")
    settings_dict = get_pixel_settings(pixel_id)
    if settings_dict:
        settings_df = pd.DataFrame([settings_dict])
    else:
        settings_df = pd.DataFrame()

    results["pixel_settings"] = settings_df

    # ------------------------------
    # 2. Pixel Event Volume
    # ------------------------------
    print(f"[INFO] Fetching event stats for pixel {pixel_id}...")
    ev_df = event_volume_df_for_pixel(
        pixel_id=pixel_id,
        start_time=start_time
    )

    # Guarantee proper column ordering & presence
    if not ev_df.empty:
        required_cols = [
            "pixel_id", "start_time", "end_time",
            "event_name", "event_count_web", "event_count_server", "event_count_total"
        ]
        for c in required_cols:
            if c not in ev_df.columns:
                ev_df[c] = None
        ev_df = ev_df[required_cols]

    results["event_volume"] = ev_df

    # ------------------------------
    # 3. Dataset Quality
    # ------------------------------
    print(f"[INFO] Fetching dataset quality for dataset_id={pixel_id}...")
    dq_df = dataset_quality_df_for_pixel(pixel_id)

    if not dq_df.empty:
        # Ensure pixel_id & dataset_id present
        if "dataset_id" not in dq_df.columns:
            dq_df["dataset_id"] = pixel_id
        if "pixel_id" not in dq_df.columns:
            dq_df.insert(0, "pixel_id", pixel_id)

    results["dataset_quality"] = dq_df

    # ------------------------------
    # Final Assembly
    # ------------------------------
    print(f"[INFO] Pixel-level signal health extraction complete for pixel {pixel_id}")
    print(f"       Settings rows:        {len(settings_df)}")
    print(f"       Event volume rows:    {len(ev_df)}")
    print(f"       Dataset quality rows: {len(dq_df)}")

    return results



# ================================================================
# BUSINESS-LEVEL WORKFLOW
# ================================================================

def analyze_signal_health_for_business(
    business_id: str,
    since: Optional[Union[str, date]] = None,
    until: Optional[Union[str, date]] = None,
    *,
    days_back: Optional[int] = 7,
    include_zero_rows: bool = False,
    max_pages: int = 50,
    run_pixel_workflow: bool = True,
    pixel_start_time: Optional[str] = None,
    **kwargs,  # <-- add this line

) -> Dict[str, pd.DataFrame]:
    """
    Business-level workflow + daily spend + SINGLE pixel-level extraction (hardcoded pixel id).

    This keeps parity with your current notebook behavior while you validate schemas and BQ loads.
    """
    results: Dict[str, pd.DataFrame] = {
        "pixels": get_business_pixels(business_id),
        "pixel_settings": pd.DataFrame(),
        "pixel_event_volume": pd.DataFrame(),
        "pixel_dataset_quality": pd.DataFrame(),
        "ad_account_daily_spend": pd.DataFrame(),
    }

    # 1) Daily spend across BM ad accounts
    results["ad_account_daily_spend"] = get_daily_spend_for_business_adaccounts(
        business_id=business_id,
        since=since,
        until=until,
        days_back=days_back,
        include_zero_rows=include_zero_rows,
        max_pages=max_pages,
    )

    # 2) Pixel-level extraction for the hardcoded pixel id
    print(f"[INFO] Running pixel-level extraction for pixel_id={HARDCODED_PIXEL_ID}")
    pixel_results = analyze_signal_health_for_pixel(pixel_id=HARDCODED_PIXEL_ID)

    results["pixel_settings"] = pixel_results.get("pixel_settings", pd.DataFrame())
    results["pixel_event_volume"] = pixel_results.get("event_volume", pd.DataFrame())
    results["pixel_dataset_quality"] = pixel_results.get("dataset_quality", pd.DataFrame())

    return results
# ================================================================
# BIGQUERY LOADER
# ================================================================

def load_df_to_bq(df, project_id, dataset_id, table_id, if_exists="append"):
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = (
        bigquery.WriteDisposition.WRITE_APPEND if if_exists == "append"
        else bigquery.WriteDisposition.WRITE_TRUNCATE
        if if_exists == "replace"
        else bigquery.WriteDisposition.WRITE_EMPTY
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"Loaded → {project_id}.{dataset_id}.{table_id}")


def load_signal_health_to_bq(results, project_id, dataset_id, if_exists="append"):
    for name, df in results.items():
        if df.empty:
            print(f"[SKIP] {name} empty")
            continue

        table = name.replace("-", "_")
        load_df_to_bq(df, project_id, dataset_id, table, if_exists)
