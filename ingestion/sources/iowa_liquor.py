import os
import time
import requests
from datetime import datetime, UTC


API_URL = "https://data.iowa.gov/resource/m3tr-qhgy.json"
DEFAULT_BATCH_SIZE = 500
DEFAULT_MAX_BATCHES = 50
DEFAULT_REQUEST_TIMEOUT_SECONDS = 30
DEFAULT_REQUEST_MAX_RETRIES = 4
DEFAULT_REQUEST_BACKOFF_BASE_SECONDS = 2


def fetch_rows(
    start_date: str | None = None,
    end_date: str | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    max_batches: int = DEFAULT_MAX_BATCHES,
    on_batch=None,
) -> list[dict]:
    """
    Fetch Iowa liquor data in batches and normalize it into
    the RAW_IOWA_LIQUOR table shape expected by the ingestion runner.

    Optionally filter the API request by start and end date and control pagination.
    """
    where_clauses: list[str] = []
    if start_date:
        where_clauses.append(f"date >= '{start_date}T00:00:00'")
    if end_date:
        where_clauses.append(f"date <= '{end_date}T23:59:59'")

    app_token = os.getenv("SOCRATA_APP_TOKEN")
    if app_token:
        print("[iowa_liquor] Using Socrata app token")
    else:
        print("[iowa_liquor] No Socrata app token found")

    all_rows: list[dict] = []
    offset = 0
    batch_count = 0
    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    request_timeout_seconds = int(
        os.getenv("PLANNING_OS_IOWA_REQUEST_TIMEOUT_SECONDS", str(DEFAULT_REQUEST_TIMEOUT_SECONDS))
    )
    request_max_retries = int(
        os.getenv("PLANNING_OS_IOWA_REQUEST_MAX_RETRIES", str(DEFAULT_REQUEST_MAX_RETRIES))
    )
    request_backoff_base_seconds = int(
        os.getenv(
            "PLANNING_OS_IOWA_REQUEST_BACKOFF_BASE_SECONDS",
            str(DEFAULT_REQUEST_BACKOFF_BASE_SECONDS),
        )
    )

    while batch_count < max_batches:
        params = {
            "$limit": batch_size,
            "$offset": offset,
        }
        if app_token:
            params["$$app_token"] = app_token

        if where_clauses:
            params["$where"] = " AND ".join(where_clauses)

        data: list[dict] | None = None
        for request_attempt in range(1, request_max_retries + 1):
            try:
                response = requests.get(API_URL, params=params, timeout=request_timeout_seconds)
                response.raise_for_status()
                data = response.json()
                break
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
                if request_attempt == request_max_retries:
                    raise

                backoff_seconds = request_backoff_base_seconds * (2 ** (request_attempt - 1))
                print(
                    "[iowa_liquor] transient network error "
                    f"(attempt {request_attempt}/{request_max_retries}, "
                    f"offset={offset}, limit={batch_size}): {exc}. "
                    f"retrying in {backoff_seconds}s"
                )
                time.sleep(backoff_seconds)
            except requests.exceptions.HTTPError:
                status_code = response.status_code
                if status_code < 500 or request_attempt == request_max_retries:
                    raise

                backoff_seconds = request_backoff_base_seconds * (2 ** (request_attempt - 1))
                print(
                    "[iowa_liquor] upstream 5xx error "
                    f"(attempt {request_attempt}/{request_max_retries}, "
                    f"offset={offset}, limit={batch_size}, status={status_code}). "
                    f"retrying in {backoff_seconds}s"
                )
                time.sleep(backoff_seconds)

        if data is None:
            raise RuntimeError("Iowa API request failed without response payload")

        if not data:
            break

        batch_rows: list[dict] = []
        for record in data:
            batch_rows.append(
                {
                    "invoice_item_number": record.get("invoice_line_no"),
                    "date": record.get("date", "")[:10] if record.get("date") else None,
                    "store_number": record.get("store"),
                    "store_name": record.get("name"),
                    "address": record.get("address"),
                    "city": record.get("city"),
                    "zip_code": record.get("zipcode"),
                    "store_location": (
                        str(record.get("store_location"))
                        if record.get("store_location") is not None
                        else None
                    ),
                    "county_number": record.get("county_number"),
                    "county": record.get("county"),
                    "category": record.get("category"),
                    "category_name": record.get("category_name"),
                    "vendor_number": record.get("vendor_no"),
                    "vendor_name": record.get("vendor_name"),
                    "item_number": record.get("itemno"),
                    "item_description": record.get("im_desc"),
                    "pack": record.get("pack"),
                    "bottle_volume_ml": record.get("bottle_volume_ml"),
                    "state_bottle_cost": record.get("state_bottle_cost"),
                    "state_bottle_retail": record.get("state_bottle_retail"),
                    "bottles_sold": record.get("sale_bottles"),
                    "sale_dollars": record.get("sale_dollars"),
                    "volume_sold_liters": record.get("sale_liters"),
                    "volume_sold_gallons": record.get("sale_gallons"),
                    "loaded_at": now,
                }
            )

        if on_batch is not None:
            on_batch(batch_rows)
        else:
            all_rows.extend(batch_rows)

        if len(data) < batch_size:
            break

        offset += batch_size
        batch_count += 1

    if on_batch is not None:
        return []

    return all_rows