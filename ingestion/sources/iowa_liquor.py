import requests
from datetime import datetime, UTC


API_URL = "https://data.iowa.gov/resource/m3tr-qhgy.json"
DEFAULT_BATCH_SIZE = 500
DEFAULT_MAX_BATCHES = 50


def fetch_rows(
    start_date: str | None = None,
    end_date: str | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    max_batches: int = DEFAULT_MAX_BATCHES,
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

    all_rows: list[dict] = []
    offset = 0
    batch_count = 0
    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

    while batch_count < max_batches:
        params = {
            "$limit": batch_size,
            "$offset": offset,
        }

        if where_clauses:
            params["$where"] = " AND ".join(where_clauses)

        response = requests.get(API_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if not data:
            break

        for record in data:
            all_rows.append(
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

        if len(data) < batch_size:
            break

        offset += batch_size
        batch_count += 1

    return all_rows