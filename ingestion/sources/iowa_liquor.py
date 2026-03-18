import requests
from datetime import datetime, UTC


API_URL = "https://data.iowa.gov/resource/m3tr-qhgy.json"


def fetch_rows() -> list[dict]:
    """
    Fetch a small sample of Iowa liquor data and normalize it
    into the ingestion contract expected by the runner.
    """

    params = {
        "$limit": 5  # keep it small for now
    }

    response = requests.get(API_URL, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()

    rows = []
    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

    for record in data:
        rows.append(
            {
                # pick stable identifiers from API
                "source_id": record.get("invoice_line_no", "unknown"),
                "source_name": record.get("store_name", "unknown"),
                "loaded_at": now,
            }
        )

    return rows