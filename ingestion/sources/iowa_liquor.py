from datetime import datetime, UTC


def fetch_rows() -> list[dict]:
    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

    return [
        {
            "source_id": "iowa_1",
            "source_name": "iowa_liquor_mock",
            "loaded_at": now,
        }
    ]