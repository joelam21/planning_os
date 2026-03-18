from datetime import datetime, UTC


def fetch_rows() -> list[dict]:
    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

    return [
        {"source_id": "10", "source_name": "sample_alpha", "loaded_at": now},
        {"source_id": "20", "source_name": "sample_beta", "loaded_at": now},
    ]