import argparse
from ingestion.common.config import load_env_file, get_snowflake_config
from ingestion.common.snowflake import (
    connect,
    create_sample_table,
    insert_sample_rows,
)
from ingestion.sources.sample import fetch_rows as fetch_sample_rows


def get_rows_for_source(source: str) -> list[dict]:
    if source == "sample":
        return fetch_sample_rows()

    if source == "iowa_liquor":
        from ingestion.sources.iowa_liquor import fetch_rows as fetch_iowa_rows

        return fetch_iowa_rows()

    raise ValueError(f"Unsupported source: {source}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run ingestion for a selected source")
    parser.add_argument(
        "--source",
        default="sample",
        help="Source to ingest from (default: sample)",
    )
    args = parser.parse_args()

    print(f"[ingestion] Starting ingestion step for source={args.source}")
    load_env_file(".env")
    print("[ingestion] Loaded .env")

    config = get_snowflake_config()
    print(
        f"[ingestion] Loaded config for database={config['database']} schema={config['schema']}"
    )

    conn = None
    try:
        conn = connect(config)
        print("[ingestion] Connected to Snowflake")

        schema = config["schema"]
        create_sample_table(conn, schema)
        print(
            f"[ingestion] Ensured table exists: {config['database']}.{schema}.RAW_INGESTION_SAMPLE"
        )

        rows = get_rows_for_source(args.source)
        insert_sample_rows(conn, schema, rows)
        print(f"[ingestion] Inserted {len(rows)} rows for source={args.source}")

    finally:
        if conn is not None:
            conn.close()
            print("[ingestion] Snowflake connection closed")


if __name__ == "__main__":
    main()