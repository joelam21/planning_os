import argparse
from ingestion.common.config import load_env_file, get_snowflake_config
from ingestion.common.snowflake import (
    connect,
    create_sample_table,
    insert_sample_rows,
)
from ingestion.sources.sample import fetch_rows as fetch_sample_rows


def get_rows_for_source(
    source: str,
    start_date: str | None = None,
    end_date: str | None = None,
    batch_size: int | None = None,
    max_batches: int | None = None,
) -> list[dict]:
    if source == "sample":
        return fetch_sample_rows()

    if source == "iowa_liquor":
        from ingestion.sources.iowa_liquor import fetch_rows as fetch_iowa_rows

        kwargs = {"start_date": start_date, "end_date": end_date}
        if batch_size is not None:
            kwargs["batch_size"] = batch_size
        if max_batches is not None:
            kwargs["max_batches"] = max_batches

        return fetch_iowa_rows(**kwargs)

    raise ValueError(f"Unsupported source: {source}")


def get_table_name_for_source(source: str) -> str:
    if source == "sample":
        return "RAW_INGESTION_SAMPLE"

    if source == "iowa_liquor":
        return "RAW_IOWA_LIQUOR"

    raise ValueError(f"Unsupported source: {source}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run ingestion for a selected source")
    parser.add_argument(
        "--source",
        default="sample",
        help="Source to ingest from (default: sample)",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Rows to request per API batch",
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        default=None,
        help="Maximum number of API batches to fetch",
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
        table_name = get_table_name_for_source(args.source)
        create_sample_table(conn, schema, table_name)
        print(
            f"[ingestion] Ensured table exists: {config['database']}.{schema}.{table_name}"
        )

        rows = get_rows_for_source(
            args.source,
            start_date=args.start_date,
            end_date=args.end_date,
            batch_size=args.batch_size,
            max_batches=args.max_batches,
        )
        insert_sample_rows(conn, schema, table_name, rows)
        print(f"[ingestion] Inserted {len(rows)} rows for source={args.source}")

    finally:
        if conn is not None:
            conn.close()
            print("[ingestion] Snowflake connection closed")


if __name__ == "__main__":
    main()