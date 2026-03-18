from ingestion.common.config import load_env_file, get_snowflake_config
from ingestion.common.snowflake import (
    connect,
    create_sample_table,
    insert_sample_rows,
)


def main() -> None:
    print("[ingestion] Starting ingestion step")
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

        rows = [
            {"source_id": "1", "source_name": "sample_alpha"},
            {"source_id": "2", "source_name": "sample_beta"},
        ]
        insert_sample_rows(conn, schema, rows)
        print(f"[ingestion] Inserted {len(rows)} sample rows")

    finally:
        if conn is not None:
            conn.close()
            print("[ingestion] Snowflake connection closed")


if __name__ == "__main__":
    main()