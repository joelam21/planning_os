from ingestion.common.config import load_env_file, get_snowflake_config
from ingestion.common.snowflake import connect


def main() -> None:
    print("[ingestion] Starting ingestion step")
    load_env_file(".env")
    print("[ingestion] Loaded .env")

    config = get_snowflake_config()
    print(f"[ingestion] Loaded config for database={config['database']} schema={config['schema']}")

    conn = None
    try:
        conn = connect(config)
        print("[ingestion] Connected to Snowflake")
    finally:
        if conn is not None:
            conn.close()
            print("[ingestion] Snowflake connection closed")


if __name__ == "__main__":
    main()