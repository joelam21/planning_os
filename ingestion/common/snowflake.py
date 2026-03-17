from __future__ import annotations

import snowflake.connector


def connect(config: dict[str, str]):
    """
    Open a Snowflake connection.
    """
    return snowflake.connector.connect(
        account=config["account"],
        user=config["user"],
        password=config["password"],
        warehouse=config["warehouse"],
        database=config["database"],
        schema=config["schema"],
        role=config["role"],
    )


def create_sample_table(conn, schema: str) -> None:
    """
    Create a very small raw ingestion table for proving the pipeline works.
    """
    sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.RAW_INGESTION_SAMPLE (
        source_id VARCHAR,
        source_name VARCHAR,
        loaded_at TIMESTAMP_NTZ
    )
    """
    with conn.cursor() as cur:
        cur.execute(sql)


def insert_sample_rows(conn, schema: str, rows: list[dict]) -> None:
    """
    Insert sample rows into the raw ingestion table.
    """
    sql = f"""
    INSERT INTO {schema}.RAW_INGESTION_SAMPLE (source_id, source_name, loaded_at)
    VALUES (%s, %s, %s)
    """

    values = [(row["source_id"], row["source_name"], row["loaded_at"]) for row in rows]

    with conn.cursor() as cur:
        cur.executemany(sql, values)

    conn.commit()