from __future__ import annotations

from datetime import datetime, UTC

import snowflake.connector


def connect(config: dict[str, str]):
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
    sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.RAW_INGESTION_SAMPLE (
        source_id VARCHAR,
        source_name VARCHAR,
        loaded_at TIMESTAMP_NTZ
    )
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def insert_sample_rows(conn, schema: str, rows: list[dict]) -> None:
    sql = f"""
    INSERT INTO {schema}.RAW_INGESTION_SAMPLE (
        source_id,
        source_name,
        loaded_at
    )
    VALUES (%s, %s, %s)
    """

    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    values = [(row["source_id"], row["source_name"], now) for row in rows]

    with conn.cursor() as cur:
        cur.executemany(sql, values)
    conn.commit()