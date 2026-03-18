from __future__ import annotations

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


def create_sample_table(conn, schema: str, table_name: str) -> None:
    if table_name == "RAW_IOWA_LIQUOR":
        sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
            invoice_item_number VARCHAR,
            date DATE,
            store_number VARCHAR,
            store_name VARCHAR,
            address VARCHAR,
            city VARCHAR,
            zip_code VARCHAR,
            store_location VARCHAR,
            county_number VARCHAR,
            county VARCHAR,
            category VARCHAR,
            category_name VARCHAR,
            vendor_number VARCHAR,
            vendor_name VARCHAR,
            item_number VARCHAR,
            item_description VARCHAR,
            pack NUMBER,
            bottle_volume_ml NUMBER,
            state_bottle_cost NUMBER(12,2),
            state_bottle_retail NUMBER(12,2),
            bottles_sold NUMBER,
            sale_dollars NUMBER(12,2),
            volume_sold_liters NUMBER(12,4),
            volume_sold_gallons NUMBER(12,4),
            loaded_at TIMESTAMP_NTZ
        )
        """
    else:
        sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
            source_id VARCHAR,
            source_name VARCHAR,
            loaded_at TIMESTAMP_NTZ
        )
        """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def delete_iowa_liquor_date_range(
    conn,
    schema: str,
    table_name: str,
    start_date: str | None,
    end_date: str | None,
) -> None:
    if table_name != "RAW_IOWA_LIQUOR":
        return

    if not start_date and not end_date:
        return

    where_clauses = []
    params: list[str] = []

    if start_date:
        where_clauses.append("date >= %s")
        params.append(start_date)

    if end_date:
        where_clauses.append("date <= %s")
        params.append(end_date)

    if not where_clauses:
        return

    sql = f"""
    DELETE FROM {schema}.{table_name}
    WHERE {" AND ".join(where_clauses)}
    """

    with conn.cursor() as cur:
        cur.execute(sql, params)

    conn.commit()


def insert_sample_rows(conn, schema: str, table_name: str, rows: list[dict]) -> None:
    if table_name == "RAW_IOWA_LIQUOR":
        sql = f"""
        INSERT INTO {schema}.{table_name} (
            invoice_item_number,
            date,
            store_number,
            store_name,
            address,
            city,
            zip_code,
            store_location,
            county_number,
            county,
            category,
            category_name,
            vendor_number,
            vendor_name,
            item_number,
            item_description,
            pack,
            bottle_volume_ml,
            state_bottle_cost,
            state_bottle_retail,
            bottles_sold,
            sale_dollars,
            volume_sold_liters,
            volume_sold_gallons,
            loaded_at
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        )
        """

        values = [
            (
                row.get("invoice_item_number"),
                row.get("date"),
                row.get("store_number"),
                row.get("store_name"),
                row.get("address"),
                row.get("city"),
                row.get("zip_code"),
                row.get("store_location"),
                row.get("county_number"),
                row.get("county"),
                row.get("category"),
                row.get("category_name"),
                row.get("vendor_number"),
                row.get("vendor_name"),
                row.get("item_number"),
                row.get("item_description"),
                row.get("pack"),
                row.get("bottle_volume_ml"),
                row.get("state_bottle_cost"),
                row.get("state_bottle_retail"),
                row.get("bottles_sold"),
                row.get("sale_dollars"),
                row.get("volume_sold_liters"),
                row.get("volume_sold_gallons"),
                row.get("loaded_at"),
            )
            for row in rows
        ]
    else:
        sql = f"""
        INSERT INTO {schema}.{table_name} (
            source_id,
            source_name,
            loaded_at
        )
        VALUES (%s, %s, %s)
        """

        values = [(row["source_id"], row["source_name"], row["loaded_at"]) for row in rows]

    with conn.cursor() as cur:
        cur.executemany(sql, values)
    conn.commit()