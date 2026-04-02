from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine


def get_project_root(start: str | Path | None = None) -> Path:
    """Return repository root by walking up until dbt_project.yml is found."""
    current = Path(start or Path.cwd()).resolve()
    for candidate in [current, *current.parents]:
        if (candidate / "dbt_project.yml").exists():
            return candidate
    raise FileNotFoundError("Could not locate project root (dbt_project.yml not found)")


def get_engine_from_env() -> Any:
    """Create a Snowflake SQLAlchemy engine from .env/dbt env vars."""
    load_dotenv()
    user = os.getenv("DBT_USER")
    password = os.getenv("DBT_PASSWORD")
    account = os.getenv("DBT_ACCOUNT")
    warehouse = os.getenv("DBT_WAREHOUSE")
    database = os.getenv("DBT_DATABASE")
    schema = os.getenv("DBT_DEV_SCHEMA")

    missing = [
        name
        for name, value in {
            "DBT_USER": user,
            "DBT_PASSWORD": password,
            "DBT_ACCOUNT": account,
            "DBT_WAREHOUSE": warehouse,
            "DBT_DATABASE": database,
            "DBT_DEV_SCHEMA": schema,
        }.items()
        if not value
    ]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    return create_engine(
        f"snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}"
    )


def read_sql(project_root: Path, relative_path: str) -> str:
    sql_path = project_root / relative_path
    return sql_path.read_text(encoding="utf-8")


def render_sql(sql_template: str, **params: Any) -> str:
    """Render SQL template with python format placeholders like {month_start}."""
    return sql_template.format(**params)


def run_sql(engine: Any, sql: str) -> pd.DataFrame:
    return pd.read_sql(sql, engine)


def csv_filter(values: str | list[str] | None) -> str:
    """Normalize filter input for SQL params. Returns 'ALL' or comma-separated values."""
    if values is None:
        return "ALL"
    if isinstance(values, str):
        cleaned = values.strip()
        return cleaned if cleaned else "ALL"

    cleaned_values = [str(v).strip() for v in values if str(v).strip()]
    return ",".join(cleaned_values) if cleaned_values else "ALL"


def trailing_window(month_start: str, years: int = 3) -> dict[str, str]:
    """Return a trailing annual window ending at month_start.

    Example:
    month_start='2024-12-01', years=3 -> {
        'window_start': '2022-01-01',
        'window_end': '2024-12-31',
    }
    """
    anchor = pd.to_datetime(month_start)
    start = (anchor - pd.DateOffset(years=years - 1)).replace(month=1, day=1)
    end = anchor.replace(month=12, day=31)
    return {
        "window_start": start.strftime("%Y-%m-%d"),
        "window_end": end.strftime("%Y-%m-%d"),
    }
