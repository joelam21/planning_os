import os
from pathlib import Path


def load_env_file(env_path: str = ".env") -> None:
    """
    Minimal .env loader for local development.
    Loads KEY=VALUE pairs into os.environ if not already set.
    """
    env_file = Path(env_path)
    if not env_file.exists():
        return

    for raw_line in env_file.read_text().splitlines():
        line = raw_line.strip()

        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")

        if key and key not in os.environ:
            os.environ[key] = value


def get_snowflake_config() -> dict[str, str]:
    """
    Return the minimal Snowflake config needed for ingestion.
    """
    required = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
    }

    missing = [key for key, value in required.items() if not value]
    if missing:
        raise ValueError(
            f"Missing required Snowflake environment variables: {', '.join(missing)}"
        )

    return required