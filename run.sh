#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# run.sh
# Purpose: Unified entry point for project workflows.
# This script standardizes how common tasks are executed.
# ============================================================

PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_ROOT"
EXPECTED_VENV="$PROJECT_ROOT/.venv"

COMMAND="${1:-help}"

require_project_venv() {
    if [ "${VIRTUAL_ENV:-}" != "$EXPECTED_VENV" ]; then
        echo "[run] Project virtual environment is not active."
        echo "[run] Expected: $EXPECTED_VENV"
        echo "[run] Current:  ${VIRTUAL_ENV:-<none>}"
        echo "[run] Run: source ./enter.sh"
        exit 1
    fi
}

require_project_venv

case "$COMMAND" in

    help)
        echo ""
        echo "Available commands:"
        echo ""
        echo "  ./scripts/run.sh help          # Show commands"
        echo "  ./scripts/run.sh doctor        # Run environment health check"
        echo "  ./scripts/run.sh snow-test     # List Snowflake CLI connections"
        echo "  ./scripts/run.sh snow-sql-test # Run a simple SQL health check"
        echo "  ./scripts/run.sh ingest        # Run ingestion step (project-local)"
        echo "  ./scripts/run.sh transform     # Run dbt models (dbt run)"
        echo "  ./scripts/run.sh test          # Run dbt tests (dbt test)"
        echo "  ./scripts/run.sh pipeline      # ingest -> transform -> test"
        echo ""
        ;;

    doctor)
        echo "[run] Running environment doctor"
        bash doctor.sh
        ;;

    snow-test)
        echo "[run] Listing Snowflake connections (CLI)"
        .venv/bin/snow connection list
        ;;

    snow-sql-test)
        # Use SNOW_CONNECTION if set, otherwise prefer "my_snowflake" (your current default)
        SNOW_CONNECTION="${SNOW_CONNECTION:-my_snowflake}"
        echo "[run] Running Snowflake SQL health check using connection: ${SNOW_CONNECTION}"
        .venv/bin/snow sql -c "${SNOW_CONNECTION}" -q "select current_user(), current_role(), current_warehouse();"
        ;;

    dbt-debug)
        echo "[run] Running dbt debug"
        dbt debug
        ;;

    ingest)
        echo "[run] Running ingestion step"
        # Convention: prefer a project-local ingestion script if present.
        # You can implement one of these later:
        #   - ./ingestion/run_ingestion.sh
        #   - ./ingestion/run_ingestion.py
        #   - ./ingestion/scripts/run.py
        if [ -x "./ingestion/run_ingestion.sh" ]; then
            ./ingestion/run_ingestion.sh
        elif [ -f "./ingestion/run_ingestion.py" ]; then
            python ./ingestion/run_ingestion.py
        elif [ -f "./ingestion/scripts/run.py" ]; then
            python ./ingestion/scripts/run.py
        else
            echo "[run] No ingestion runner found. Create one of:"
            echo "  ./ingestion/run_ingestion.sh (preferred)"
            echo "  ./ingestion/run_ingestion.py"
            echo "  ./ingestion/scripts/run.py"
            exit 2
        fi
        ;;

    transform)
        echo "[run] Running dbt models (transform)"
        dbt run
        ;;

    test)
        echo "[run] Running dbt tests"
        dbt test
        ;;

    pipeline)
        echo "[run] Running full pipeline: ingest -> transform -> test"
        "$0" ingest
        "$0" transform
        "$0" test
        ;;

    *)
        echo "Unknown command: $COMMAND"
        echo "Run './scripts/run.sh help' to see available commands."
        exit 1
        ;;

esac
