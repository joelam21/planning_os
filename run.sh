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
PROJECT_PYTHON="$EXPECTED_VENV/bin/python"
PROJECT_SNOW="$EXPECTED_VENV/bin/snow"
PROJECT_DBT="$EXPECTED_VENV/bin/dbt"

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
        echo "  ./run.sh help          # Show commands"
        echo "  ./run.sh doctor        # Run environment health check"
        echo "  ./run.sh snow-test     # List Snowflake CLI connections"
        echo "  ./run.sh snow-sql-test # Run a simple SQL health check"
        echo "  ./run.sh dbt-debug     # Run dbt debug"
        echo "  ./run.sh ingest        # Run ingestion step (project-local)"
        echo "  ./run.sh transform     # Run dbt models (dbt run)"
        echo "  ./run.sh test          # Run dbt tests (dbt test)"
        echo "  ./run.sh pipeline      # ingest -> transform -> test"
        echo ""
        ;;

    doctor)
        echo "[run] Running environment doctor"
        bash doctor.sh
        ;;

    snow-test)
        echo "[run] Listing Snowflake connections (CLI)"
        "$PROJECT_SNOW" connection list
        ;;

    snow-sql-test)
        # Use SNOW_CONNECTION if set, otherwise prefer "my_snowflake" (your current default)
        SNOW_CONNECTION="${SNOW_CONNECTION:-my_snowflake}"
        echo "[run] Running Snowflake SQL health check using connection: ${SNOW_CONNECTION}"
        "$PROJECT_SNOW" sql -c "${SNOW_CONNECTION}" -q "select current_user(), current_role(), current_warehouse();"
        ;;

    dbt-debug)
        echo "[run] Running dbt debug"
        "$PROJECT_DBT" debug
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
            "$PROJECT_PYTHON" -m ingestion.run_ingestion
        elif [ -f "./ingestion/scripts/run.py" ]; then
            "$PROJECT_PYTHON" ./ingestion/scripts/run.py
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
        "$PROJECT_DBT" run
        ;;

    test)
        echo "[run] Running dbt tests"
        "$PROJECT_DBT" test
        ;;

    pipeline)
        # Optional args for ingestion window
        PIPELINE_SOURCE="iowa_liquor"
        PIPELINE_START_DATE=""
        PIPELINE_END_DATE=""
        PIPELINE_BATCH_SIZE=""
        PIPELINE_MAX_BATCHES=""

        # Parse args passed after "pipeline"
        shift
        while [[ $# -gt 0 ]]; do
            case "$1" in
                --source)
                    PIPELINE_SOURCE="${2:-}"
                    shift 2
                    ;;
                --start-date)
                    PIPELINE_START_DATE="${2:-}"
                    shift 2
                    ;;
                --end-date)
                    PIPELINE_END_DATE="${2:-}"
                    shift 2
                    ;;
                --batch-size)
                    PIPELINE_BATCH_SIZE="${2:-}"
                    shift 2
                    ;;
                --max-batches)
                    PIPELINE_MAX_BATCHES="${2:-}"
                    shift 2
                    ;;
                *)
                    echo "[run] Unknown pipeline argument: $1"
                    echo "[run] Usage: ./run.sh pipeline [--source <name>] [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD] [--batch-size N] [--max-batches N]"
                    exit 2
                    ;;
            esac
        done

        echo "[run] Running full pipeline: ingest -> snapshot -> transform -> test"

        echo "[run] Running ingestion step"
        INGEST_CMD=("$PROJECT_PYTHON" -m ingestion.run_ingestion --source "$PIPELINE_SOURCE")

        if [[ -n "$PIPELINE_START_DATE" ]]; then
            INGEST_CMD+=(--start-date "$PIPELINE_START_DATE")
        fi
        if [[ -n "$PIPELINE_END_DATE" ]]; then
            INGEST_CMD+=(--end-date "$PIPELINE_END_DATE")
        fi
        if [[ -n "$PIPELINE_BATCH_SIZE" ]]; then
            INGEST_CMD+=(--batch-size "$PIPELINE_BATCH_SIZE")
        fi
        if [[ -n "$PIPELINE_MAX_BATCHES" ]]; then
            INGEST_CMD+=(--max-batches "$PIPELINE_MAX_BATCHES")
        fi

        "${INGEST_CMD[@]}"

        echo "[run] Running snapshots"
        "$PROJECT_DBT" snapshot

        echo "[run] Running dbt models (transform)"
        "$PROJECT_DBT" run

        echo "[run] Running dbt tests"
        "$PROJECT_DBT" test
        ;;

        *)
            echo "Unknown command: $COMMAND"
            echo "Run './run.sh help' to see available commands."
            exit 1
            ;;

esac
