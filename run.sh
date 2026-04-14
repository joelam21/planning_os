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
ACTIVE_PYTHON="python"
ACTIVE_SNOW="snow"
ACTIVE_DBT="dbt"
RUNTIME_MODE="unknown"

resolve_runtime() {
    if [ "${VIRTUAL_ENV:-}" = "$EXPECTED_VENV" ]; then
        RUNTIME_MODE="project_venv"
        ACTIVE_PYTHON="$PROJECT_PYTHON"
        ACTIVE_SNOW="$PROJECT_SNOW"
        ACTIVE_DBT="$PROJECT_DBT"
        return 0
    fi

    if command -v python >/dev/null 2>&1 && command -v dbt >/dev/null 2>&1; then
        RUNTIME_MODE="ambient"
        ACTIVE_PYTHON="$(command -v python)"
        ACTIVE_DBT="$(command -v dbt)"

        if command -v snow >/dev/null 2>&1; then
            ACTIVE_SNOW="$(command -v snow)"
        else
            ACTIVE_SNOW=""
        fi
        return 0
    fi

    echo "[run] No usable runtime found."
    echo "[run] Expected local venv: $EXPECTED_VENV"
    echo "[run] Current VIRTUAL_ENV: ${VIRTUAL_ENV:-<none>}"
    echo "[run] Local use: source ./enter.sh"
    echo "[run] Orchestrated use requires python and dbt on PATH."
    exit 1
}

require_command() {
    local binary_path="$1"
    local command_name="$2"

    if [ -z "$binary_path" ] || ! command -v "$binary_path" >/dev/null 2>&1; then
        echo "[run] Required command not available: ${command_name}"
        echo "[run] Runtime mode: ${RUNTIME_MODE}"
        exit 1
    fi
}

require_runtime_for_command() {
    local requested_command="$1"

    case "$requested_command" in
        help)
            ;;
        doctor)
            require_command "$ACTIVE_PYTHON" "python"
            ;;
        snow-test|snow-sql-test)
            require_command "$ACTIVE_SNOW" "snow"
            ;;
        dbt-debug|transform|test)
            require_command "$ACTIVE_DBT" "dbt"
            ;;
        ingest|pipeline)
            require_command "$ACTIVE_PYTHON" "python"
            require_command "$ACTIVE_DBT" "dbt"
            ;;
        *)
            require_command "$ACTIVE_PYTHON" "python"
            ;;
    esac
}

print_pipeline_success() {
    echo "[run] PIPELINE SUCCEEDED"
    if [ -n "${PIPELINE_INSERTED_ROWS:-}" ]; then
        echo "[run] Records loaded: ${PIPELINE_INSERTED_ROWS}"
    fi
}

print_pipeline_failure() {
    local failed_step="$1"
    local exit_code="$2"
    echo "[run] PIPELINE FAILED during: ${failed_step}"
    if [ -n "${PIPELINE_INSERTED_ROWS:-}" ]; then
        echo "[run] Records loaded before failure: ${PIPELINE_INSERTED_ROWS}"
    fi
    return "$exit_code"
}

run_pipeline_command() {
    local step_name="$1"
    shift
    "$@"
    local exit_code=$?
    if [ "$exit_code" -ne 0 ]; then
        print_pipeline_failure "$step_name" "$exit_code"
        exit "$exit_code"
    fi
}

COMMAND="${1:-help}"
PIPELINE_INSERTED_ROWS=""

resolve_runtime
require_runtime_for_command "$COMMAND"

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
        echo "  ./run.sh pipeline      # ingest -> snapshot -> transform -> test"
        echo ""
        echo "[run] Runtime mode: local .venv or any orchestrated environment with python/dbt on PATH"
        echo ""
        ;;

    doctor)
        echo "[run] Running environment doctor"
        bash doctor.sh
        ;;

    snow-test)
        echo "[run] Listing Snowflake connections (CLI)"
        "$ACTIVE_SNOW" connection list
        ;;

    snow-sql-test)
        # Use SNOW_CONNECTION if set, otherwise prefer "my_snowflake" (your current default)
        SNOW_CONNECTION="${SNOW_CONNECTION:-my_snowflake}"
        echo "[run] Running Snowflake SQL health check using connection: ${SNOW_CONNECTION}"
        "$ACTIVE_SNOW" sql -c "${SNOW_CONNECTION}" -q "select current_user(), current_role(), current_warehouse();"
        ;;

    dbt-debug)
        echo "[run] Running dbt debug"
        "$ACTIVE_DBT" debug --project-dir "$PROJECT_ROOT"
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
            "$ACTIVE_PYTHON" -m ingestion.run_ingestion
        elif [ -f "./ingestion/scripts/run.py" ]; then
            "$ACTIVE_PYTHON" ./ingestion/scripts/run.py
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
        "$ACTIVE_DBT" run --project-dir "$PROJECT_ROOT"
        ;;

    test)
        echo "[run] Running dbt tests"
        "$ACTIVE_DBT" test --project-dir "$PROJECT_ROOT"
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
        INGEST_CMD=("$ACTIVE_PYTHON" -m ingestion.run_ingestion --source "$PIPELINE_SOURCE")

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

        INGEST_OUTPUT="$("${INGEST_CMD[@]}" 2>&1)" || {
            exit_code=$?
            printf '%s\n' "$INGEST_OUTPUT"
            print_pipeline_failure "ingestion" "$exit_code"
            exit "$exit_code"
        }
        printf '%s\n' "$INGEST_OUTPUT"
        PIPELINE_INSERTED_ROWS="$(printf '%s\n' "$INGEST_OUTPUT" | sed -n 's/.*Inserted \([0-9][0-9]*\) rows.*/\1/p' | tail -n 1)"

        echo "[run] Running snapshots"
        run_pipeline_command "snapshots" "$ACTIVE_DBT" snapshot --project-dir "$PROJECT_ROOT"

        echo "[run] Running dbt models (transform)"
        run_pipeline_command "transform" "$ACTIVE_DBT" run --project-dir "$PROJECT_ROOT"

        echo "[run] Running dbt tests"
        run_pipeline_command "test" "$ACTIVE_DBT" test --project-dir "$PROJECT_ROOT"
        print_pipeline_success
        ;;

        *)
            echo "Unknown command: $COMMAND"
            echo "Run './run.sh help' to see available commands."
            exit 1
            ;;

esac
