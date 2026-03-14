#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"
EXPECTED_VENV="$PROJECT_ROOT/.venv"

require_project_venv() {
  if [ "${VIRTUAL_ENV:-}" != "$EXPECTED_VENV" ]; then
    echo "====================================="
    echo "Project Preflight Check"
    echo "====================================="
    echo "Repo root: $PROJECT_ROOT"
    echo ""
    echo "✘ Project virtual environment is not active"
    echo "  expected: $EXPECTED_VENV"
    echo "  current:  ${VIRTUAL_ENV:-<none>}"
    echo "  run: source ./enter.sh"
    exit 1
  fi
}
require_project_venv

echo "====================================="
echo "Project Preflight Check"
echo "====================================="
echo "Repo root: $PROJECT_ROOT"
echo ""

failures=0
warnings=0

check_file() {
  local path="$1"
  local label="$2"

  if [ -f "$path" ]; then
    echo "✔ $label: $path"
  else
    echo "✘ $label missing: $path"
    failures=$((failures + 1))
  fi
}

check_dir() {
  local path="$1"
  local label="$2"

  if [ -d "$path" ]; then
    echo "✔ $label: $path"
  else
    echo "✘ $label missing: $path"
    failures=$((failures + 1))
  fi
}

check_project_command() {
  local label="$1"
  local expected="$2"
  local required="${3:-false}"
  local resolved=""

  if [ -x "$expected" ]; then
    if resolved="$(command -v "$label" 2>/dev/null)"; then
      if [ "$resolved" = "$expected" ]; then
        echo "✔ $label resolves to project venv: $resolved"
      else
        echo "⚠ $label resolves outside project venv: $resolved"
        echo "  expected: $expected"
        warnings=$((warnings + 1))
        if [ "$required" = "true" ]; then
          failures=$((failures + 1))
        fi
      fi
    else
      echo "✘ $label is not on PATH"
      if [ "$required" = "true" ]; then
        failures=$((failures + 1))
      else
        warnings=$((warnings + 1))
      fi
    fi
  else
    echo "⚠ $label not installed in project venv: $expected"
    warnings=$((warnings + 1))
  fi
}

echo "[1] Core project container"
check_file ".python-version" ".python-version"
check_dir ".venv" ".venv"
check_file ".gitignore" ".gitignore"
check_file ".env.example" ".env.example"
check_file "enter.sh" "enter.sh"
check_file "doctor.sh" "doctor.sh"
check_file "run.sh" "run.sh"

echo ""
echo "[2] Python resolution"
if command -v python >/dev/null 2>&1; then
  echo "✔ python: $(which python)"
  python --version
else
  echo "✘ python not found"
  failures=$((failures + 1))
fi
check_project_command "python" "$PROJECT_ROOT/.venv/bin/python" "true"

echo ""
echo "[3] Snowflake + dbt tooling"
if [ -x ".venv/bin/snow" ]; then
  echo "✔ snowflake-cli installed in project venv"
  .venv/bin/snow --version
else
  echo "⚠ snowflake-cli not found in project venv"
fi
check_project_command "snow" "$PROJECT_ROOT/.venv/bin/snow"

if command -v dbt >/dev/null 2>&1; then
  echo "✔ dbt available: $(command -v dbt)"
  dbt --version | head -n 3 || true
else
  echo "⚠ dbt not installed"
fi
check_project_command "dbt" "$PROJECT_ROOT/.venv/bin/dbt"

echo ""
echo "[4] dbt project structure"
check_dir "dbt" "dbt directory"

if [ -f "dbt_project.yml" ]; then
  echo "✔ dbt_project.yml found at repo root"
elif [ -f "dbt/dbt_project.yml" ]; then
  echo "✔ dbt_project.yml found in dbt/"
else
  echo "⚠ No dbt_project.yml found yet"
fi

if [ -f "dbt/profiles.yml" ]; then
  echo "✔ dbt/profiles.yml found"
else
  echo "⚠ dbt/profiles.yml not found yet"
fi

echo ""
echo "[5] Ingestion entrypoint"
if [ -x "./ingestion/run_ingestion.sh" ]; then
  echo "✔ ingestion/run_ingestion.sh"
elif [ -f "./ingestion/run_ingestion.py" ]; then
  echo "✔ ingestion/run_ingestion.py"
elif [ -f "./ingestion/scripts/run.py" ]; then
  echo "✔ ingestion/scripts/run.py"
else
  echo "⚠ No ingestion runner found yet"
fi

echo ""
echo "[6] Modeling discipline docs"
check_file "docs/PROJECT_START.md" "PROJECT_START.md"
check_file "docs/MODEL_CHECKLIST.md" "MODEL_CHECKLIST.md"

echo ""
echo "====================================="
if [ "$failures" -eq 0 ]; then
  if [ "$warnings" -eq 0 ]; then
    echo "Preflight complete: core project container is healthy."
  else
    echo "Preflight complete: core project container is healthy with $warnings warning(s)."
  fi
else
  echo "Preflight complete: $failures core issue(s) and $warnings warning(s) need attention."
fi
echo "====================================="
