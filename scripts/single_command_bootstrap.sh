#!/usr/bin/env bash

# ============================================================
# Single-command project bootstrap
#
# Creates a new project “container” with:
# - repo directory + git init
# - pinned Python via pyenv (.python-version)
# - .venv + upgraded pip tooling
# - baseline deps (python-dotenv, snowflake-cli, dbt-core, dbt-snowflake)
# - .env.example + .env (local)
# - .gitignore
# - VS Code interpreter pin (.vscode/settings.json)
# - enter.sh, doctor.sh, run.sh
#
# Usage:
#   ./scripts/01_create_repo.sh <project_name>
#
# Optional env overrides:
#   PYTHON_VERSION=3.12.8
#   SKIP_DEPS=1          (skip pip installs)
#   SKIP_CODE=1          (don’t open VS Code)
# ============================================================

set -euo pipefail

PROJECT_NAME="${1:-}"
PYTHON_VERSION="${PYTHON_VERSION:-3.12.8}"
SKIP_DEPS="${SKIP_DEPS:-0}"
SKIP_CODE="${SKIP_CODE:-0}"

if [ -z "$PROJECT_NAME" ]; then
  echo "Usage: ./scripts/01_create_repo.sh <project_name>"
  exit 1
fi

PROJECT_DIR="$HOME/docs/Data_Science/$PROJECT_NAME"

echo ""
echo "====================================="
echo "Bootstrap project: $PROJECT_NAME"
echo "Target directory:  $PROJECT_DIR"
echo "Python version:    $PYTHON_VERSION"
echo "====================================="
echo ""

# -------------------------------
# 1) Create project directory
# -------------------------------
mkdir -p "$PROJECT_DIR"
cd "$PROJECT_DIR"

# -------------------------------
# 2) Git init (idempotent)
# -------------------------------
if [ ! -d ".git" ]; then
  git init
  echo "✔ Initialized git repository"
else
  echo "✔ Git repository already exists"
fi

# -------------------------------
# 3) Project folders (idempotent)
# -------------------------------
mkdir -p docs dbt ingestion notebooks scripts .vscode

# -------------------------------
# 4) Python version pin (pyenv)
# -------------------------------
if command -v pyenv >/dev/null 2>&1; then
  # Install if missing (-s = skip if already installed)
  pyenv install -s "$PYTHON_VERSION" >/dev/null 2>&1 || true
  pyenv local "$PYTHON_VERSION"
  echo "✔ Pinned Python via .python-version"
else
  echo "⚠ pyenv not found. Skipping Python pin step. (Install pyenv for reproducible Python versions.)"
fi

# -------------------------------
# 5) Create venv (idempotent)
# -------------------------------
if [ ! -d ".venv" ]; then
  python -m venv .venv
  echo "✔ Created .venv"
else
  echo "✔ .venv already exists"
fi

# Activate venv for the remainder of this script
# shellcheck disable=SC1091
source .venv/bin/activate

python -m pip install --upgrade pip setuptools wheel >/dev/null

# -------------------------------
# 6) Baseline deps (optional)
# -------------------------------
if [ "$SKIP_DEPS" = "1" ]; then
  echo "⚠ SKIP_DEPS=1 set. Skipping pip installs."
else
  echo "Installing baseline dependencies into .venv..."
  python -m pip install \
    python-dotenv \
    snowflake-cli \
    dbt-core \
    dbt-snowflake
fi

# -------------------------------
# 7) .env templates (idempotent)
# -------------------------------
if [ ! -f ".env.example" ]; then
  cat > .env.example << 'EOF'
# Copy this file to .env and fill values for local development

# Snowflake (prefer externalbrowser auth via Snowflake CLI; avoid passwords in files)
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_ROLE=
SNOWFLAKE_WAREHOUSE=
SNOWFLAKE_DATABASE=
SNOWFLAKE_SCHEMA=

# dbt
DBT_TARGET=dev
DBT_PROFILES_DIR=./dbt

# Optional: run.sh can use this for Snowflake SQL health checks
SNOW_CONNECTION=my_snowflake
EOF
  echo "✔ Created .env.example"
else
  echo "✔ .env.example already exists"
fi

if [ ! -f ".env" ]; then
  cp .env.example .env
  echo "✔ Created .env (from .env.example)"
else
  echo "✔ .env already exists (not overwritten)"
fi

# -------------------------------
# 8) .gitignore (idempotent)
# -------------------------------
if [ ! -f ".gitignore" ]; then
  cat > .gitignore << 'EOF'
# Python
.venv/
__pycache__/
*.pyc

# Local env
.env

# OS
.DS_Store
EOF
  echo "✔ Created .gitignore"
else
  echo "✔ .gitignore already exists"
fi

# -------------------------------
# 9) VS Code settings (idempotent)
# -------------------------------
if [ ! -f ".vscode/settings.json" ]; then
  cat > .vscode/settings.json << 'EOF'
{
  "python.defaultInterpreterPath": "${workspaceFolder}/.venv/bin/python",
  "python.terminal.activateEnvironment": true
}
EOF
  echo "✔ Created .vscode/settings.json (pins interpreter to .venv)"
else
  echo "✔ .vscode/settings.json already exists"
fi

# -------------------------------
# 10) enter.sh / doctor.sh / run.sh
# -------------------------------
if [ ! -f "enter.sh" ]; then
  cat > enter.sh << 'EOF'
#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# enter.sh
# Purpose: Enter the project "capsule" deterministically.
# Usage:
#   source ./enter.sh
# ============================================================

cd "$(dirname "$0")"

if [ ! -d ".venv" ]; then
  echo "[enter] .venv not found. Creating with: python -m venv .venv"
  python -m venv .venv
fi

# shellcheck disable=SC1091
source ".venv/bin/activate"

if [ -f ".env" ]; then
  echo "[enter] Loading .env"
  set -a
  # shellcheck disable=SC1091
  source ".env"
  set +a
else
  echo "[enter] No .env found (ok). Create from .env.example if needed."
fi

export PATH="$(pwd)/.venv/bin:$PATH"

echo ""
echo "[enter] Repo root: $(pwd)"
echo "[enter] Venv:       ${VIRTUAL_ENV:-<none>}"
echo "[enter] Python:     $(which python)"
python --version
python -m pip --version

if [ -x ".venv/bin/snow" ]; then
  echo "[enter] Snow CLI:   $(pwd)/.venv/bin/snow"
  .venv/bin/snow --version
else
  echo "[enter] Snow CLI:   not installed in .venv (run: python -m pip install snowflake-cli)"
fi

if command -v dbt >/dev/null 2>&1; then
  echo "[enter] dbt:        $(command -v dbt)"
  dbt --version | head -n 2 || true
else
  echo "[enter] dbt:        not installed in .venv (run: python -m pip install dbt-core dbt-snowflake)"
fi

echo ""
echo "[enter] Next common commands:"
echo "  ./doctor.sh"
echo "  ./run.sh help"
EOF
  chmod +x enter.sh
  echo "✔ Created enter.sh"
else
  echo "✔ enter.sh already exists"
fi

if [ ! -f "doctor.sh" ]; then
  cat > doctor.sh << 'EOF'
#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# doctor.sh
# Purpose: Environment health check (no mutations).
# ============================================================

PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_ROOT"

echo "====================================="
echo "Project Environment Doctor"
echo "====================================="
echo "Repo root: $PROJECT_ROOT"
echo ""

echo "[1] Python"
if command -v python >/dev/null 2>&1; then
  echo "✔ python: $(which python)"
  python --version
else
  echo "✘ python not found"
fi

echo ""
echo "[2] Virtual Environment"
if [ -d ".venv" ]; then
  echo "✔ .venv exists"
else
  echo "✘ .venv missing"
fi
if [ -n "${VIRTUAL_ENV:-}" ]; then
  echo "✔ active venv: $VIRTUAL_ENV"
else
  echo "⚠ no active venv (run: source ./enter.sh)"
fi

echo ""
echo "[3] pip"
if command -v pip >/dev/null 2>&1; then
  echo "✔ pip: $(which pip)"
  python -m pip --version
else
  echo "✘ pip not found"
fi

echo ""
echo "[4] Snowflake CLI"
if [ -x ".venv/bin/snow" ]; then
  echo "✔ project snow: $PROJECT_ROOT/.venv/bin/snow"
  .venv/bin/snow --version
else
  echo "⚠ snowflake-cli not installed in .venv"
fi
if command -v snow >/dev/null 2>&1; then
  echo "system snow: $(which snow)"
fi

echo ""
echo "[5] dbt"
if command -v dbt >/dev/null 2>&1; then
  echo "✔ dbt: $(command -v dbt)"
  dbt --version | head -n 3 || true
else
  echo "⚠ dbt not installed"
fi

echo ""
echo "[6] Env files"
if [ -f ".env" ]; then
  echo "✔ .env present"
else
  echo "⚠ .env missing"
fi
if [ -f ".env.example" ]; then
  echo "✔ .env.example present"
else
  echo "⚠ .env.example missing"
fi

echo ""
echo "[7] requirements.txt"
if [ -f "requirements.txt" ]; then
  echo "✔ requirements.txt present"
else
  echo "⚠ requirements.txt missing"
fi

echo ""
echo "====================================="
echo "Doctor check complete"
echo "====================================="
EOF
  chmod +x doctor.sh
  echo "✔ Created doctor.sh"
else
  echo "✔ doctor.sh already exists"
fi

if [ ! -f "run.sh" ]; then
  cat > run.sh << 'EOF'
#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# run.sh
# Purpose: Unified entry point for project workflows.
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
    bash ./doctor.sh
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
    echo "Run './run.sh help' to see available commands."
    exit 1
    ;;

esac
EOF
  chmod +x run.sh
  echo "✔ Created run.sh"
else
  echo "✔ run.sh already exists"
fi

# -------------------------------
# 11) requirements.txt (lock)
# -------------------------------
if [ "$SKIP_DEPS" = "1" ]; then
  echo "⚠ SKIP_DEPS=1 set. Not generating requirements.txt."
else
  python -m pip freeze > requirements.txt
  echo "✔ Wrote requirements.txt"
fi

# -------------------------------
# 12) README + docs stub (optional)
# -------------------------------
if [ ! -f "README.md" ]; then
  cat > README.md << EOF
# $PROJECT_NAME

Purpose: <fill>

## Architecture (initial)
- Warehouse: Snowflake
- Transforms: dbt
- Ingestion: Python (project-local)

## Quickstart
\
1) Enter environment:\
\
\
\`\`\`bash
source ./enter.sh
\`\`\`
\
2) Doctor check:\
\
\`\`\`bash
./doctor.sh
\`\`\`
\
3) List commands:\
\
\`\`\`bash
./run.sh help
\`\`\`
EOF
  echo "✔ Created README.md"
fi

if [ ! -f "docs/PROJECT_START.md" ]; then
  cat > docs/PROJECT_START.md << 'EOF'
# Project Start Notes

- Grain:
- Facts:
- Dimensions:
- Drift rules:

EOF
  echo "✔ Created docs/PROJECT_START.md"
fi

echo ""
echo "✅ Bootstrap complete."
echo "Next:"
echo "  cd \"$PROJECT_DIR\""
echo "  source ./enter.sh"
echo "  ./doctor.sh"

if [ "$SKIP_CODE" = "0" ] && command -v code >/dev/null 2>&1; then
  code .
fi
