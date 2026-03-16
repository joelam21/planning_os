#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# doctor.sh
# Purpose: Environment health check for this project.
# This script DOES NOT modify anything — it only inspects
# and reports the state of the environment to detect drift.
# ============================================================

PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_ROOT"
EXPECTED_VENV="$PROJECT_ROOT/.venv"

require_project_venv() {
    if [ "${VIRTUAL_ENV:-}" != "$EXPECTED_VENV" ]; then
        echo "[doctor] Project virtual environment is not active."
        echo "[doctor] Expected: $EXPECTED_VENV"
        echo "[doctor] Current:  ${VIRTUAL_ENV:-<none>}"
        echo "[doctor] Run: source ./enter.sh"
        exit 1
    fi
}

require_project_venv

echo "====================================="
echo "Project Environment Doctor"
echo "====================================="
echo "Repo root: $PROJECT_ROOT"
echo ""

# ------------------------------------------------------------
# 1. Python
# ------------------------------------------------------------

echo "[1] Python"

if command -v python >/dev/null 2>&1; then
    echo "✔ python found: $(which python)"
    python --version
else
    echo "✘ python not found"
fi

echo ""

# ------------------------------------------------------------
# 2. Virtual Environment
# ------------------------------------------------------------

echo "[2] Virtual Environment"

if [ -d ".venv" ]; then
    echo "✔ .venv exists"
else
    echo "✘ .venv missing"
fi

if [ -n "${VIRTUAL_ENV:-}" ]; then
    echo "✔ active venv: $VIRTUAL_ENV"
else
    echo "⚠ no virtual environment currently active"
fi

echo ""

# ------------------------------------------------------------
# 3. Pip
# ------------------------------------------------------------

echo "[3] pip"

if command -v pip >/dev/null 2>&1; then
    echo "✔ pip found: $(which pip)"
    python -m pip --version
else
    echo "✘ pip not found"
fi

echo ""

# ------------------------------------------------------------
# 4. Snowflake CLI
# ------------------------------------------------------------

echo "[4] Snowflake CLI"

if [ -x ".venv/bin/snow" ]; then
    echo "✔ project Snowflake CLI detected"
    .venv/bin/snow --version || echo "⚠ snow --version failed"
else
    echo "⚠ Snowflake CLI not installed in project venv"
fi

if command -v snow >/dev/null 2>&1; then
    echo "system snow: $(which snow)"
fi

echo ""

# ------------------------------------------------------------
# 5. dbt
# ------------------------------------------------------------

echo "[5] dbt"

if command -v dbt >/dev/null 2>&1; then
    echo "✔ dbt detected"
    dbt --version | head -n 3 || true
else
    echo "⚠ dbt not installed"
fi

echo ""

# ------------------------------------------------------------
# 6. Environment Variables
# ------------------------------------------------------------

echo "[6] Environment Variables"

if [ -f ".env" ]; then
    echo "✔ .env file present"
else
    echo "⚠ .env file missing"
fi

# Validate .env has all keys from .env.example
if [ -f ".env.example" ]; then
    missing_keys=0
    while IFS= read -r line; do
        # Skip comments and blank lines
        [[ "$line" =~ ^#.*$ || -z "$line" ]] && continue
        key="${line%%=*}"
        if [ -f ".env" ] && grep -q "^${key}=" ".env"; then
            true
        else
            echo "⚠ $key defined in .env.example but missing from .env"
            missing_keys=$((missing_keys + 1))
        fi
    done < .env.example
    if [ "$missing_keys" -eq 0 ]; then
        echo "✔ .env has all keys from .env.example"
    fi
else
    echo "⚠ .env.example not found (cannot validate .env keys)"
fi

for var in SNOWFLAKE_ACCOUNT SNOWFLAKE_USER SNOWFLAKE_ROLE SNOWFLAKE_WAREHOUSE; do
    if [ -n "${!var:-}" ]; then
        echo "✔ $var set"
    else
        echo "⚠ $var not set"
    fi
done

echo ""

# ------------------------------------------------------------
# 7. Dependency lock
# ------------------------------------------------------------

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
