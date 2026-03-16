
#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# enter.sh
# Purpose: Enter the project "capsule" deterministically.
# - cd to repo root (directory containing this script)
# - ensure .venv exists (create if missing)
# - activate .venv
# - load .env into the current shell (if present)
# - print key diagnostics to prevent drift
#
# Usage:
#   source ./enter.sh
#
# Note: Use `source` so that activation + env vars persist
# in your current shell session.
# ============================================================

# Always run from the repo root (directory containing this file)
cd "$(dirname "$0")"

# Create .venv if missing
if [ ! -d ".venv" ]; then
  echo "[enter] .venv not found. Creating with: python -m venv .venv"
  python -m venv .venv
fi

# Activate venv (must be sourced)
# shellcheck disable=SC1091
source ".venv/bin/activate"

# Load .env if present (export all variables defined within)
if [ -f ".env" ]; then
  echo "[enter] Loading .env"
  set -a
  # shellcheck disable=SC1091
  source ".env"
  set +a
else
  echo "[enter] No .env found (ok). If needed, create one from .env.example"
fi

# Prefer project-local CLI binaries
export PATH="$(pwd)/.venv/bin:$PATH"

# --------------------
# Drift diagnostics
# --------------------

echo ""
echo "[enter] Repo root: $(pwd)"
echo "[enter] Venv:       ${VIRTUAL_ENV:-<none>}"
echo "[enter] Python:     $(which python)"
python --version
python -m pip --version

# Snowflake CLI: ensure we're using the project-local one
if [ -x ".venv/bin/snow" ]; then
  echo "[enter] Snow CLI:   $(pwd)/.venv/bin/snow"
  .venv/bin/snow --version
else
  echo "[enter] Snow CLI:   not installed in .venv (run: python -m pip install snowflake-cli)"
fi

# dbt: optional, only if installed
if command -v dbt >/dev/null 2>&1; then
  echo "[enter] dbt:        $(command -v dbt)"
  dbt --version | head -n 2 || true
else
  echo "[enter] dbt:        not installed in .venv (run: python -m pip install dbt-core dbt-snowflake)"
fi

# Helpful hint (does not execute)
echo ""
echo "[enter] Next common commands:"
echo "  .venv/bin/snow --info"
echo "  .venv/bin/snow connection list"
echo "  dbt debug   (after you set up dbt/profiles.yml + env vars)"