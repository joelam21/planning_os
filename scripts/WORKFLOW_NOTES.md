# Workflow Notes

Quick-reference commands for common project operations.

## Bootstrap

```bash
# Cmd + Shift + P  → Open Command Palette (VS Code, not terminal)

chmod +x scripts/single_command_bootstrap.sh
./scripts/single_command_bootstrap.sh planning_os
```

## Reload shell configuration

```bash
source ~/.zshrc
```

## Activate the environment and verify

```bash
source ./enter.sh
which python
python -c "import sys; print(sys.executable)"
```

## Freeze / install requirements

```bash
pip list
python -m pip freeze > requirements.txt
python -m pip install -r requirements.txt
```

## Run individual bootstrap scripts

```bash
chmod +x scripts/01_create_repo.sh
./scripts/01_create_repo.sh planning_os

chmod +x scripts/02_pin_python_create_venv.sh
./scripts/02_pin_python_create_venv.sh

chmod +x scripts/preflight.sh
./scripts/preflight.sh
```

## Daily workflow

```bash
git st
source ./enter.sh
./scripts/preflight.sh
./run.sh help
./run.sh dbt-debug
```
