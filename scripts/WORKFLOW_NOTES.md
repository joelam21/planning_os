# Workflow Notes

Quick-reference commands for common project operations.

## Bootstrap

```bash
# Cmd + Shift + P  → Open Command Palette (VS Code, not terminal)

chmod +x scripts/single_command_bootstrap.sh
./scripts/single_command_bootstrap.sh planning_os
```

## Open shell configuration

```bash
code ~/.zshrc
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

## dbt_project.yml

```yml
name: 'planning_os'
version: '1.0.0'
config-version: 2

profile: 'planning_os'

model-paths: ["dbt/models"]
analysis-paths: ["dbt/analyses"]
test-paths: ["dbt/tests"]
seed-paths: ["dbt/seeds"]
macro-paths: ["dbt/macros"]
snapshot-paths: ["dbt/snapshots"]

clean-targets:
  - "target"
  - "dbt_packages"
```

## create dbt directories
```bash
mkdir -p dbt/models dbt/analyses dbt/tests dbt/seeds dbt/macros dbt/snapshots
```