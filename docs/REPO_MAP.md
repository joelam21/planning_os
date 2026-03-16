Repository Map

This document explains the structure and intended purpose of the planning_os repository.

The goal of this repo is to build a structured analytics system for planning, forecasting, and operational intelligence. The project is currently in a scaffold stage and will expand over time.

⸻

Root Directory

These files represent the primary entrypoints and operational scripts for the repository.

enter.sh
Activates the project’s local Python environment (.venv).
This should typically be run before executing other scripts.

run.sh
Primary task runner for the project.
Intended to provide a consistent command interface for workflows such as running checks, executing dbt tasks, or starting ingestion processes.

doctor.sh
Diagnostic script used to verify the system environment and detect configuration issues.

requirements.txt
Pinned Python dependencies used by the project.

README.md
Project overview and quickstart instructions.

⸻

scripts/

Automation and workflow helper scripts.

These scripts implement operational tooling used by the repo.

preflight.sh
Verifies repository assumptions before running workflows.
Examples include checking required files, environment configuration, and project structure.

WORKFLOW_NOTES.md
Quick-reference commands for common project workflows and bootstrap steps.

git_commands.sh
Convenience commands for Git operations used during development.

⸻

docs/

Documentation and operational references.

PROJECT_START.md
Instructions for starting work in the project.

MODEL_CHECKLIST.md
Checklist for validating analytics or modeling work.

REPO_MAP.md
This document. Provides a structural overview of the repository.

Future documentation may include:
	•	RUNBOOK.md (operational procedures)
	•	DECISIONS.md (architectural decisions)
	•	STATUS.md (current project maturity and focus)

⸻

dbt/

Planned location for dbt transformation models.

Expected contents may include:
	•	dbt project configuration
	•	model definitions
	•	tests
	•	macros

This directory is currently scaffolded but not yet implemented.

⸻

ingestion/

Planned location for data ingestion logic.

Future responsibilities may include:
	•	pulling raw data from APIs or databases
	•	staging raw datasets
	•	preparing data for transformation

Currently scaffolded but not yet implemented.

⸻

notebooks/

Exploratory analysis and experimentation.

These notebooks are not considered production code and may contain temporary analysis.

⸻

logs/

Runtime logs produced by scripts or workflows.

These files are generally not version controlled and exist for local debugging and diagnostics.

⸻

.venv/

Local Python virtual environment.

This directory contains installed Python dependencies and is not tracked in Git.

⸻

Current Project State

The repository currently functions as a scaffold and workflow foundation.

Completed:
	•	Git workflow setup
	•	environment management
	•	repository structure
	•	workflow and validation scripts

Planned next areas of development:
	•	dbt transformation models
	•	ingestion pipelines
	•	operational analytics workflows

⸻

Design Principles

This repository is structured around several goals:
	•	predictable project structure
	•	reproducible environments
	•	clear operational entrypoints
	•	minimal workflow drift between documentation and scripts
	•	long-term maintainability
:::
