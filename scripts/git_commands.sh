#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# git_commands.sh
# Purpose: scenario-based Git helper for this project.
# Usage examples:
#   ./scripts/git_commands.sh status
#   ./scripts/git_commands.sh init-repo
#   ./scripts/git_commands.sh start-feature feature/api-ingestion-migration
#   ./scripts/git_commands.sh daily-commit "feat: add ingestion pipeline"
#   ./scripts/git_commands.sh create-pr feature/api-ingestion-migration "Add API ingestion" "PR body"
#   ./scripts/git_commands.sh sync-branches
#   ./scripts/git_commands.sh cleanup feature/api-ingestion-migration
#   ./scripts/git_commands.sh emergency-hotfix hotfix/urgent-fix
#
# Notes:
# - This script is intentionally explicit and scenario-based.
# - It is safer than keeping one long scratchpad of commands.
# - Default branch model here is: main = stable, dev = active work.
# ============================================================

COMMAND="${1:-help}"
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
EXPECTED_VENV="$PROJECT_ROOT/.venv"

require_project_venv() {
  if [ "${VIRTUAL_ENV:-}" != "$EXPECTED_VENV" ]; then
    echo "[git] Project virtual environment is not active."
    echo "[git] Expected: $EXPECTED_VENV"
    echo "[git] Current:  ${VIRTUAL_ENV:-<none>}"
    echo "[git] Run: source ./enter.sh"
    exit 1
  fi
}

require_clean_worktree() {
  if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "[git] Working tree is not clean. Commit, stash, or discard changes first."
    exit 1
  fi
}

current_branch() {
  git rev-parse --abbrev-ref HEAD
}

help_text() {
  cat <<'EOF'
Available commands:

  help
    Show this help message.

  status
    Show git status, branches, and recent graph.

  init-repo
    Initialize repo branch structure for this project:
    - rename current branch to main
    - create dev if missing
    - switch to dev

  start-feature <feature_branch>
    Start a new feature branch from dev.

  daily-check
    Show branch, status, and recent commit graph.

  stage-all
    Stage all tracked, untracked, and deleted files.

  stage-paths <path...>
    Stage only the paths you specify.

  daily-commit <commit_message>
    Commit only what is already staged.

  create-pr <feature_branch> <title> <body>
    Create a GitHub PR from feature_branch into dev.

  sync-branches
    Update local dev and main from origin.
    Optionally fast-forward main from dev if already merged remotely.

  cleanup <feature_branch>
    Delete local feature branch and attempt to delete remote branch.

  emergency-hotfix <hotfix_branch>
    Create a hotfix branch from main.

  stash-save [message]
    Save working changes to stash.

  stash-pop
    Restore latest stash.
EOF
}

case "$COMMAND" in
  help)
    help_text
    ;;

  status)
    require_project_venv
    echo "[git] Current branch: $(current_branch)"
    git status
    echo ""
    git branch -a
    echo ""
    git log --oneline --graph --decorate --all -10
    ;;

  init-repo)
    require_project_venv
    echo "[git] Initializing branch structure"
    git branch -M main
    if git show-ref --verify --quiet refs/heads/dev; then
      echo "[git] dev branch already exists"
    else
      git checkout -b dev
      echo "[git] Created dev branch"
    fi
    git checkout dev
    ;;

  start-feature)
    require_project_venv
    FEATURE_BRANCH="${2:-}"
    if [ -z "$FEATURE_BRANCH" ]; then
      echo "Usage: ./scripts/git_commands.sh start-feature <feature_branch>"
      exit 1
    fi
    require_clean_worktree
    git checkout dev
    git pull origin dev || true
    git checkout -b "$FEATURE_BRANCH"
    echo "[git] Started feature branch: $FEATURE_BRANCH"
    ;;

  daily-check)
    require_project_venv
    echo "[git] Current branch: $(current_branch)"
    git status
    echo ""
    git log --oneline --graph --decorate --all -10
    ;;

  stage-all)
    require_project_venv
    git add -A
    git status --short
    ;;

  stage-paths)
    require_project_venv
    shift || true
    if [ "$#" -eq 0 ]; then
      echo "Usage: ./scripts/git_commands.sh stage-paths <path...>"
      exit 1
    fi
    git add -- "$@"
    git status --short
    ;;

  daily-commit)
    require_project_venv
    COMMIT_MESSAGE="${2:-}"
    if [ -z "$COMMIT_MESSAGE" ]; then
      echo "Usage: ./scripts/git_commands.sh daily-commit \"<commit_message>\""
      exit 1
    fi
    if git diff --cached --quiet; then
      echo "[git] No staged changes found."
      echo "[git] Stage intentionally with one of:"
      echo "  ./scripts/git_commands.sh stage-all"
      echo "  ./scripts/git_commands.sh stage-paths <path...>"
      git status --short
      exit 1
    fi
    git commit -m "$COMMIT_MESSAGE"
    echo "[git] Commit created on branch: $(current_branch)"
    ;;

  create-pr)
    require_project_venv
    FEATURE_BRANCH="${2:-}"
    PR_TITLE="${3:-}"
    PR_BODY="${4:-}"
    if [ -z "$FEATURE_BRANCH" ] || [ -z "$PR_TITLE" ] || [ -z "$PR_BODY" ]; then
      echo "Usage: ./scripts/git_commands.sh create-pr <feature_branch> \"<title>\" \"<body>\""
      exit 1
    fi
    git push -u origin "$FEATURE_BRANCH"
    gh pr create --base dev --head "$FEATURE_BRANCH" --title "$PR_TITLE" --body "$PR_BODY"
    ;;

  sync-branches)
    require_project_venv
    require_clean_worktree
    git checkout dev
    git pull origin dev || true
    git checkout main
    git pull origin main || true
    echo "[git] Local dev and main refreshed from origin"
    ;;

  cleanup)
    require_project_venv
    FEATURE_BRANCH="${2:-}"
    if [ -z "$FEATURE_BRANCH" ]; then
      echo "Usage: ./scripts/git_commands.sh cleanup <feature_branch>"
      exit 1
    fi
    require_clean_worktree
    if [ "$(current_branch)" = "$FEATURE_BRANCH" ]; then
      git checkout dev
    fi
    git branch -d "$FEATURE_BRANCH" || true
    git push origin --delete "$FEATURE_BRANCH" || true
    echo "[git] Cleanup attempted for: $FEATURE_BRANCH"
    ;;

  emergency-hotfix)
    require_project_venv
    HOTFIX_BRANCH="${2:-}"
    if [ -z "$HOTFIX_BRANCH" ]; then
      echo "Usage: ./scripts/git_commands.sh emergency-hotfix <hotfix_branch>"
      exit 1
    fi
    require_clean_worktree
    git checkout main
    git pull origin main || true
    git checkout -b "$HOTFIX_BRANCH"
    echo "[git] Hotfix branch created: $HOTFIX_BRANCH"
    ;;

  stash-save)
    require_project_venv
    STASH_MESSAGE="${2:-WIP on $(current_branch)}"
    git stash push -m "$STASH_MESSAGE"
    ;;

  stash-pop)
    require_project_venv
    git stash pop
    ;;

  *)
    echo "Unknown command: $COMMAND"
    echo ""
    help_text
    exit 1
    ;;
esac
