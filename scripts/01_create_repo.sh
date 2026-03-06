#!/usr/bin/env bash

set -e

PROJECT_NAME=$1

if [ -z "$PROJECT_NAME" ]; then
  echo "Usage: ./01_create_repo.sh <project_name>"
  exit 1
fi

PROJECT_DIR=~/docs/Data_Science/$PROJECT_NAME

mkdir -p "$PROJECT_DIR"
cd "$PROJECT_DIR"

if [ ! -d ".git" ]; then
  git init
fi

code .