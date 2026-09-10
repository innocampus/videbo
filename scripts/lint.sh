#!/usr/bin/env bash
# Runs type checker and linters.

source "$(dirname $(realpath $0))/_util.sh"

echo 'Performing type checks...'
run mypy
echo

echo 'Linting source and test files...'
run ruff check src/ tests/
echo -e "${bold_green}No issues found${color_reset}\n"
