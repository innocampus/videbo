#!/usr/bin/env bash
# Runs unit tests and end-to-end-tests.

source "$(dirname $(realpath $0))/_util.sh"

echo 'Running unit tests...'
run coverage run
typeset percentage
typeset color
percentage="$(run coverage report | awk '$1 == "TOTAL" {print $NF}')"
[[ $percentage == "100%" ]] && color="${bold_green}" || color="${yellow}"
echo -e "${color}${percentage} coverage${color_reset}\n"

echo 'Running end-to-end tests...'
run python -m tests 'e2e_test*.py'
echo
