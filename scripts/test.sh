#!/usr/bin/env bash
# Runs unit tests and end-to-end-tests.

# Ensure that we return to the current working directory
# and exit the script immediately in case of an error:
trap "cd $(realpath ${PWD}); exit 1" ERR
# Change into project root directory:
cd "$(dirname $(dirname $(realpath $0)))"

echo 'Running unit tests...'
coverage run
echo -e "$(coverage report | awk '$1 == "TOTAL" {print $NF; exit}') coverage.\n"

echo 'Running end-to-end tests...'
python -m tests 'e2e_test*.py'
echo
