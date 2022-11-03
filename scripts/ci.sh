#!/usr/bin/env bash
# Runs full CI pipeline (test, check, lint).

# Ensure that we return to the current working directory
# and exit the script immediately in case of an error:
trap "cd $(realpath ${PWD}); exit 1" ERR
# Change into project root directory:
cd "$(dirname $(dirname $(realpath $0)))"

bash ./scripts/test.sh
bash ./scripts/lint.sh

echo 'All checks passed!'
