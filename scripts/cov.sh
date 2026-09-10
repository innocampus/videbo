#!/usr/bin/env bash
# Runs unit tests and prints only coverage percentage, if successful.
# If an error occurs, prints the entire unit tests progress output.

source "$(dirname $(realpath $0))/_util.sh"

run coverage erase
# Capture the test progression in a variable:
typeset progress
# If tests failed or produced errors, write progress/messages to stderr and exit:
if ! progress=$(run coverage run 2>&1); then
    >&2 echo "${progress}"
    exit 1
fi
# Otherwise extract the total coverage percentage from the produced report and write it to stdout:
run coverage report | awk '$1 == "TOTAL" {print $NF}'
