#!/usr/bin/env bash

coverage erase
# Capture the test progression in a variable:
typeset progress
progress=$(coverage run 2>&1)
# If tests failed or produced errors, write progress/messages to stderr and exit:
[[ $? -eq 0 ]] || { >&2 echo "${progress}"; exit 1; }
# Otherwise extract the total coverage percentage from the produced report and write it to stdout:
coverage report | awk '$1 == "TOTAL" {print $NF; exit}'
