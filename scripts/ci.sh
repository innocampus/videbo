#!/usr/bin/env bash
# Runs full CI pipeline (test, check, lint).

source "$(dirname $(realpath $0))/_util.sh"

bash ./scripts/test.sh
bash ./scripts/lint.sh

echo -e "${background_black}${bold_green}✅ 🎉 All checks passed!${color_reset}"
