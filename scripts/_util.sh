# Ensure that we return to the current working directory
# and exit the script immediately in case of an error:
trap "cd $(realpath ${PWD}); exit 1" ERR
# Fail a pipeline if any command in it fails, not just the last:
set -o pipefail
# Change into project root directory:
cd "$(dirname $(dirname $(realpath $0)))"

# Define the prefix for invoking project tools.
# Inside an activated environment, tools are called directly.
# Outside one, `uv` provides the environment, if it is available.
if [[ -n "${VIRTUAL_ENV:-}" ]]; then
    run() { "$@"; }
elif command -v uv > /dev/null 2>&1; then
    run() { uv run "$@"; }
else
    run() { "$@"; }
fi

# Define a few colors.
typeset background_black='\033[40m'
typeset bold_green='\033[1;92m'
typeset yellow='\033[0;33m'
typeset color_reset='\033[0m'
