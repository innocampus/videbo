#!/bin/bash

EMAIL="gauk@math.tu-berlin.de"
DOMAIN="*.tu-berlin-streaming.de"

# increase timeout in case dns propagation takes longer than expected
export INWX_POLLING_INTERVAL=10
export INWX_PROPAGATION_TIMEOUT=600

CONFIG="config.ini"

if [ "$1" != "run" ] && [ "$1" != "renew" ]; then
	echo "Usage: $0 run|renew"
	exit 1
fi

parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd "$parent_path"

eval "$(cat "$CONFIG" | awk -F ' *= *' '{ if ($1 ~ /^\[/) section=$1; else if ($1 !~ /^$/) print $1 section "=" "\"" $2 "\"" }' | grep "cloud-inwx")"

export INWX_USERNAME=${username[cloud-inwx]}
export INWX_PASSWORD=${password[cloud-inwx]}

lego --accept-tos \
        --dns="inwx" --email="$EMAIL" \
        --domains="$DOMAIN" "$1"

