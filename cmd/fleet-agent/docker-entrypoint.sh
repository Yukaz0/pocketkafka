#!/bin/sh
# fleet-agent entrypoint: state defaults to /data (the volume), so a fresh
# container enrolls once with FLEET_ENROLL_TOKEN, then restarts reuse the
# stored credential without a new token.
set -e
if [ ! -f "${FLEET_STATE_PATH:-/data/agent-state.json}" ] && [ -z "${FLEET_ENROLL_TOKEN:-}" ]; then
  echo "fleet-agent: no state file and FLEET_ENROLL_TOKEN is empty; issue a token from Fleet Settings first" >&2
  exit 1
fi
exec /usr/local/bin/fleet-agent "$@"
