#!/usr/bin/env bash
# Prints the first error in a build/test log as a GitHub annotation, so the
# cause of a failed run is visible on the run's summary page (and through the
# API) without opening the full log.
set -u
log=${1:?usage: first-error.sh <logfile>}
[ -f "$log" ] || { echo "::error::no $log was produced"; exit 0; }
clean=$(mktemp)
sed 's/\x1b\[[0-9;]*[a-zA-Z]//g' "$log" > "$clean"       # strip colour codes
line=$(grep -m1 -n -E '^(error|failures:)|panicked at|^test .* FAILED' "$clean" | cut -d: -f1)
if [ -n "$line" ]; then
  body=$(sed -n "${line},$((line + 45))p" "$clean")
else
  body=$(tail -45 "$clean")
fi
# Annotation messages are single-line: %0A is how a newline is written.
encoded=$(printf '%s' "$body" | sed 's/%/%25/g' | awk '{printf "%s%%0A", $0}')
echo "::error title=First error::$encoded"
