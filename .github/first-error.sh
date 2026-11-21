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
annotate() { # title, text. Annotation messages are single-line: %0A is a newline.
  local encoded
  encoded=$(printf '%s' "$2" | sed 's/%/%25/g' | awk '{printf "%s%%0A", $0}')
  echo "::error title=$1::$encoded"
}
annotate "First error" "$body"

# A failed build script (C libraries such as librdkafka) prints pages of
# configure output first; the real cause is at the end of its stderr.
if grep -q -- '--- stderr' "$clean"; then
  start=$(grep -n -- '--- stderr' "$clean" | head -1 | cut -d: -f1)
  annotate "Build script stderr (start)" "$(sed -n "${start},$((start + 25))p" "$clean")"
  annotate "Build script stderr (errors)" "$(sed -n "${start},\$p" "$clean" | grep -n -i -E 'error|fatal|undefined|panicked|cannot find|No such' | head -30)"
  annotate "End of log" "$(tail -40 "$clean")"
fi
