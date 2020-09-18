#!/bin/bash
SED_BIN=${SED_BIN:-sed}

# Remove one, or three or more spaces, but not two, as that is used by Markdown for line breaks.
${SED_BIN} -i 's/[^[:space:]][[:space:]]{1}$//' "$@"
${SED_BIN} -i 's/[^[:space:]][[:space:]]{3,}$//' "$@"
