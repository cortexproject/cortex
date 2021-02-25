#!/bin/bash

# Support gsed on OSX (installed via brew), falling back to sed. On Linux
# systems gsed won't be installed, so will use sed as expected.
SED_BIN=${SED_BIN:-$(which gsed 2>/dev/null || which sed)}

${SED_BIN} -i 's/[ \t]*$//' "$@"
