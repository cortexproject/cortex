#!/bin/sh
if [ -n "${CORTEX_ULIMIT_NOFILES:-}" ]; then
    current_limit=$(ulimit -Hn)
    if [ "$current_limit" != "unlimited" ]; then
        if [ $CORTEX_ULIMIT_NOFILES -gt $current_limit ]; then
            echo "Setting file description limit to $CORTEX_ULIMIT_NOFILES"
            ulimit -Hn $CORTEX_ULIMIT_NOFILES
        fi
    fi
fi
exec /bin/cortex "$@"