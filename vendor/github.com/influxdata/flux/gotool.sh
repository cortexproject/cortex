#!/bin/bash

# This script will execute a module in the internal/tools
# directory and execute it within the current working directory.
#
# The script will create an exec script that contains the current
# working directory. This script gets passed as the -exec argument
# to go run so it is used for executing the compiled go binary.
# It then changes into the tool directory so it can use the module
# dependencies in the tool module.
#
# This allows us to specify tool dependencies using go modules without
# it getting added to the flux module itself.

if [ "$#" -eq 0 ]; then
  echo "gotool.sh requires one or more arguments" 1>&2
  exit 1
fi

tool_dir=$(go list -m -f '{{.Dir}}')/internal/tools
script_file=$(mktemp)
trap '{ rm -f "$script_file"; }' EXIT

cat > "$script_file" <<EOF
#!/bin/bash
cd "$PWD" && exec "\$@"
EOF
chmod 700 "$script_file"

cd "$tool_dir" && go run -exec "$script_file" "$@"
