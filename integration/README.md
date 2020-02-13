# Cortex integration tests

## Supported environment variables

- `CORTEX_IMAGE`: Docker image used to run Cortex in integration tests (defaults to `quay.io/cortexproject/cortex:latest`)
- `CORTEX_INTEGRATION_DIR`: Absolute path to the integration/ directory on the host (defaults to `$GOPATH/src/github.com/cortexproject/cortex/integration`)

## Mounting files to Cortex container

The `integration/` directory in the repository is mounted to `/integration` within the container. If you need to add fixture files (ie. config file) you can add the files to this repository (in `integration/`) and the files will be available within the Cortex container at `/integration`.
