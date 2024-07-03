# Description: This Makefile is used to generate the dashboards, alerts, and rules files from the cortex-jsonnet repo.
# It uses a customized config.libsonnet file to generate the files.

all:
	# Clone the cortex-jsonnet repository if it doesn't exist
	git clone https://github.com/cortexproject/cortex-jsonnet --depth 1 || true
	# Show the diff between the config.libsonnet file and the cortex-jsonnet/cortex-mixin/config.libsonnet file
	git --no-pager diff --no-index cortex-jsonnet/cortex-mixin/config.libsonnet config.libsonnet || true
	# Copy the config.libsonnet file to the cortex-jsonnet directory
	cp config.libsonnet cortex-jsonnet/cortex-mixin
	# Run the build-mixin target
	docker run -v $(shell pwd)/cortex-jsonnet:/workspace -w /workspace -it quay.io/cortexproject/cortex-jsonnet-build-image:fbe4726 make build-mixin
	# Remove the dashboards, alerts.yaml, and rules.yaml files
	rm -rf dashboards alerts.yaml rules.yaml
	# Copy the files from the cortex-jsonnet/cortex-mixin/out directory to the current directory
	mv cortex-jsonnet/cortex-mixin/out/* .

clean:
	# Remove the cortex-jsonnet directory
	rm -rf cortex-jsonnet