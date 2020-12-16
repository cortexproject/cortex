# You can install Jsonnet by running following command outside of Cortex directory:
#
#   go get github.com/google/go-jsonnet/cmd/jsonnet
#
# Or follow instructions on https://github.com/google/go-jsonnet"

.DEFAULT_GOAL := docker-compose.yml

docker-compose.yml: docker-compose.jsonnet
	jsonnet -S docker-compose.jsonnet -o docker-compose.yml

check: docker-compose.yml
	@git diff --exit-code --quiet -- 'docker-compose.yml' || (echo "docker-compose.jsonnet and docker-compose.yml don't match in ${PWD}" && false)
