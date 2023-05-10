#!/usr/bin/env bash

set -ev

docker-compose up -d
go test -coverprofile=./c.out -v -race -timeout 30m ./...
docker-compose down -v

if [ ! -z "$CODECOV_TOKEN" ]; then
  cp c.out coverage.txt
  bash <(curl -s https://codecov.io/bash)
fi