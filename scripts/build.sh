#!/bin/bash

VERSION=$(cat ./VERSION)
CGO_ENABLED=0 GOARCH=arm64 GOOS=linux go build -ldflags "-X main.Branch=$GIT_BRANCH -X main.Revision=$GIT_REVISION -X main.Version=$VERSION -extldflags \"-static\" -s -w" -tags netgo -o ./cmd/cortex/cortex-arm64 ./cmd/cortex
