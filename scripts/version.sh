#!/usr/bin/env bash

# This script is used to generate a version string for the current build.
VERSION=$(cat ./VERSION)
echo "$VERSION.$GIT_BRANCH.$GIT_COMMIT_SHORT.$RIO_BUILD_NUMBER"
