#!/bin/bash

set -x
VER="29.2.1"

# Detect architecture
ARCH=$(uname -m)
case $ARCH in
  x86_64)
    DOCKER_ARCH="x86_64"
    BUILDX_ARCH="amd64"
    ;;
  aarch64)
    DOCKER_ARCH="aarch64"
    BUILDX_ARCH="arm64"
    ;;
  *)
    echo "Unsupported architecture: $ARCH"
    exit 1
    ;;
esac

echo "Installing Docker $VER for architecture: $ARCH (docker: $DOCKER_ARCH, buildx: $BUILDX_ARCH)"

curl -L -o /tmp/docker-$VER.tgz https://download.docker.com/linux/static/stable/$DOCKER_ARCH/docker-$VER.tgz
tar -xz -C /tmp -f /tmp/docker-$VER.tgz
mkdir -vp ~/.docker/cli-plugins/
curl --silent -L "https://github.com/docker/buildx/releases/download/v0.3.0/buildx-v0.3.0.linux-$BUILDX_ARCH" > ~/.docker/cli-plugins/docker-buildx
chmod a+x ~/.docker/cli-plugins/docker-buildx
mv /tmp/docker/* /usr/bin
docker run --privileged --rm tonistiigi/binfmt --install all
