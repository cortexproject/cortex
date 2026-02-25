#!/bin/bash

set -x
VER="29.2.1"
curl -L -o /tmp/docker-$VER.tgz https://download.docker.com/linux/static/stable/x86_64/docker-$VER.tgz
tar -xz -C /tmp -f /tmp/docker-$VER.tgz
mkdir -vp ~/.docker/cli-plugins/
curl --silent -L "https://github.com/docker/buildx/releases/download/v0.31.1/buildx-v0.31.1.linux-amd64" > ~/.docker/cli-plugins/docker-buildx
chmod a+x ~/.docker/cli-plugins/docker-buildx
mv /tmp/docker/* /usr/bin
docker run --privileged --rm tonistiigi/binfmt --install all
