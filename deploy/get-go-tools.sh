#/bin/bash

## Downloads the go toolkit into a local temp directory
## Exports variables and alias to key golang tools
#
# Usage: source get-go-tools.sh

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

function get_go() {
    WORK_DIR=$SCRIPT_DIR/tmp

    GO_VERSION=1.11
    ARCH=amd64
    if [[ $OSTYPE == linux-gnu ]]; then
        OS=linux
        GO_CHECKSUM=b3fcf280ff86558e0559e185b601c9eade0fd24c900b4c63cd14d1d38613e499
    else
        echo "Unsupported OS. Please use Linux."
        exit 1
    fi
    GO_DOWNLOAD_URL=https://dl.google.com/go/go$GO_VERSION.$OS-$ARCH.tar.gz
    GO_DOWNLOAD_FILE=$WORK_DIR/go$GO_VERSION.$OS-$ARCH.tar.gz

    mkdir -p $WORK_DIR/
    if [ -f $GO_DOWNLOAD_FILE ]; then
        SHASUM=$(sha256sum $GO_DOWNLOAD_FILE)
        SUM=$(echo $SHASUM | cut -d' ' -f1)
        if [ "$SUM" == "$GO_CHECKSUM" ]; then
            echo "--> Using cached gotools at $GO_DOWNLOAD_FILE"
        fi
    else
        echo "--> Downloading $GO_DOWNLOAD_FILE..."
        curl -Lo $GO_DOWNLOAD_FILE $GO_DOWNLOAD_URL
    fi

    echo "--> Unpacking go tools"
    mkdir -p $(pwd)/go
    rm -rf $(pwd)/go/*
    tar -zxf $GO_DOWNLOAD_FILE -C $(pwd)/
}

get_go

# To avoid conflicting with system-wide installations of go
# setup aliases and environment variables for the downloaded toolkit
export PATH=$PATH:$(pwd)/go/bin/
export GOPATH=$(pwd)/gopath
mkdir -p $GOPATH/
export PATH=$PATH:$GOPATH/bin
