#!/bin/bash

DIR=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)
cd $DIR

set -e

remote=$(git rev-parse "@{u}") # "@{u}" gets the current upstream branch
local=$(git rev-parse @) # '@' gets the current local branch
# check if local commit syncs with remote
if [ "$remote" != "$local" ]; then
    echo "Error: local commit does not match remote. Exiting release script."
    exit 1
fi

state=`curl -s https://api.github.com/repos/influxdata/flux/commits/${local}/status | jq -r ".state"`
if [ "$state" != "success" ]; then
    echo "Error: commit state is \"$state\". Exiting release script"
    exit 1
fi

# remove any excess brackets, space/tab characters and 'origin' branch and sort the tags
git_remote_tags () { git ls-remote --tags origin | grep -v '{}' | sort | tr -d [[:blank:]] ; }
git_local_tags () { git show-ref --tags | grep -v '{}' | grep -v 'origin'| grep -v 'list'| sort | tr -d [[:blank:]] ; }

# check if local tags are different from remote tags
if ! diff -q <(git_remote_tags) <(git_local_tags) &>/dev/null; then
    echo "Error: local tags do not match remote. Exiting release script."
    diff -u <(git_remote_tags) <(git_local_tags)
    exit 1
fi

# cut the next Flux release
version=$1
if [ "$version" == "" ]; then
    version=$(./gotool.sh github.com/influxdata/changelog nextver)
fi
if [[ $version != v* ]]; then
    echo "Error: invalid tag specified. Tag must be prefixed with 'v'."
    exit 1
fi
git tag -s -m "Release $version" "$version"
git push origin "$version"
