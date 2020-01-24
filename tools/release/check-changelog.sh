#!/bin/bash

# Expect as input parameter the commits range to analyze.
if [ $# -ne 1 ]; then
    echo "Usage: $0 range"
    echo ""
    echo "  range   The commit range to compare as documented at:"
    echo "          https://git-scm.com/docs/gitrevisions"
    echo ""
    echo "Example:"
    echo "  $0 v0.4.0...master"
    echo ""
    exit 1
fi

# Find all merged PRs.
GIT_LOG=$(git log --pretty=format:"%s" $1)
PR_LIST=$(echo "$GIT_LOG" | grep -Eo '#[0-9]+')
PR_LIST_COUNT=$(echo "$PR_LIST" | wc -l | grep -Eo '[0-9]+')
PR_AUTHORS_COUNT=$(git log --pretty=format:"%an" $1 | sort | uniq -i | wc -l | grep -Eo '[0-9]+')
echo "Found ${PR_LIST_COUNT} PRs from ${PR_AUTHORS_COUNT} authors."
echo ""

# For each PR check if it's mentioned in the changelog.
echo "List of missing PR in the CHANGELOG.md:"
for PR in $PR_LIST; do
    grep -q "$PR" CHANGELOG.md
    if [ $? -eq 0 ]; then
        continue
    fi

    # Print 1 line for the missing PR
    echo -n "- ${PR}: "
    echo "$GIT_LOG" | grep "$PR"
done
