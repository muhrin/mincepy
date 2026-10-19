#!/bin/bash
# Release a new version: bump the version on the current branch (usually develop), merge it into
# main and push both.  CI then publishes to PyPI and creates the v<version> tag and GitHub release.
#
# Usage: ./release.sh <version>

set -e

PACKAGE="mincepy"
REMOTE="muhrin"
RELEASE_BRANCH="main"
VERSION_FILE="src/${PACKAGE}/version.py"

version=$1
if [[ ! $version =~ ^[0-9]+\.[0-9]+\.[0-9]+([a-z0-9.]*)$ ]]; then
    echo "Usage: $0 <version> (e.g. 0.2.0)"
    exit 1
fi

if [[ -n $(git status --porcelain --untracked-files=no) ]]; then
    echo "There are uncommitted changes, please commit or stash them first"
    exit 1
fi

current_branch=$(git rev-parse --abbrev-ref HEAD)

while true; do
    read -p "Release version ${version} from ${current_branch}? " yn
    case $yn in
        [Yy]* ) break;;
        [Nn]* ) exit;;
        * ) echo "Please answer yes or no.";;
    esac
done

set -x

sed -i "s/^__version__ = .*/__version__ = \"${version}\"/" $VERSION_FILE
git add $VERSION_FILE
git commit -m "Release ${version}"

# Merge into the release branch (creating it if needed)
git fetch $REMOTE
if git show-ref --verify --quiet refs/heads/$RELEASE_BRANCH; then
    git checkout $RELEASE_BRANCH
elif git show-ref --verify --quiet refs/remotes/$REMOTE/$RELEASE_BRANCH; then
    git checkout -b $RELEASE_BRANCH --track $REMOTE/$RELEASE_BRANCH
else
    git checkout -b $RELEASE_BRANCH
fi
git merge --no-edit $current_branch
git checkout $current_branch

# Pushing the release branch triggers the publish job
git push $REMOTE $RELEASE_BRANCH $current_branch
