
PACKAGE="mincepy"
REMOTE="muhrin"
VERSION_FILE=${PACKAGE}/__init__.py

version=$1
while true; do
    read -p "Release version ${version}? " yn
    case $yn in
        [Yy]* ) break;;
        [Nn]* ) exit;;
        * ) echo "Please answer yes or no.";;
    esac
done

set -x

sed -i "/^__version__/c __version__ = ${version}" $VERSION_FILE

current_branch=`git rev-parse --abbrev-ref HEAD`

tag="v${version}"
relbranch="release-${version}"

echo Releasing version $version

git checkout -b $relbranch
git add ${VERSION_FILE}
git commit --no-verify -m "Release ${version}"

git tag -a $tag -m "Version $version"


# Merge into master

git checkout master
git merge $relbranch

# And back into the working branch (usually develop)
git checkout $current_branch
git merge $relbranch

git branch -d $relbranch

# Push everything
git push --tags $REMOTE master $current_branch


# Release on pypi
flit publish
