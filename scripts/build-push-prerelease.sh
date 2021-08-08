#!/bin/bash
#
# Build a prerelease tag for the package so it can be tested before
# a final version is merged and released
#
# Similar to how toniq sdk does prerelease uploads to pypi server

GIT_SHA=`git rev-parse --short HEAD`
VERSION_TAG=`cat VERSION | tr -d '\n'`

if [ -z $PYPI_ADMIN_PASSWORD ]; then
  echo "PYPI_ADMIN_PASSWORD cannot be empty"
  exit 1
fi

# Split a.b.c into a, b, c
VERSION_NUMBERS=($(echo "$VERSION_TAG" | tr '.' '\n'))
MAJOR="${VERSION_NUMBERS[0]}"
MINOR="${VERSION_NUMBERS[1]}"
PATCH="${VERSION_NUMBERS[2]}"

# We want major.minor.dev{patch}+sha
export VERSION=$MAJOR.$MINOR.dev$PATCH+${GIT_SHA}
python setup.py sdist bdist_wheel

ARCHIVE_FORMAT=json-eater-$VERSION*

#twine doesn't upload the same package twice
twine upload dist/* -u admin -p $PYPI_ADMIN_PASSWORD \
 --repository-url https://pypiserver.dev.docai.beer \
 --verbose --skip-existing