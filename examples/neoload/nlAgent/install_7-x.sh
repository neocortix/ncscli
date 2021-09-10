#!/usr/bin/env bash
set -ex
nlVersion=${1:-nlVersionNotSet}
truncVersion=${2:-truncVersionNotSet}
scoredVersion=${3:-scoredVersionNotSet}

curl -L -s -S https://d24mnm5myvorwj.cloudfront.net/documents/download/neoload/v$truncVersion/loadGenerator_"$scoredVersion"_linux_x64.tar.gz > loadGenerator_7_X_linux_x64.tar.gz
tar zxf loadGenerator_7_X_linux_x64.tar.gz
cp nlAgent/*.properties ~/neoload$truncVersion/conf
