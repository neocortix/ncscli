#!/usr/bin/env bash
set -ex

curl -L -s -S https://d24mnm5myvorwj.cloudfront.net/documents/download/neoload/v7.10/loadGenerator_7_10_0_linux_x64.tar.gz > loadGenerator_7_10_0_linux_x64.tar.gz
tar zxf loadGenerator_7_10_0_linux_x64.tar.gz
cp nlAgent/*.properties ~/neoload7.10/conf
