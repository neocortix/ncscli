#!/usr/bin/env bash
set -e

curl -L -s -S https://archive-1.neocortix.net/neotys/neoload/v7.10/loadGenerator_7_10_0_slim.tar.gz > loadGenerator_7_10_0_slim.tar.gz
#curl -L -s -S https://archive.neocortix.net/neotys/neoload/v7.10/loadGenerator_7_10_0_slim.tar.gz > loadGenerator_7_10_0_slim.tar.gz
#curl -L -s -S https://d1ki6ltcdxd6oo.cloudfront.net/neotys/neoload/v7.10/loadGenerator_7_10_0_slim.tar.gz > loadGenerator_7_10_0_slim.tar.gz
#curl -L -s -S https://d24mnm5myvorwj.cloudfront.net/documents/download/neoload/v7.10/loadGenerator_7_10_0_unix.tar.gz > loadGenerator_7_10_0_unix.tar.gz
#mv nlAgent/loadGenerator_7_10_0_slim.tar.gz .
tar zxf loadGenerator_7_10_0_slim.tar.gz

cp nlAgent/*.properties ~/neoload7.10/conf
# a symlink so neoload agent can find java to run the LG
ln -s /usr/lib/jvm/java-11-openjdk-arm64 $HOME/neoload7.10/jre

#COULD check available memory and bound ports
