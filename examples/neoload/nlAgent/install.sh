#!/usr/bin/env bash
sudo apt-get -qq update

sudo apt-get install -y wget
sudo apt-get install -y fontconfig  # not sure if this is needed

wget --no-verbose https://d24mnm5myvorwj.cloudfront.net/documents/download/neoload/v7.6/loadGenerator_7_6_0_linux_x64.tar.gz
tar zxf loadGenerator_7_6_0_linux_x64.tar.gz

cp nlAgent/agent.properties ~/neoload7.6/conf
ls -al ~/neoload7.6/conf

