#!/usr/bin/env bash

curl -L -s -S https://d24mnm5myvorwj.cloudfront.net/documents/download/neoload/v7.6/loadGenerator_7_6_0_linux_x64.tar.gz > loadGenerator_7_6_0_linux_x64.tar.gz
tar zxf loadGenerator_7_6_0_linux_x64.tar.gz

cp nlAgent/agent.properties ~/neoload7.6/conf
