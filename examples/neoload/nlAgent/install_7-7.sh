#!/usr/bin/env bash
set -ex

sudo apt-get -q update
sudo apt-get -q remove -y openjdk-11-jre-headless
sudo apt-get -q remove -y gpg

#sudo apt-get -q upgrade -y
sudo apt-get -q install -y wget apt-transport-https gnupg1
wget -qO - https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | sudo apt-key add -
echo "deb https://adoptopenjdk.jfrog.io/adoptopenjdk/deb buster main" | sudo tee /etc/apt/sources.list.d/adoptopenjdk.list
sudo apt-get -q update
sudo apt-get -q install -y adoptopenjdk-8-hotspot-jre=8u232-b09-2

curl -L -s -S https://d24mnm5myvorwj.cloudfront.net/documents/download/neoload/v7.7/loadGenerator_7_7_0_linux_x64.tar.gz > loadGenerator_7_7_0_linux_x64.tar.gz
tar zxf loadGenerator_7_7_0_linux_x64.tar.gz
cp nlAgent/*.properties ~/neoload7.7/conf
