#!/usr/bin/env bash
sudo apt-get -qq update
#echo installing jdk
#sudo apt-get -qq install -y openjdk-11-jdk-headless > /dev/null
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
export PATH=$PATH:$JAVA_HOME/bin
echo curling gatling
curl -L https://repo1.maven.org/maven2/io/gatling/highcharts/gatling-charts-highcharts-bundle/3.4.0/gatling-charts-highcharts-bundle-3.4.0-bundle.zip > gatling-charts-highcharts-bundle-3.4.0-bundle.zip
echo installing unzip
sudo apt-get -qq install -y unzip > /dev/null
echo unzipping gatling
unzip -q gatling-charts-highcharts-bundle-3.4.0-bundle.zip
cp gatlingWorker/gatling.conf ~/gatling-charts-highcharts-bundle-3.4.0/conf
