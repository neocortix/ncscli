#!/usr/bin/env bash
sudo apt-get -qq update
sudo apt-get -qq install -y openjdk-11-jdk-headless
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
export PATH=$PATH:$JAVA_HOME/bin
curl -L https://repo1.maven.org/maven2/io/gatling/highcharts/gatling-charts-highcharts-bundle/3.4.0/gatling-charts-highcharts-bundle-3.4.0-bundle.zip > gatling-charts-highcharts-bundle-3.4.0-bundle.zip
sudo apt-get -qq install -y unzip
unzip -q gatling-charts-highcharts-bundle-3.4.0-bundle.zip
