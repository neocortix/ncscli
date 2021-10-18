#!/usr/bin/env bash
# installs a specific version of gatling in the current dir, may leave a zip file as a side effect
version=${1:-"3.6.1"}

sudo apt-get -qq update > /dev/null
#echo installing jdk
#sudo apt-get -qq install -y openjdk-11-jdk-headless > /dev/null
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
export PATH=$PATH:$JAVA_HOME/bin
echo curling gatling
curl -SsL https://repo1.maven.org/maven2/io/gatling/highcharts/gatling-charts-highcharts-bundle/$version/gatling-charts-highcharts-bundle-$version-bundle.zip > gatling-charts-highcharts-bundle-$version-bundle.zip
echo installing unzip
sudo apt-get -qq install -y unzip > /dev/null
echo unzipping gatling
unzip -q gatling-charts-highcharts-bundle-$version-bundle.zip
cp gatlingWorker/gatling.conf ~/gatling-charts-highcharts-bundle-$version/conf
