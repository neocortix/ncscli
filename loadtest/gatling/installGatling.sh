#!/usr/bin/env bash
# installs a specific version of gatling in the current dir, may leave a zip file as a side effect
version=${1:-"3.6.1"}
curl -L https://repo1.maven.org/maven2/io/gatling/highcharts/gatling-charts-highcharts-bundle/$version/gatling-charts-highcharts-bundle-$version-bundle.zip > gatling-charts-highcharts-bundle-$version-bundle.zip
unzip -q gatling-charts-highcharts-bundle-$version-bundle.zip
