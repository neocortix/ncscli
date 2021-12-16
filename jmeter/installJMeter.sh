#!/usr/bin/env bash
set -e
jmeterVersion=${1:-'5.4.1'}

jmeterBinFilePath=apache-jmeter-$jmeterVersion/bin/jmeter.sh
if test -f "$jmeterBinFilePath"; then
    echo "$jmeterBinFilePath is already installed."
    exit 0
fi

if hash javac 2>/dev/null; then
    jdkVersion="$(javac -version 2>&1)"
    echo jdk $jdkVersion is installed
else
    echo NO jdk is installed
    exit 1
fi

curl -s -S -L https://dlcdn.apache.org/jmeter/binaries/apache-jmeter-$jmeterVersion.tgz > apache-jmeter-$jmeterVersion.tgz
tar zxf apache-jmeter-$jmeterVersion.tgz

apache-jmeter-$jmeterVersion/bin/jmeter.sh --version
