#!/usr/bin/env bash
set -e

jmeterVersion='5.4.1'
jmeterBinFilePath=apache-jmeter-$jmeterVersion/bin/jmeter
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

curl -s -S -L https://mirrors.sonic.net/apache/jmeter/binaries/apache-jmeter-$jmeterVersion.tgz > apache-jmeter.tgz
tar zxf apache-jmeter.tgz

apache-jmeter-$jmeterVersion/bin/jmeter --version
