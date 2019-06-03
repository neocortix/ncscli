#!/bin/bash
instanceId=$1
set -e

#date --iso-8601=seconds
date --iso-8601=seconds > installerDateTimes.txt


# add backports repo to apt sources
#/bin/bash --login -c "echo 'deb http://deb.debian.org/debian stretch-backports main' >> /etc/apt/sources.list"

#apt-get update
#date --iso-8601=seconds >> installerDateTimes.txt


# apt-install cffi and other python packages
#/bin/bash --login -c "apt-get install -q -y python3-cffi  python3-flask python3-gevent python3-zmq python3-requests python3-psutil"
#date --iso-8601=seconds >> installerDateTimes.txt


# apt-install msgpack (from backports)
#/bin/bash --login -c "apt-get install -q -y -t stretch-backports  python3-msgpack"

# install rsync
#/bin/bash --login -c "apt-get install -q -y rsync"

# create instanceId.txt
echo $instanceId > instanceId.txt

# capture ip addr from ipinfo.io
#curl ipinfo.io/ip > ipAddr.txt 2> /dev/null
date --iso-8601=seconds >> installerDateTimes.txt

# show dateTimes
cat installerDateTimes.txt
