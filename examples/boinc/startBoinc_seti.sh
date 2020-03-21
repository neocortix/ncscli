#!/bin/bash
sudo apt-get -qq update
sudo apt-get -qq -y install boinc-client > /dev/null
boinc --dir /var/lib/boinc-client >>/var/log/boinc.log 2>>/var/log/boincerr.log &
sleep 5
boinccmd --client_version
sleep 5
boinccmd --project_attach http://setiathome.berkeley.edu/ 10911648_0d25e477b2a6700cb18011dff7db8274
