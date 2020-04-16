#!/bin/bash
nInstances=${1:-0}
encryptFiles=${2:-True}
echo nInstances $nInstances
echo encryptFiles $encryptFiles

export PYTHONPATH=~/ncscli/examples/boinc:~/ncscli/ncscli
export PATH=~/ncscli/examples/boinc:$PATH:~/ncscli/ncscli

sdt=$(date -u --iso-8601=seconds)
sdtTag=$(date --date=$sdt +"%Y-%m-%d_%H%M%S")

cd ~/ncscli/examples/boinc

farm="rosetta_2"

dataDirPath=data/$farm
mkdir -p $dataDirPath
errLogPath=$dataDirPath/boincOps.log
echo $errLogPath

echo ">>>boincOpsCron.sh" >> $errLogPath
echo $sdtTag >> $errLogPath

./boincOps.py check @../../../myAuthToken --timeLimit 180 \
    --mongoHost codero4.neocortix.com --farm $farm 2>> $errLogPath

./boincOps.py terminateBad @../../../myAuthToken \
    --mongoHost codero4.neocortix.com --farm $farm 2>> $errLogPath

./boincOps.py collectStatus \
    --mongoHost codero4.neocortix.com --farm $farm 2>> $errLogPath

./boincOps.py report \
    --mongoHost codero4.neocortix.com --farm $farm 2>> $errLogPath

./boincOps.py launch @../../../myAuthToken --target 30 --timeLimit 300 \
    --mongoHost codero4.neocortix.com --farm $farm 2>> $errLogPath

echo ">>>boincOpsCron.sh finished"
echo ">>>boincOpsCron.sh finished" >> $errLogPath
