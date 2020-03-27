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

farm="seti_3"

dataDirPath=data/$farm
mkdir -p $dataDirPath
errLogPath=$dataDirPath/boincOpsErr.log
echo $errLogPath

echo ">>>boincOpsCron.sh" >> $errLogPath
echo $sdtTag >> $errLogPath

./boincOps.py check \
    --mongoHost codero4.neocortix.com --farm $farm 2>> $errLogPath

./boincOps.py terminateBad @../../../myAuthToken \
    --mongoHost codero4.neocortix.com --farm $farm  2>> $errLogPath

./boincOps.py collectStatus \
    --mongoHost codero4.neocortix.com --farm $farm 2>> $errLogPath

./boincOps.py report \
    --mongoHost codero4.neocortix.com --farm $farm 2>> $errLogPath

./boincOps.py launch @../../../myAuthToken --target 3 --timeLimit 300 \
    --mongoHost codero4.neocortix.com --farm $farm 2>> $errLogPath

echo ">>>boincOpsCron.sh finished"
echo ">>>boincOpsCron.sh finished" >> $errLogPath
