#!/bin/bash
farm=${1:-farmNotSet}
argFile=${2:-reaper.conf}
echo $0
echo farm $farm
echo argFile $argFile


export PYTHONPATH=~/ncscli
export PATH=$PATH:~/ncscli/ncscli

sdt=$(date -u --iso-8601=seconds)
sdtTag=$(date --date=$sdt +"%Y-%m-%d_%H%M%S")

cd ~/ncscli/examples/neoload

dataDirPath=data/$farm
#mkdir -p $dataDirPath
errLogPath=$dataDirPath/checkAgentsInfinite.log
echo $errLogPath
touch $errLogPath

echo ">>>$0" >> $errLogPath
echo $sdtTag >> $errLogPath

while true
do
    #logrotate --state $dataDirPath/logrotate.state $dataDirPath/logrotate.conf
    
    ./checkAgents.py @$argFile --terminateBad True \
        --dataDirPath $dataDirPath 2>> $errLogPath

    echo ">>>$0 end of loop" >> $errLogPath
    sleep 120
done
