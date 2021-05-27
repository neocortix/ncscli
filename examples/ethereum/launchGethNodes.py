#!/usr/bin/env python3
from concurrent import futures
import datetime
import json
import logging
import os
import subprocess
import sys
import time
# third-party module(s)
import requests
# neocortix modules
import ncscli.batchRunner as batchRunner
import ncscli.tellInstances as tellInstances


#gethVersion = '1.9.25'  # old version, deprecated
gethVersion = '1.10.1'  # currently supported
configName = 'priv_5'

'''
class g_:
    signaled = False
    interrupted = False

def sigtermSignaled():
    return g_.signaled
'''

class gethFrameProcessor(batchRunner.frameProcessor):
    '''defines details for installing a geth node on a worker'''

    def installerCmd( self ):
        cmd = 'netconfig/installGeth%s.sh' % gethVersion
        cmd += ' && geth init --datadir ether/%s netconfig/%s.genesis.json' % (configName, configName)
        cmd += ' && geth account new --datadir ether/%s --password pw.txt >accountInfo.txt' % (configName)
        cmd += r" && grep -o 'key:\s*\(.*\)' accountInfo.txt | grep -o '0x.*' > accountAddr.txt"
        return cmd


# configure logger formatting
logger = logging.getLogger(__name__)
logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
logDateFmt = '%Y/%m/%d %H:%M:%S'
formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
logging.basicConfig(format=logFmt, datefmt=logDateFmt)
#batchRunner.logger.setLevel(logging.DEBUG)  # for more verbosity
logger.setLevel(logging.INFO)

dateTimeTag = datetime.datetime.now().strftime( '%Y-%m-%d_%H%M%S' )
outDataDir = 'data/geth_' + dateTimeTag

'''
# if you are forwarding from a host other than this one, change this forwarderHost-setting code
forwarderHost = None
try:
    forwarderHost = requests.get( 'https://api.ipify.org' ).text
except forwarderHost:
    logger.warning( 'could not get public ip addr of this host')
if not forwarderHost:
    logger.error( 'forwarderHost not set')
    exit(1)
'''

try:
    # call runBatch to launch worker instances and install the geth client on them
    rc = batchRunner.runBatch(
        frameProcessor = gethFrameProcessor(),
        recruitOnly=True,
        pushDeviceLocs=False,
        commonInFilePath = 'netconfig',
        authToken = os.getenv('NCS_AUTH_TOKEN') or 'YourAuthTokenHere',
        encryptFiles=not True,
        timeLimit = 60*60,
        instTimeLimit = 20*60,
        filter = '{"dpr": ">=51", "ram:": ">=4000000000", "storage": ">=20000000000"}',
        outDataDir = outDataDir,
        nWorkers = 48
    )
    if rc == 0:
        #portRangeStart=7100
        launchedJsonFilePath = outDataDir +'/recruitLaunched.json'
        launchedInstances = []
        # get details of launched instances from the json file
        #TODO should get list of instances with good install, rather than all started instances
        with open( launchedJsonFilePath, 'r') as jsonInFile:
            try:
                launchedInstances = json.load(jsonInFile)  # an array
            except Exception as exc:
                logger.warning( 'could not load json (%s) %s', type(exc), exc )
        startedInstances = [inst for inst in launchedInstances if inst['state'] == 'started' ]
        logger.info( '%d instances were launched', len(startedInstances) )

        starterCmd = 'geth --config netconfig/%s.config.toml --password pw.txt --unlock $(cat accountAddr.txt) >> ether/%s/stdout.txt 2>>ether/%s/geth.log </dev/null &' % \
            (configName, configName, configName)
        # start the client on each instance 
        stepStatuses = tellInstances.tellInstances( startedInstances, command=starterCmd,
            resultsLogFilePath=outDataDir +'/startClients.jlog',
            timeLimit=30*60,
            knownHostsOnly=True
            )
        logger.debug( 'starter statuses: %s', stepStatuses )
        # make a list of instances where the client was started
        goodIids = []
        for status in stepStatuses:
            if isinstance( status['status'], int) and status['status'] == 0:
                goodIids.append( status['instanceId'])
            else:
                logger.warning( 'could not start agent on %s', status['instanceId'][0:8] )
        if launchedInstances:
            print( 'when you want to terminate these instances, use %s terminateGethNodes.py "%s"'
                % (sys.executable, outDataDir))
    sys.exit( rc )
except KeyboardInterrupt:
    logger.warning( 'an interuption occurred')
