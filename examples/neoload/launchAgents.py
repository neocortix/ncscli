#!/usr/bin/env python3
import datetime
import json
import logging
import os
import subprocess
import sys
import time

import ncscli.batchRunner as batchRunner
import ncscli.tellInstances as tellInstances
import startForwarders  # expected to be in the same directory


class neoloadFrameProcessor(batchRunner.frameProcessor):
    '''defines details for installing Neotys Load Generator agent on a worker'''

    def installerCmd( self ):
        return 'nlAgent/install.sh'


# configure logger formatting
#logging.basicConfig()
logger = logging.getLogger(__name__)
logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
logDateFmt = '%Y/%m/%d %H:%M:%S'
formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
logging.basicConfig(format=logFmt, datefmt=logDateFmt)
#batchRunner.logger.setLevel(logging.DEBUG)  # for more verbosity
logger.setLevel(logging.INFO)

dateTimeTag = datetime.datetime.now().strftime( '%Y-%m-%d_%H%M%S' )
outDataDir = 'data/neoload_' + dateTimeTag

try:
    # call runBatch to launch worker instances and install the load generator agent on them
    rc = batchRunner.runBatch(
        frameProcessor = neoloadFrameProcessor(),
        recruitOnly=True,
        commonInFilePath = 'nlAgent',
        authToken = os.getenv('NCS_AUTH_TOKEN') or 'YourAuthTokenHere',
        encryptFiles=False,
        timeLimit = 60*60,
        instTimeLimit = 15*60,
        filter = '{"dpr": ">=48","ram:":">=2800000000","app-version": ">=2.1.11"}',
        outDataDir = outDataDir,
        nWorkers = 10
    )
    if rc == 0:
        forwarderHost = 'localhost'
        portRangeStart=7102
        launchedJsonFilePath = outDataDir +'/recruitLaunched.json'
        launchedInstances = []
        # get details of launched instances from the json file
        with open( launchedJsonFilePath, 'r') as jsonInFile:
            try:
                launchedInstances = json.load(jsonInFile)  # an array
            except Exception as exc:
                logger.warning( 'could not load json (%s) %s', type(exc), exc )
        startedInstances = [inst for inst in launchedInstances if inst['state'] == 'started' ]
        logger.info( 'started %d instances', len(startedInstances) )

        agentLogFilePath = '/root/.neotys/neoload/v7.6/logs/agent.log'
        #agentLogFilePath = '/root/.neotys/neoload/v7.7/logs/agent.log'  # future
        # start the agent on each instance 
        starterCmd = 'cd ~/neoload7.6/ && /usr/bin/java -Dneotys.vista.headless=true -Xmx512m -Dvertx.disableDnsResolver=true -classpath $HOME/neoload7.6/.install4j/i4jruntime.jar:$HOME/neoload7.6/.install4j/launcherc0a362f9.jar:$HOME/neoload7.6/bin/*:$HOME/neoload7.6/lib/crypto/*:$HOME/neoload7.6/lib/*:$HOME/neoload7.6/lib/jdbcDrivers/*:$HOME/neoload7.6/lib/plugins/ext/* install4j.com.neotys.nl.agent.launcher.AgentLauncher_LoadGeneratorAgentService start &'
        #starterCmd = 'cd ~/neoload7.7/ && /usr/bin/java -Dneotys.vista.headless=true -Xmx512m -Dvertx.disableDnsResolver=true -classpath $HOME/neoload7.7/.install4j/i4jruntime.jar:$HOME/neoload7.7/.install4j/launcherc0a362f9.jar:$HOME/neoload7.7/bin/*:$HOME/neoload7.7/lib/crypto/*:$HOME/neoload7.7/lib/*:$HOME/neoload7.7/lib/jdbcDrivers/*:$HOME/neoload7.7/lib/plugins/ext/* install4j.com.neotys.nl.agent.launcher.AgentLauncher_LoadGeneratorAgentService start &'
        stepStatuses = tellInstances.tellInstances( startedInstances, command=starterCmd,
            resultsLogFilePath=outDataDir +'/startAgents.jlog',
            timeLimit=30*60,
            knownHostsOnly=True
            )
        logger.info( 'starter statuses: %s', stepStatuses )
        # make a list of instances where the agent was started
        goodIids = []
        for status in stepStatuses:
            if isinstance( status['status'], int) and status['status'] == 0:
                goodIids.append( status['instanceId'])
            else:
                logger.warning( 'could not start agent on %s', status['instanceId'][0:8] )
        goodInstances = [inst for inst in startedInstances if inst['instanceId'] in goodIids ]
        if goodInstances:
            time.sleep( 60 )
            # download the agent.log file from each instance
            stepStatuses = tellInstances.tellInstances( goodInstances,
                download=agentLogFilePath, downloadDestDir=outDataDir +'/agentLogs',
                timeLimit=30*60,
                knownHostsOnly=True
                )
            logger.info( 'download statuses: %s', stepStatuses )
            # make a list of instances where the log file was downloaded
            goodIids = []
            for status in stepStatuses:
                if isinstance( status['status'], int) and status['status'] == 0:
                    goodIids.append( status['instanceId'])
                else:
                    logger.warning( 'could not download log from %s', status['instanceId'][0:8] )
            goodInstances = [inst for inst in goodInstances if inst['instanceId'] in goodIids ]
            with open( outDataDir + '/startedAgents.json','w' ) as outFile:
                json.dump( goodInstances, outFile )

            # plot map of workers
            if os.path.isfile( outDataDir +'/startedAgents.json' ):
                rc2 = subprocess.call( ['./plotAgentMap.py', '--dataDirPath', outDataDir],
                    stdout=subprocess.DEVNULL )
                if rc2:
                    logger.warning( 'plotAgentMap exited with returnCode %d', rc2 )

            # start the ssh port-forwarding
            logger.info( 'would forward ports for %d instances', len(goodInstances) )
            startForwarders.startForwarders( goodInstances,
                forwarderHost=forwarderHost,
                portRangeStart=portRangeStart, maxPort=portRangeStart+100,
                forwardingCsvFilePath=outDataDir+'/agentForwarding.csv'
                )
        if launchedInstances:
            print( 'when you want to terminate these instances, use python3 terminateAgents.py "%s"'
                % (outDataDir + '/recruitLaunched.json'))
    sys.exit( rc )
except KeyboardInterrupt:
    logger.warning( 'an interuption occurred')
