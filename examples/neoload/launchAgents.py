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


class neoloadFrameProcessor(batchRunner.frameProcessor):
    '''defines details for installing Neotys Load Generator agent on a worker'''

    def installerCmd( self ):
        return 'nlAgent/install.sh'

def startForwarders( agentInstances, forwarderHost, portRangeStart=7102, maxPort = 7199 ):
    forwardingCsvFilePath = outDataDir +'/agentForwarding.csv'
    with open( forwardingCsvFilePath, 'w' ) as csvOutFile:
        print( 'forwarding', 'instanceId', 'instHost', 'instSshPort', 'assignedPort',
            sep=',', file=csvOutFile
            )
        assignedPort = portRangeStart
        mappings = []
        for inst in agentInstances:
            iid = inst['instanceId']
            iidAbbrev = iid[0:8]
            sshSpecs = inst['ssh']
            instHost = sshSpecs['host']
            instPort = sshSpecs['port']
            user = sshSpecs['user']
            logger.info( '%d ->%s %s@%s:%s', assignedPort, iidAbbrev, user, instHost, instPort )
            cmd = ['ssh', '-fNT', '-o', 'ExitOnForwardFailure=yes', '-p', str(instPort), '-L',
                '*:'+str(assignedPort)+':localhost:7100', 
                '%s@%s' % (user, instHost)
            ]
            #logger.info( 'cmd: %s', cmd )
            rc = subprocess.call( cmd, shell=False, stdin=subprocess.DEVNULL )
            if rc:
                logger.warning( 'could not forward to %s (rc %d)', iidAbbrev, rc )
                continue

            mapping = '%s:%d' % (forwarderHost, assignedPort)
            mappings.append( mapping )
            print( mapping, iid, instHost, instPort, assignedPort,
                sep=',', file=csvOutFile
                )

            assignedPort += 1
            if assignedPort > maxPort:
                logger.warning( 'exceeded maxport (%d vs %d)', assignedPort, maxPort )
    print( 'forwarding:', ', '.join(mappings) )

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
    rc = batchRunner.runBatch(
        frameProcessor = neoloadFrameProcessor(),
        recruitOnly=True,
        commonInFilePath = 'nlAgent',
        authToken = os.getenv('NCS_AUTH_TOKEN') or 'YourAuthTokenHere',
        encryptFiles=False,
        timeLimit = 89*60,
        instTimeLimit = 12*60,
        frameTimeLimit = 15*60,
        filter = '{"dpr": ">=48","ram:":">=2800000000","app-version": ">=2.1.11"}',
        outDataDir = outDataDir,
        limitOneFramePerWorker = True,
        autoscaleMax = 1,
        startFrame = 1,
        endFrame = 12,
        nWorkers = 12
    )
    if rc == 0:
        forwarderHost = 'localhost'
        launchedJsonFilePath = outDataDir +'/recruitLaunched.json'
        launchedInstances = []
        # get instances from the launched json file
        with open( launchedJsonFilePath, 'r') as jsonInFile:
            try:
                launchedInstances = json.load(jsonInFile)  # an array
            except Exception as exc:
                logger.warning( 'could not load json (%s) %s', type(exc), exc )
        jobId = None
        if launchedInstances:
            jobId = launchedInstances[0]['job']
        logger.info( 'jobId: %s', jobId )
        startedInstances = [inst for inst in launchedInstances if inst['state'] == 'started' ]
        logger.info( 'started %d instances', len(startedInstances) )

        agentLogFilePath = '/root/.neotys/neoload/v7.6/logs/agent.log'
        # start the agent on each instance 
        starterCmd = 'cd ~/neoload7.6/ && /usr/bin/java -Dneotys.vista.headless=true -Xmx512m -Dvertx.disableDnsResolver=true -classpath $HOME/neoload7.6/.install4j/i4jruntime.jar:$HOME/neoload7.6/.install4j/launcherc0a362f9.jar:$HOME/neoload7.6/bin/*:$HOME/neoload7.6/lib/crypto/*:$HOME/neoload7.6/lib/*:$HOME/neoload7.6/lib/jdbcDrivers/*:$HOME/neoload7.6/lib/plugins/ext/* install4j.com.neotys.nl.agent.launcher.AgentLauncher_LoadGeneratorAgentService start &'
        stepStatuses = tellInstances.tellInstances( startedInstances, command=starterCmd,
            resultsLogFilePath=outDataDir +'/startAgents.jlog',
            timeLimit=3600,
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
                timeLimit=3600,
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
            logger.info( 'would forward ports for %d instances', len(goodInstances) )
            startForwarders( goodInstances, forwarderHost )
        if jobId:
            print( 'when you want to terminate these instances, use ncs.py sc terminate --jobId "%s"' % jobId)
    sys.exit( rc )
except KeyboardInterrupt:
    logger.warning( 'an interuption occurred')
