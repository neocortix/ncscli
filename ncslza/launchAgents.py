#!/usr/bin/env python3
'''launches new NCS instances and starts the loadzilla agent on them'''
import argparse
import collections
#import datetime
import json
import logging
import os
import subprocess
import sys
#import time
# third-party module(s)
#import requests
# neocortix modules
import ncscli.batchRunner as batchRunner
import ncscli.tellInstances as tellInstances


def readJLog( inFilePath ):
    '''read JLog file, return list of decoded objects'''
    recs = []
    topLevelKeys = collections.Counter()  # for debugging
    # demux by instance
    with open( inFilePath, 'rb' ) as inFile:
        for line in inFile:
            decoded = json.loads( line )
            if isinstance( decoded, dict ):
                for key in decoded:
                    topLevelKeys[ key ] += 1
            recs.append( decoded )
    logger.debug( 'topLevelKeys: %s', topLevelKeys )
    return recs

def scriptDirPath():
    '''returns the absolute path to the directory containing this script'''
    return os.path.dirname(os.path.realpath(__file__))


class loadzillaFrameProcessor(batchRunner.frameProcessor):
    '''defines details for installing loadzilla agent on an instance'''
    agentDirPath = None  # will be set from cmd-line arg
    def installerCmd( self ):
        # cd into the agent dir and run it
        cmd = 'cd %s' % self.agentDirPath
        # might pass a dedication string here
        cmd += ' && java -jar %s -d %s &' % \
            (args.jarFileName, args.lzServer)
        cmd += ' sleep 10 && ps -ef'  # for debugging
        return cmd


# configure logger formatting
logger = logging.getLogger(__name__)
logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
logDateFmt = '%Y/%m/%d %H:%M:%S'
formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
logging.basicConfig(format=logFmt, datefmt=logDateFmt)
#batchRunner.logger.setLevel(logging.DEBUG)  # for more verbosity
logger.setLevel(logging.INFO)

ap = argparse.ArgumentParser( description=__doc__,
    fromfile_prefix_chars='@', formatter_class=argparse.ArgumentDefaultsHelpFormatter )
ap.add_argument( '--authToken', help='the NCS authorization token to use (or leave this out, to use NCS_AUTH_TOKEN env var' )
ap.add_argument( '--outDataDir', required=True, help='a path to the output data dir for this run' )
ap.add_argument( '--filter', help='json to filter instances for launch',
    default = '{ "app-version": ">=2.1.14", "regions": ["usa"], "dar": "==100", "dpr": ">=48", "ram": ">=3800000000", "storage": ">=2000000000" }'
    )
ap.add_argument( '--jarFileName', help='the name of the agent jar file',
    default='agent.jar'
    )
ap.add_argument( '--lzServer', help='the loadzilla server',
    default='saas.loadzilla.net'
    )
ap.add_argument( '--agentDir', help='the directory to upload to agents',
    default='lzAgent'
    )
ap.add_argument( '--nAgents', type=int, default=1, help='the number of load-generating agents to launch' )
args = ap.parse_args()


outDataDir = args.outDataDir

# abort if outDataDir is not empty enough
if os.path.isfile( outDataDir+'/batchRunner_results.jlog') \
    or os.path.isfile( outDataDir+'/recruitLaunched.json'):
    logger.error( 'please use a different outDataDir for each run' )
    sys.exit( 1 )

agentDirPath = args.agentDir.rstrip( '/' )  # trailing slash could cause problems with rsync
if agentDirPath:
    if not os.path.isdir( agentDirPath ):
        logger.error( 'the agentDirPath "%s" is not a directory', agentDirPath )
        sys.exit( 1 )
    loadzillaFrameProcessor.agentDirPath = agentDirPath
else:
    logger.error( 'this version requires an agentDirPath' )
    sys.exit( 1 )
logger.debug( 'agentDirPath: %s', agentDirPath )


try:
    # call runBatch to launch worker instances and install the load generator agent on them
    rc = batchRunner.runBatch(
        frameProcessor = loadzillaFrameProcessor(),
        recruitOnly=True,
        pushDeviceLocs=False,
        commonInFilePath = loadzillaFrameProcessor.agentDirPath,
        authToken = args.authToken or os.getenv('NCS_AUTH_TOKEN'),
        encryptFiles=False,
        timeLimit = 12*60,
        instTimeLimit = 6*60,
        filter = args.filter,
        outDataDir = outDataDir,
        nWorkers = args.nAgents
    )
    if rc == 0:
        launchedJsonFilePath = outDataDir +'/recruitLaunched.json'
        launchedInstances = []
        # get details of launched instances from the json file
        with open( launchedJsonFilePath, 'r') as jsonInFile:
            try:
                launchedInstances = json.load(jsonInFile)  # an array
            except Exception as exc:
                logger.warning( 'could not load json (%s) %s', type(exc), exc )
        instancesByIid = { inst['instanceId']: inst for inst in launchedInstances }
        startedInstances = [inst for inst in launchedInstances if inst['state'] == 'started' ]
        logger.info( '%d instances were launched', len(startedInstances) )

        installedInstances = []
        recruiterLog = readJLog( outDataDir +'/recruitInstances.jlog' )
        for logEntry in recruiterLog:
            if 'returncode' in logEntry and logEntry['returncode'] == 0:
                installedInstances.append( instancesByIid[ logEntry['instanceId'] ] )
        logger.info( '%d instances were installed', len(installedInstances) )
        installedIids = [inst['instanceId'] for inst in installedInstances ]

        # get the ip addr of each instance
        cmd = 'curl -s -S https://api.ipify.org > ipAddr.txt'
        stepStatuses = tellInstances.tellInstances( installedInstances, command=cmd,
            resultsLogFilePath=outDataDir +'/getIpAddr.jlog',
            download='ipAddr.txt', downloadDestDir=outDataDir +'/agentLogs',
            timeLimit=6*60,
            knownHostsOnly=True
            )
        logger.debug( 'download statuses: %s', stepStatuses )

        # merge the retrieved ip addrs into instances records in a new array
        goodInstances = []
        for iid in installedIids:
            ipFilePath = os.path.join( outDataDir, 'agentLogs', iid, 'ipAddr.txt' )
            if os.path.isfile( ipFilePath ):
                with open( ipFilePath, 'r' ) as ipFile:
                    ipAddr = ipFile.read().strip()
                    if ipAddr:
                        inst = instancesByIid.get( iid )
                        if inst:
                            inst['ipAddr'] = ipAddr
                            goodInstances.append( inst )

        # save json file of good instances with ipAddr fields
        with open( outDataDir+'/agents.json', 'w') as jsonOutFile:
            json.dump( goodInstances, jsonOutFile, indent=2 )

        # print some instance info just for fun
        for inst in goodInstances:
            loc = inst.get('device-location', {} )
            print( inst.get('instanceId','')[0:8],
                inst.get('ipAddr' ),
                loc.get( 'country-code'), loc.get( 'area'), loc.get( 'locality')
                )
        # plot a map of instances, if possible
        if goodInstances:
            plotterBinPath = os.path.join( scriptDirPath(), 'plotInstanceMap.py' )
            if os.path.isfile( plotterBinPath ):
                rc2 = subprocess.call( [sys.executable, plotterBinPath,
                    os.path.join( outDataDir, 'agents.json' ),
                    os.path.join( outDataDir, 'worldMap.png' )
                    ],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL )
                if rc2:
                    logger.warning( 'plotInstanceMap exited with returnCode %d', rc2 )

        if launchedInstances:
            print( 'when you want to terminate these instances, use %s terminateAgents.py "%s"'
                % (sys.executable, outDataDir), file=sys.stderr )
            if args.authToken:
                print( "(and don't forget to pass a --authToken arg)", file=sys.stderr )
    sys.exit( rc )
except KeyboardInterrupt:
    logger.warning( 'an interuption occurred')
