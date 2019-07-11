#!/usr/bin/env python3
''' rsync some file(s) to some target(s)'''

# standard library modules
import argparse
import asyncio
import asyncio.subprocess
import collections
import datetime
import getpass
import json
import logging
import os
import shutil
import subprocess
import sys
import time

# third-party modules
#import asyncssh

# neocortix modules
import asyncRsync
import tellInstances

logger = logging.getLogger(__name__)


def boolArg( v ):
    if v.lower() == 'true':
        return True
    elif v.lower() == 'false':
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

class eventTiming(object):
    '''stores name and beginning and ending of an arbitrary "event"'''
    def __init__(self, eventName, startDateTime=None, endDateTime=None):
        self.eventName = eventName
        self.startDateTime = startDateTime if startDateTime else datetime.datetime.now(datetime.timezone.utc)
        self.endDateTime = endDateTime
    
    def __repr__( self ):
        return str(self.toStrList())

    def finish(self):
        self.endDateTime = datetime.datetime.now(datetime.timezone.utc)

    def duration(self):
        if self.endDateTime:
            return self.endDateTime - self.startDateTime
        else:
            return datetime.timedelta(0)

    def toStrList(self):
        return [self.eventName, 
            self.startDateTime.isoformat(), 
            self.endDateTime.isoformat() if self.endDateTime else None
            ]


def logResult( key, value, instanceId ):
    pass
    '''
    if resultsLogFile:
        toLog = {key: value, 'instanceId': instanceId,
            'dateTime': datetime.datetime.now(datetime.timezone.utc).isoformat() }
        print( json.dumps( toLog, sort_keys=True ), file=resultsLogFile )
    '''
def triage( statuses ):
    goodOnes = []
    badOnes = []

    for status in statuses:
        if isinstance( status['status'], int) and status['status'] == 0:
            goodOnes.append( status['instanceId'])
        else:
            badOnes.append( status )
    return (goodOnes, badOnes)

def demuxResults( inFilePath ):
    byInstance = {}
    badOnes = set()
    topLevelKeys = collections.Counter()
    # demux by instance
    with open( inFilePath, 'rb' ) as inFile:
        for line in inFile:
            decoded = json.loads( line )
            for key in decoded:
                topLevelKeys[ key ] += 1
            iid = decoded.get( 'instanceId', '<unknown>')
            have = byInstance.get( iid, [] )
            have.append( decoded )
            byInstance[iid] = have
            if 'returncode' in decoded:
                rc = decoded['returncode']
                if rc:
                    logger.info( 'returncode %d for %s', rc, iid )
                    badOnes.add( iid )
            if 'exception' in decoded:
                logger.info( 'exception %s for %s', decoded['exception'], iid )
                badOnes.add( iid )
    return byInstance

def reportResults( byInstance, fullDetails, outFile ):
    logger.info( '%d instance keys', len(byInstance) )
    for iid, data in sorted(byInstance.items()):
        if iid == '<master>':
            continue
        for entry in data:
            if 'exception' in entry:                   
                print( entry['dateTime'], iid[0:16], 'exception',
                    entry['exception']['type'], entry['exception']['msg'], file=outFile )
            elif 'operation' in entry:
                pass
            elif 'returncode' in entry:
                if entry['returncode']:
                    print( entry['dateTime'], iid[0:16], 'returncode', entry['returncode'], file=outFile )
            elif 'stderr' in entry:
                if 'ttyname' not in entry['stderr']:
                    print( entry['dateTime'], iid[0:16], 'stderr', entry['stderr'], file=outFile )
            elif 'stdout' in entry:
                #print( entry['dateTime'], iid[0:16], 'stdout', entry['stdout'], file=outFile )
                if fullDetails:
                    print( entry['dateTime'], iid[0:16], entry['stdout'], file=outFile )
                elif 'packets transmitted, ' in entry['stdout']:
                    print( entry['dateTime'], iid[0:16], entry['stdout'], file=outFile )
                elif 'rtt min/avg/max/mdev' in entry['stdout']:
                    print( entry['dateTime'], iid[0:16], entry['stdout'], file=outFile )
            else:
                print( entry['dateTime'], iid[0:16], 'UNRECOGNIZED', entry, file=outFile )

    #if outFilePath:
    #    with open( outFilePath, 'w') as outFile:
    #        json.dump( byInstance, outFile, indent=2 )



if __name__ == "__main__":
    # configure logging
    logging.basicConfig(format='%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s', datefmt='%Y/%m/%d %H:%M:%S')
    logger.setLevel(logging.DEBUG)
    logger.debug('the logger is configured')
    asyncRsync.logger.setLevel(logging.INFO)
    tellInstances.logger.setLevel(logging.INFO)
    #asyncssh.set_log_level( logging.WARNING )
    #logging.getLogger("asyncio").setLevel(logging.DEBUG)

    dataDirPath = 'data'

    ap = argparse.ArgumentParser( description=__doc__ )
    ap.add_argument('instanceJsonFilePath', default=dataDirPath+'/launched.json')
    ap.add_argument('--sshAgent', type=boolArg, default=False, help='whether or not to use ssh agent')
    ap.add_argument('--extraTime', type=float, default=10, help='extra (in seconds) for master to wait for results')
    ap.add_argument('--fullDetails', type=boolArg, default=False, help='true for full details, false for summaries only')
    ap.add_argument('--interval', type=float, default=1, help='time (in seconds) between pings by an instance')
    ap.add_argument('--nPings', type=int, default=10, help='# of ping packets to send per instance')
    ap.add_argument('--timeLimit', type=float, default=10, help='maximum time (in seconds) to take (default=none (unlimited)')
    ap.add_argument('--targetHost', default='codero2.neocortix.com')
    args = ap.parse_args()
    logger.info( "args: %s", str(args) )

    startTime = time.time()
    eventTimings = []
    starterTiming = eventTiming('startup')

    instanceJsonFilePath = args.instanceJsonFilePath
    resultsLogFilePath = dataDirPath + '/pingALot.jlog'
    # truncate the resultsLogFile
    with open( resultsLogFilePath, 'wb' ) as xFile:
        pass # xFile.truncate()

    timeLimit = args.timeLimit  # seconds


    loadedInstances = None
    with open( instanceJsonFilePath, 'r' ) as jsonFile:
        loadedInstances = json.load(jsonFile)  # a list of dicts

    fieldsWanted = ['instanceId', 'state', 'ssh', 'device-location']
    strippedInstances = []
    for inst in loadedInstances:
        stripped = { key: inst[key] for key in fieldsWanted }
        strippedInstances.append( stripped )

    startedInstances = [inst for inst in strippedInstances if inst['state'] == 'started' ]
    goodInstances = startedInstances

    starterTiming.finish()
    eventTimings.append(starterTiming)

    allBad = []


    pingCmd = 'ping %s -D -c %s -w %f -i %f' \
        % (args.targetHost, args.nPings, args.timeLimit,  args.interval )
    if not args.fullDetails:
        pingCmd += ' -q'
    # tell them to ping
    stepTiming = eventTiming('tellInstances ping')
    logger.info( 'calling tellInstances')
    stepStatuses = tellInstances.tellInstances( startedInstances, pingCmd,
        resultsLogFilePath=resultsLogFilePath,
        download=None, downloadDestDir=None, jsonOut=None, sshAgent=args.sshAgent,
        timeLimit=args.timeLimit+args.extraTime, upload=None
        )
    stepTiming.finish()
    eventTimings.append(stepTiming)
    (goodOnes, badOnes) = triage( stepStatuses )
    allBad.extend( badOnes )
    #logger.info( 'asyncRsync statuses %s', stepStatuses )
    logger.info( 'ping %d good', len(goodOnes) )
    logger.info( 'ping bad %s', badOnes )

    goodInstances = [inst for inst in goodInstances if inst['instanceId'] in goodOnes ]


    logger.info( 'allBad has %d instances', len(allBad) )
    for badInst in allBad:
        status = badInst['status']
        logger.info( '%s status (%s) %s', badInst['instanceId'][0:16], type(status), status )


    resultsByInstance = demuxResults( resultsLogFilePath )
    reportResults( resultsByInstance, args.fullDetails, sys.stdout )

    elapsed = time.time() - startTime
    logger.info( 'finished; elapsed time %.1f seconds (%.1f minutes)', elapsed, elapsed/60 )
