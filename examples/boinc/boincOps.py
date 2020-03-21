#!/usr/bin/env python3
"""
sumnmarizes the status of instances running boinc client
"""

# standard library modules
import argparse
import collections
#import contextlib
from concurrent import futures
import errno
import datetime
#import getpass
import json
import logging
#import math
import os
import re
#import socket
import shutil
import signal
import subprocess
import sys
import threading
import time
import uuid

# third-party module(s)
import pymongo
import requests

# neocortix modules
try:
    import ncs
except ImportError:
    # set system and python paths for default places, since path seems to be not set properly
    ncscliPath = os.path.expanduser('~/ncscli/ncscli')
    sys.path.append( ncscliPath )
    os.environ["PATH"] += os.pathsep + ncscliPath
    import ncs
import tellInstances

logger = logging.getLogger(__name__)


global resultsLogFile

# possible place for globals is this class's attributes
class g_:
    signaled = False


def sigtermHandler( sig, frame ):
    g_.signaled = True
    logger.warning( 'SIGTERM received; will try to shut down gracefully' )
    #raise SigTerm()

def sigtermSignaled():
    return g_.signaled

def sigtermNotSignaled():
    return not sigtermSignaled()


def boolArg( v ):
    '''use with ArgumentParser add_argument for (case-insensitive) boolean arg'''
    if v.lower() == 'true':
        return True
    elif v.lower() == 'false':
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


def demuxResults( inFilePath ):
    '''deinterleave jlog items into separate lists for each instance'''
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
                    #logger.info( 'returncode %d for %s', rc, iid )
                    badOnes.add( iid )
            if 'exception' in decoded:
                #logger.info( 'exception %s for %s', decoded['exception'], iid )
                badOnes.add( iid )
            if 'timeout' in decoded:
                #logger.info( 'timeout %s for %s', decoded['timeout'], iid )
                badOnes.add( iid )
    return byInstance, badOnes


def getStartedInstances( launchedJsonFilePath ):
    launchedInstances = []
    # get instances from the launched json file
    with open( launchedJsonFilePath, 'r') as jsonInFile:
        try:
            launchedInstances = json.load(jsonInFile)  # an array
        except Exception as exc:
            logger.warning( 'could not load json (%s) %s', type(exc), exc )
    exhaustedIids = [inst['instanceId'] for inst in launchedInstances if inst['state'] == 'exhausted' ]
    if exhaustedIids:
        logger.warning( 'terminating exhausted instances %s', exhaustedIids )
        ncs.terminateInstances( args.authToken, exhaustedIids )
    # proceed with instances that were actually started
    startedInstances = [inst for inst in launchedInstances if inst['state'] == 'started' ]

    return startedInstances

def ingestJson( srcFilePath, dbName, collectionName=None ):
    # uses some args from the global ArgumentParser
    cmd = [
        'mongoimport', '--host', args.mongoHost, '--port', str(args.mongoPort),
        '--drop',
        '-d', dbName, '-c', collectionName,
        srcFilePath
    ]
    if srcFilePath.endswith( '.json' ):
        cmd.append( '--jsonArray' )
    #logger.info( 'cmd: %s', cmd )
    subprocess.check_call( cmd )

def getStartedInstances( db ):
    collNames = db.list_collection_names( filter={ 'name': {'$regex': r'^launchedInstances_.*'} } )
    logger.info( 'launched collections: %s', collNames )
    startedInstances = []
    for collName in collNames:
        logger.info( 'getting instances from %s', collName )
        launchedColl = db[collName]
        inRecs = list( launchedColl.find() ) # fully iterates the cursor, getting all records
        if len(inRecs) <= 0:
            logger.warn( 'no launched instances found for %s', launchedTag )
        for inRec in inRecs:
            if 'instanceId' not in inRec:
                logger.warning( 'no instance ID in input record')
        startedInstances.extend( [inst for inst in inRecs if inst['state'] == 'started'] )
    return startedInstances

if __name__ == "__main__":
    # configure logger formatting
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)
    ncs.logger.setLevel(logging.INFO)
    #runDistributedBlender.logger.setLevel(logging.INFO)
    logger.setLevel(logging.INFO)
    tellInstances.logger.setLevel(logging.WARNING)
    logger.debug('the logger is configured')

    ap = argparse.ArgumentParser( description=__doc__,
        fromfile_prefix_chars='@', formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    ap.add_argument( 'action', help='the action to perform', 
        choices=['launch', 'list', 'import', 'check']
        )
    ap.add_argument( '--authToken', required=False, help='the NCS authorization token to use (required)' )
    ap.add_argument( '--dataDir', help='data directory', default='./data/' )
    ap.add_argument( '--inFilePath', help='a file to read (for some actions)', default='./data/' )
    ap.add_argument('--mongoHost', help='the host of mongodb server', default='localhost')
    ap.add_argument('--mongoPort', help='the port of mongodb server', default=27017)
    ap.add_argument( '--sshAgent', type=boolArg, default=False, help='whether or not to use ssh agent' )
    ap.add_argument( '--tag', required=False, help='tag for data dir and collection names' )
    ap.add_argument( '--timeLimit', type=int, help='time limit (in seconds) for the whole job',
        default=2*60 )
    args = ap.parse_args()

    dataDirPath = args.dataDir
    mclient = pymongo.MongoClient(args.mongoHost, args.mongoPort)
    dbName = 'boinc_seti_0'
    db = mclient[dbName]
    launchedTag = args.tag  # '2020-03-20_173400'  # '2019-03-19_130200'
    if args.action == 'import':
        launchedJsonFileName = 'launched_%s.json' % launchedTag
        launchedJsonFilePath = os.path.join( dataDirPath, launchedJsonFileName )
        collName = 'launchedInstances_' + launchedTag
        logger.info( 'importing %s to %s', launchedJsonFilePath, collName )
        ingestJson( launchedJsonFilePath, dbName, collName )
        collection = db[collName]
        logger.info( '%s has %d documents', collName, collection.count_documents({}) )

        sys.exit()
    elif args.action == 'check':
        startedInstances = getStartedInstances( db )

        instancesByIid = {inst['instanceId']: inst for inst in startedInstances }

        wereChecked = list( db['checkedInstances'].find() ) # fully iterates the cursor, getting all records
        checkedByIid = { inst['_id']: inst for inst in wereChecked }

        checkables = []
        if True:
            for iid, inst in instancesByIid.items():
                if iid in checkedByIid and checkedByIid[iid]['state'] == 'inaccessible':
                    pass
                else:
                    checkables.append( inst )
        else:
            # older logic just goes through already-checked
            for inRec in wereChecked:
                if inRec['state'] != 'inaccessible':
                    iid = inRec['_id']
                    inst = instancesByIid[ iid ]
                    ssh = inst['ssh']
                    checkables.append( {'instanceId': iid, 'ssh': ssh })
        logger.info( '%d instances checkable', len(checkables) )

        resultsLogFilePath=dataDirPath+'/checkInstances.jlog'
        workerCmd = 'boinccmd --get_tasks | grep "fraction done"'
        logger.info( 'calling tellInstances on %d instances', len(checkables))
        stepStatuses = tellInstances.tellInstances( checkables, workerCmd,
            resultsLogFilePath=resultsLogFilePath,
            download=None, downloadDestDir=None, jsonOut=None, sshAgent=args.sshAgent,
            timeLimit=min(args.timeLimit, args.timeLimit), upload=None, stopOnSigterm=True,
            knownHostsOnly=False
            )

        (eventsByInstance, badIidSet) = demuxResults( resultsLogFilePath )
        goodIids = []
        failedIids = []
        exceptedIids = []
        coll = db['checkedInstances']
        logger.info( 'updating database' )
        for iid, events in eventsByInstance.items():
            if iid.startswith( '<'):  # skip instances with names like '<master>'
                continue
            if iid not in instancesByIid:
                logger.warning( 'instance not found')
            inst = instancesByIid[ iid ]
            if not inst['ssh']:
                logger.warning( 'no ssh info')
            abbrevIid = iid[0:16]
            for event in events:
                #logger.info( 'event %s', event )
                state = 'checked'
                if 'exception' in event:
                    exceptedIids.append( iid )
                    state = 'inaccessible'
                    coll.update_one( {'_id': iid},
                        { "$set": { "state": state } },
                        upsert=True
                        )
                elif 'returncode' in event:
                    if event['returncode']:
                        state = 'failed'
                        failedIids.append( iid )
                    else:
                        state = 'checked'
                        goodIids.append( iid )
                    coll.update_one( {'_id': iid},
                        { "$set": { "state": state } },
                        upsert=True
                        )

        logger.info( '%d good; %d excepted; %d failed instances',
            len(goodIids), len(exceptedIids), len(failedIids) )
        sys.exit()

    useDb = True
    if useDb:
        collNames = db.list_collection_names( filter={ 'name': {'$regex': r'^launchedInstances_.*'} } )
        logger.info( 'launched collections: %s', collNames )
        #collName = 'launchedInstances_' + launchedTag
        #if collName not in db.list_collection_names():
        #    logger.warn( 'no collection found ("%s"', collName )
        startedInstances = []
        for collName in collNames:
            logger.info( 'getting instances from %s', collName )
            launchedColl = db[collName]
            inRecs = list( launchedColl.find() ) # fully iterates the cursor, getting all records
            if len(inRecs) <= 0:
                logger.warn( 'no launched instances found for %s', launchedTag )
            for inRec in inRecs:
                if 'instanceId' not in inRec:
                    logger.warning( 'no instance ID in input record')
            startedInstances.extend( [inst for inst in inRecs if inst['state'] == 'started'] )
    else:
        launchedJsonFileName = 'launched_2019-03-19_130200.json'
        #launchedJsonFileName = 'launched_2019-03-19_170900.json'
        launchedJsonFilePath = os.path.join( dataDirPath, launchedJsonFileName )
        startedInstances = getStartedInstances( launchedJsonFilePath )

    if not sigtermSignaled():
        resultsLogFilePath=dataDirPath+'/jobsSucceeded.jlog'
        workerCmd = "boinccmd --get_project_status | grep 'jobs succeeded: [^0]'"
        logger.info( 'calling tellInstances get task success on %d instances', len(startedInstances))
        stepStatuses = tellInstances.tellInstances( startedInstances, workerCmd,
            resultsLogFilePath=resultsLogFilePath,
            download=None, downloadDestDir=None, jsonOut=None, sshAgent=args.sshAgent,
            timeLimit=min(args.timeLimit, args.timeLimit), upload=None, stopOnSigterm=True,
            knownHostsOnly=False
            )
        #logger.info( 'stepStatuses %s', stepStatuses )
        # triage the statuses
        goodIids = []
        failedIids = []
        exceptedIids = []
        for statusRec in stepStatuses:
            #logger.info( 'keys: %s', statusRec.keys() )
            iid = statusRec['instanceId']
            abbrevId = iid[0:16]
            status = statusRec['status']
            #logger.info( "%s (%s) %s", abbrevId, type(status), status )
            if isinstance( status, int) and status == 0:
                goodIids.append( iid )
            elif isinstance( status, int ):
                failedIids.append( iid )
            else:
                exceptedIids.append( iid )
        logger.info( '%d good', len( goodIids ) )
        logger.info( '%d failed', len( failedIids ) )
        logger.info( '%d excepted', len( exceptedIids ) )

        (eventsByInstance, badIidSet) = demuxResults( resultsLogFilePath )
        totJobsSucc = 0
        for iid in goodIids:
            abbrevIid = iid[0:16]
            #logger.info( 'iid: %s', iid)
            events = eventsByInstance[ iid ]
            for event in events:
                #logger.info( 'event %s', event )
                #eventType = event.get('type')
                if 'stdout' in event:
                    #logger.info( 'STDOUT: %s', event )
                    stdoutStr = event['stdout']
                    if 'jobs succeeded: ' in stdoutStr:
                        numPart = stdoutStr.rsplit( 'jobs succeeded: ')[1]
                        #logger.info( 'numPart: %s', numPart)
                        nJobsSucc = int( numPart )
                        #if nJobsSucc >= 3:
                        #    logger.info( '%s nJobsSucc: %d', abbrevIid, nJobsSucc)
                        totJobsSucc += nJobsSucc
                    else:
                        logger.warning( 'not matched (%s)', event )
        logger.info( 'totJobsSucc: %d', totJobsSucc )
