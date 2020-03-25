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


def getStartedInstancesFromFile( launchedJsonFilePath ):
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

def launchInstances( authToken, nInstances, sshClientKeyName, launchedJsonFilepath,
        filtersJson=None, encryptFiles=True ):
    returnCode = 13
    logger.info( 'launchedJsonFilepath %s', launchedJsonFilepath )
    try:
        with open( launchedJsonFilepath, 'w' ) as launchedJsonFile:
            returnCode = ncs.launchScInstances( authToken, encryptFiles, numReq=nInstances,
                sshClientKeyName=sshClientKeyName, jsonFilter=filtersJson,
                okToContinueFunc=sigtermNotSignaled, jsonOutFile=launchedJsonFile )
    except Exception as exc: 
        logger.error( 'exception while launching instances (%s) %s', type(exc), exc, exc_info=True )
        returnCode = 99
    return returnCode

def recruitInstances( nWorkersWanted, launchedJsonFilePath, launchWanted, resultsLogFilePath ):
    '''launch instances and install boinc on them;
        terminate those that could not install; return list of good instances'''
    logger.info( 'recruiting up to %d instances', nWorkersWanted )
    goodInstances = []
    if launchWanted:
        nAvail = ncs.getAvailableDeviceCount( args.authToken, filtersJson=args.filter )
        if nWorkersWanted > (nAvail + 0):
            logger.error( 'not enough devices available (%d requested, %d avail)', nWorkersWanted, nAvail )
            raise ValueError( 'not enough devices available')
        # upload an sshClientKey for launch (unless one was provided)
        if args.sshClientKeyName:
            sshClientKeyName = args.sshClientKeyName
        else:
            keyContents = loadSshPubKey()
            randomPart = str( uuid.uuid4() )[0:13]
            keyContents += ' #' + randomPart
            sshClientKeyName = 'boinc_%s' % (randomPart)
            respCode = ncs.uploadSshClientKey( args.authToken, sshClientKeyName, keyContents )
            if respCode < 200 or respCode >= 300:
                logger.warning( 'ncs.uploadSshClientKey returned %s', respCode )
                sys.exit( 'could not upload SSH client key')
        #launch
        #logResult( 'operation', {'launchInstances': nWorkersWanted}, '<recruitInstances>' )
        #logOperation( 'launchInstances', nWorkersWanted, '<recruitInstances>' )
        rc = launchInstances( args.authToken, nWorkersWanted,
            sshClientKeyName, launchedJsonFilePath, filtersJson=args.filter,
            encryptFiles = args.encryptFiles
            )
        if rc:
            logger.info( 'launchInstances returned %d', rc )
        # delete sshClientKey only if we just uploaded it
        if sshClientKeyName != args.sshClientKeyName:
            logger.info( 'deleting sshClientKey %s', sshClientKeyName)
            ncs.deleteSshClientKey( args.authToken, sshClientKeyName )
    launchedInstances = []
    # get instances from the launched json file
    with open( launchedJsonFilePath, 'r') as jsonInFile:
        try:
            launchedInstances = json.load(jsonInFile)  # an array
        except Exception as exc:
            logger.warning( 'could not load json (%s) %s', type(exc), exc )
    if len( launchedInstances ) < nWorkersWanted:
        logger.warning( 'could not launch as many instances as wanted (%d vs %d)',
            len( launchedInstances ), nWorkersWanted )
    nonstartedIids = [inst['instanceId'] for inst in launchedInstances if inst['state'] != 'started' ]
    if nonstartedIids:
        logger.warning( 'terminating non-started instances %s', nonstartedIids )
        ncs.terminateInstances( args.authToken, nonstartedIids )
    # proceed with instances that were actually started
    startedInstances = [inst for inst in launchedInstances if inst['state'] == 'started' ]
    if not startedInstances:
        return []
    if not sigtermSignaled():
        installerCmd = './startBoinc_seti.sh'
        logger.info( 'calling tellInstances to install on %d instances', len(startedInstances))
        stepStatuses = tellInstances.tellInstances( startedInstances, installerCmd,
            resultsLogFilePath=resultsLogFilePath,
            download=None, downloadDestDir=None, jsonOut=None, sshAgent=args.sshAgent,
            timeLimit=args.timeLimit, upload='startBoinc_seti.sh', stopOnSigterm=False,
            knownHostsOnly=False
            )
        # SHOULD restore our handler because tellInstances may have overridden it
        #signal.signal( signal.SIGTERM, sigtermHandler )
        if not stepStatuses:
            logger.warning( 'no statuses returned from installer')
            startedIids = [inst['instanceId'] for inst in startedInstances]
            #logOperation( 'terminateBad', startedIids, '<recruitInstances>' )
            ncs.terminateInstances( args.authToken, startedIids )
            return []
        #(goodOnes, badOnes) = triage( stepStatuses )
        # separate good tellInstances statuses from bad ones
        goodOnes = []
        badOnes = []
        for status in stepStatuses:
            if isinstance( status['status'], int) and status['status'] == 0:
                goodOnes.append( status['instanceId'])
            else:
                badOnes.append( status )
        
        logger.info( '%d good installs, %d bad installs', len(goodOnes), len(badOnes) )
        #logger.info( 'stepStatuses %s', stepStatuses )
        goodInstances = [inst for inst in startedInstances if inst['instanceId'] in goodOnes ]
        badIids = []
        for status in badOnes:
            badIids.append( status['instanceId'] )
        if badIids:
            #logOperation( 'terminateBad', badIids, '<recruitInstances>' )
            ncs.terminateInstances( args.authToken, badIids )

    return goodInstances

def loadSshPubKey():
    '''returns the contents of current user public ssh client key'''
    pubKeyFilePath = os.path.expanduser( '~/.ssh/id_rsa.pub' )
    with open( pubKeyFilePath ) as inFile:
        contents = inFile.read()
    return contents

def report_cc_status( db, dataDirPath ):
    # will report only on "checked" instances
    wereChecked = db['checkedInstances'].find( {'state': 'checked' } )
    reportables = []
    for inst in wereChecked:
        #iid =inst['_id']
        if 'ssh' not in inst:
            logger.warning( 'no ssh info from checkedInstances for %s', inst )
        else:
            inst['instanceId'] =inst ['_id']
            reportables.append( inst )

    resultsLogFilePath=dataDirPath+'/report_cc_status.jlog'
    workerCmd = "boinccmd --get_cc_status"
    logger.info( 'calling tellInstances to get cc_status report on %d instances', len(reportables))
    stepStatuses = tellInstances.tellInstances( reportables, workerCmd,
        resultsLogFilePath=resultsLogFilePath,
        download=None, downloadDestDir=None, jsonOut=None, sshAgent=args.sshAgent,
        timeLimit=min(args.timeLimit, args.timeLimit), upload=None, stopOnSigterm=True,
        knownHostsOnly=False
        )
    # triage the statuses
    goodIids = []
    failedIids = []
    exceptedIids = []
    for statusRec in stepStatuses:
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
    logger.info( '%d completed, %d failed, %d exceptions',
        len( goodIids ), len( failedIids ), len( exceptedIids ) )
    # read back the results
    (eventsByInstance, badIidSet) = demuxResults( resultsLogFilePath )
    nCounted = 0
    for iid in goodIids:
        abbrevIid = iid[0:16]
        events = eventsByInstance[ iid ]
        onBatteries = False
        for event in events:
            if 'stdout' in event:
                stdoutStr = event['stdout']
                if 'batteries' in stdoutStr:
                    logger.info( "%s", stdoutStr )
                    onBatteries = True
        if onBatteries:
            nCounted += 1
            logger.warning( 'instance %s on batteries', abbrevIid )
    logger.info( 'nOnBatteries: %d', nCounted )


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
        choices=['launch', 'report', 'import', 'check', 'terminateBad', 'terminateAll']
        )
    ap.add_argument( '--authToken', help='the NCS authorization token to use (required for launch or terminate)' )
    ap.add_argument( '--count', type=int, help='the number of instances (for launch)' )
    ap.add_argument( '--target', type=int, help='target number of working instances (for launch)' )
    ap.add_argument( '--dataDir', help='data directory', default='./data/' )
    ap.add_argument( '--encryptFiles', type=boolArg, default=False, help='whether to encrypt files on launched instances' )
    ap.add_argument( '--farm', required=True, help='the name of the virtual boinc farm' )
    ap.add_argument( '--filter', help='json to filter instances for launch',
        default='{"dpr": ">=33","ram:":">=3400000000", "user":"shell.cortix@gmail.com"}' )
    ap.add_argument( '--inFilePath', help='a file to read (for some actions)', default='./data/' )
    ap.add_argument( '--mongoHost', help='the host of mongodb server', default='localhost')
    ap.add_argument( '--mongoPort', help='the port of mongodb server', default=27017)
    ap.add_argument( '--sshAgent', type=boolArg, default=False, help='whether or not to use ssh agent' )
    ap.add_argument( '--sshClientKeyName', help='the name of the uploaded ssh client key to use (advanced)' )
    ap.add_argument( '--tag', required=False, help='tag for data dir and collection names' )
    ap.add_argument( '--timeLimit', type=int, help='time limit (in seconds) for the whole job',
        default=2*60 )
    args = ap.parse_args()

    logger.info( 'starting action "%s" for farm "%s"', args.action, args.farm )

    dataDirPath = os.path.join( args.dataDir, args.farm )
    os.makedirs( dataDirPath, exist_ok=True )
    
    mclient = pymongo.MongoClient(args.mongoHost, args.mongoPort)
    dbName = 'boinc_' + args.farm  # was 'boinc_seti_0'
    db = mclient[dbName]
    launchedTag = args.tag  # '2020-03-20_173400'  # '2019-03-19_130200'
    if args.action == 'launch':
        startDateTime = datetime.datetime.now( datetime.timezone.utc )
        dateTimeTagFormat = '%Y-%m-%d_%H%M%S'  # cant use iso format dates in filenames because colons
        dateTimeTag = startDateTime.strftime( dateTimeTagFormat )

        launchedJsonFilePath = os.path.join( dataDirPath, 'launched_%s.json' % dateTimeTag )
        resultsLogFilePath = os.path.join( dataDirPath, 'startBoinc_%s.jlog' % dateTimeTag )
        collName = 'launchedInstances_' + dateTimeTag

        if args.count and (args.count > 0):
            nToLaunch = args.count
        elif args.target and (args.target > 0):
            logger.info( 'launcher target: %s', args.target )
            # try to reach target by counting existing working instances and launching more
            mQuery =  {'state': 'checked' }
            nExisting = db['checkedInstances'].count_documents( mQuery )
            logger.info( '%d workers checked', nExisting )
            nToLaunch = int( (args.target - nExisting) * 1.5 )
            nToLaunch = max( nToLaunch, 0 )

            nAvail = ncs.getAvailableDeviceCount( args.authToken, filtersJson=args.filter )
            logger.info( '%d devices available', nAvail )

            nToLaunch = min( nToLaunch, nAvail )
        else:
            sys.exit( 'error: no positive --count or --target supplied')
        logger.info( 'would launch %d instances', nToLaunch )
        if nToLaunch <= 0:
            sys.exit( 'NOT launching additional instances')
        logger.info( 'local files %s and %s', launchedJsonFilePath, resultsLogFilePath )

        goodInstances = recruitInstances( nToLaunch, launchedJsonFilePath, True, resultsLogFilePath )
        if goodInstances:
            logger.info( 'ingesting into %s', collName )
            ingestJson( launchedJsonFilePath, dbName, collName )
        else:
            logger.warning( 'no instances recruited' )
        sys.exit()
    elif args.action == 'import':
        launchedJsonFileName = 'launched_%s.json' % launchedTag
        launchedJsonFilePath = os.path.join( dataDirPath, launchedJsonFileName )
        collName = 'launchedInstances_' + launchedTag
        logger.info( 'importing %s to %s', launchedJsonFilePath, collName )
        ingestJson( launchedJsonFilePath, dbName, collName )
        collection = db[collName]
        logger.info( '%s has %d documents', collName, collection.count_documents({}) )

        sys.exit()
    elif args.action == 'check':
        if not db.list_collection_names():
            logger.warn( 'no collections found found for db %s', dbName )
            sys.exit()

        startedInstances = getStartedInstances( db )

        instancesByIid = {inst['instanceId']: inst for inst in startedInstances }

        wereChecked = list( db['checkedInstances'].find() ) # fully iterates the cursor, getting all records
        checkedByIid = { inst['_id']: inst for inst in wereChecked }
        # after checking, each checked instance will have "state" set to "checked", "failed", "inaccessible", or "terminated"
        checkables = []
        for iid, inst in instancesByIid.items():
            if iid in checkedByIid and checkedByIid[iid]['state'] != 'checked':
                pass
            else:
                checkables.append( inst )
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
            abbrevIid = iid[0:16]
            instCheck = checkedByIid.get( iid )
            if instCheck:
                nExceptions = instCheck.get( 'nExceptions', 0)
                nFailures = instCheck.get( 'nFailures', 0)
            else:
                nExceptions = 0
                nFailures = 0
            if nExceptions:
                logger.warning( 'instance %s previouosly had %d exceptions', iid, nExceptions )
            state = 'checked'
            for event in events:
                #logger.info( 'event %s', event )
                if 'exception' in event:
                    nExceptions += 1
                    if nExceptions >= 2:
                        state = 'inaccessible'
                    exceptedIids.append( iid )
                    coll.update_one( {'_id': iid},
                        { "$set": { "state": state, 'nExceptions': nExceptions } },
                        upsert=True
                        )
                elif 'returncode' in event:
                    if event['returncode']:
                        nFailures += 1
                        if nFailures >= 2:
                            state = 'failed'
                        failedIids.append( iid )
                    else:
                        goodIids.append( iid )
                    coll.update_one( {'_id': iid},
                        { "$set": { "state": state, 'nFailures': nFailures,
                            'ssh': inst.get('ssh'), 'devId': inst.get('device-id') } 
                            },
                        upsert=True
                        )
        logger.info( '%d good; %d excepted; %d failed instances',
            len(goodIids), len(exceptedIids), len(failedIids) )
        sys.exit()
    elif args.action == 'terminateBad':
        if not args.authToken:
            sys.exit( 'error: can not terminate because no authToken was passed')
        coll = db['checkedInstances']
        wereChecked = list( coll.find() ) # fully iterates the cursor, getting all records
        terminatedIids = []
        for checkedInst in wereChecked:
            state = checkedInst.get( 'state')
            if state in ['failed', 'inaccessible' ]:
                iid = checkedInst['_id']
                abbrevIid = iid[0:16]
                logger.warning( 'would terminate %s', abbrevIid )
                terminatedIids.append( iid )
                coll.update_one( {'_id': iid},
                    { "$set": { "state": "terminated" } },
                    upsert=False
                    )
        logger.info( 'terminating %d instances', len( terminatedIids ))
        ncs.terminateInstances( args.authToken, terminatedIids )
        sys.exit()
    elif args.action == 'terminateAll':
        if not args.authToken:
            sys.exit( 'error: can not terminate because no authToken was passed')
        logger.info( 'checking for instances to terminate')
        # will terminate all instances and update checkedInstances accordingly
        startedInstances = getStartedInstances( db )  # expensive, could just query for iids
        coll = db['checkedInstances']
        wereChecked = coll.find()
        checkedByIid = { inst['_id']: inst for inst in wereChecked }
        terminatedIids = []
        for inst in startedInstances:
            iid = inst['instanceId']
            #abbrevIid = iid[0:16]
            #logger.warning( 'would terminate %s', abbrevIid )
            terminatedIids.append( iid )
            checkedInst = checkedByIid[iid]
            if checkedInst['state'] != 'terminated':
                coll.update_one( {'_id': iid},
                    { "$set": { "state": "terminated" } },
                    upsert=False
                    )
        logger.info( 'terminating %d instances', len( terminatedIids ))
        ncs.terminateInstances( args.authToken, terminatedIids )
        sys.exit()
    elif args.action == 'report':
        report_cc_status( db, dataDirPath )

        logger.info( 'would report' )
        # get all the instance info; TODO avoid this by keeping ssh info in checkedInstances (or elsewhere)
        startedInstances = getStartedInstances( db )
        instancesByIid = {inst['instanceId']: inst for inst in startedInstances }

        # will report only on "checked" instances
        wereChecked = db['checkedInstances'].find()
        reportables = []
        for inst in wereChecked:
            if inst['state'] == 'checked':
                iid =inst['_id']
                if iid in instancesByIid:
                    inst[ 'instanceId'] = iid
                    if 'ssh' not in inst:
                        logger.info( 'getting ssh info from launchedInstances for %s', inst )
                        inst['ssh'] = instancesByIid[iid]['ssh']
                    reportables.append( inst )
        logger.info( '%d instances reportable', len(reportables) )

        resultsLogFilePath=dataDirPath+'/reportInstances.jlog'
        workerCmd = "boinccmd --get_project_status | grep 'jobs succeeded: [^0]'"
        logger.info( 'calling tellInstances to get success report on %d instances', len(reportables))
        stepStatuses = tellInstances.tellInstances( reportables, workerCmd,
            resultsLogFilePath=resultsLogFilePath,
            download=None, downloadDestDir=None, jsonOut=None, sshAgent=args.sshAgent,
            timeLimit=min(args.timeLimit, args.timeLimit), upload=None, stopOnSigterm=True,
            knownHostsOnly=False
            )
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
        logger.info( '%d with successful jobs', len( goodIids ) )
        logger.info( '%d failed', len( failedIids ) )
        logger.info( '%d excepted', len( exceptedIids ) )
        # read back the results
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
                        if nJobsSucc >= 1:
                            logger.info( '%s nJobsSucc: %d', abbrevIid, nJobsSucc)
                        totJobsSucc += nJobsSucc
                    else:
                        logger.warning( 'not matched (%s)', event )
        logger.info( 'totJobsSucc: %d', totJobsSucc )

        sys.exit()
    # the rest is legacy code
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
        startedInstances = getStartedInstancesFromFile( launchedJsonFilePath )

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
