#!/usr/bin/env python3
"""
maintains and tracks instances in a folding@home farm on NCS
"""

# standard library modules
import argparse
import asyncio
import collections
from concurrent import futures
import errno
import datetime
import json
import logging
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import uuid

# third-party module(s)
import dateutil.parser
import pymongo

# neocortix modules
try:
    import ncs
except ImportError:
    # set system and python paths for default places, since path seems to be not set properly
    ncscliPath = os.path.expanduser('~/ncscli/ncscli')
    sys.path.append( ncscliPath )
    os.environ["PATH"] += os.pathsep + ncscliPath
    import ncs
import jsonToKnownHosts
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

def datetimeIsAware( dt ):
    if not dt: return None
    return (dt.tzinfo is not None) and (dt.tzinfo.utcoffset( dt ) is not None)

def universalizeDateTime( dt ):
    if not dt: return None
    if datetimeIsAware( dt ):
        return dt.astimezone(datetime.timezone.utc)
    return dt.replace( tzinfo=datetime.timezone.utc )


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


def getStartedInstances( db ):
    collName = 'launchedInstances'
    logger.info( 'getting instances from %s', collName )
    launchedColl = db[collName]
    inRecs = list( launchedColl.find() ) # fully iterates the cursor, getting all records
    if len(inRecs) <= 0:
        logger.warning( 'no launched instances found for %s', collName )
    for inRec in inRecs:
        if 'instanceId' not in inRec:
            logger.warning( 'no instance ID in input record')
    startedInstances = [inst for inst in inRecs if inst['state'] == 'started']
    return startedInstances

def ingestJson( srcFilePath, dbName, collectionName, append ):
    # uses some args from the global ArgumentParser
    cmd = [
        'mongoimport', '--host', args.mongoHost, '--port', str(args.mongoPort),
        '-d', dbName, '-c', collectionName,
        srcFilePath
    ]
    if srcFilePath.endswith( '.json' ):
        cmd.append( '--jsonArray' )
    if not append:
        cmd.append( '--drop' )
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

def terminateNcsScInstances( authToken, instanceIds ):
    '''try to terminate instances; return list of instances terminated (empty if none confirmed)'''
    terminationLogFilePath = os.path.join( dataDirPath, 'badTerminations.log' )  # using global dataDirPath
    dateTimeStr = datetime.datetime.now( datetime.timezone.utc ).isoformat()
    try:
        ncs.terminateInstances( authToken, instanceIds )
        logger.info( 'terminateInstances returned' )
    except Exception as exc:
        logger.warning( 'got exception terminating %d instances (%s) %s', 
            len( instanceIds ), type(exc), exc )
        try:
            with open( terminationLogFilePath, 'a' ) as logFile:
                for iid in instanceIds:
                    print( dateTimeStr, iid, sep=',', file=logFile )
        except Exception as exc:
            logger.warning( 'got exception (%s) appending to terminationLogFile %s',
                type(exc), terminationLogFilePath )
        return []  # empty list meaning none may have been terminated
    else:
        return instanceIds

def purgeHostKeys( instanceRecs ):
    '''try to purgeKnownHosts; warn if any exception'''
    logger.info( 'purgeKnownHosts for %d instances', len(instanceRecs) )
    try:
        ncs.purgeKnownHosts( instanceRecs )
    except Exception as exc:
        logger.warning( 'exception from purgeKnownHosts (%s) %s', type(exc), exc, exc_info=True )
        return 1
    else:
        return 0

def recruitInstances( nWorkersWanted, launchedJsonFilePath, launchWanted,
    resultsLogFilePath, installerFileName ):
    '''launch instances and install client on them;
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
            keyContents = loadSshPubKey().strip()
            randomPart = str( uuid.uuid4() )[0:13]
            #keyContents += ' #' + randomPart
            sshClientKeyName = 'geth_%s' % (randomPart)
            respCode = ncs.uploadSshClientKey( args.authToken, sshClientKeyName, keyContents )
            if respCode < 200 or respCode >= 300:
                logger.warning( 'ncs.uploadSshClientKey returned %s', respCode )
                sys.exit( 'could not upload SSH client key')
        #launch
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
        terminateNcsScInstances( args.authToken, nonstartedIids )
        logger.info( 'done terminating non-started instances' )
    # proceed with instances that were actually started
    startedInstances = [inst for inst in launchedInstances if inst['state'] == 'started' ]
    if not startedInstances:
        return ([], [])
    # add instances to knownHosts
    with open( os.path.expanduser('~/.ssh/known_hosts'), 'a' ) as khFile:
        jsonToKnownHosts.jsonToKnownHosts( startedInstances, khFile )

    if not sigtermSignaled():
        installerCmd = '%s %s'% (installerFileName, args.configName ) 
        logger.info( 'calling tellInstances to install on %d instances', len(startedInstances))
        stepStatuses = tellInstances.tellInstances( startedInstances, installerCmd,
            resultsLogFilePath=resultsLogFilePath,
            download=None, downloadDestDir=None, jsonOut=None, sshAgent=args.sshAgent,
            timeLimit=args.timeLimit, upload=args.uploads, stopOnSigterm=False,
            knownHostsOnly=False
            )
        # COULD restore our handler because tellInstances may have overridden it
        #signal.signal( signal.SIGTERM, sigtermHandler )
        if not stepStatuses:
            logger.warning( 'no statuses returned from installer')
            startedIids = [inst['instanceId'] for inst in startedInstances]
            #logOperation( 'terminateBad', startedIids, '<recruitInstances>' )
            terminateNcsScInstances( args.authToken, startedIids )
            return ([], [])
        # separate good tellInstances statuses from bad ones
        goodIids = []
        badStatuses = []
        for status in stepStatuses:
            if isinstance( status['status'], int) and status['status'] == 0:
                goodIids.append( status['instanceId'])
            else:
                badStatuses.append( status )
                if isinstance( status['status'], asyncio.TimeoutError ):
                    logger.info( 'installer status asyncio.TimeoutError' )
        
        logger.info( '%d good installs, %d bad installs', len(goodIids), len(badStatuses) )
        #logger.info( 'stepStatuses %s', stepStatuses )
        goodInstances = [inst for inst in startedInstances if inst['instanceId'] in goodIids ]
        badIids = []
        for status in badStatuses:
            badIids.append( status['instanceId'] )
        if badIids:
            #logOperation( 'terminateBad', badIids, '<recruitInstances>' )
            terminateNcsScInstances( args.authToken, badIids )

    return goodInstances, badStatuses

def loadSshPubKey():
    '''returns the contents of current user public ssh client key'''
    pubKeyFilePath = os.path.expanduser( '~/.ssh/id_rsa.pub' )
    with open( pubKeyFilePath ) as inFile:
        contents = inFile.read()
    return contents

def lastGenDateTime( coll ):
    found = coll.find().sort([( '$natural', -1 )]).limit(1)
    if found:
        return found[0]['_id'].generation_time
    return None

def getLiveInstances( startedIids, authToken ):
    '''query cloudserver to see which instances are still live'''
    logger.info( 'querying to see which instances are still live' )
    liveInstances = []
    #deadInstances = []
    for iid in startedIids:
        reqParams = {"show-device-info":True}
        try:
            response = ncs.queryNcsSc( 'instances/%s' % iid, authToken, reqParams=reqParams, maxRetries=1)
        except Exception as exc:
            logger.warning( 'querying instance status got exception (%s) %s',
                type(exc), exc )
        else:
            if response['statusCode'] != 200:
                logger.warning( 'cloud server returned bad status code %s', response['statusCode'] )
                continue
            inst = response['content']
            instState = inst['state']
            if instState not in ['started', 'stopped']:
                logger.info( 'state "%s" for instance %s', instState, iid )
            if 'instanceId' not in inst:
                inst['instanceId'] = iid
            if instState == 'started':
                liveInstances.append( inst )
            else:
                logger.info( 'state "%s" for instance %s', instState, iid )
            #    deadInstances.append( inst )
    return liveInstances

def launchEdgeNodes( dataDirPath, db, args ):
    '''recruits edge nodes, updates database; may sys.exit'''
    startDateTime = datetime.datetime.now( datetime.timezone.utc )
    dateTimeTagFormat = '%Y-%m-%d_%H%M%S'  # cant use iso format dates in filenames because colons
    dateTimeTag = startDateTime.strftime( dateTimeTagFormat )

    launchedJsonFilePath = os.path.join( dataDirPath, 'launched_%s.json' % dateTimeTag )
    resultsLogFilePath = os.path.join( dataDirPath, 'startGeth_%s.jlog' % dateTimeTag )

    logger.info( 'the launch filter is %s', args.filter )
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
        nToLaunch = min( nToLaunch, 60 )
    else:
        sys.exit( 'error: no positive --count or --target supplied')
    logger.info( 'would launch %d instances', nToLaunch )
    if nToLaunch <= 0:
        logger.debug( 'NOT launching additional instances' )
        return
    logger.info( 'local files %s and %s', launchedJsonFilePath, resultsLogFilePath )

    if not args.installerFileName:
        logger.error( 'no installer script file name; use --installerFileName to supply one' )
        raise ValueError( 'no installer script file name' )
    if not os.path.isfile( args.installerFileName ):
        logger.error( 'installer script file not found (%s)', args.installerFileName )
        raise ValueError( 'installer script file not found' )
    # launch and install, passing name of installer script to upload and run
    (goodInstances, badStatuses) = recruitInstances( nToLaunch, launchedJsonFilePath, True,
        resultsLogFilePath, args.installerFileName )
    if nToLaunch:
        collName = 'launchedInstances'
        logger.info( 'ingesting into %s', collName )
        ingestJson( launchedJsonFilePath, dbName, collName, append=True )
        # remove the launchedJsonFile to avoid local accumulation of data
        os.remove( launchedJsonFilePath )
        if not os.path.isfile( resultsLogFilePath ):
            logger.warning( 'no results file %s', resultsLogFilePath )
        else:
            ingestJson( resultsLogFilePath, dbName, 'startGeth_'+dateTimeTag, append=False )
            # remove the resultsLogFile to avoid local accumulation of data
            #os.remove( resultsLogFilePath )
        for statusRec in badStatuses:
            statusRec['status'] = str(statusRec['status'])
            statusRec['dateTime'] = startDateTime.isoformat()
            statusRec['_id' ] = statusRec['instanceId']
            db['badInstalls'].insert_one( statusRec )
    else:
        logger.warning( 'no instances recruited' )
    return

def checkGethProcesses( instances, dataDirPath ):
    logger.info( 'checking %d instance(s)', len(instances) )

    cmd = "ps -ef | grep -v grep | grep 'geth' > /dev/null"
    # check for a running geth process on each instance
    stepStatuses = tellInstances.tellInstances( instances, cmd,
        timeLimit=15*60,
        resultsLogFilePath = dataDirPath + '/checkProcesses.jlog',
        knownHostsOnly=True
        )
    #logger.info( 'proc statuses: %s', stepStatuses )
    errorsByIid = {status['instanceId']: status['status'] for status in stepStatuses if status['status'] }
    logger.info( 'errorsByIid: %s', errorsByIid )
    return errorsByIid

def checkInstanceClocks( liveInstances, dataDirPath ):
    jlogFilePath = dataDirPath + '/checkInstanceClocks.jlog'
    allIids = [inst['instanceId'] for inst in liveInstances ]
    unfoundIids = set( allIids )
    cmd = "date --iso-8601=seconds"
    # check for a running geth process on each instance
    stepStatuses = tellInstances.tellInstances( liveInstances, cmd,
        timeLimit=2*60,
        resultsLogFilePath = jlogFilePath,
        knownHostsOnly=True, sshAgent=not True
        )
    #logger.info( 'proc statuses: %s', stepStatuses )
    errorsByIid = {status['instanceId']: status for status in stepStatuses if status['status'] }
    for iid, status in errorsByIid.items():
        logger.warning( 'instance %s gave error "%s"', iid, status )

    with open( jlogFilePath, 'rb' ) as inFile:
        for line in inFile:
            decoded = json.loads( line )
            iid = decoded['instanceId']
            if decoded.get( 'stdout' ):
                #logger.info( decoded )
                masterDateTime = dateutil.parser.parse( decoded['dateTime'] )
                try:
                    nodeDateTime = dateutil.parser.parse( decoded['stdout'] )
                except Exception as exc:
                    logger.warning( 'exception parsing %s', decoded['stdout'] )
                    errorsByIid[ iid ] = {'exception': exc }
                else:
                    unfoundIids.discard( iid )
                    delta = masterDateTime - nodeDateTime
                    discrep =delta.total_seconds()
                    logger.info( 'discrep: %.1f seconds on inst %s',
                        discrep, iid )
                    if discrep > 4 or discrep < -1:
                        logger.warning( 'bad time discrep: %.1f', discrep )
                        errorsByIid[ iid ] = {'discrep': discrep }
    if unfoundIids:
        logger.warning( 'unfoundIids: %s', unfoundIids )
        for iid in list( unfoundIids ):
            if iid not in errorsByIid:
                errorsByIid[ iid ] = {'found': False }
    logger.info( '%d errorsByIid: %s', len(errorsByIid), errorsByIid )
    return errorsByIid

def checkEdgeNodes( dataDirPath, db, args ):
    '''checks status of edge nodes, updates database'''
    if not db.list_collection_names():
        logger.warning( 'no collections found for db %s', dbName )
        return
    checkerTimeLimit = args.timeLimit
    startedInstances = getStartedInstances( db )

    instancesByIid = {inst['instanceId']: inst for inst in startedInstances }

    wereChecked = list( db['checkedInstances'].find() ) # fully iterates the cursor, getting all records
    checkedByIid = { inst['_id']: inst for inst in wereChecked }
    # after checking, each checked instance will have "state" set to "checked", "failed", "inaccessible", or "terminated"
    # "checkable" instances are ones that are started, not badly installed, and not badly checked
    checkables = []
    for iid, inst in instancesByIid.items():
        if iid in checkedByIid and checkedByIid[iid]['state'] != 'checked':
            pass
        elif db['badInstalls'].find_one( {'_id': iid }):
            pass
            #logger.info('skipping badinstalled instance %s', iid )
        else:
            checkables.append( inst )
    logger.info( '%d instances checkable', len(checkables) )
    checkedDateTime = datetime.datetime.now( datetime.timezone.utc )
    checkedDateTimeStr = checkedDateTime.isoformat()
    dateTimeTagFormat = '%Y-%m-%d_%H%M%S'  # cant use iso format dates in filenames because colons
    dateTimeTag = checkedDateTime.strftime( dateTimeTagFormat )
    
    # find out which checkable instances are live
    checkableIids = [inst['instanceId'] for inst in checkables ]
    liveInstances = getLiveInstances( checkableIids, args.authToken )
    liveIidSet = set( [inst['instanceId'] for inst in liveInstances ] )

    coll = db['checkedInstances']
    # update a checkedInstances record for each checkable instance, creating any that do not exist
    for inst in checkables:
        iid = inst['instanceId']
        abbrevIid = iid[0:16]
        launchedDateTimeStr = inst.get( 'started-at' )
        instCheck = checkedByIid.get( iid )
        if 'ram' in inst:
            ramMb = inst['ram']['total'] / 1000000
        else:
            ramMb = 0
        if instCheck:
            nFailures = instCheck.get( 'nFailures', 0)
            reasonTerminated = instCheck.get( 'reasonTerminated', None)
        else:
            nFailures = 0
            reasonTerminated = None
            coll.insert_one( {'_id': iid,
                'state': 'started',
                'devId': inst.get('device-id'),
                'launchedDateTime': launchedDateTimeStr,
                'ssh': inst.get('ssh'),
                'ramMb': ramMb,
                'nFailures': 0,
                'nExceptions': 0,
            } )
        if iid in liveIidSet:
            state = 'checked'
        else:
            state = 'terminated'
            nFailures += 1
            reasonTerminated = 'foundDead'
        coll.update_one( {'_id': iid},
            { "$set": { "state": state, 'nFailures': nFailures,
                'reasonTerminated': reasonTerminated,
                'checkedDateTime': checkedDateTimeStr } },
            upsert=True
            )
    checkables = [inst for inst in checkables if inst['instanceId'] in liveIidSet]

    errorsByIid = checkInstanceClocks( liveInstances, dataDirPath )
    for iid, err in errorsByIid.items():
        if 'discrep' in err:
            discrep = err['discrep']
            logger.info( 'updating clockOffset %.1f for inst %s', discrep, iid[0:8] )
            coll.update_one( {'_id': iid}, { "$set": { "clockOffset": discrep } } )
            coll.update_one( {'_id': iid}, { "$inc": { "nFailures": 1 } } )
        elif 'status' in err:
            status = err['status']
            logger.warning( 'instance %s gave status (%s) "%s"', iid[0:8], type(status), status )
            if isinstance( status, Exception ):
                coll.update_one( {'_id': iid}, { "$inc": { "nExceptions": 1 } } )
            else:
                coll.update_one( {'_id': iid}, { "$inc": { "nFailures": 1 } } )
        else:
            logger.warning( 'instance %s gave err (%s) "%s"', iid[0:8], type(err), err )
        # change (or omit) this if wanting to allow more than one failure before marking it failed
        coll.update_one( {'_id': iid}, { "$set": { "state": "failed" } } )


    errorsByIid = checkGethProcesses( checkables, dataDirPath )
    for iid, status in errorsByIid.items():
        coll.update_one( {'_id': iid}, { "$inc": { "nFailures": 1 } } )
        # change (or omit) this if wanting to allow more than one failure before marking it failed
        coll.update_one( {'_id': iid}, { "$set": { "state": "failed" } } )

    return
    '''
    resultsLogFilePath=dataDirPath+('/checkInstances_%s.jlog' % dateTimeTag)
    workerCmd = 'stat --format=%%y ether/%s/geth.log' % args.configName
    logger.info( 'calling tellInstances on %d instances', len(checkables))
    stepStatuses = tellInstances.tellInstances( checkables, workerCmd,
        resultsLogFilePath=resultsLogFilePath,
        download=None, downloadDestDir=None, jsonOut=None, sshAgent=args.sshAgent,
        timeLimit=checkerTimeLimit, upload=None, stopOnSigterm=True,
        knownHostsOnly=False
        )
    '''


if __name__ == "__main__":
    startTime = time.time()
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
        choices=['launch', 'import', 'check', 'terminateBad', 'terminateAll', 'reterminate']
        )
    ap.add_argument( '--authToken', help='the NCS authorization token to use (required for launch or terminate)' )
    ap.add_argument( '--count', type=int, help='the number of instances (for launch)' )
    ap.add_argument( '--target', type=int, help='target number of working instances (for launch)',
        default=2 )
    ap.add_argument( '--configName', help='the name of the network config', default='priv_5' )
    ap.add_argument( '--dataDir', help='data directory', default='./data/' )
    ap.add_argument( '--encryptFiles', type=boolArg, default=False, help='whether to encrypt files on launched instances' )
    ap.add_argument( '--farm', required=True, help='the name of the virtual folding@home farm' )
    ap.add_argument( '--filter', help='json to filter instances for launch',
        default='{"dpr": ">=51", "ram:": ">=4000000000", "storage": ">=20000000000"}' )
    ap.add_argument( '--installerFileName', help='a script to run on the instances',
        default='netconfig/installAndStartGeth.sh' )
    ap.add_argument( '--uploads', help='glob for filenames to upload to workers', default='netconfig' )
    ap.add_argument( '--mongoHost', help='the host of mongodb server', default='localhost')
    ap.add_argument( '--mongoPort', help='the port of mongodb server', default=27017)
    ap.add_argument( '--sshAgent', type=boolArg, default=False, help='whether or not to use ssh agent' )
    ap.add_argument( '--sshClientKeyName', help='the name of the uploaded ssh client key to use (advanced)' )
    ap.add_argument( '--timeLimit', type=int, help='time limit (in seconds) for the whole job',
        default=20*60 )
    args = ap.parse_args()

    logger.info( 'performing action "%s" for farm "%s"', args.action, args.farm )

    dataDirPath = os.path.join( args.dataDir, args.farm )
    os.makedirs( dataDirPath, exist_ok=True )
    
    mclient = pymongo.MongoClient(args.mongoHost, args.mongoPort)
    dbName = 'geth_' + args.farm
    db = mclient[dbName]
    if args.action == 'launch':
        launchEdgeNodes( dataDirPath, db, args )
    elif args.action == 'check':
        checkEdgeNodes( dataDirPath, db, args )
    elif args.action == 'terminateBad':
        if not args.authToken:
            sys.exit( 'error: can not terminate because no authToken was passed')
        terminatedDateTimeStr = datetime.datetime.now( datetime.timezone.utc ).isoformat()
        coll = db['checkedInstances']
        wereChecked = list( coll.find() ) # fully iterates the cursor, getting all records
        toTerminate = []  # list of iids
        toPurge = []  # list of instance-like dicts containing instanceId and ssh fields
        for checkedInst in wereChecked:
            state = checkedInst.get( 'state')
            #logger.info( 'checked state %s', state )
            if state in ['failed', 'inaccessible', 'stopped' ]:
                iid = checkedInst['_id']
                abbrevIid = iid[0:16]
                logger.warning( 'would terminate %s', abbrevIid )
                toTerminate.append( iid )
                toPurge.append({ 'instanceId': iid, 'ssh': checkedInst['ssh'] })
                coll.update_one( {'_id': iid}, { "$set": { 'reasonTerminated': state } } )
        logger.info( 'terminating %d instances', len( toTerminate ))
        terminated=terminateNcsScInstances( args.authToken, toTerminate )
        logger.info( 'actually terminated %d instances', len( terminated ))
        purgeHostKeys( toPurge )
        # update states in checkedInstances
        for iid in terminated:
            coll.update_one( {'_id': iid},
                { "$set": { "state": "terminated",
                    'terminatedDateTime': terminatedDateTimeStr } } )
    elif args.action == 'terminateAll':
        if not args.authToken:
            sys.exit( 'error: can not terminate because no authToken was passed')
        logger.info( 'checking for instances to terminate')
        # will terminate all instances and update checkedInstances accordingly
        startedInstances = getStartedInstances( db )  # expensive, could just query for iids
        startedIids = [inst['instanceId'] for inst in startedInstances]
        terminatedDateTimeStr = datetime.datetime.now( datetime.timezone.utc ).isoformat()
        toTerminate = startedIids
        logger.info( 'terminating %d instances', len( toTerminate ))
        terminated=terminateNcsScInstances( args.authToken, toTerminate )
        purgeHostKeys( startedInstances )

        # update states in checkedInstances
        coll = db['checkedInstances']
        wereChecked = coll.find()  # could use a projection to make it more efficient
        checkedByIid = { inst['_id']: inst for inst in wereChecked }
        for iid in terminated:
            if iid in checkedByIid:
                checkedInst = checkedByIid[iid]
                if checkedInst['state'] != 'terminated':
                    coll.update_one( {'_id': iid},
                        { "$set": { "state": "terminated",
                            'reasonTerminated': 'manual',
                            'terminatedDateTime': terminatedDateTimeStr } 
                            } )
    elif args.action == 'reterminate':
        '''redundantly terminate instances that are listed as already terminated'''
        if not args.authToken:
            sys.exit( 'error: can not terminate because no authToken was passed')
        # get list of terminated instance IDs
        coll = db['checkedInstances']
        wereChecked = list( coll.find({'state': 'terminated'},
            {'_id': 1, 'terminatedDateTime': 1 }) )
        toTerminate = []
        for checkedInst in wereChecked:
            iid = checkedInst['_id']
            abbrevIid = iid[0:16]
            if checkedInst['terminatedDateTime'] >= '2020-07-24':
                logger.info( 'would reterminate %s from %s', abbrevIid, checkedInst['terminatedDateTime'] )
                toTerminate.append( iid )
        logger.info( 'reterminating %d instances', len( toTerminate ))
        terminated = terminateNcsScInstances( args.authToken, toTerminate )
        logger.info( 'reterminated %d instances', len( terminated ) )
        #TODO purgeHostKeys
    else:
        logger.warning( 'action "%s" unimplemented', args.action )
    elapsed = time.time() - startTime
    logger.info( 'finished action "%s"; elapsed time %.1f minutes',
        args.action, elapsed/60 )
