#!/usr/bin/env python3
"""
maintains and tracks instances in a geth (go-ethereum) farm on NCS
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
import ncsgeth
import tellInstances

logger = logging.getLogger(__name__)


global resultsLogFile

# possible place for globals is this class's attributes
class g_:
    signaled = False


def sigtermHandler( sig, frame ):
    g_.signaled = True
    logger.warning( 'SIGTERM received; graceful exit may take a few minutes' )
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

def interpretDateTimeField( field ):
    '''return utc datetime for a given datetime or a string parseable as a date/time'''
    if isinstance( field, datetime.datetime ):
        return universalizeDateTime( field )
    elif isinstance( field, str ):
        return universalizeDateTime( dateutil.parser.parse( field ) )
    else:
        raise TypeError( 'datetime or parseable string required' )


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

def importAnchorNodes( srcFilePath, db, collectionName ):
    # uses some args from the global ArgumentParser
    cmd = [
        'mongoimport', '--host', args.mongoHost, '--port', str(args.mongoPort),
        '-d', db.name, '-c', collectionName, '--jsonArray',
        '--mode=upsert', '--upsertFields=instanceId',
        srcFilePath
    ]
    #logger.info( 'cmd: %s', cmd )
    subprocess.check_call( cmd )
    db[collectionName].create_index( 'instanceId' )

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
        logger.debug( 'terminateInstances returned' )
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
            timeLimit=args.timeLimit, upload=args.uploads, stopOnSigterm=True,
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
    '''return modification dateTime of a mongo collection, as a (hopefully) tz-aware datetime'''
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
        nToLaunch = int( (args.target - nExisting) * 3 )
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

def saveErrorsByIid( errorsByIid, db, operation=None ):
    checkedColl = db.checkedInstances
    errorsColl = db.instanceErrors
    checkedDateTime = datetime.datetime.now( datetime.timezone.utc )
    checkedDateTimeStr = checkedDateTime.isoformat()

    for iid, err in errorsByIid.items():
        record = { 'instanceId': iid, 'checkedDateTime': checkedDateTimeStr, 'operation': operation }
        if 'discrep' in err:
            discrep = err['discrep']
            logger.debug( 'updating clockOffset %.1f for inst %s', discrep, iid[0:8] )
            checkedColl.update_one( {'_id': iid}, { "$set": { "clockOffset": discrep } } )
            checkedColl.update_one( {'_id': iid}, { "$inc": { "nFailures": 1 } } )
            record.update( { 'status': 'badClock', 'returnCode': discrep } )
            errorsColl.insert_one( record )
        elif 'status' in err:
            status = err['status']
            if isinstance( status, Exception ):
                checkedColl.update_one( {'_id': iid}, { "$inc": { "nExceptions": 1 } } )
                excType = type(status).__name__  # the name of the particular exception class
                checkedColl.update_one( {'_id': iid}, { "$set": { "exceptionType": excType } } )
                record.update({ 'status': 'exception', 'exceptionType': excType, 'msg': str(status.args) })
                errorsColl.insert_one( record )
            else:
                checkedColl.update_one( {'_id': iid}, { "$inc": { "nFailures": 1 } } )
                record.update({ 'status': 'error', 'returnCode': status })
                errorsColl.insert_one( record )
        else:
            logger.warning( 'checkInstanceClocks instance %s gave status (%s) "%s"', iid[0:8], type(err), err )
            record.update({ 'status': 'misc', 'msg': str(err) })
            errorsColl.insert_one( record )
        # change (or omit) this if wanting to allow more than one failure before marking it failed
        checkedInst = checkedColl.find_one( {'_id': iid }, {'ssh':0} )
        if checkedInst['nExceptions'] > checkedInst['nFailures']:
            state = 'excepted'
        else:
            state = 'failed'
        checkedColl.update_one( {'_id': iid}, { "$set": { "state": state } } )

def checkGethProcesses( instances, dataDirPath ):
    logger.info( 'checking %d instance(s)', len(instances) )

    cmd = "ps -ef | grep -v grep | grep 'geth' > /dev/null"
    # check for a running geth process on each instance
    stepStatuses = tellInstances.tellInstances( instances, cmd,
        timeLimit=15*60,
        resultsLogFilePath = dataDirPath + '/checkGethProcesses.jlog',
        knownHostsOnly=True, stopOnSigterm=True
        )
    #logger.info( 'proc statuses: %s', stepStatuses )
    errorsByIid = {status['instanceId']: status for status in stepStatuses if status['status'] }
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
        knownHostsOnly=True, sshAgent=not True, stopOnSigterm=True
        )
    #logger.info( 'proc statuses: %s', stepStatuses )
    errorsByIid = {status['instanceId']: status for status in stepStatuses if status['status'] }
    #for iid, status in errorsByIid.items():
    #    logger.warning( 'instance %s gave error "%s"', iid, status )

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
                    logger.debug( 'discrep: %.1f seconds on inst %s',
                        discrep, iid )
                    if discrep > 4 or discrep < -1:
                        #logger.warning( 'bad time discrep: %.1f', discrep )
                        errorsByIid[ iid ] = {'discrep': discrep }
    if unfoundIids:
        logger.warning( 'unfoundIids: %s', unfoundIids )
        for iid in list( unfoundIids ):
            if iid not in errorsByIid:
                errorsByIid[ iid ] = {'found': False }
    logger.info( '%d errorsByIid: %s', len(errorsByIid), errorsByIid )
    return errorsByIid

def retrieveLogs( goodInstances, dataDirPath, farmDirPath ):
    '''retrieve logs from nodes, storing them in dataDirPath/nodeLogs subdirectories'''
    logger.info( 'retrieving logs for %d instance(s)', len(goodInstances) )

    nodeLogFilePath = farmDirPath + '/geth.*log'

    # download the log file from each instance
    stepStatuses = tellInstances.tellInstances( goodInstances,
        download=nodeLogFilePath, downloadDestDir=dataDirPath +'/nodeLogs',
        timeLimit=15*60,
        knownHostsOnly=True, stopOnSigterm=True
        )
    #logger.info( 'download statuses: %s', stepStatuses )
    errorsByIid = {status['instanceId']: status['status'] for status in stepStatuses if status['status'] }
    return errorsByIid

def checkLogs( liveInstances, dataDirPath ):
    '''check contents of logs from nodes, save errorSummary.csv, return errorsByIid'''
    logsDirPath = os.path.join( dataDirPath, 'nodeLogs' )
    logDirs = os.listdir( logsDirPath )
    logger.info( '%d logDirs found', len(logDirs ) )

    liveIids = [inst['instanceId'] for inst in liveInstances]

    errorsByIid = {}

    for logDir in logDirs:
        iid = logDir
        iidAbbrev = iid[0:8]
        if iid not in liveIids:
            logger.debug( 'not checking non-live instance %s', iidAbbrev )
            continue
        logFilePath = os.path.join( logsDirPath, logDir, 'geth.log' )
        if not os.path.isfile( logFilePath ):
            logger.warning( 'no log file %s', logFilePath )
        elif os.path.getsize( logFilePath ) <= 0:
            logger.warning( 'empty log file "%s"', logFilePath )
        else:
            connected = False
            sealed = False
            latestSealLine = None
            with open( logFilePath ) as logFile:
                try:
                    for line in logFile:
                        line = line.rstrip()
                        if not line:
                            continue
                        if 'ERROR' in line:
                            if 'Ethereum peer removal failed' in line:
                                logger.debug( 'ignoring "peer removal failed" for %s', iidAbbrev )
                                continue
                            #logger.info( '%s: %s', iidAbbrev, line )
                            theseErrors = errorsByIid.get( iid, [] )
                            theseErrors.append( line )
                            errorsByIid[iid] = theseErrors
                        elif 'Started P2P networking' in line:
                            connected = True
                        elif 'Successfully sealed' in line:
                            sealed = True
                            latestSealLine = line
                    if not connected:
                        logger.warning( 'instance %s did not initialize fully', iidAbbrev )
                    if sealed:
                        #logger.info( 'instance %s has successfully sealed', iidAbbrev )
                        logger.debug( '%s latestSealLine: %s', iidAbbrev, latestSealLine )
                        pat = r'\[(.*\|.*)\]'
                        dateStr = re.search( pat, latestSealLine ).group(1)
                        if dateStr:
                            dateStr = dateStr.replace( '|', ' ' )
                            latestSealDateTime = dateutil.parser.parse( dateStr )
                            logger.debug( 'latestSealDateTime: %s for %s', latestSealDateTime.isoformat(), iid[0:8] )
                except Exception as exc:
                    logger.warning( 'caught exception (%s) %s', type(exc), exc )
    logger.info( 'found errors for %d instance(s)', len(errorsByIid) )
    #print( 'errorsByIid', errorsByIid  )
    summaryCsvFilePath = os.path.join( dataDirPath, 'errorSummary.csv' )
    fileExisted = os.path.isfile( summaryCsvFilePath )
    with open( summaryCsvFilePath, 'a' ) as csvOutFile:
        if not fileExisted:
            print( 'instanceId', 'msg',
                sep=',', file=csvOutFile
                )
        for iid in errorsByIid:
            lines = errorsByIid[iid]
            logger.debug( 'saving %d errors for %s', len(lines), iid[0:8])
            for line in lines:
                print( iid, line,
                    sep=',', file=csvOutFile
                )
    return errorsByIid

def collectGethLogEntries( db, inFilePath, collName ):
    '''ingest records from a geth node log into a new or existing mongo collection'''
    collection = db[ collName ]
    nPreexisting = collection.count_documents( {} )
    logger.debug( '%d preexisting records in %s', nPreexisting, collName )

    lastExistingDateTime = None
    lastExisting = {}
    latestMsgs = []
    if nPreexisting > 0:
        # get the record with the latset dateTime
        cursor = collection.find().sort('dateTime', -1).limit(1)
        lastExisting = cursor[0]
        lastExistingDateTime = interpretDateTimeField( lastExisting['dateTime'] )
    if not lastExistingDateTime:
        lastExistingDateTime = datetime.datetime.fromtimestamp( 0, datetime.timezone.utc )
    logger.debug( 'lastExistingDateTime: %s', lastExistingDateTime )
    logger.debug( 'lastExisting: %s', lastExisting )

    if lastExisting:
        found = collection.find({ 'dateTime': lastExisting['dateTime'] })
        allLatest = list( found )
        latestMsgs = [rec['msg'] for rec in allLatest]
        if len( allLatest ) > 1:
            logger.info( '%d records match latest dateTime in %s', len(allLatest), collName )
            logger.debug( 'latestMsgs: %s', latestMsgs )
    datePat = r'\[(.*\|.*)\]'
    lineDateTime = datetime.datetime.fromtimestamp( 0, datetime.timezone.utc )
    lineLevel = None
    recs = []

    with open( inFilePath, 'r' ) as logFile:
        try:
            for line in logFile:
                line = line.rstrip()
                if not line:
                    continue
                levelPart = line.split()[0]
                if levelPart in ['INFO', 'WARN', 'ERROR']:
                    lineLevel = levelPart
        
                #lineDateTime = None
                dateMatch = re.search( datePat, line )
                dateStr = dateMatch.group(1) if dateMatch else None
                if dateStr:
                    dateStr = dateStr.replace( '|', ' ' )
                    lineDateTime = universalizeDateTime( dateutil.parser.parse( dateStr ) )
                if lineDateTime < lastExistingDateTime:
                    #logger.info( 'skipping old' )
                    continue
                msg = line
                if lineDateTime == lastExistingDateTime and msg in latestMsgs:
                    logger.debug( 'skipping dup' )
                    continue
                #logger.info( 'appending: %s', msg[26:56])
                recs.append({
                    'dateTime': lineDateTime.isoformat(),
                    'level': lineLevel,
                    'msg': msg,
                    })
                #logger.info( 'rec: %s', recs[-1] )
        except Exception as exc:
            logger.warning( 'exception (%s) %s', type(exc), exc )
        if len(recs):
            #startTime = time.time()
            collection.insert_many( recs, ordered=True )
            #logger.info( 'inserted %d records in %.1f seconds', len(recs), time.time()-startTime )
            if not nPreexisting:
                collection.create_index( 'dateTime' )
                collection.create_index( 'level' )

def ingestGethLog( logFile, coll ):
    '''ingest records from a geth node log into a mongo collection'''
    logger.debug( 'would ingest %s into %s', logFile.name, coll.name )
    datePat = r'\[(.*\|.*)\]'
    lineDateTime = datetime.datetime.fromtimestamp( 0, datetime.timezone.utc )
    lineLevel = None
    recs = []
    for line in logFile:
        line = line.rstrip()
        if not line:
            continue
        levelPart = line.split()[0]
        if levelPart in ['INFO', 'WARN', 'ERROR']:
            lineLevel = levelPart

        #lineDateTime = None
        dateMatch = re.search( datePat, line )
        dateStr = dateMatch.group(1) if dateMatch else None
        if dateStr:
            dateStr = dateStr.replace( '|', ' ' )
            lineDateTime = dateutil.parser.parse( dateStr )
        msg = line
        recs.append( {
            'dateTime': lineDateTime.isoformat(),
            'level': lineLevel,
            'msg': msg,
            } )
    if len(recs):
        coll.insert_many( recs, ordered=True )
        coll.create_index( 'dateTime' )
        coll.create_index( 'level' )

def collectGethJLogEntries( db, inFilePath, collName ):
    collection = db[ collName ]
    nPreexisting = collection.count_documents( {} )
    logger.info( '%d preexisting records in %s', nPreexisting, collName )

    lastExistingDateStr = None
    lastExisting = {}
    latestMsgs = []
    if nPreexisting > 0:
        # get the record with the latset dateTime
        cursor = collection.find().sort('dateTime', -1).limit(1)
        lastExisting = cursor[0]
        lastExistingDateStr = lastExisting['dateTime']
    if not lastExistingDateStr:
        lastExistingDateStr = datetime.datetime.fromtimestamp( 0, datetime.timezone.utc ).isoformat()
    logger.debug( 'lastExistingDateStr: %s', lastExistingDateStr )
    logger.debug( 'lastExisting: %s', lastExisting )

    if lastExisting:
        found = collection.find({ 'dateTime': lastExisting['dateTime'] })
        allLatest = list( found )
        latestMsgs = [rec['msg'] for rec in allLatest]
        if len( allLatest ) > 1:
            logger.info( '%d records match latest dateTime', len(allLatest) )
            logger.info( 'latestMsgs: %s', latestMsgs )
    recs = []

    with open( inFilePath, 'r' ) as logFile:
        for line in logFile:
            line = line.rstrip()
            if not line:
                continue
            try:
                rec = json.loads( line )
                rec['level'] = rec.pop( 'lvl' ).upper()
                rec['dateTime'] = rec.pop( 't' ).upper()
                
                lineDateStr = rec['dateTime']
                if lineDateStr < lastExistingDateStr:
                    #logger.info( 'skipping old' )
                    continue
                msg = rec['msg']
                if lineDateStr == lastExistingDateStr and msg in latestMsgs:
                    logger.info( 'skipping dup' )
                    continue
                
                recs.append( rec )
            except Exception as exc:
                logger.warning( 'could not parse json (%s) %s', type(exc), exc )
                continue
        if len(recs):
            logger.info( 'inserting %d records', len(recs) )
            collection.insert_many( recs, ordered=True )
            if not nPreexisting:
                collection.create_index( 'dateTime' )
                collection.create_index( 'level' )

def processNodeLogFiles( dataDirPath ):
    '''ingest all new or updated node logs; delete very old log files'''
    logsDirPath = os.path.join( dataDirPath, 'nodeLogs' )
    logDirs = os.listdir( logsDirPath )
    logger.debug( '%d logDirs found', len(logDirs ) )
    # but first, delete very old log files
    lookbackDays = 7
    thresholdDateTime = datetime.datetime.now( datetime.timezone.utc ) \
        - datetime.timedelta( days=lookbackDays )
    #lookBackHours = 24
    #newerThresholdDateTime = datetime.datetime.now( datetime.timezone.utc ) \
    #    - datetime.timedelta( hours=lookBackHours )

    for logDir in logDirs:
        inFilePath = os.path.join( logsDirPath, logDir, 'geth.jlog' )
        if not os.path.isfile( inFilePath ):
            inFilePath = os.path.join( logsDirPath, logDir, 'geth.log' )
        if os.path.isfile( inFilePath ):
            fileModDateTime = datetime.datetime.fromtimestamp( os.path.getmtime( inFilePath ) )
            fileModDateTime = universalizeDateTime( fileModDateTime )
            if fileModDateTime <= thresholdDateTime:
                os.remove( inFilePath )
                logDirPath = os.path.join( logsDirPath, logDir )
                try:
                    os.rmdir( logDirPath )
                except Exception as exc:
                    logger.warning( 'exception (%s) removing log dir %s', type(exc), logDirPath )
            #elif fileModDateTime <= newerThresholdDateTime:
            #    logger.warning( 'old fileModDateTime for %s (%s)', logDir, fileModDateTime )
    
    # ingest all new or updated node logs
    loggedCollNames = db.list_collection_names(
        filter={ 'name': {'$regex': r'^nodeLog_.*'} } )
    iStartTime = time.time()
    nIngested = 0
    for logDir in logDirs:
        # logDir is also the instanceId
        # read geth.jlog, if present, otherwise geth.log
        inFilePath = os.path.join( logsDirPath, logDir, 'geth.jlog' )
        if not os.path.isfile( inFilePath ):
            inFilePath = os.path.join( logsDirPath, logDir, 'geth.log' )
        if not os.path.isfile( inFilePath ):
            logger.info( 'no file "%s"', inFilePath )
            continue
        elif os.path.getsize( inFilePath ) <= 0:
            logger.warning( 'empty log file "%s"', inFilePath )
            continue
        collName = 'nodeLog_' + logDir
        if collName in loggedCollNames:
            existingGenTime = lastGenDateTime( db[collName] ) - datetime.timedelta(minutes=1)
            fileModDateTime = datetime.datetime.fromtimestamp( os.path.getmtime( inFilePath ) )
            fileModDateTime = universalizeDateTime( fileModDateTime )
            if existingGenTime >= fileModDateTime:
                #logger.info( 'already posted %s %s %s',
                #    logDir[0:8], fmtDt( existingGenTime ), fmtDt( fileModDateTime )  )
                continue
        logger.debug( 'ingesting log for %s', logDir[0:16] )
        try:
            if inFilePath.endswith( '.jlog' ):
                collectGethJLogEntries( db, inFilePath, collName )
            else:
                collectGethLogEntries( db, inFilePath, collName )
            nIngested += 1
        except Exception as exc:
            logger.warning( 'exception (%s) ingesting %s', type(exc), inFilePath, exc_info=not False )
    logger.info( 'ingesting %d logs took %.1f seconds', nIngested, time.time()-iStartTime)

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
    # after checking, each checked instance will have "state" set to "checked", "failed", "excepted", or "terminated"
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
        if instCheck:
            nFailures = instCheck.get( 'nFailures', 0)
            reasonTerminated = instCheck.get( 'reasonTerminated', None)
        else:
            nFailures = 0
            reasonTerminated = None
            coll.insert_one( {'_id': iid, 'instanceId': iid,
                'state': 'started',
                'devId': inst.get('device-id'),
                'launchedDateTime': launchedDateTimeStr,
                'ssh': inst.get('ssh'),
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
            { "$set": { 'instanceId': iid, 'state': state, 'nFailures': nFailures,
                'reasonTerminated': reasonTerminated,
                'checkedDateTime': checkedDateTimeStr } },
            upsert=True
            )
    checkables = [inst for inst in checkables if inst['instanceId'] in liveIidSet]
    logger.info( '%d instances checkable', len(checkables) )
    errorsByIid = checkInstanceClocks( liveInstances, dataDirPath )
    saveErrorsByIid( errorsByIid, db, operation='checkInstanceClocks' )

    checkables = [inst for inst in checkables if inst['instanceId'] not in errorsByIid]
    logger.info( '%d instances checkable', len(checkables) )
    errorsByIid = checkGethProcesses( checkables, dataDirPath )
    saveErrorsByIid( errorsByIid, db, operation='checkGethProcesses' )

    checkables = [inst for inst in checkables if inst['instanceId'] not in errorsByIid]
    logger.info( '%d instances checkable', len(checkables) )

    # populate the primaryAccount field of any checked instances that lack it
    errorsByIid = {}
    for inst in checkables:
        iid = inst['instanceId']
        instCheck = checkedByIid.get( iid )
        if not instCheck or 'primaryAccount' not in instCheck:
            logger.info( 'getting primary account of inst %s', iid[0:16])
            instanceAccountPairs = ncsgeth.collectPrimaryAccounts( [inst], args.configName )
            if not instanceAccountPairs:
                # this shouldn't happen, because you should at least get a status return
                logger.error( 'no account info returned for inst %s', iid )
            elif instanceAccountPairs[0].get('status'):
                # an error (or exception) getting the account
                errorsByIid[ iid ] = { 'instanceIid': iid, 
                    'status': instanceAccountPairs[0].get('status') 
                    }
            else:
                accountAddr = instanceAccountPairs[0].get('accountAddr')
                if accountAddr:
                    coll.update_one( {'_id': iid}, { "$set": { "primaryAccount": accountAddr } } )
    logger.info( '(collectPrimaryAccounts) errorsByIid %s', errorsByIid )
    saveErrorsByIid( errorsByIid, db, operation='collectPrimaryAccounts' )

    nodeDataDirPath = 'ether/%s' % args.configName
    errorsByIid = retrieveLogs( checkables, dataDirPath, nodeDataDirPath )
    #checkLogs( checkables, dataDirPath )
    processNodeLogFiles( dataDirPath )
    return

def collectSigners( db, configName ):
    anchorInstances = list( db['anchorInstances'].find() ) # fully iterates the cursor, getting all records
    coll = db['checkedInstances']
    wereChecked = list( coll.find({'state': 'checked'} ) )
    for inst in wereChecked:
        inst['instanceId'] = inst['_id']
    instances = anchorInstances + wereChecked

    # get the currently authorized signers (and true proposees)
    signerInfos = ncsgeth.collectSignerInstances( instances, configName )
    logger.debug( 'signerInfos: %s', signerInfos )
    coll = db['allSigners']
    logger.info( 'saving signers')
    for signer in signerInfos:
        signerId = signer['accountAddr']
        iid = signer['instanceId']
        auth = signer['auth']
        logger.info( 'signer acct %s: iid %s, auth: %s', signerId, iid, auth )
        coll.update_one( {'_id': iid}, { "$set": 
            { "instanceId": iid, "accountAddr": signerId, "auth": auth } }, upsert=True
            )

def waitForAuth( victimAccount, shouldAuth, instances, configName, timeLimit ):
    '''wait for the authorization of victimAccount to match shouldAuth; return success (true or false)'''
    curSigners = ncsgeth.collectAuthSigners( instances, configName )
    nowAuth = victimAccount in curSigners
    logger.info( 'now authorized? %s', nowAuth )
    deadline = time.time() + timeLimit
    while nowAuth != shouldAuth:
        if time.time() >= deadline:
            logger.warning( 'took too long for auth/deauth to propagate')
            break
        curSigners = ncsgeth.collectAuthSigners( instances, configName )
        nowAuth = victimAccount in curSigners
        logger.info( 'now authorized? %s', nowAuth )
        if nowAuth != shouldAuth:
            time.sleep(1)
    return nowAuth == shouldAuth

def authorizeSigner( db, configName, victimIid, shouldAuth ):
    logger.info( 'shouldAuth: %s', shouldAuth )

    allSigners = list( db['allSigners'].find() )
    logger.info( 'len(allSigners): %d', len(allSigners) )
    signersByIid = {signer['instanceId']: signer for signer in allSigners }
    logger.debug( 'signer iids: %s', signersByIid.keys() )

    wasAuth = (victimIid in signersByIid) and signersByIid[victimIid].get('auth)')
    if wasAuth == shouldAuth:
        logger.warning( 'instance %s auth status is already %s', victimIid, shouldAuth )
        #return

    wereChecked = list( db['checkedInstances'].find( {'state': 'checked'}) ) # fully iterates the cursor, getting all records
    logger.info( '%d instances were checked', len(wereChecked))
    for inst in wereChecked:
        inst['instanceId'] = inst['_id']
    anchorInstances = list( db['anchorInstances'].find() ) # fully iterates the cursor, getting all records

    instances = anchorInstances + wereChecked
    instancesByIid = {inst['instanceId']: inst for inst in instances }

    victimAccount = None
    if victimIid in signersByIid:
        # this instance is (or has been) a signer
        victimAccount = signersByIid[ victimIid ].get( 'accountAddr' )
    else:
        victimInst = instancesByIid[ victimIid ]
        if not victimInst:
            logger.warning( 'instance %s not found', victimIid )
            return
        logger.debug ('victimInst: %s', victimInst )
        instanceAccountPairs = ncsgeth.collectPrimaryAccounts( [victimInst], configName )
        if not instanceAccountPairs or not instanceAccountPairs[0].get( 'accountAddr' ):
            logger.error( 'NOT authorizing, no account found for inst %s', victimIid )
            return
        victimAccount = instanceAccountPairs[0]['accountAddr']

    authorizers = [inst for inst in instances if inst['instanceId'] in signersByIid ]
    logger.info( '%d authorizers', len(authorizers) )
    if not authorizers:
        logger.warning( 'no authorizers found')

    # starting all miners should not be necessary
    #minerResults = ncsgeth.startMiners( authorizers, configName )
    #logger.info( 'all minerResults: %s', minerResults )
    # start miner on the one to be authorized
    if victimIid and victimIid in instancesByIid:
        inst = instancesByIid[ victimIid ]
        minerResults = ncsgeth.startMiners( [inst], configName )
        logger.info( 'minerResults: %s', minerResults )

    authResults = ncsgeth.authorizeSigner( authorizers, configName, victimAccount, shouldAuth )
    logger.debug( 'authResults: %s', authResults )
    anySucceeded = False
    for result in authResults:
        if 'returnCode' in result and result['returnCode'] == 0:
            anySucceeded = True
        else:
            logger.warning( 'bad result from authorizeSigner: %s', result )
    if anySucceeded:
        itWorked = waitForAuth( victimAccount, shouldAuth, authorizers, configName, timeLimit=600 )
        if itWorked:
            db['allSigners'].update_one( {'_id': victimIid}, { "$set": 
                { "instanceId": victimIid, "accountAddr": victimAccount, "auth": shouldAuth }
                    }, upsert=True
                )
    return

def authorizeGood( db, configName, maxNewAuths, trustedThresh ):
    # retrieve list of signers so we can know which are already authorized
    allSigners = list( db['allSigners'].find() )
    logger.info( 'len(allSigners): %d', len(allSigners) )
    signersByIid = {signer['instanceId']: signer for signer in allSigners }
    logger.debug( 'signer iids: %s', signersByIid.keys() )

    wereChecked = list( db['checkedInstances'].find( {'state': 'checked'}) ) # fully iterates the cursor, getting all records
    logger.info( '%d instances were checked', len(wereChecked))
    nNewAuths = 0
    for inst in wereChecked:
        if nNewAuths >= maxNewAuths:
            break
        iid = inst['_id']
        logger.debug( 'considering instance %s', iid )
        signerInfo = signersByIid.get(iid)
        if signerInfo and signerInfo.get( 'auth' ):
            logger.debug( 'already authorized, account %s', signerInfo.get('accountAddr') )
            continue
        launchedDateTime = dateutil.parser.parse( inst['launchedDateTime'] )
        checkedDateTime = dateutil.parser.parse( inst['checkedDateTime'] )
        #logger.info( 'uptime: %s', checkedDateTime-launchedDateTime )
        uptimeHrs = (checkedDateTime-launchedDateTime).total_seconds() / 3600
        logger.info( 'uptimeHrs: %.1f', uptimeHrs )
        if uptimeHrs < trustedThresh:
            continue
        logger.info( 'would authorize %s', iid )
        authorizeSigner( db, configName, iid, True )
        nNewAuths += 1
        if nNewAuths < maxNewAuths:
            time.sleep( 15 )


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
        choices=['launch', 'authorizeGood', 'authorizeSigner', 'deauthorizeSigner',
            'check', 'collectSigners', 'importAnchorNodes', 'maintain',
            'terminateBad', 'terminateAll', 'reterminate']
        )
    ap.add_argument( '--authToken', help='the NCS authorization token to use (required for launch or terminate)' )
    ap.add_argument( '--count', type=int, help='the number of instances (for launch)' )
    ap.add_argument( '--target', type=int, help='target number of working instances (for launch)',
        default=2 )
    ap.add_argument( '--configName', help='the name of the network config', default='priv_5' )
    ap.add_argument( '--dataDir', help='data directory', default='./data/' )
    ap.add_argument( '--encryptFiles', type=boolArg, default=False, help='whether to encrypt files on launched instances' )
    ap.add_argument( '--farm', required=True, help='the name of the virtual geth farm' )
    ap.add_argument( '--filter', help='json to filter instances for launch',
        default='{"dpr": ">=51", "ram:": ">=4000000000", "storage": ">=20000000000"}' )
    ap.add_argument( '--installerFileName', help='a script to run on new instances',
        default='netconfig/installAndStartGeth.sh' )
    ap.add_argument( '--instanceId', help='id of instance (for authorize or deauthorize)' )
    ap.add_argument( '--maxNewAuths', type=int, default=0, help='maximum number of new authorized signers (for authorizeGood)' )
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
    elif args.action in ['authorizeSigner', 'deauthorizeSigner' ]:
        victimIid = args.instanceId
        configName = args.configName
        shouldAuth = args.action == 'authorizeSigner'
        authorizeSigner( db, configName, victimIid, shouldAuth )
    elif args.action == 'authorizeGood':
        # authorize good instances that have been up for long enough
        maxNewAuths = args.maxNewAuths
        if maxNewAuths <= 0:
            sys.exit()
        authorizeGood( db, args.configName, maxNewAuths, trustedThresh=18 )
    elif args.action == 'check':
        checkEdgeNodes( dataDirPath, db, args )
    elif args.action == 'collectSigners':
        collectSigners( db, args.configName )
    elif args.action == 'importAnchorNodes':
        anchorNodesFilePath = os.path.join( dataDirPath, 'anchorInstances.json' )
        importAnchorNodes( anchorNodesFilePath, db, 'anchorInstances' )
    elif args.action == 'maintain':
        myPid = os.getpid()
        logger.info( 'process id %d', myPid )
        signal.signal( signal.SIGTERM, sigtermHandler )
        actionFirst = args.action == sys.argv[1]
        if not actionFirst:
            logger.error( '"%s" was not specified as the first argument', args.action )
            sys.exit(1)
        if mclient:
            mclient.close()
        logger.info( 'argv: %s', sys.argv )
        cmd = sys.argv.copy()
        while sigtermNotSignaled():
            cmd[1] = 'check'
            logger.info( 'cmd: %s', cmd )
            subprocess.check_call( cmd )
            if sigtermNotSignaled():
                cmd[1] = 'authorizeGood'
                logger.info( 'cmd: %s', cmd )
                subprocess.check_call( cmd )
            if sigtermNotSignaled():
                cmd[1] = 'terminateBad'
                logger.info( 'cmd: %s', cmd )
                subprocess.check_call( cmd )
            if sigtermNotSignaled():
                if args.target > 0:
                    cmd[1] = 'launch'
                    logger.info( 'cmd: %s', cmd )
                    subprocess.check_call( cmd )
            if sigtermNotSignaled():
                time.sleep( 30 )

    elif args.action == 'terminateBad':
        if not args.authToken:
            sys.exit( 'error: can not terminate because no authToken was passed')
        terminatedDateTimeStr = datetime.datetime.now( datetime.timezone.utc ).isoformat()
        # retrieve list of signers so we can deauthorize each before terminating
        allSigners = list( db['allSigners'].find() )
        logger.info( 'len(allSigners): %d', len(allSigners) )
        signersByIid = {signer['instanceId']: signer for signer in allSigners }
        logger.debug( 'signer iids: %s', signersByIid.keys() )

        coll = db['checkedInstances']
        wereChecked = list( coll.find() ) # fully iterates the cursor, getting all records
        toTerminate = []  # list of iids
        toPurge = []  # list of instance-like dicts containing instanceId and ssh fields
        for checkedInst in wereChecked:
            state = checkedInst.get( 'state')
            #logger.info( 'checked state %s', state )
            if state in ['failed', 'excepted', 'stopped' ]:
                iid = checkedInst['_id']
                if iid in signersByIid:
                    logger.info( 'terminateBad deauthorizing %s', iid )
                    authorizeSigner( db, args.configName, iid, False )
                abbrevIid = iid[0:16]
                logger.warning( 'would terminate %s', abbrevIid )
                toTerminate.append( iid )
                if 'instanceId' not in checkedInst:
                    checkedInst['instanceId'] = iid
                toPurge.append( checkedInst )
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

        logger.info( 'checking for instances to deauthorize')
        # retrieve list of signers so we can deauthorize each before terminating
        allSigners = list( db['allSigners'].find() )
        logger.info( 'len(allSigners): %d', len(allSigners) )
        signersByIid = {signer['instanceId']: signer for signer in allSigners }
        logger.debug( 'signer iids: %s', signersByIid.keys() )

        nToDeauth = 0
        for iid in startedIids:
            if iid in signersByIid and signersByIid[iid].get('auth'):
                nToDeauth += 1
                logger.info( 'terminateAll deauthorizing %s', iid )
                authorizeSigner( db, args.configName, iid, False )
                time.sleep( 30 )
        logger.info( 'deauthorized %d nodes', nToDeauth )

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
