#!/usr/bin/env python3
"""
maintains and tracks instances in a squid-based proxy pool on NCS
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
from ncscli.tellInstances import anyFound
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
import jsonToKnownHosts
import sshForwarding  # expected to be in the same directory
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

def logLevelArg( arg ):
    '''return a logging level (int) for the given case-insensitive level name'''
    arg = arg.lower()
    map = {
        'critical': logging.CRITICAL,
        'error': logging.ERROR,
        'warning': logging.WARNING,
        'info': logging.INFO,
        'debug': logging.DEBUG
        }
    if arg not in map:
        logger.warning( 'the given logLevel "%s" is not recognized (try INFO, DEBUG, WARNING,, DEBUG, etc)', arg )
        raise argparse.ArgumentTypeError('Boolean value expected.')
    setting = map.get( arg, logging.INFO )
    return setting

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
    # this version uses global dataDirPath for badTerminations.log
    terminationLogFilePath = os.path.join( dataDirPath, 'badTerminations.log' )
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

def recruitInstances( nWorkersWanted, launchedJsonFilePath, authToken,
    resultsLogFilePath, installerFileName, preopenedPorts ):
    '''launch instances and install client on them;
        terminate those that could not install; return list of good instances'''
    logger.info( 'recruiting up to %d instances', nWorkersWanted )
    workerDirPath = args.configName
    goodInstances = []
    portMap = {}
    if True:
        nAvail = ncs.getAvailableDeviceCount( authToken, filtersJson=args.filter )
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
            sshClientKeyName = 'sproxy_%s' % (randomPart)
            respCode = ncs.uploadSshClientKey( authToken, sshClientKeyName, keyContents )
            if respCode < 200 or respCode >= 300:
                logger.warning( 'ncs.uploadSshClientKey returned %s', respCode )
                sys.exit( 'could not upload SSH client key')
        #launch
        rc = launchInstances( authToken, nWorkersWanted,
            sshClientKeyName, launchedJsonFilePath, filtersJson=args.filter,
            encryptFiles = args.encryptFiles
            )
        if rc:
            logger.info( 'launchInstances returned %d', rc )
        # delete sshClientKey only if we just uploaded it
        if sshClientKeyName != args.sshClientKeyName:
            logger.info( 'deleting sshClientKey %s', sshClientKeyName)
            ncs.deleteSshClientKey( authToken, sshClientKeyName )
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
        terminateNcsScInstances( authToken, nonstartedIids )
        logger.info( 'done terminating non-started instances' )
    # proceed with instances that were actually started
    startedInstances = [inst for inst in launchedInstances if inst['state'] == 'started' ]
    if not startedInstances:
        return ([], [], {})
    # add instances to knownHosts
    with open( os.path.expanduser('~/.ssh/known_hosts'), 'a' ) as khFile:
        jsonToKnownHosts.jsonToKnownHosts( startedInstances, khFile )

    if not sigtermSignaled():
        installerCmd = '%s %s'% (installerFileName, 'noVersion' ) 
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
            terminateNcsScInstances( authToken, startedIids )
            return ([], [], {})
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
            terminateNcsScInstances( authToken, badIids )
    if goodInstances and sigtermNotSignaled():
        # assign a forwarding port for each instance
        portMap = {}
        #ports = []
        index = 0
        for inst in goodInstances:
            port = preopenedPorts[index]
            #ports.append( port )
            iid = inst['instanceId']
            portMap[iid] = port
            index += 1
        #logger.info( 'ports: %s', ports )
        logger.info( 'portMap: %s', portMap )

        # configure proxies (by simply copying some files)
        configDirPath = '/etc/squid'
        configCmd = "cp -p -r %s/conf/* %s" % (workerDirPath, configDirPath )
        logger.debug( 'configCmd: %s', configCmd )
        stepStatuses = tellInstances.tellInstances( goodInstances, configCmd,
            #resultsLogFilePath=resultsLogFilePath,
            download=None, downloadDestDir=None, jsonOut=None, sshAgent=args.sshAgent,
            timeLimit=args.timeLimit, upload=None, stopOnSigterm=True,
            knownHostsOnly=True
            )
        # separate good tellInstances statuses from bad ones
        goodIids = []
        #badStatuses = []
        for status in stepStatuses:
            if isinstance( status['status'], int) and status['status'] == 0:
                goodIids.append( status['instanceId'])
            else:
                badStatuses.append( status )
                if isinstance( status['status'], asyncio.TimeoutError ):
                    logger.info( 'config status asyncio.TimeoutError' )
        logger.info( '%d good configs, %d bad configs', len(goodIids), len(badStatuses) )
        goodInstances = [inst for inst in startedInstances if inst['instanceId'] in goodIids ]
        badIids = []
        for status in badStatuses:
            badIids.append( status['instanceId'] )
        if badIids:
            #logOperation( 'terminateBad', badIids, '<recruitInstances>' )
            terminateNcsScInstances( authToken, badIids )

    if goodInstances and not sigtermSignaled():
        # start squid on the good instances
        logger.info( 'starting %d proxies', len(goodInstances) )
        starterCmd = 'squid'
        stepStatuses = tellInstances.tellInstances( goodInstances, starterCmd,
            #resultsLogFilePath=resultsLogFilePath,
            download=None, downloadDestDir=None, jsonOut=None, sshAgent=args.sshAgent,
            timeLimit=args.timeLimit, upload=None, stopOnSigterm=True,
            knownHostsOnly=True
            )
        # separate good tellInstances statuses from bad ones
        goodIids = []
        #badStatuses = []
        for status in stepStatuses:
            if isinstance( status['status'], int) and status['status'] == 0:
                goodIids.append( status['instanceId'])
            else:
                badStatuses.append( status )
                if isinstance( status['status'], asyncio.TimeoutError ):
                    logger.info( 'starter status asyncio.TimeoutError' )
        logger.info( '%d good starts, %d bad starts', len(goodIids), len(badStatuses) )
        goodInstances = [inst for inst in startedInstances if inst['instanceId'] in goodIids ]
        badIids = []
        for status in badStatuses:
            badIids.append( status['instanceId'] )
        if badIids:
            #logOperation( 'terminateBad', badIids, '<recruitInstances>' )
            terminateNcsScInstances( authToken, badIids )

    return goodInstances, badStatuses, portMap

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

def exportProxyAddrs( dataDirPath, forwarders, forwarderHost ):
    '''save forwarding addresses (host:port) for forwarded instances'''
    proxyListFilePath = os.path.join( dataDirPath, 'proxyAddrs.txt' )
    if forwarders:
        with open( proxyListFilePath, 'w' ) as proxyListFile:
            for fw in forwarders:
                print( '%s:%s' % (forwarderHost, fw['port']), file=proxyListFile )

def launchProxies( dataDirPath, db, authToken, args ):
    '''recruits proxy nodes, starts forwarders, updates database; may raise raise ValueError'''
    startDateTime = datetime.datetime.now( datetime.timezone.utc )
    dateTimeTagFormat = '%Y-%m-%d_%H%M%S'  # cant use iso format dates in filenames because colons
    dateTimeTag = startDateTime.strftime( dateTimeTagFormat )

    launchedJsonFilePath = os.path.join( dataDirPath, 'launched_%s.json' % dateTimeTag )
    resultsLogFilePath = os.path.join( dataDirPath, 'installProxy_%s.jlog' % dateTimeTag )
    forwardingFilePath = os.path.join( dataDirPath, 'sshForwarding_%s.csv' % dateTimeTag )
    proxyListFilePath = os.path.join( dataDirPath, 'proxyAddrs.txt' )

    logger.info( 'the launch filter is %s', args.filter )
    if args.count and (args.count > 0):
        nToLaunch = args.count
    elif args.target and (args.target > 0):
        logger.info( 'launcher target: %s', args.target )
        # try to reach target by counting existing working instances and launching more
        mQuery =  {'state': 'checked' }
        nExisting = db['checkedInstances'].count_documents( mQuery )
        logger.info( '%d workers checked', nExisting )
        nToLaunch = int( (args.target - nExisting) * 1.0 )
        nToLaunch = max( nToLaunch, 0 )

        nAvail = ncs.getAvailableDeviceCount( authToken, filtersJson=args.filter )
        logger.info( '%d devices available', nAvail )

        nToLaunch = min( nToLaunch, nAvail )
        nToLaunch = min( nToLaunch, 60 )
    else:
        raise ValueError( 'error: no positive --count or --target supplied')
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

    forwarderHost = args.forwarderHost
    if not forwarderHost:
        try:
            forwarderHost = requests.get( 'https://api.ipify.org' ).text
        except forwarderHost:
            logger.warning( 'could not get public ip addr of this host')
    if not forwarderHost:
        logger.error( 'forwarderHost not set')
        raise ValueError( 'forwarderHost not set' )

    preopened = sshForwarding.preopenPorts( args.portRangeStart, 
        args.maxPort, nToLaunch, ipAddr='0.0.0.0'
        )
    preopenedPorts = preopened.get('ports', [])
    logger.info( 'preopened ports: %s', preopenedPorts )
    if len( preopenedPorts ) < nToLaunch:
        logger.error( 'not enough ports available (%d not %d)', len( preopenedPorts ), nToLaunch )
        raise RuntimeError( 'not enough ports available' )

    # launch and install, passing name of installer script to upload and run
    (goodInstances, badStatuses, portMap) = recruitInstances( nToLaunch,
        launchedJsonFilePath, authToken,
        resultsLogFilePath, args.installerFileName, preopenedPorts )
    if not goodInstances:
        logger.warning( 'no instances recruited' )
    recruitedIids = [inst['instanceId'] for inst in goodInstances ]
    nRecruited = len( recruitedIids )
    if not nRecruited:
        logger.warning( 'nRecruited is %d', nRecruited )
    logger.debug( 'goodInstances: %s', goodInstances )
    nRecruited = nToLaunch  # CHEATING

    if nRecruited and sigtermNotSignaled():
        # save information about the recruiting
        collName = 'launchedInstances'
        logger.info( 'ingesting into %s', collName )
        ingestJson( launchedJsonFilePath, dbName, collName, append=True )
        # remove the launchedJsonFile to avoid local accumulation of data
        os.remove( launchedJsonFilePath )
        if not os.path.isfile( resultsLogFilePath ):
            logger.warning( 'no results file %s', resultsLogFilePath )
        else:
            ingestJson( resultsLogFilePath, dbName, 'installProxy_'+dateTimeTag, append=False )
            # remove the resultsLogFile to avoid local accumulation of data
            os.remove( resultsLogFilePath )
        for statusRec in badStatuses:
            # carefully convert the status (which may be an exception) into a string
            # (using repr() only when str() returns empty)
            statusStr = str(statusRec['status']) or repr(statusRec['status'])
            statusRec['status'] = statusStr
            statusRec['dateTime'] = startDateTime.isoformat()
            statusRec['_id' ] = statusRec['instanceId']
            db['badInstalls'].insert_one( statusRec )
    if nRecruited and sigtermNotSignaled():
        logger.info( 'preclosing ports')
        preclosedPorts = sshForwarding.preclosePorts( preopened )

        # start the ssh port-forwarding
        logger.info( 'would forward ports for %d instances', len(goodInstances) )
        forwarders = sshForwarding.startForwarders( goodInstances,
            forwarderHost=forwarderHost,
            portMap=portMap, targetPort=3128,
            portRangeStart=args.portRangeStart, maxPort=args.maxPort,
            forwardingCsvFilePath=forwardingFilePath
            )
        if len( forwarders ) < len( goodInstances ):
            logger.warning( 'some instances could not be forwarded to' )
        logger.debug( 'forwarders: %s', forwarders )
        # get iids only for successfully forwarded proxies
        forwardedIids = [fw['instanceId'] for fw in forwarders ]
        # save forwarding addresses (host:port) for forwarded instances
        if forwarders:
            with open( proxyListFilePath, 'a' ) as proxyListFile:
                for fw in forwarders:
                    print( fw['mapping'], file=proxyListFile )

        goodInstances = [inst for inst in goodInstances if inst['instanceId'] in forwardedIids ]
        unusableIids = list( set(recruitedIids) - set( forwardedIids) )
        terminateNcsScInstances( authToken, unusableIids )

def saveErrorsByIid( errorsByIid, db, operation=None ):
    checkedColl = db.checkedInstances
    errorsColl = db.instanceErrors
    checkedDateTime = datetime.datetime.now( datetime.timezone.utc )
    checkedDateTimeStr = checkedDateTime.isoformat()

    for iid, err in errorsByIid.items():

        record = { 'instanceId': iid, 'checkedDateTime': checkedDateTimeStr, 'operation': operation }
        # COULD do this test:
        '''
        try:
            iterator = iter(err)
        except TypeError:
            # not iterable
        else:
            # iterable
        '''
        if 'discrep' in err:
            discrep = err['discrep']
            logger.debug( 'updating clockOffset %.1f for inst %s', discrep, iid[0:8] )
            checkedColl.update_one( {'_id': iid}, { "$set": { "clockOffset": discrep } } )
            checkedColl.update_one( {'_id': iid}, { "$inc": { "nFailures": 1 } } )
            record.update( { 'status': 'badClock', 'returnCode': discrep } )
            errorsColl.insert_one( record )
        elif 'notForwarded' in err:
            returnCode = err['notForwarded']
            checkedColl.update_one( {'_id': iid}, { "$inc": { "nFailures": 1 } } )
            record.update( { 'status': 'notForwarded', 'returnCode': returnCode } )
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
            logger.warning( 'instance %s gave status (%s) "%s"', iid[0:8], type(err), err )
            record.update({ 'status': 'misc', 'msg': str(err) })
            errorsColl.insert_one( record )
        # change (or omit) this if wanting to allow more than one failure before marking it failed
        checkedInst = checkedColl.find_one( {'_id': iid }, {'ssh':0} )
        if checkedInst['nExceptions'] > checkedInst['nFailures']:
            state = 'excepted'
        else:
            state = 'failed'
        checkedColl.update_one( {'_id': iid}, { "$set": { "state": state } } )

def checkForwarders( liveInstances,  forwarders ):
    errorsByIid = {}
    forwardedHosts = set( [fw['host'] for fw in forwarders] )
    #logger.info( 'forwardedHosts: %s', forwardedHosts )

    portsUsed = {}
    for fw in forwarders:
        fwPort = fw['port']
        if fwPort in portsUsed:
            logger.warning( 'non-unique port %d for host %s in use by %s',
                fwPort, fw['host'], portsUsed[ fwPort ] )
        else:
            portsUsed[fwPort] = fw['host']

    for inst in liveInstances:
        iid = inst['instanceId']
        instHost = inst['ssh']['host']
        if instHost not in forwardedHosts:
            logger.warning( 'NOT forwarding port for %s', iid[0:8] )
            errorsByIid[ iid ] = {'notForwarded': 1 }
    return errorsByIid

def checkRequests( instances,  forwarders, forwarderHost ):
    targetUrl = 'https://loadtest-target.neocortix.com'
    logger.info( 'checking %d instance(s)', len(instances) )
    errorsByIid = {}
    timeouts = (10, 30)
    forwardersByHost = { fw['host']: fw for fw in forwarders }
    for inst in instances:
        instHost = inst['ssh']['host']
        fw = forwardersByHost[ instHost ]
        proxyAddr = '%s:%s' % (forwarderHost, fw['port'])
        proxyUrl = 'http://' + proxyAddr
        proxyDict = {'http': proxyUrl, 'https': proxyUrl, 'ftp': proxyUrl }
        iid = inst['instanceId']
        logger.debug( 'checking %s; iid: %s', fw, iid )
        try:
            resp = requests.get( targetUrl, proxies=proxyDict, verify=False, timeout=timeouts )
            if resp.status_code in range( 200, 400 ):
                logger.debug( 'good response %d for %s (inst %s)', resp.status_code, proxyAddr, iid[0:8] )
            else:
                logger.warning( 'bad response %d for %s (instance %s)', resp.status_code, proxyAddr, iid )
                errorsByIid[ iid ] = {'status': resp.status_code}
        except Exception as exc:
            logger.warning( 'exception (%s) for iid %s "%s"', type(exc), iid, exc )
            errorsByIid[ iid ] = {'exception': exc }
    return errorsByIid

def checkWorkerProcesses( instances, dataDirPath, procName ):
    '''check for a running process with the given (partial) process name on each instance'''
    logger.debug( 'checking %d instance(s)', len(instances) )

    cmd = "ps -ef | grep -v grep | grep '%s' > /dev/null" % procName
    stepStatuses = tellInstances.tellInstances( instances, cmd,
        timeLimit=15*60,
        resultsLogFilePath = dataDirPath + '/checkWorkerProcesses.jlog',
        knownHostsOnly=True, stopOnSigterm=True
        )
    #logger.info( 'proc statuses: %s', stepStatuses )
    errorsByIid = {status['instanceId']: status for status in stepStatuses if status['status'] }
    logger.debug( 'errorsByIid: %s', errorsByIid )
    return errorsByIid

def checkInstanceClocks( liveInstances, dataDirPath ):
    jlogFilePath = dataDirPath + '/checkInstanceClocks.jlog'
    allIids = [inst['instanceId'] for inst in liveInstances ]
    unfoundIids = set( allIids )
    cmd = "date --iso-8601=seconds"
    # check current time on each instance
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
                    if discrep > 4 or discrep < -2:
                        #logger.warning( 'bad time discrep: %.1f', discrep )
                        errorsByIid[ iid ] = {'discrep': discrep }
    if unfoundIids:
        logger.warning( 'unfoundIids: %s', unfoundIids )
        for iid in list( unfoundIids ):
            if iid not in errorsByIid:
                errorsByIid[ iid ] = {'found': False }
    logger.debug( '%d errorsByIid: %s', len(errorsByIid), errorsByIid )
    return errorsByIid

def retrieveLogs( goodInstances, dataDirPath, logParentDirPath=None ):
    '''retrieve logs from nodes, storing them in dataDirPath/workerLogs subdirectories'''
    logger.info( 'retrieving logs for %d instance(s)', len(goodInstances) )

    nodeLogFilePath = '/var/log/squid/*.log'

    # download the log file from each instance
    stepStatuses = tellInstances.tellInstances( goodInstances,
        download=nodeLogFilePath, downloadDestDir=dataDirPath +'/workerLogs',
        timeLimit=15*60,
        knownHostsOnly=True, stopOnSigterm=True
        )
    #logger.info( 'download statuses: %s', stepStatuses )
    errorsByIid = {status['instanceId']: status['status'] for status in stepStatuses if status['status'] }
    return errorsByIid

def checkLogs( liveInstances, dataDirPath ):
    '''check contents of logs from nodes, save errorSummary.csv, return errorsByIid'''
    logsDirPath = os.path.join( dataDirPath, 'workerLogs' )
    logDirs = os.listdir( logsDirPath )
    logger.info( '%d logDirs found', len(logDirs ) )

    liveIids = [inst['instanceId'] for inst in liveInstances]

    errorsByIid = {}

    logFileName = 'cache.log'
    for logDir in logDirs:
        iid = logDir
        iidAbbrev = iid[0:8]
        if iid not in liveIids:
            logger.debug( 'not checking non-live instance %s', iidAbbrev )
            continue
        logFilePath = os.path.join( logsDirPath, logDir, logFileName )
        if not os.path.isfile( logFilePath ):
            logger.warning( 'no log file %s', logFilePath )
        elif os.path.getsize( logFilePath ) <= 0:
            logger.warning( 'empty log file "%s"', logFilePath )
        else:
            ready = False
            with open( logFilePath ) as logFile:
                try:
                    for line in logFile:
                        line = line.rstrip()
                        if not line:
                            continue
                        if anyFound( ['FATAL', 'ERROR', 'SECURITY' ], line ):
                            logger.info( '%s: %s', iidAbbrev, line )
                            theseErrors = errorsByIid.get( iid, [] )
                            theseErrors.append( line )
                            errorsByIid[iid] = theseErrors
                        elif 'Accepting ' in line:
                            ready = True
                except Exception as exc:
                    logger.warning( 'caught exception (%s) %s', type(exc), exc )
            if not ready:
                logger.warning( 'instance %s did not say "Accepting"', iidAbbrev )
    logger.debug( 'found errors for %d instance(s)', len(errorsByIid) )
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

def collectSquidLogEntries( db, inFilePath, collName ):
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
            logger.debug( '%d records match latest dateTime in %s', len(allLatest), collName )
            logger.debug( 'latestMsgs: %s', latestMsgs )
    #datePat = r'\[(.*\|.*)\]'
    lineDateTime = datetime.datetime.fromtimestamp( 0, datetime.timezone.utc )
    lineLevel = None
    recs = []

    with open( inFilePath, 'r' ) as logFile:
        try:
            for line in logFile:
                line = line.rstrip()
                if not line:
                    continue
                if anyFound( ['FATAL', 'ERROR', 'SECURITY' ], line ):
                    lineLevel = 'ERROR'
                elif 'WARNING' in line:
                    lineLevel = 'ERROR'
                else:
                    lineLevel = 'INFO'
                # tricky parsing of squid date stamp
                dateStr = ' '.join( line.split('|')[0].split(' ')[0:2])
                if dateStr:
                    dt = dateutil.parser.parse( dateStr )
                    lineDateTime = universalizeDateTime( dt )
                if lineDateTime < lastExistingDateTime:
                    logger.debug( 'skipping old (%s < %s)', lineDateTime, lastExistingDateTime )
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
            logger.debug( '%d records match latest dateTime', len(allLatest) )
            logger.debug( 'latestMsgs: %s', latestMsgs )
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

def processWorkerLogFiles( dataDirPath ):
    '''ingest all new or updated node logs; delete very old log files'''
    logsDirPath = os.path.join( dataDirPath, 'workerLogs' )
    logDirs = os.listdir( logsDirPath )
    logger.debug( '%d logDirs found', len(logDirs ) )
    # but first, delete very old log files
    lookbackDays = 7
    thresholdDateTime = datetime.datetime.now( datetime.timezone.utc ) \
        - datetime.timedelta( days=lookbackDays )
    #lookBackHours = 24
    #newerThresholdDateTime = datetime.datetime.now( datetime.timezone.utc ) \
    #    - datetime.timedelta( hours=lookBackHours )

    # delete very old log files
    for logDir in logDirs:
        inFilePath = os.path.join( logsDirPath, logDir, 'cache.jlog' )
        if not os.path.isfile( inFilePath ):
            inFilePath = os.path.join( logsDirPath, logDir, 'cache.log' )
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
        filter={ 'name': {'$regex': r'^workerLog_.*'} } )
    iStartTime = time.time()
    nIngested = 0
    jlogFileName = 'cache.jlog'  # not really useful for squid
    logFileName = 'cache.log'
    for logDir in logDirs:
        # logDir is also the instanceId
        # read jlog file, if present, otherwise log file
        inFilePath = os.path.join( logsDirPath, logDir, jlogFileName )
        if not os.path.isfile( inFilePath ):
            inFilePath = os.path.join( logsDirPath, logDir, logFileName )
        if not os.path.isfile( inFilePath ):
            logger.info( 'no file "%s"', inFilePath )
            continue
        elif os.path.getsize( inFilePath ) <= 0:
            logger.info( 'empty log file "%s"', inFilePath )
            continue
        collName = 'workerLog_' + logDir
        if collName in loggedCollNames:
            existingGenTime = lastGenDateTime( db[collName] ) - datetime.timedelta(minutes=1)
            fileModDateTime = datetime.datetime.fromtimestamp( os.path.getmtime( inFilePath ) )
            fileModDateTime = universalizeDateTime( fileModDateTime )
            if existingGenTime >= fileModDateTime:
                logger.debug( 'already ingested %s (%s >= %s)',
                    logDir[0:8], existingGenTime, fileModDateTime  )
                continue
        logger.debug( 'ingesting log for %s', logDir[0:16] )
        try:
            if inFilePath.endswith( '.jlog' ):
                collectGethJLogEntries( db, inFilePath, collName )
            else:
                collectSquidLogEntries( db, inFilePath, collName )
            nIngested += 1
        except Exception as exc:
            logger.warning( 'exception (%s) ingesting %s', type(exc), inFilePath, exc_info=not False )
    logger.info( 'ingesting %d logs took %.1f seconds', nIngested, time.time()-iStartTime)

def killForwarders( instances ):
    # get list of forwarders so we can terminate those for dying instances
    forwarders = sshForwarding.findForwarders()
    forwardersByHost = { fw['host']: fw for fw in forwarders }

    for inst in instances:
        iid = inst.get('instanceId') or inst.get('_id')
        instHost = inst['ssh']['host']
        if instHost in forwardersByHost:
            pid = forwardersByHost[instHost].get('pid')
            if pid:
                abbrevIid = iid[0:8]
                logger.info( 'canceling forwarding (pid %d) for %s', pid, abbrevIid )
                os.kill( pid, signal.SIGTERM )

def checkWorkers( dataDirPath, db, authToken, forwarderHost, args ):
    '''checks status of worker instances, updates database'''
    if not db.list_collection_names():
        logger.warning( 'no collections found for db %s', dbName )
        return
    #checkerTimeLimit = args.timeLimit
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
    #dateTimeTagFormat = '%Y-%m-%d_%H%M%S'  # cant use iso format dates in filenames because colons
    #dateTimeTag = checkedDateTime.strftime( dateTimeTagFormat )
    
    # find out which checkable instances are live
    checkableIids = [inst['instanceId'] for inst in checkables ]
    liveInstances = getLiveInstances( checkableIids, authToken )
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
    logger.debug( '%d instances checkable', len(checkables) )
    errorsByIid = checkWorkerProcesses( checkables, dataDirPath, 'squid' )
    saveErrorsByIid( errorsByIid, db, operation='checkWorkerProcesses' )

    checkables = [inst for inst in checkables if inst['instanceId'] not in errorsByIid]
    logger.debug( '%d instances checkable', len(checkables) )

    errorsByIid = retrieveLogs( checkables, dataDirPath )
    errorsByIid = checkLogs( checkables, dataDirPath )
    saveErrorsByIid( errorsByIid, db, operation='checkLogs' )
    processWorkerLogFiles( dataDirPath )

    forwarders = sshForwarding.findForwarders()
    errorsByIid = checkForwarders( checkables,  forwarders )
    if errorsByIid:
        logger.info( 'checkForwarders errorsByIid: %s', errorsByIid )
    saveErrorsByIid( errorsByIid, db, operation='checkForwarders' )
    checkables = [inst for inst in checkables if inst['instanceId'] not in errorsByIid]

    # suppress warnings that would occur for untrusted https certs
    logging.getLogger('py.warnings').setLevel( logging.ERROR )
    errorsByIid = checkRequests( checkables,  forwarders, forwarderHost )
    # set warnings back to normal
    logging.getLogger('py.warnings').setLevel( logging.WARNING )
    saveErrorsByIid( errorsByIid, db, operation='checkRequests' )
    checkables = [inst for inst in checkables if inst['instanceId'] not in errorsByIid]

    checkedHosts = [inst['ssh']['host'] for inst in checkables]
    goodForwarders = [fw for fw in forwarders if fw['host'] in checkedHosts ]
    exportProxyAddrs( dataDirPath, goodForwarders, forwarderHost )


if __name__ == "__main__":
    startTime = time.time()
    # configure logger formatting
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)
    logging.captureWarnings( True )


    ap = argparse.ArgumentParser( description=__doc__,
        fromfile_prefix_chars='@', formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    ap.add_argument( 'action', help='the action to perform', 
        choices=[ 'launch', 'check', 'maintain', 'terminateBad', 'terminateAll', 'reterminate'
            ]
        )
    ap.add_argument( '--logLevel', type=logLevelArg, default=logging.INFO, help='verbosity of log (e.g. debug, info, warning, error)' )
    ap.add_argument( '--authToken', help='the NCS authorization token to use (required for launch or terminate)' )
    ap.add_argument( '--count', type=int, help='the number of instances (for launch)' )
    ap.add_argument( '--target', type=int, help='target number of working instances (for launch)',
        default=2 )
    ap.add_argument( '--configName', help='the name of the squid config', default='squidWorker' )
    ap.add_argument( '--dataDir', help='data directory', default='./pools/' )
    ap.add_argument( '--encryptFiles', type=ncs.boolArg, default=False, help='whether to encrypt files on launched instances' )
    ap.add_argument( '--pool', required=True, help='the name of the proxy pool (required)' )
    ap.add_argument( '--filter', help='json to filter instances for launch',
        default = '{ "cpu-arch": "aarch64", "dar": ">=99", "storage": ">=2000000000" }' )
    ap.add_argument( '--installerFileName', help='a script to run on new instances',
        default='squidWorker/installCustomSquid.sh' )
    ap.add_argument( '--instanceId', help='id of instance (for authorize or deauthorize)' )
    ap.add_argument( '--forwarderHost', help='IP addr (or host name) of the forwarder host',
        default='localhost' )
    ap.add_argument( '--portRangeStart', type=int, default=7100,
        help='the beginning of the range of port numbers to forward' )
    ap.add_argument( '--maxPort', type=int, help='maximum port number to forward',
        default=7199 )
    ap.add_argument( '--uploads', help='glob for filenames to upload to workers', default='netconfig' )
    ap.add_argument( '--mongoHost', help='the host of mongodb server', default='localhost')
    ap.add_argument( '--mongoPort', help='the port of mongodb server', default=27017)
    ap.add_argument( '--sshAgent', type=boolArg, default=False, help='whether or not to use ssh agent' )
    ap.add_argument( '--sshClientKeyName', help='the name of the uploaded ssh client key to use (advanced)' )
    ap.add_argument( '--timeLimit', type=int, help='time limit (in seconds) for the whole job',
        default=11*60 )
    args = ap.parse_args()

    logger.setLevel( args.logLevel )
    ncs.logger.setLevel(args.logLevel)
    logger.setLevel(args.logLevel)
    tellInstances.logger.setLevel(logging.WARNING)
    logger.debug('the logger is configured')

    logger.info( 'performing action "%s" for pool "%s"', args.action, args.pool )

    dataDirPath = os.path.join( args.dataDir, args.pool )
    os.makedirs( dataDirPath, exist_ok=True )

    mclient = pymongo.MongoClient(args.mongoHost, args.mongoPort)
    dbName = 'sproxy_' + args.pool
    db = mclient[dbName]

    forwarderHost = args.forwarderHost
    if not forwarderHost:
        try:
            forwarderHost = requests.get( 'https://api.ipify.org' ).text
        except forwarderHost:
            logger.warning( 'could not get public ip addr of this host')
    
    authToken = args.authToken or os.getenv( 'NCS_AUTH_TOKEN' )
    if not authToken:
        logger.error( 'no authToken was given as argument or $NCS_AUTH_TOKEN' )
        sys.exit( 1 )
    if not ncs.validAuthToken( authToken ):
        logger.error( 'the given authToken was not an alphanumeric ascii string' )
        sys.exit( 1 )
    # do a query to check whether the authToken is actually authorized
    resp = ncs.queryNcsSc( 'instances', authToken )
    if resp['statusCode'] == 403:
        logger.error( 'the given authToken was not accepted' )
        sys.exit( 1 )
    elif resp['statusCode'] not in range( 200, 300 ):
        logger.error( 'service error (%d) while validating authToken' )
        sys.exit( 1 )
    logger.info( 'authToken ok (%s)', resp['statusCode'])

    if args.action == 'launch':
        launchProxies( dataDirPath, db, authToken, args )
    elif args.action == 'check':
        if not forwarderHost:
            logger.error( 'forwarderHost not set')
            sys.exit( 'forwarderHost not set' )
        checkWorkers( dataDirPath, db, authToken, forwarderHost, args )
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
        #logger.debug( 'argv: %s', sys.argv )
        cmd = sys.argv.copy()
        while sigtermNotSignaled():
            cmd[1] = 'check'
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
        if not authToken:
            sys.exit( 'error: can not terminate because no authToken was passed')
        terminatedDateTimeStr = datetime.datetime.now( datetime.timezone.utc ).isoformat()
        # get list of forwarders so we can terminate those for dying instances
        forwarders = sshForwarding.findForwarders()
        forwardersByHost = { fw['host']: fw for fw in forwarders }

        coll = db['checkedInstances']
        wereChecked = list( coll.find() ) # fully iterates the cursor, getting all records
        toTerminate = []  # list of iids
        toPurge = []  # list of instance-like dicts containing instanceId and ssh fields
        for checkedInst in wereChecked:
            state = checkedInst.get( 'state')
            #logger.info( 'checked state %s', state )
            if state in ['failed', 'excepted', 'stopped', 'notForwarded' ]:
                iid = checkedInst['_id']
                abbrevIid = iid[0:16]
                logger.warning( 'would terminate %s', abbrevIid )
                instHost = checkedInst['ssh']['host']
                if instHost in forwardersByHost:
                    pid = forwardersByHost[instHost].get('pid')
                    if pid:
                        logger.info( 'canceling forwarding (pid %d) for %s', pid, iid[0:8] )
                        os.kill( pid, signal.SIGTERM )

                toTerminate.append( iid )
                if 'instanceId' not in checkedInst:
                    checkedInst['instanceId'] = iid
                toPurge.append( checkedInst )
                coll.update_one( {'_id': iid}, { "$set": { 'reasonTerminated': state } } )
        logger.info( 'terminating %d instances', len( toTerminate ))
        terminated=terminateNcsScInstances( authToken, toTerminate )
        logger.info( 'actually terminated %d instances', len( terminated ))
        purgeHostKeys( toPurge )
        # update states in checkedInstances
        for iid in terminated:
            coll.update_one( {'_id': iid},
                { "$set": { "state": "terminated",
                    'terminatedDateTime': terminatedDateTimeStr } } )
    elif args.action == 'terminateAll':
        if not authToken:
            sys.exit( 'error: can not terminate because no authToken was passed')
        logger.info( 'checking for instances to terminate')
        # will terminate all instances and update checkedInstances accordingly
        startedInstances = getStartedInstances( db )
        startedIids = [inst['instanceId'] for inst in startedInstances]
        
        killForwarders( startedInstances )
        #sys.exit( 'DEBUGGING' )

        terminatedDateTimeStr = datetime.datetime.now( datetime.timezone.utc ).isoformat()
        toTerminate = startedIids
        logger.info( 'terminating %d instances', len( toTerminate ))
        terminated=terminateNcsScInstances( authToken, toTerminate )
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
        if not authToken:
            sys.exit( 'error: can not terminate because no authToken was passed')
        # get list of terminated instance IDs
        coll = db['checkedInstances']
        wereChecked = list( coll.find({'state': 'terminated'},
            {'_id': 1, 'ssh': 1, 'terminatedDateTime': 1 }) )
        toTerminate = []
        terminalInstances = []
        for checkedInst in wereChecked:
            iid = checkedInst['_id']
            abbrevIid = iid[0:8]
            tdt = checkedInst.get( 'terminatedDateTime', '(no terminatedDateTime)' )
            logger.info( 'would reterminate %s from %s', abbrevIid, tdt )

            toTerminate.append( iid )
            terminalInstances.append( checkedInst )
        
        toUnforward = [inst for inst in terminalInstances if 'ssh' in inst ]
        logger.info( 'unforwarding up to %d instances', len( toUnforward ))
        killForwarders( toUnforward )
        logger.info( 'reterminating %d instances', len( toTerminate ))
        terminated = terminateNcsScInstances( authToken, toTerminate )
        logger.info( 'reterminated %d instances', len( terminated ) )
        #TODO purgeHostKeys
    else:
        logger.warning( 'action "%s" unimplemented', args.action )
    elapsed = time.time() - startTime
    logger.info( 'finished action "%s"; elapsed time %.1f minutes',
        args.action, elapsed/60 )
