#!/usr/bin/env python3
"""
sumnmarizes the status of instances running boinc client
"""

# standard library modules
import argparse
import asyncio
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
import dateutil.parser
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
import waitForScript

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


def anyFound( a, b ):
    ''' return true iff any items from iterable a is found in iterable b '''
    for x in a:
        if x in b:
            return True
    return False

def boolArg( v ):
    '''use with ArgumentParser add_argument for (case-insensitive) boolean arg'''
    if v.lower() == 'true':
        return True
    elif v.lower() == 'false':
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def isNumber( sss ):
    try:
        float(sss)
        return True
    except ValueError:
        return False

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
    #logger.info( 'launched collections: %s', collNames )
    startedInstances = []
    for collName in collNames:
        #logger.info( 'getting instances from %s', collName )
        launchedColl = db[collName]
        inRecs = list( launchedColl.find() ) # fully iterates the cursor, getting all records
        if len(inRecs) <= 0:
            logger.warn( 'no launched instances found for %s', launchedTag )
        for inRec in inRecs:
            if 'instanceId' not in inRec:
                logger.warning( 'no instance ID in input record')
        startedInstances.extend( [inst for inst in inRecs if inst['state'] == 'started'] )
    return startedInstances

def ingestJson( srcFilePath, dbName, collectionName ):
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

def recruitInstances( nWorkersWanted, launchedJsonFilePath, launchWanted,
    resultsLogFilePath, installerFileName ):
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
            keyContents = loadSshPubKey().strip()
            randomPart = str( uuid.uuid4() )[0:13]
            #keyContents += ' #' + randomPart
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
        installerCmd = './' + installerFileName
        logger.info( 'calling tellInstances to install on %d instances', len(startedInstances))
        stepStatuses = tellInstances.tellInstances( startedInstances, installerCmd,
            resultsLogFilePath=resultsLogFilePath,
            download=None, downloadDestDir=None, jsonOut=None, sshAgent=args.sshAgent,
            timeLimit=args.timeLimit, upload=installerFileName, stopOnSigterm=False,
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
                if isinstance( status['status'], asyncio.TimeoutError ):
                    logger.info( 'installer status asyncio.TimeoutError' )
        
        logger.info( '%d good installs, %d bad installs', len(goodOnes), len(badOnes) )
        #logger.info( 'stepStatuses %s', stepStatuses )
        goodInstances = [inst for inst in startedInstances if inst['instanceId'] in goodOnes ]
        badIids = []
        for status in badOnes:
            badIids.append( status['instanceId'] )
        if badIids:
            #logOperation( 'terminateBad', badIids, '<recruitInstances>' )
            ncs.terminateInstances( args.authToken, badIids )

    return goodInstances, badOnes

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

def ingestBoincLog( logFile, coll ):
    '''ingest records from a boinc log innto a mongo collection'''
    linePat = r'(.*)\[(.*)\](.*)'
    #projSet = set()  # for debugging
    recs = []
    for line in logFile:
        line = line.rstrip()
        if not line:
            continue
        if line.endswith( 'Initialization completed' ):
            datePart = line.split(' Init')[0]
            dateTime = universalizeDateTime( dateutil.parser.parse( datePart ) )
            projPart = '---'  # really none, but this is what most non-project lines contain
            msg = 'Initialization completed'
            recs.append( {
                'dateTime': dateTime.isoformat(),
                'project': projPart,
                'msg': msg
                } )
            continue
        match = re.match( linePat, line )
        if not match:
            logger.warning( 'no match in %s', line )
            continue
        if len( match.groups() ) == 3:
            datePart = match.group(1)
            projPart = match.group(2)
            msgPart = match.group(3)
            
            #if projPart not in projSet:
            #    logger.info( 'proj %s', projPart )
            #    projSet.add( projPart )
            dateTime = universalizeDateTime( dateutil.parser.parse( datePart ) )
            
            msg = msgPart.strip()
            recs.append( {
                'dateTime': dateTime.isoformat(),
                'project': projPart,
                'msg': msg
                } )
    if len(recs):
        coll.insert_many( recs, ordered=True )
    #return recs

def collectBoincStatus( db, dataDirPath, statusType ):
    # will collect data only from "checked" instances
    wereChecked = db['checkedInstances'].find( {'state': 'checked' } )
    reportables = []
    for inst in wereChecked:
        #iid =inst['_id']
        if 'ssh' not in inst:
            logger.warning( 'no ssh info from checkedInstances for %s', inst )
        else:
            if 'instanceId' not in inst:
                inst['instanceId'] =inst ['_id']
            reportables.append( inst )

    startDateTime = datetime.datetime.now( datetime.timezone.utc )
    dateTimeTagFormat = '%Y-%m-%d_%H%M%S'  # cant use iso format dates in filenames because colons
    dateTimeTag = startDateTime.strftime( dateTimeTagFormat )

    resultsLogFilePath=dataDirPath+'/%s_%s.jlog' % (statusType, dateTimeTag )
    collName = '%s_%s' % (statusType, dateTimeTag )
    
    workerCmd = "boinccmd --%s || (sleep 5 && boinccmd --%s)" % (statusType, statusType)
    #logger.info( 'calling tellInstances to get status report on %d instances', len(reportables))
    stepStatuses_ = tellInstances.tellInstances( reportables, workerCmd,
        resultsLogFilePath=resultsLogFilePath,
        download=None, downloadDestDir=None, jsonOut=None, sshAgent=args.sshAgent,
        timeLimit=min(args.timeLimit, args.timeLimit), upload=None, stopOnSigterm=True,
        knownHostsOnly=False
        )
    (eventsByInstance, badIidSet_) = demuxResults( resultsLogFilePath )
    # save json file just for debugging
    #with open( resultsLogFilePath+'.json', 'w') as jsonOutFile:
    #    json.dump( eventsByInstance, jsonOutFile, indent=2 )
    # create a list of cleaned-up records to insert
    insertables = []
    for iid, events in eventsByInstance.items():
        for event in events:
            if 'instanceId' in event:
                del event['instanceId']
        insertables.append( {'instanceId': iid, 'events': events,
            'dateTime': events[0]['dateTime'] } )
    logger.info( 'inserting %d records into %s', len(insertables), collName )
    db[ collName ].insert_many( insertables )
    db[ collName ].create_index( 'instanceId' )
    db[ collName ].create_index( 'dateTime' )
    #ingestJson( resultsLogFilePath, db.name, collName )
    os.remove( resultsLogFilePath )
    return db[ collName ]

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

def parseProjectLines( lines ):
    props = {}
    ignorables = ['  name: ', '  description: ', '  URL: ' ]
    firstLine = True
    for line in lines:
        line = line.rstrip()
        if not line:
            continue
        if firstLine:
            firstLine = False
            if '== Projects ==' in line:
                continue
            else:
                logger.warning( 'improper first line')
                break
        if line[0] != ' ':
            if line.startswith( '2)' ):
                logger.info( 'found a second project; exiting')
                break
            #logger.info( 'ignoring %s', line.rstrip())
            continue
        if anyFound( ignorables, line ):
            #logger.info( 'ignoring %s', line.rstrip())
            continue
        if ':' in line:
            stripped = line.strip()
            parts = stripped.split( ':', 1 )  # only the first colon will be significant
            # convert to numeric or None type, if appropriate
            val = parts[1].strip()
            if val is None:
                pass
            elif val.isnumeric():
                val = int( val )
            elif isNumber( val ):
                val = float( val )
            props[ parts[0] ] = val
            continue
        logger.info( '> %s', line )
    return props

def mergeProjectData( srcColl, destColl ):
    # iterate over records, each containing output for an instance
    for inRec in srcColl.find():
        iid = inRec['instanceId']
        eventDateTime = inRec['dateTime']
        projLines = []
        if iid == '<master>':
            #logger.info( 'found <master> record' )
            pass
        else:
            #logger.info( 'iid: %s', iid )
            events = inRec['events']
            for event in events:
                if 'stdout' in event:
                    stdoutStr =  event['stdout']
                    projLines.append( stdoutStr )
        props = parseProjectLines( projLines )
        if not props:
            pass
            #logger.info( 'empty props for %s', iid )
        else:
            props['instanceId'] = iid
            props['checkedDateTime'] = eventDateTime
            #logger.info( 'props: %s', props  )
            destColl.replace_one( {'_id': iid }, props, upsert=True )

def parseTaskLines( lines ):
    tasks = []
    curTask = {}
    firstLine = True
    for line in lines:
        line = line.rstrip()
        if not line:
            continue
        if firstLine and '== Tasks ==' in line:
            continue
        if line[0] != ' ':
            #logger.info( 'task BOUNDARY %s', line )
            numPart = line.split( ')' )[0]
            taskNum = int(numPart)
            #logger.info( 'TASK %d', taskNum )
            curTask = { 'num': taskNum }
            tasks.append( curTask )
            continue
        if ':' in line:
            # extract a key:value pair from this line
            stripped = line.strip()
            parts = stripped.split( ':', 1 )  # only the first colon will be significant
            # convert to numeric or None type, if appropriate
            val = parts[1].strip()
            if val is None:
                pass
            elif val.isnumeric():
                val = int( val )
            elif isNumber( val ):
                val = float( val )
            # store the value
            curTask[ parts[0] ] = val
            continue
        logger.info( '> %s', line )
    return tasks

def mergeTaskData( srcColl, destColl ):
    preexisting = destColl.find( {}, {'instanceId':1, 'name':1} )
    preexMap = { rec['name']: rec for rec in preexisting }
    allTasks = {}
    # iterate over records, each containing output for an instance
    for inRec in srcColl.find():
        iid = inRec['instanceId']
        eventDateTime = inRec['dateTime']
        #abbrevIid = iid[0:16]
        taskLines = []
        if iid == '<master>':
            #logger.info( 'found <master> record' )
            pass
        else:
            #logger.info( 'iid: %s', iid )
            events = inRec['events']
            for event in events:
                if 'stdout' in event:
                    stdoutStr =  event['stdout']
                    taskLines.append( stdoutStr )
                    #logger.info( '%s: %s', abbrevIid, stdoutStr )
                    #if anyFound( ['WU name:', 'fraction done', 'UNINITIALIZED'], stdoutStr ):
                    #    logger.info( "%s: %s, %s", abbrevIid, eventDateTime[0:19], stdoutStr.strip() )
        tasks = parseTaskLines( taskLines )
        #print( 'tasks for', abbrevIid, 'from', eventDateTime[0:19] )
        #print( tasks  )
        for task in tasks:
            task['checkedDateTime'] = eventDateTime
            if task['name'] not in preexMap:
                task['startDateTimeApprox'] = eventDateTime
            if task['name'] not in allTasks:
                task['instanceId'] = iid
                allTasks[ task['name']] = task
            else:
                allTasks[ task['name']].update( task )

    #countsByIid = collections.Counter()
    for task in allTasks.values():
        taskName =  task['name']
        task['_id'] = taskName
        destColl.replace_one( {'_id': taskName }, task, upsert=True )
        #countsByIid[ task['instanceId'] ] += 1
    #logger.info( 'totTasks per instance: %s', countsByIid )
    return allTasks

def reportAll( db, dataDirPath ):
    collNames = sorted( db.list_collection_names(
        filter={ 'name': {'$regex': r'^get_cc_status_.*'} } ) )
    logger.info( 'get_cc_status_ collections: %s', collNames )
    for collName in collNames:
        logger.info( 'getting data from %s', collName )
        coll = db[collName]
        # iterate over records, each containing output for an instance
        for inRec in coll.find():
            iid = inRec['instanceId']
            eventDateTime = inRec['dateTime']
            abbrevIid = iid[0:16]
            if iid == '<master>':
                #logger.info( 'found <master> record' )
                pass
            else:
                #logger.info( 'iid: %s', iid )
                events = inRec['events']
                for event in events:
                    if 'stdout' in event:
                        stdoutStr =  event['stdout']
                        #logger.info( '%s: %s', abbrevIid, stdoutStr )
                        #TODO extract more info, depending on context
                        if 'batteries' in stdoutStr:
                            logger.info( "%s: %s, %s", abbrevIid, eventDateTime, stdoutStr.strip() )

    collNames = sorted( db.list_collection_names(
        filter={ 'name': {'$regex': r'^get_project_status_.*'} } ) )
    logger.info( 'get_project_status_ collections: %s', collNames )
    for collName in collNames:
        logger.info( 'getting data from %s', collName )
        coll = db[collName]
        # iterate over records, each containing output for an instance
        for inRec in coll.find():
            iid = inRec['instanceId']
            eventDateTime = inRec['dateTime']
            abbrevIid = iid[0:16]
            if iid == '<master>':
                #logger.info( 'found <master> record' )
                pass
            else:
                #logger.info( 'iid: %s', iid )
                events = inRec['events']
                for event in events:
                    if 'stdout' in event:
                        stdoutStr =  event['stdout']
                        #logger.info( '%s: %s', abbrevIid, stdoutStr )
                        #TODO extract more info, depending on context
                        #if 'downloaded' in stdoutStr or 'jobs succeeded' in stdoutStr:
                            #logger.info( "%s: %s, %s", abbrevIid, eventDateTime[0:19], stdoutStr.strip() )

    allTasks = {}
    collNames = sorted( db.list_collection_names(
        filter={ 'name': {'$regex': r'^get_tasks_.*'} } ) )
    logger.info( 'get_tasks_ collections: %s', collNames )
    for collName in collNames:
        logger.info( 'getting data from %s', collName )
        coll = db[collName]
        
        # iterate over records, each containing output for an instance
        for inRec in coll.find():
            iid = inRec['instanceId']
            eventDateTime = inRec['dateTime']
            abbrevIid = iid[0:16]
            taskLines = []
            if iid == '<master>':
                #logger.info( 'found <master> record' )
                pass
            else:
                #logger.info( 'iid: %s', iid )
                events = inRec['events']
                for event in events:
                    if 'stdout' in event:
                        stdoutStr =  event['stdout']
                        taskLines.append( stdoutStr )
                        #logger.info( '%s: %s', abbrevIid, stdoutStr )
                        #if anyFound( ['WU name:', 'fraction done', 'UNINITIALIZED'], stdoutStr ):
                        #    logger.info( "%s: %s, %s", abbrevIid, eventDateTime[0:19], stdoutStr.strip() )
            tasks = parseTaskLines( taskLines )
            #print( 'tasks for', abbrevIid, 'from', eventDateTime[0:19] )
            #print( tasks  )
            for task in tasks:
                if task['name'] not in allTasks:
                    task['startTimeApprox'] = eventDateTime
                    task['instanceId'] = iid
                    allTasks[ task['name']] = task
                else:
                    task['lastCheckTime'] = eventDateTime
                    allTasks[ task['name']].update( task )
        
    # save json file just for debugging
    with open( dataDirPath+'/allTasks.json', 'w') as jsonOutFile:
        json.dump( allTasks, jsonOutFile, indent=2 )
    #with open( dataDirPath+'/allTasksFlat.json', 'w') as jsonOutFile:
    #    json.dump( list( allTasks.values() ), jsonOutFile, indent=2 )
    countsByIid = collections.Counter()
    for task in allTasks.values():
        taskName =  task['name']
        task['_id'] = taskName
        countsByIid[ task['instanceId'] ] += 1
    logger.info( '%d total tasks listed', len( allTasks ) )
    logger.info( 'totTasks per instance: %s', countsByIid )

    logger.info( 'total tasks in db %d', db['allTasks'].count({}) )
    #mFilter = {"fraction done": {"$gt": 0} }
    mFilter = {"active_task_state": 'EXECUTING' }
    logger.info( 'current tasks in db %d', db['allTasks'].count(mFilter) )
    return

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

    waitForScript.logger.setLevel(logging.INFO)
    # wait for any running instance of this script to exit
    #waitForScript.waitForScript( os.path.basename( __file__ ) )

    ap = argparse.ArgumentParser( description=__doc__,
        fromfile_prefix_chars='@', formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    ap.add_argument( 'action', help='the action to perform', 
        choices=['launch', 'report', 'reportAll', 'import', 'check', 'collectStatus', 'terminateBad', 'terminateAll']
        )
    ap.add_argument( '--authToken', help='the NCS authorization token to use (required for launch or terminate)' )
    ap.add_argument( '--count', type=int, help='the number of instances (for launch)' )
    ap.add_argument( '--target', type=int, help='target number of working instances (for launch)',
        default=150 )
    ap.add_argument( '--dataDir', help='data directory', default='./data/' )
    ap.add_argument( '--encryptFiles', type=boolArg, default=False, help='whether to encrypt files on launched instances' )
    ap.add_argument( '--farm', required=True, help='the name of the virtual boinc farm' )
    ap.add_argument( '--filter', help='json to filter instances for launch',
        default='{"dpr": ">=39", "ram": ">=3000000000", "storage": ">=2000000000", "app-version":2104}' )
    ap.add_argument( '--inFilePath', help='a file to read (for some actions)', default='./data/' )
    ap.add_argument( '--installerFileName', help='a script to upload and run on the instances', default='startBoinc_rosetta.sh' )
    ap.add_argument( '--projectUrl', help='the URL to the boinc science project', default='http://boinc.bakerlab.org/rosetta/' )
    ap.add_argument( '--mongoHost', help='the host of mongodb server', default='localhost')
    ap.add_argument( '--mongoPort', help='the port of mongodb server', default=27017)
    ap.add_argument( '--sshAgent', type=boolArg, default=False, help='whether or not to use ssh agent' )
    ap.add_argument( '--sshClientKeyName', help='the name of the uploaded ssh client key to use (advanced)' )
    ap.add_argument( '--tag', required=False, help='tag for data dir and collection names' )
    ap.add_argument( '--timeLimit', type=int, help='time limit (in seconds) for the whole job',
        default=90 )
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
            nToLaunch = min( nToLaunch, 60 )
        else:
            sys.exit( 'error: no positive --count or --target supplied')
        logger.info( 'would launch %d instances', nToLaunch )
        if nToLaunch <= 0:
            sys.exit( 'NOT launching additional instances')
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
            logger.info( 'ingesting into %s', collName )
            ingestJson( launchedJsonFilePath, dbName, collName )
            # remove the launchedJsonFile to avoid local accumulation of data
            os.remove( launchedJsonFilePath )
            if not os.path.isfile( resultsLogFilePath ):
                logger.warning( 'no results file %s', resultsLogFilePath )
            else:
                ingestJson( resultsLogFilePath, dbName, 'startBoinc_'+dateTimeTag )
                # remove the resultsLogFile to avoid local accumulation of data
                os.remove( resultsLogFilePath )
            for statusRec in badStatuses:
                statusRec['status'] = str(statusRec['status'])
                statusRec['dateTime'] = startDateTime.isoformat()
                statusRec['_id' ] = statusRec['instanceId']
                db['badInstalls'].insert_one( statusRec )
        else:
            logger.warning( 'no instances recruited' )
    elif args.action == 'import':
        launchedJsonFileName = 'launched_%s.json' % launchedTag
        launchedJsonFilePath = os.path.join( dataDirPath, launchedJsonFileName )
        collName = 'launchedInstances_' + launchedTag
        logger.info( 'importing %s to %s', launchedJsonFilePath, collName )
        ingestJson( launchedJsonFilePath, dbName, collName )
        collection = db[collName]
        logger.info( '%s has %d documents', collName, collection.count_documents({}) )

        #sys.exit()
    elif args.action == 'check':
        if not db.list_collection_names():
            logger.warn( 'no collections found found for db %s', dbName )
            sys.exit()
        checkerTimeLimit = 360
        startedInstances = getStartedInstances( db )

        instancesByIid = {inst['instanceId']: inst for inst in startedInstances }

        wereChecked = list( db['checkedInstances'].find() ) # fully iterates the cursor, getting all records
        checkedByIid = { inst['_id']: inst for inst in wereChecked }
        # after checking, each checked instance will have "state" set to "checked", "failed", "inaccessible", or "terminated"
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

        resultsLogFilePath=dataDirPath+('/checkInstances_%s.jlog' % dateTimeTag)
        #workerCmd = 'boinccmd --get_tasks | grep "fraction done"'
        #workerCmd = r'boinccmd --get_tasks | grep \"active_task_state: EXEC\" || sleep 5 && boinccmd --get_tasks | grep \"active_task_state: EXEC\"'
        workerCmd = r'boinccmd --get_tasks | grep active_task_state || sleep 6 && boinccmd --get_tasks | grep active_task_state'
        #logger.info( 'calling tellInstances on %d instances', len(checkables))
        stepStatuses = tellInstances.tellInstances( checkables, workerCmd,
            resultsLogFilePath=resultsLogFilePath,
            download=None, downloadDestDir=None, jsonOut=None, sshAgent=args.sshAgent,
            timeLimit=checkerTimeLimit, upload=None, stopOnSigterm=True,
            knownHostsOnly=False
            )
        ingestJson( resultsLogFilePath, db.name, 'checkInstances_'+dateTimeTag )
        (eventsByInstance, badIidSet) = demuxResults( resultsLogFilePath )
        # remove the resultsLogFile to avoid local accumulation of data
        os.remove( resultsLogFilePath )
        # scan the results of the command
        goodIids = []
        failedIids = []
        exceptedIids = []
        timedOutIids = []
        coll = db['checkedInstances']
        #logger.info( 'updating database' )
        for iid, events in eventsByInstance.items():
            if iid.startswith( '<'):  # skip instances with names like '<master>'
                continue
            if iid not in instancesByIid:
                logger.warning( 'instance not found')
            inst = instancesByIid[ iid ]
            abbrevIid = iid[0:16]
            launchedDateTimeStr = inst.get( 'started-at' )
            instCheck = checkedByIid.get( iid )
            if 'ram' in inst:
                ramMb = inst['ram']['total'] / 1000000
            else:
                ramMb = 0
            if instCheck:
                nExceptions = instCheck.get( 'nExceptions', 0)
                nFailures = instCheck.get( 'nFailures', 0)
                nSuccesses = instCheck.get( 'nTasksComputed', 0)
                nTimeouts = instCheck.get( 'nTimeouts', 0)
            else:
                # this is the first time checking this instance
                nExceptions = 0
                nFailures = 0
                nSuccesses = 0
                nTimeouts = 0
                coll.insert_one( {'_id': iid,
                    'devId': inst.get('device-id'),
                    'launchedDateTime': launchedDateTimeStr,
                    'ssh': inst.get('ssh'),
                    'ramMb': ramMb,
                    'nExceptions': 0,
                    'nFailures': 0,
                    'nTasksComputed': 0,
                    'nTimeouts': 0
                } )

            if nExceptions:
                logger.warning( 'instance %s previously had %d exceptions', iid, nExceptions )
            state = 'checked'
            nCurTasks = 0
            for event in events:
                #logger.info( 'event %s', event )
                if 'exception' in event:
                    nExceptions += 1
                    if event['exception']['type']=='gaierror':
                        logger.warning( 'checkInstances found gaierror, will mark as inaccessible %s', iid )
                    if event['exception']['type']=='ConnectionRefusedError':
                        logger.warning( 'checkInstances found ConnectionRefusedError for %s', iid )
                    #    nExceptions += 1
                    if ((nExceptions - nSuccesses*1.5) >= 12) or(event['exception']['type']=='gaierror'):
                        state = 'inaccessible'
                    exceptedIids.append( iid )
                    coll.update_one( {'_id': iid},
                        { "$set": { "state": state, 'nExceptions': nExceptions,
                            'checkedDateTime': checkedDateTimeStr } },
                        upsert=True
                        )
                elif 'timeout' in event:
                    #logger.info('timeout in checkInstances (%d)', event['timeout'] )
                    nTimeouts +=1
                    timedOutIids.append( iid )
                    coll.update_one( {'_id': iid},
                        { "$set": { "state": state, 'nTimeouts': nTimeouts,
                            'checkedDateTime': checkedDateTimeStr } },
                        upsert=True
                        )
                elif 'returncode' in event:
                    if event['returncode']:
                        nFailures += 1
                        if (nFailures - nSuccesses) >= 10:
                            state = 'failed'
                        failedIids.append( iid )
                    else:
                        goodIids.append( iid )
                    coll.update_one( {'_id': iid},
                        { "$set": { "state": state, 'nFailures': nFailures,
                            'launchedDateTime': launchedDateTimeStr,
                            'checkedDateTime': checkedDateTimeStr,
                            'ssh': inst.get('ssh'), 'devId': inst.get('device-id') } 
                            },
                        upsert=True
                        )
                elif 'stdout' in event:
                    try:
                        stdoutStr = event['stdout']
                        if 'active_task_state: EXECUTING' in stdoutStr:
                            nCurTasks += 1
                        elif 'fraction done' in stdoutStr:  #TODO remove this
                            numPart = stdoutStr.rsplit( 'fraction done: ')[1]
                            fractionDone = float( numPart )
                            if fractionDone > 0:
                                #logger.info( 'fractionDone %.3f', fractionDone )
                                nCurTasks += 0*1
                    except Exception as exc:
                        logger.warning( 'could not parse <hostid> line "%s"', stdoutStr.rstrip() )
            logger.info( '%d nCurTasks for %s', nCurTasks, abbrevIid )
            coll.update_one( {'_id': iid},
                { "$set": { "nCurTasks": nCurTasks, "ramMb": ramMb } },
                upsert=False
                )

        logger.info( '%d good; %d excepted; %d timed out; %d failed instances',
            len(goodIids), len(exceptedIids), len(timedOutIids), len(failedIids) )
        reachables = [inst for inst in checkables if inst['instanceId'] not in exceptedIids ]

        # query cloudserver to see if any of the excepted instances are dead
        for iid in exceptedIids:
            response = ncs.queryNcsSc( 'instances/%s' % iid, args.authToken, maxRetries=1)
            if response['statusCode'] == 200:
                inst = response['content']
                if 'events' in inst:
                    coll.update_one( {'_id': iid}, { "$set": { "events": inst['events'] } } )
                    lastEvent = inst['events'][-1]
                    if (lastEvent['category'] == 'instance') and ('stop' in lastEvent['event']):
                        logger.warning( 'instance found to be stopped: %s', iid )
                        coll.update_one( {'_id': iid}, { "$set": { "state": 'stopped' } } )

        logger.info( 'downloading boinc*.log from %d instances', len(reachables))
        stepStatuses = tellInstances.tellInstances( reachables,
            download='/var/log/boinc*.log', downloadDestDir=dataDirPath+'/boincLogs', 
            timeLimit=args.timeLimit, sshAgent=args.sshAgent,
            stopOnSigterm=True, knownHostsOnly=False
            )
        # prepare to ingest all new or updated boinc logs
        logsDirPath = os.path.join( dataDirPath, 'boincLogs' )
        logDirs = os.listdir( logsDirPath )
        logger.info( '%d logDirs found', len(logDirs ) )
        # but first, delete very old log files
        lookbackDays = 7
        thresholdDateTime = datetime.datetime.now( datetime.timezone.utc ) \
            - datetime.timedelta( days=lookbackDays )
        for logDir in logDirs:
            errLogPath = os.path.join( logsDirPath, logDir, 'boincerr.log' )
            if os.path.isfile( errLogPath ):
                fileModDateTime = datetime.datetime.fromtimestamp( os.path.getmtime( errLogPath ) )
                fileModDateTime = universalizeDateTime( fileModDateTime )
                if fileModDateTime <= thresholdDateTime:
                    logger.info( 'deleting errlog %s', errLogPath )
                    os.remove( errLogPath )
            inFilePath = os.path.join( logsDirPath, logDir, 'boinc.log' )
            if os.path.isfile( inFilePath ):
                fileModDateTime = datetime.datetime.fromtimestamp( os.path.getmtime( inFilePath ) )
                fileModDateTime = universalizeDateTime( fileModDateTime )
                if fileModDateTime <= thresholdDateTime:
                    logger.info( 'deleting log %s', inFilePath )
                    os.remove( inFilePath )
            else:
                # no log file in dir, so check to see if the dir is old enough to remove
                logDirPath = os.path.join( logsDirPath, logDir )
                dirModDateTime = datetime.datetime.fromtimestamp( os.path.getmtime( logDirPath ) )
                dirModDateTime = universalizeDateTime( dirModDateTime )
                if dirModDateTime <= thresholdDateTime:
                    logger.info( 'obsolete dir %s', logDirPath )
                    #logger.info( 'contains %s', os.listdir( logDirPath ) )
        
        # ingest all new or updated boinc logs
        loggedCollNames = db.list_collection_names(
            filter={ 'name': {'$regex': r'^boincLog_.*'} } )
        for logDir in logDirs:
            # logDir is also the instanceId
            inFilePath = os.path.join( logsDirPath, logDir, 'boinc.log' )
            if not os.path.isfile( inFilePath ):
                continue
            collName = 'boincLog_' + logDir
            if collName in loggedCollNames:
                existingGenTime = lastGenDateTime( db[collName] ) - datetime.timedelta(hours=1)
                fileModDateTime = datetime.datetime.fromtimestamp( os.path.getmtime( inFilePath ) )
                fileModDateTime = universalizeDateTime( fileModDateTime )
                if existingGenTime >= fileModDateTime:
                    #logger.info( 'already posted %s %s %s',
                    #    logDir[0:8], fmtDt( existingGenTime ), fmtDt( fileModDateTime )  )
                    continue
            if not os.path.isfile( inFilePath ):
                logger.warning( 'no file "%s"', inFilePath )
            else:
                #logger.info( 'parsing log for %s', logDir[0:16] )
                try:
                    with open( inFilePath, 'r' ) as logFile:
                        # for safety, ingest to a temp collection and then rename (with replace) when done
                        ingestBoincLog( logFile, db['boincLog_temp'] )
                        db['boincLog_temp'].rename( collName, dropTarget=True )
                except Exception as exc:
                    logger.warning( 'exception (%s) ingesting %s', type(exc), inFilePath, exc_info=False )


        logger.info( 'checking for project hostId for %d instances', len(reachables))
        resultsLogFilePath=dataDirPath+'/getHostId.jlog'
        stepStatuses = tellInstances.tellInstances( reachables,
            command=r'grep \<hostid\> /var/lib/boinc-client/client_state.xml',
            resultsLogFilePath=resultsLogFilePath,
            timeLimit=min(args.timeLimit, 90), sshAgent=args.sshAgent,
            stopOnSigterm=True, knownHostsOnly=False
            )
        # extract hostids from stdouts
        hostIdsByIid = {}
        with open( resultsLogFilePath, 'rb' ) as inFile:
            for line in inFile:
                decoded = json.loads( line )
                if 'stdout' in decoded:
                    stdoutLine = decoded['stdout']
                    iid = decoded.get( 'instanceId')
                    if iid and ('<hostid>' in stdoutLine):
                        #logger.info( '%s %s', iid[0:16], stdoutLine )
                        hostId = 0
                        try:
                            numPart = re.search( r'\<hostid\>(.*)\</hostid\>', stdoutLine ).group(1)
                            hostId = int( numPart )
                        except Exception as exc:
                            logger.warning( 'could not parse <hostid> line "%s"',
                                stdoutLine.rstrip() )
                        if hostId:
                            hostIdsByIid[ iid ] = hostId
        #logger.info( 'hostIds: %s', hostIdsByIid )
        for iid, inst in checkedByIid.items():
            oldHostId = inst.get( 'bpsHostId' )
            if iid in hostIdsByIid and hostIdsByIid[iid] != oldHostId:
                coll.update_one( {'_id': iid},
                { "$set": { "bpsHostId": hostIdsByIid[iid] } },
                upsert=False
                )
        # do a blind boinccmd update to trigger communication with the project server
        logger.info( 'boinccmd --project update for %d instances', len(reachables))
        stepStatuses = tellInstances.tellInstances( reachables,
            command='boinccmd --project %s update' % args.projectUrl,
            resultsLogFilePath=dataDirPath+'/boinccmd_update.jlog',
            timeLimit=min(args.timeLimit, 90), sshAgent=args.sshAgent,
            stopOnSigterm=True, knownHostsOnly=False
            )

    elif args.action == 'collectStatus':
        collectBoincStatus( db, dataDirPath, 'get_cc_status' )
        #time.sleep( 6 )  # couldn't hurt (or could it?)
        projColl = collectBoincStatus( db, dataDirPath, 'get_project_status' )
        mergeProjectData( projColl, db['projectStatus'] )
        #time.sleep( 6 )  # couldn't hurt (or could it?)
        tasksColl = collectBoincStatus( db, dataDirPath, 'get_tasks' )
        # could parse and merge into allTasks here
        mergeTaskData( tasksColl, db['allTasks'] )
    elif args.action == 'terminateBad':
        if not args.authToken:
            sys.exit( 'error: can not terminate because no authToken was passed')
        terminatedDateTimeStr = datetime.datetime.now( datetime.timezone.utc ).isoformat()
        coll = db['checkedInstances']
        wereChecked = list( coll.find() ) # fully iterates the cursor, getting all records
        terminatedIids = []
        for checkedInst in wereChecked:
            state = checkedInst.get( 'state')
            if state in ['failed', 'inaccessible', 'stopped' ]:
                iid = checkedInst['_id']
                abbrevIid = iid[0:16]
                logger.warning( 'would terminate %s', abbrevIid )
                terminatedIids.append( iid )
                coll.update_one( {'_id': iid},
                    { "$set": { "state": "terminated",
                        'terminatedDateTime': terminatedDateTimeStr } },
                    upsert=False
                    )
        logger.info( 'terminating %d instances', len( terminatedIids ))
        ncs.terminateInstances( args.authToken, terminatedIids )
        #sys.exit()
    elif args.action == 'terminateAll':
        if not args.authToken:
            sys.exit( 'error: can not terminate because no authToken was passed')
        logger.info( 'checking for instances to terminate')
        # will terminate all instances and update checkedInstances accordingly
        startedInstances = getStartedInstances( db )  # expensive, could just query for iids
        coll = db['checkedInstances']
        wereChecked = coll.find()
        checkedByIid = { inst['_id']: inst for inst in wereChecked }
        terminatedDateTimeStr = datetime.datetime.now( datetime.timezone.utc ).isoformat()
        terminatedIids = []
        for inst in startedInstances:
            iid = inst['instanceId']
            #abbrevIid = iid[0:16]
            #logger.warning( 'would terminate %s', abbrevIid )
            terminatedIids.append( iid )
            if iid not in checkedByIid:
                logger.warning( 'terminating unchecked instance %s', iid )
            else:
                checkedInst = checkedByIid[iid]
                if checkedInst['state'] != 'terminated':
                    coll.update_one( {'_id': iid},
                        { "$set": { "state": "terminated",
                            'terminatedDateTime': terminatedDateTimeStr } },
                        upsert=False
                        )
        logger.info( 'terminating %d instances', len( terminatedIids ))
        ncs.terminateInstances( args.authToken, terminatedIids )
        #sys.exit()
    elif args.action == 'report':
        #report_cc_status( db, dataDirPath )

        #logger.info( 'would report' )
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

        projStatusTimeLimit = 180
        resultsLogFilePath=dataDirPath+'/reportInstances.jlog'
        workerCmd = "boinccmd --get_project_status | grep 'jobs succeeded: [^0]'"
        logger.info( 'calling tellInstances to get success report on %d instances', len(reportables))
        stepStatuses = tellInstances.tellInstances( reportables, workerCmd,
            resultsLogFilePath=resultsLogFilePath,
            download=None, downloadDestDir=None, jsonOut=None, sshAgent=args.sshAgent,
            timeLimit=projStatusTimeLimit, upload=None, stopOnSigterm=True,
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
            events = eventsByInstance[ iid ]
            for event in events:
                if 'stdout' in event:
                    stdoutStr = event['stdout']
                    if 'jobs succeeded: ' in stdoutStr:
                        numPart = stdoutStr.rsplit( 'jobs succeeded: ')[1]
                        nJobsSucc = int( numPart )
                        if nJobsSucc >= 1:
                            logger.info( '%s nJobsSucc: %d', abbrevIid, nJobsSucc)
                            db['checkedInstances'].update_one( 
                                {'_id': iid}, { "$set": { "nTasksComputed": nJobsSucc } }
                                )
                        totJobsSucc += nJobsSucc
                    else:
                        logger.warning( 'not matched (%s)', event )
        logger.info( 'totJobsSucc: %d', totJobsSucc )

        #sys.exit()
    elif args.action == 'reportAll':
        reportAll( db, dataDirPath )
    else:
        logger.warning( 'action "%s" unimplemented', args.action )
    elapsed = time.time() - startTime
    logger.info( 'finished action "%s"; elapsed time %.1f minutes',
        args.action, elapsed/60 )
