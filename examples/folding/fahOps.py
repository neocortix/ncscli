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


def getStartedInstances( db ):
    collNames = db.list_collection_names( filter={ 'name': {'$regex': r'^launchedInstances_.*'} } )
    #logger.info( 'launched collections: %s', collNames )
    startedInstances = []
    for collName in collNames:
        #logger.info( 'getting instances from %s', collName )
        launchedColl = db[collName]
        inRecs = list( launchedColl.find() ) # fully iterates the cursor, getting all records
        if len(inRecs) <= 0:
            logger.warn( 'no launched instances found for %s', collName )
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
        except:
            logger.warning( 'got exception (%s) appending to terminationLogFile %s',
                type(exc), terminationLogFilePath )
        return []  # empty list meaning none may have been terminated
    else:
        return instanceIds

def recruitInstances( nWorkersWanted, launchedJsonFilePath, launchWanted,
    resultsLogFilePath, installerFileName ):
    '''launch instances and install fahclient on them;
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
            sshClientKeyName = 'fah_%s' % (randomPart)
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
    if not sigtermSignaled():
        installerCmd = './' + installerFileName
        logger.info( 'calling tellInstances to install on %d instances', len(startedInstances))
        stepStatuses = tellInstances.tellInstances( startedInstances, installerCmd,
            resultsLogFilePath=resultsLogFilePath,
            download=None, downloadDestDir=None, jsonOut=None, sshAgent=args.sshAgent,
            timeLimit=args.timeLimit, upload=args.uploads, stopOnSigterm=False,
            knownHostsOnly=False
            )
        # SHOULD restore our handler because tellInstances may have overridden it
        #signal.signal( signal.SIGTERM, sigtermHandler )
        if not stepStatuses:
            logger.warning( 'no statuses returned from installer')
            startedIids = [inst['instanceId'] for inst in startedInstances]
            #logOperation( 'terminateBad', startedIids, '<recruitInstances>' )
            terminateNcsScInstances( args.authToken, startedIids )
            return ([], [])
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
            terminateNcsScInstances( args.authToken, badIids )

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

def ingestFahLog( logFile, coll ):
    '''ingest records from a fah client log.txt into a mongo collection'''
    # these patterns are found in the different tyoes of log lines
    startedLinePat = r'\* Log Started (.*) \*'
    dateLinePat = r'\* Date: (.*) \*'
    timePat = r'^[^0-2]{0,5}([0-2][0-9]:[0-5][0-9]:[0-5][0-9]):(.*)' # extracts time and then the rest of the line

    dateTime = datetime.datetime.now( datetime.timezone.utc )  # default datetime in case none is found
    tzInfo = dateTime.tzinfo
    recs = []
    for line in logFile:
        line = line.rstrip()
        if not line:
            continue
        if '* Log Started' in line:
            dateTimeMatch = re.search( startedLinePat, line )
            if dateTimeMatch:
                dateTimePart = dateTimeMatch.group(1)
                dateTime = universalizeDateTime( dateutil.parser.parse( dateTimePart ) )
                tzInfo = dateTime.tzinfo
            msg = line
            recs.append( {
                'dateTime': dateTime.isoformat(),
                'mType': 'logStarted',
                'msg': msg,
                } )
        elif '* Date:' in line:
            dateMatch = re.search( dateLinePat, line )
            if dateMatch:
                # this is a date without a time
                datePart = dateMatch.group(1)
                dateTime = dateutil.parser.parse( datePart )
                dateTime = dateTime.replace( tzinfo=tzInfo ) # may not need this
        else:
            # strip out any leading terminal-escape code
            escaped = re.search( r'^\x1b\[..m', line )
            if escaped:
                line = line[5:]
            # find the timestamp, if present
            timeMatch = re.search( timePat, line )
            if not timeMatch:
                logger.warning( 'no timeMatch in %s', line )
                # previous dateTime will carry over, in this case
                msg = line
            else:
                timePart = timeMatch.group(1)
                lineTime = dateutil.parser.parse( timePart )
                # combine lineTime with previously set date and tz
                dateTime = datetime.datetime.combine( dateTime, lineTime.time(), tzInfo )
                msg = timeMatch.group(2)
            mType = None
            if 'WARNING:' in msg:
                mType = 'warning'
            elif ':Upload' in msg:
                mType = 'upload'
            elif ':Sending unit results' in msg:
                mType = 'upload'
            elif ':Completed' in msg:
                mType = 'complete'
            elif 'WORK_ACK' in msg:
                mType = 'work_acc'
            
            # append it to the list of log entries to save          
            recs.append( {
                'dateTime': dateTime.isoformat(),
                'mType': mType,
                'msg': msg
                } )
    if len(recs):
        coll.insert_many( recs, ordered=True )
        coll.create_index( 'dateTime' )
        coll.create_index( 'mType' )
    #return recs


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
        default=150 )
    ap.add_argument( '--dataDir', help='data directory', default='./data/' )
    ap.add_argument( '--encryptFiles', type=boolArg, default=False, help='whether to encrypt files on launched instances' )
    ap.add_argument( '--farm', required=True, help='the name of the virtual folding@home farm' )
    ap.add_argument( '--filter', help='json to filter instances for launch',
        default='{"dpr": ">=39", "ram": ">=3000000000", "storage": ">=2000000000", "app-version":2104}' )
    ap.add_argument( '--installerFileName', help='a script to upload and run on the instances', default='fahclient_start.sh' )
    ap.add_argument( '--uploads', help='glob for filenames to upload to workers', default='fahclient_*' )
    ap.add_argument( '--mongoHost', help='the host of mongodb server', default='localhost')
    ap.add_argument( '--mongoPort', help='the port of mongodb server', default=27017)
    ap.add_argument( '--sshAgent', type=boolArg, default=False, help='whether or not to use ssh agent' )
    ap.add_argument( '--sshClientKeyName', help='the name of the uploaded ssh client key to use (advanced)' )
    ap.add_argument( '--tag', required=False, help='tag for data dir and collection names (for import)' )
    ap.add_argument( '--timeLimit', type=int, help='time limit (in seconds) for the whole job',
        default=90 )
    args = ap.parse_args()

    logger.info( 'starting action "%s" for farm "%s"', args.action, args.farm )

    dataDirPath = os.path.join( args.dataDir, args.farm )
    os.makedirs( dataDirPath, exist_ok=True )
    
    mclient = pymongo.MongoClient(args.mongoHost, args.mongoPort)
    dbName = 'fah_' + args.farm
    db = mclient[dbName]
    launchedTag = args.tag  # '2020-03-20_173400'  # '2019-03-19_130200'
    if args.action == 'launch':
        startDateTime = datetime.datetime.now( datetime.timezone.utc )
        dateTimeTagFormat = '%Y-%m-%d_%H%M%S'  # cant use iso format dates in filenames because colons
        dateTimeTag = startDateTime.strftime( dateTimeTagFormat )

        launchedJsonFilePath = os.path.join( dataDirPath, 'launched_%s.json' % dateTimeTag )
        resultsLogFilePath = os.path.join( dataDirPath, 'startfah_%s.jlog' % dateTimeTag )
        collName = 'launchedInstances_' + dateTimeTag

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
                ingestJson( resultsLogFilePath, dbName, 'startfah_'+dateTimeTag )
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
        # this import implementation is incomplete
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
        workerCmd = 'stat --format=%y /var/lib/fahclient/log.txt'
        logger.info( 'calling tellInstances on %d instances', len(checkables))
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
                        # could do something with the cmd output here
                    except Exception as exc:
                        logger.warning( 'could not parse <hostid> line "%s"', stdoutStr.rstrip() )
            coll.update_one( {'_id': iid},
                { "$set": { "ramMb": ramMb } },
                upsert=False
                )

        logger.info( '%d good; %d excepted; %d timed out; %d failed instances',
            len(goodIids), len(exceptedIids), len(timedOutIids), len(failedIids) )
        reachables = [inst for inst in checkables if inst['instanceId'] not in exceptedIids ]

        # query cloudserver to see if any of the excepted instances are dead
        logger.info( 'querying to see if instances are "stopped""' )
        for iid in exceptedIids:
            try:
                response = ncs.queryNcsSc( 'instances/%s' % iid, args.authToken, maxRetries=1)
            except Exception as exc:
                logger.warning( 'querying instance status got exception (%s) %s',
                    type(exc), exc )
            else:
                if response['statusCode'] == 200:
                    inst = response['content']
                    if 'events' in inst:
                        coll.update_one( {'_id': iid}, { "$set": { "events": inst['events'] } } )
                        lastEvent = inst['events'][-1]
                        if (lastEvent['category'] == 'instance') and ('stop' in lastEvent['event']):
                            logger.warning( 'instance found to be stopped: %s', iid )
                            coll.update_one( {'_id': iid}, { "$set": { "state": 'stopped' } } )

        simInfoTimeLimit = 120
        resultsLogFilePath=dataDirPath+'/simInfo.jlog'
        workerCmd = "/usr/bin/FAHClient --send-command 'simulation-info 0'"
        logger.info( 'calling tellInstances to get simulation-info on %d instances', len(reachables))
        stepStatuses = tellInstances.tellInstances( reachables, workerCmd,
            resultsLogFilePath=resultsLogFilePath,
            sshAgent=args.sshAgent,
            timeLimit=simInfoTimeLimit, stopOnSigterm=True
            )

        # read back the results
        (eventsByInstance, badIidSet) = demuxResults( resultsLogFilePath )
        nFoundInfo = 0
        for iid in goodIids:
            events = eventsByInstance[ iid ]
            inPyOn = False
            gotInfo = False
            for event in events:
                if 'stdout' in event:
                    stdoutStr = event['stdout']
                    if stdoutStr.startswith( 'PyON'):
                        #logger.info( '<PyON> %s', stdoutStr )
                        inPyOn = True
                    elif stdoutStr.startswith( '---'):
                        #logger.info( 'end <PyON>' )
                        inPyOn = False
                    elif stdoutStr.strip().startswith( '{'):
                        #logger.info( 'maybe sim-info %s', stdoutStr )
                        if not inPyOn:
                            logger.info( 'unexpected "{", not in PyOn %s', stdoutStr )
                        else:
                            if gotInfo:
                                logger.warning( 'got an unexpected second pyOn object <%s>', stdoutStr )
                            simInfo = None
                            try:
                                simInfo = json.loads( stdoutStr )
                            except Exception as exc:
                                logger.info( 'exception parsing pyon (%s) %s', type(exc), exc )
                            if simInfo != None:
                                gotInfo = True
                                #logger.info( 'simInfo %s', simInfo )
                                db['checkedInstances'].update_one( 
                                    {'_id': iid}, { "$set": { "simInfo": simInfo } }
                                    )
                                nFoundInfo += 1
            if not gotInfo:
                #instCheck = checkedByIid.get( iid )
                instCheck = db['checkedInstances'].find_one( {'_id': iid} )
                if not instCheck:
                    logger.warning( 'checked instance not found (%s)', iid )
                else:
                    nFailures = instCheck.get( 'nFailures', 0) + 1
                    logger.info( '%d failures for %s', nFailures, iid[0:16] )
                    if (nFailures - nSuccesses) >= 10:
                        state = 'failed'
                    else:
                        state = 'checked'
                    coll.update_one( {'_id': iid},
                        { "$set": { "state": state, 'nFailures': nFailures
                            } },
                        upsert=True
                        )
        
        queueInfoTimeLimit = 180
        resultsLogFilePath=dataDirPath+'/queueInfo.jlog'
        workerCmd = "/usr/bin/FAHClient --send-command 'queue-info'"
        logger.info( 'calling tellInstances to get queue-info on %d instances', len(reachables))
        stepStatuses = tellInstances.tellInstances( reachables, workerCmd,
            resultsLogFilePath=resultsLogFilePath,
            sshAgent=args.sshAgent,
            timeLimit=queueInfoTimeLimit, stopOnSigterm=True
            )

        # read and parse the results
        (eventsByInstance, badIidSet) = demuxResults( resultsLogFilePath )
        nFoundInfo = 0
        for iid in goodIids:
            events = eventsByInstance[ iid ]
            inPyOn = False
            gotInfo = False
            for event in events:
                if 'stdout' in event:
                    stdoutStr = event['stdout']
                    if stdoutStr.startswith( 'PyON'):
                        inPyOn = True
                    elif stdoutStr.startswith( '---'):
                        inPyOn = False
                    elif stdoutStr.strip().startswith( '{'):
                        #logger.info( 'maybe queue-info %s', stdoutStr )
                        if not inPyOn:
                            logger.info( 'unexpected "{", not in PyOn %s', stdoutStr )
                        else:
                            if gotInfo:
                                logger.warning( 'got an unexpected second pyOn object <%s>', stdoutStr )
                            queueInfo = None
                            try:
                                queueInfo = json.loads( stdoutStr )
                            except Exception as exc:
                                logger.info( 'exception parsing pyon (%s) %s', exc, stdoutStr )
                            if queueInfo != None:
                                gotInfo = True
                                #logger.info( 'queueInfo %s', queueInfo )
                                db['checkedInstances'].update_one( 
                                    {'_id': iid}, { "$set": { "queueInfo": queueInfo } }
                                    )
                                nFoundInfo += 1
        logger.info( 'got queueInfo from %d instances', nFoundInfo )
        logger.info( 'downloading log.txt from %d instances', len(reachables))
        stepStatuses = tellInstances.tellInstances( reachables,
            download='/var/lib/fahclient/log.txt', downloadDestDir=dataDirPath+'/clientLogs', 
            timeLimit=args.timeLimit, sshAgent=args.sshAgent,
            stopOnSigterm=True, knownHostsOnly=False
            )
        # prepare to ingest all new or updated client logs
        logsDirPath = os.path.join( dataDirPath, 'clientLogs' )
        logDirs = os.listdir( logsDirPath )
        logger.info( '%d logDirs found', len(logDirs ) )
        # but first, delete very old log files
        lookbackDays = 7
        thresholdDateTime = datetime.datetime.now( datetime.timezone.utc ) \
            - datetime.timedelta( days=lookbackDays )
        lookBackHours = 24
        newerThresholdDateTime = datetime.datetime.now( datetime.timezone.utc ) \
            - datetime.timedelta( hours=lookBackHours )

        for logDir in logDirs:
            inFilePath = os.path.join( logsDirPath, logDir, 'log.txt' )
            if os.path.isfile( inFilePath ):
                fileModDateTime = datetime.datetime.fromtimestamp( os.path.getmtime( inFilePath ) )
                fileModDateTime = universalizeDateTime( fileModDateTime )
                if fileModDateTime <= thresholdDateTime:
                    os.remove( inFilePath )
                elif fileModDateTime <= newerThresholdDateTime:
                    logger.warning( 'old fileModDateTime for %s (%s)', logDir, fileModDateTime )
        
        # ingest all new or updated client logs
        loggedCollNames = db.list_collection_names(
            filter={ 'name': {'$regex': r'^clientLog_.*'} } )
        for logDir in logDirs:
            # logDir is also the instanceId
            inFilePath = os.path.join( logsDirPath, logDir, 'log.txt' )
            if not os.path.isfile( inFilePath ):
                continue
            collName = 'clientLog_' + logDir
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
                        ingestFahLog( logFile, db['clientLog_temp'] )
                        db['clientLog_temp'].rename( collName, dropTarget=True )
                        logger.info( 'ingested %s', collName )
                except Exception as exc:
                    logger.warning( 'exception (%s) ingesting %s', type(exc), inFilePath, exc_info=False )
    elif args.action == 'terminateBad':
        if not args.authToken:
            sys.exit( 'error: can not terminate because no authToken was passed')
        terminatedDateTimeStr = datetime.datetime.now( datetime.timezone.utc ).isoformat()
        coll = db['checkedInstances']
        wereChecked = list( coll.find() ) # fully iterates the cursor, getting all records
        toTerminate = []
        for checkedInst in wereChecked:
            state = checkedInst.get( 'state')
            #logger.info( 'checked state %s', state )
            if state in ['failed', 'inaccessible', 'stopped' ]:
                iid = checkedInst['_id']
                abbrevIid = iid[0:16]
                logger.warning( 'would terminate %s', abbrevIid )
                toTerminate.append( iid )
        logger.info( 'terminating %d instances', len( toTerminate ))
        terminated=terminateNcsScInstances( args.authToken, toTerminate )
        logger.info( 'actually terminated %d instances', len( terminated ))
        # update states in checkedInstances
        for iid in terminated:
            coll.update_one( {'_id': iid},
                { "$set": { "state": "terminated",
                    'terminatedDateTime': terminatedDateTimeStr } },
                upsert=False
                )

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
                            'terminatedDateTime': terminatedDateTimeStr } },
                        upsert=False
                        )
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
    else:
        logger.warning( 'action "%s" unimplemented', args.action )
    elapsed = time.time() - startTime
    logger.info( 'finished action "%s"; elapsed time %.1f minutes',
        args.action, elapsed/60 )
