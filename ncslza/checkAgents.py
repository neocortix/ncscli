#!/usr/bin/env python3
"""
check instances with installed LG agents; terminate some that appear unusable
"""

# standard library modules
import argparse
import datetime
import json
import logging
import os
import subprocess
import sys
# third-party modules
import psutil
import requests
# neocortix modules
import ncscli.ncs as ncs
import ncscli.tellInstances as tellInstances


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def datetimeIsAware( dt ):
    if not dt: return None
    return (dt.tzinfo is not None) and (dt.tzinfo.utcoffset( dt ) is not None)

def universalizeDateTime( dt ):
    if not dt: return None
    if datetimeIsAware( dt ):
        #return dt
        return dt.astimezone(datetime.timezone.utc)
    return dt.replace( tzinfo=datetime.timezone.utc )

def checkProcesses( liveInstances ):
    '''check that the expected process is running on each instance'''
    logger.info( 'checking %d instance(s)', len(liveInstances) )

    #maybe should weed out some instances
    goodInstances = liveInstances

    cmd = "ps -ef | grep -v grep | grep 'agent.jar' > /dev/null"
    #cmd = "free --mega 1>&2 ; ps -ef | grep -v grep | grep 'LoadGeneratorAgent start' > /dev/null"
    # check for a running agent process on each instance
    stepStatuses = tellInstances.tellInstances( goodInstances, cmd,
        timeLimit=30*60,
        knownHostsOnly=True
        )
    #logger.info( 'cmd statuses: %s', stepStatuses )
    errorsByIid = {}
    for statusRec in stepStatuses:
        status = statusRec['status']
        if status:
            logger.info( 'statusRec: %s', statusRec )
            # only certain non-null outcomes are true errors
            if isinstance( status, Exception ):
                errorsByIid[ statusRec['instanceId'] ] = statusRec
            elif status not in [0, 1]:
                errorsByIid[ statusRec['instanceId'] ] = statusRec
    logger.info( 'errorsByIid: %s', errorsByIid )
    return errorsByIid

def retrieveLogs( goodInstances ):
    logger.info( 'retrieving logs for %d instance(s)', len(goodInstances) )

    #agentLogFilePath = 'lzAgent/Logs/*'
    agentLogFilePath = 'lzAgent/agent.*s'

    # download the agent.log file from each instance
    stepStatuses = tellInstances.tellInstances( goodInstances,
        download=agentLogFilePath, downloadDestDir=dataDirPath +'/agentLogs',
        timeLimit=30*60,
        knownHostsOnly=True
        )
    #logger.info( 'download statuses: %s', stepStatuses )


def showLogs( dataDirPath ):
    # prepare to read agent logs
    logsDirPath = os.path.join( dataDirPath, 'agentLogs' )
    logDirs = os.listdir( logsDirPath )
    logger.info( '%d logDirs found', len(logDirs ) )

    for logDir in logDirs:
        inFilePath = os.path.join( logsDirPath, logDir, 'agent.log' )
        if not os.path.isfile( inFilePath ):
            logger.warning( 'no log file %s', inFilePath )
        elif os.path.getsize( inFilePath ) <= 0:
            logger.warning( 'empty log file "%s"', inFilePath )
        else:
            fileModDateTime = datetime.datetime.fromtimestamp( os.path.getmtime( inFilePath ) )
            fileModDateTime = universalizeDateTime( fileModDateTime )
            logger.info( 'found %s; size %d; %s', logDir,
                os.path.getsize( inFilePath ), fileModDateTime.strftime( '%H:%M:%S' ) 
                )

def checkLogs( liveInstances, dataDirPath, nlWebWanted ):
    # prepare to read agent logs
    logsDirPath = os.path.join( dataDirPath, 'agentLogs' )
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
        logFilePath = os.path.join( logsDirPath, logDir, 'agent.log' )
        if not os.path.isfile( logFilePath ):
            logger.warning( 'no log file %s', logFilePath )
        elif os.path.getsize( logFilePath ) <= 0:
            logger.warning( 'empty log file "%s"', logFilePath )
        else:
            '''
            fileModDateTime = datetime.datetime.fromtimestamp( os.path.getmtime( logFilePath ) )
            fileModDateTime = universalizeDateTime( fileModDateTime )
            logger.info( 'found %s; size %d; %s', logDir,
                os.path.getsize( logFilePath ), fileModDateTime.strftime( '%H:%M:%S' ) 
                )
            '''
            connected = False
            with open( logFilePath ) as logFile:
                for line in logFile:
                    line = line.rstrip()
                    if not line:
                        continue
                    if 'ERROR' in line:
                        logger.info( '%s: %s', iidAbbrev, line )
                        theseErrors = errorsByIid.get( iid, [] )
                        theseErrors.append( line )
                        errorsByIid[iid] = theseErrors
                    if 'Neoload Web : CONNECTED status : READY' in line:
                        connected = True
                if nlWebWanted and not connected:
                    logger.warning( 'instance %s did not connect to nlweb', iidAbbrev )
        '''
        logFilePath = os.path.join( logsDirPath, logDir, 'neoload-log4j.log' )
        if not os.path.isfile( logFilePath ):
            logger.warning( 'no log file %s', logFilePath )
        elif os.path.getsize( logFilePath ) <= 0:
            logger.warning( 'empty log file "%s"', logFilePath )
        else:
            with open( logFilePath ) as logFile:
                for line in logFile:
                    line = line.rstrip()
                    if not line:
                        continue
                    if 'ERROR' in line:
                        logger.info( 'LOG4J %s: %s', iidAbbrev, line )
                        theseErrors = errorsByIid.get( iid, [] )
                        theseErrors.append( line )
                        #errorsByIid[iid] = theseErrors
    '''
    logger.info( 'found errors for %d instance(s)', len(errorsByIid) )
    #print( 'errorsByIid', errorsByIid  )
    summaryCsvFilePath = os.path.join( dataDirPath, 'errorSummary.csv' )
    with open( summaryCsvFilePath, 'a' ) as csvOutFile:
        print( 'instanceId', 'msg',
            sep=',', file=csvOutFile
            )
        for iid in errorsByIid:
            lines = errorsByIid[iid]
            logger.info( 'saving %d errors for %s', len(lines), iid[0:8])
            for line in lines:
                print( iid, line,
                    sep=',', file=csvOutFile
                )
    return errorsByIid

def parseLogLevel( arg ):
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
        logger.warning( 'the given logLevel "%s" is not recognized (using "info" level, instead)', arg )
    setting = map.get( arg, logging.INFO )

    return setting

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

def urlJoin( *parts ):
    ''' join a variable number for ags into a '/' delimited string'''
    return '/'.join( parts )

def apiGetInstances( apiTestId ):
    baseUrl = ncs.baseUrl + '/cloud-api/lt/tests'
    headers = ncs.ncscReqHeaders( authToken )
    url = urlJoin( baseUrl, apiTestId, 'instances' )
    logger.info( 'getting %s', url )
    resp = requests.get( url, headers=headers )
    if resp.status_code != 200:
        return []
    respJson = resp.json()
    logger.debug( 'api iids: %s', respJson )
    iids = respJson
    instances = []
    for iid in iids:
        url = urlJoin( baseUrl, apiTestId, 'instances', iid )
        resp = requests.get( url, headers=headers )
        if resp.status_code != 200:
            logger.warning( 'could not get %s', url )
            continue
        inst = resp.json()
        inst['instanceId'] = inst['id']
        instances.append( inst )

    return instances

if __name__ == "__main__":
    # configure logger formatting
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)
    logger.setLevel(logging.INFO)

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@', formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    ap.add_argument( '--dataDirPath', required=False, help='the path to the directory for input and output data' )
    ap.add_argument( '--apiTestId', help='the test ID, if launched via web API' )
    ap.add_argument( '--logLevel', default ='info', help='verbosity of log (e.g. debug, info, warning, error)' )
    ap.add_argument( '--terminateBad', type=ncs.boolArg, default=False, help='whether to terminate instances found bad' )
    args = ap.parse_args()

    logLevel = parseLogLevel( args.logLevel )
    logger.setLevel(logLevel)
    tellInstances.logger.setLevel( logLevel )
    logger.debug('the logger is configured')

    testId = args.apiTestId
    dataDirPath = args.dataDirPath
    authToken = os.getenv('NCS_AUTH_TOKEN') or 'YourAuthTokenHere'


    if not dataDirPath:
        if testId:
            dataDirPath = os.path.join( 'data', testId )
        else:
            logger.info( 'no dataDirPath given, so not checking instance details' )
            sys.exit( 0 )
    if testId:
        startedInstances = apiGetInstances( testId )
    else:
        # get details of launched instances from the json file
        instancesFilePath = os.path.join( dataDirPath, 'liveAgents.json' )
        if not os.path.isfile( instancesFilePath ):
            instancesFilePath = os.path.join( dataDirPath, 'agents.json' )
        startedInstances = []
        with open( instancesFilePath, 'r') as jsonInFile:
            try:
                startedInstances = json.load(jsonInFile)  # an array
            except Exception as exc:
                logger.warning( 'could not load json (%s) %s', type(exc), exc )
    logger.info( 'checking %d started instance(s)', len(startedInstances) )
    startedIids = [inst['instanceId'] for inst in startedInstances ]
    #startedIids = [inst['instanceId'] for inst in launchedInstances if inst['state'] == 'started' ]

    # query cloudserver to see which instances are still live
    logger.info( 'querying to see which instances are still live' )
    liveInstances = []
    deadInstances = []
    for iid in startedIids:
        reqParams = {"show-device-info":True}
        try:
            response = ncs.queryNcsSc( 'instances/%s' % iid, authToken, reqParams=reqParams, maxRetries=10)
        except Exception as exc:
            logger.warning( 'querying instance status got exception (%s) %s',
                type(exc), exc )
        else:
            if response['statusCode'] != 200:
                logger.error( 'cloud server returned bad status code %s', response['statusCode'] )
                # drastic hack to exit this early
                sys.exit( 1 )
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
                deadInstances.append( inst )
    if not liveInstances:
        logger.warning( 'no agent instances are still live')
        sys.exit( os.path.basename(__file__) + ' exit: no agent instances are still live')

    logger.info( '%d live instances', len(liveInstances) )
    #logger.info( 'liveInstances: %s', liveInstances )

    iidsToTerminate = []

    errorsByIid = checkProcesses( liveInstances )
    badIids = list( errorsByIid.keys() )
    if len( badIids ) > 1 and len( badIids ) == len( liveInstances ):
        logger.warning( 'NOT terminating all instances, even though all had errors' )
    else:
        iidsToTerminate.extend( badIids )

    retrieveLogs( liveInstances )
    #showLogs( dataDirPath )

    '''
    errorsByIid = checkLogs( liveInstances, dataDirPath, False )
    if errorsByIid:
        badIids = list( errorsByIid.keys() )
        logger.warning( '%d error-logged instance(s)', len( badIids ) )
        if len( badIids ) > 1 and len( badIids ) == len( liveInstances ):
            logger.warning( 'NOT terminating all instances, even though all had errors' )
        else:
            iidsToTerminate.extend( badIids )
    '''
 
    iidsToTerminate = set( iidsToTerminate )
    terminatedIids = []
    if args.terminateBad and iidsToTerminate:
        logger.info( 'terminating %d bad instance(s)', len(iidsToTerminate) )
        ncs.terminateInstances( authToken, list( iidsToTerminate ) )
        terminatedIids = list( iidsToTerminate )
        toPurge = [inst for inst in startedInstances if inst['instanceId'] in iidsToTerminate]
        purgeHostKeys( toPurge )

    stillLive = [inst for inst in liveInstances if inst['instanceId'] not in terminatedIids]
    logger.info( '%d instances are still live', len(stillLive) )
    with open( dataDirPath + '/liveAgents.json','w' ) as outFile:
        json.dump( stillLive, outFile, indent=2 )

    logger.info( 'finished' )
