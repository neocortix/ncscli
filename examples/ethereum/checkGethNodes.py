#!/usr/bin/env python3
"""
check instances with installed geth nodes; terminate some that appear unusable
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
import dateutil.parser
import psutil
import requests
# neocortix modules
import ncscli.ncs as ncs
import ncscli.tellInstances as tellInstances
import ncsgeth


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
    logger.info( 'checking %d instance(s)', len(liveInstances) )

    #maybe should weed out some instances
    goodInstances = liveInstances

    cmd = "ps -ef | grep -v grep | grep 'geth' > /dev/null"
    # check for a running geth process on each instance
    stepStatuses = tellInstances.tellInstances( goodInstances, cmd,
        timeLimit=30*60,
        knownHostsOnly=True
        )
    #logger.info( 'proc statuses: %s', stepStatuses )
    errorsByIid = {status['instanceId']: status['status'] for status in stepStatuses if status['status'] }
    #logger.info( 'errorsByIid: %s', errorsByIid )
    return errorsByIid


def retrieveLogs( liveInstances, farmDirPath ):
    '''retrieve logs from nodes, storing them in dataDirPath/nodeLogs subdirectories'''
    goodInstances = liveInstances
    logger.info( 'retrieving logs for %d instance(s)', len(goodInstances) )

    nodeLogFilePath = farmDirPath + '/geth.log'

    # download the log file from each instance
    stepStatuses = tellInstances.tellInstances( goodInstances,
        download=nodeLogFilePath, downloadDestDir=dataDirPath +'/nodeLogs',
        timeLimit=30*60,
        knownHostsOnly=True
        )
    #logger.info( 'download statuses: %s', stepStatuses )
    #TODO return errorsByIid

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
                    if 'Started P2P networking' in line:
                        connected = True
                if not connected:
                    logger.warning( 'instance %s did not initialize fully', iidAbbrev )
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

def findAuthorizers( instances, savedSigners, badIids ):
    '''return subset of instances capable of voting on authorization'''
    authorizers = []
    for inst in instances:
        iid = inst['instanceId']
        if (iid in savedSigners) and (iid not in badIids):
            if 'host' not in inst['ssh']:
                logger.warning( 'no host for authorizer %s', iid )
            else:
                authorizers.append( inst )
                logger.info( 'including authorizer %s', iid )
    return authorizers


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


if __name__ == "__main__":
    # configure logger formatting
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)
    logger.setLevel(logging.WARNING)

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@' )
    ap.add_argument( '--dataDirPath', help='the path to the directory for input and output data' )
    ap.add_argument( '--anchorNodes', required=True, help='the path to a json file of anchor nodes' )
    #ap.add_argument( '--gethVersion', default ='1.9.25', help='version of geth' )
    ap.add_argument( '--configName', default ='priv_2', help='version of geth' )
    ap.add_argument( '--logLevel', default ='info', help='verbosity of log (e.g. debug, info, warning, error)' )
    args = ap.parse_args()

    logLevel = parseLogLevel( args.logLevel )
    logger.setLevel(logLevel)
    tellInstances.logger.setLevel( logLevel )
    logger.debug('the logger is configured')

    dataDirPath = args.dataDirPath
    authToken = os.getenv('NCS_AUTH_TOKEN') or 'YourAuthTokenHere'

    configName = args.configName
    farmDirPath = 'ether/%s' % configName

    anchorNodesFilePath = args.anchorNodes

    if not anchorNodesFilePath:
        logger.error( 'no anchorNodes given')
        sys.exit(1)

    if not dataDirPath:
        logger.info( 'no dataDirPath given, so not checking instance details' )
        sys.exit( 0 )

    # get details of anchor nodes (possibly including non-anchor aws nodes)
    anchorInstances = ncsgeth.loadInstances( anchorNodesFilePath )

    # get details of launched instances from the json file
    instancesFilePath = os.path.join( dataDirPath, 'liveNodes.json' )
    if not os.path.isfile( instancesFilePath ):
        instancesFilePath = os.path.join( dataDirPath, 'recruitLaunched.json' )
    startedInstances = ncsgeth.loadInstances( instancesFilePath )
    logger.info( 'checking %d instance(s) from %s', len(startedInstances), instancesFilePath )
    startedIids = [inst['instanceId'] for inst in startedInstances ]
    #startedIids = [inst['instanceId'] for inst in launchedInstances if inst['state'] == 'started' ]

    savedSignersFilePath = os.path.join( dataDirPath, 'savedSigners.json' )
    savedSigners = {}
    if os.path.isfile( savedSignersFilePath ):
        with open( savedSignersFilePath, 'r') as jsonInFile:
            try:
                savedSigners = json.load(jsonInFile) # a dict of lists, indexed by iid
            except Exception as exc:
                logger.warning( 'could not load savedSigners json (%s) %s', type(exc), exc )

    # query cloudserver to see which instances are still live
    logger.info( 'querying to see which instances are still live' )
    liveInstances = []
    deadInstances = []
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
                deadInstances.append( inst )
    if not liveInstances:
        logger.warning( 'no node instances are still live')
        sys.exit( os.path.basename(__file__) + ' exit: no node instances are still live')

    logger.info( '%d live instances', len(liveInstances) )
    #logger.info( 'liveInstances: %s', liveInstances )

    badIids = []
    errorsByIid = checkProcesses( liveInstances )
    badIids.extend( list( errorsByIid.keys() ) )


    retrieveLogs( liveInstances, farmDirPath )

    errorsByIid = checkLogs( liveInstances, dataDirPath )
    if errorsByIid:
        badIids.extend( list( errorsByIid.keys() ) )

    authorizers = []
    terminatedIids = []
    if badIids:
        # deduplicate badIids but leave it as a list
        badIids = list( set( badIids ) )
        # build list of instances capable of voting on authorization
        if not authorizers:
            authorizers = findAuthorizers( anchorInstances+liveInstances, savedSigners, badIids )
        logger.info( '%d authorizers', len(authorizers) )
        # terminate bad instances, deauthorizing any that are also authorized
        for iid in badIids:
            logger.info( 'thinking about deauthorizing %s', iid[0:16])
            wasSigner = iid in savedSigners
            #logger.info( 'saved signer? %s', wasSigner )
            if wasSigner:
                # victim is first account in savedSigners list for this instance
                victimAccount = savedSigners[iid][0]
                logger.info( 'deauthorizing %s account %s', iid[0:16], victimAccount )
                results = ncsgeth.authorizeSigner( authorizers, configName, victimAccount, False )
                logger.info( 'authorizeSigner returned: %s', results )
            logger.info( 'terminating %s', iid)
            ncs.terminateInstances( authToken, badIids )
            terminatedIids.append( iid )

    # authorize good instances that have been up for long enough
    now = datetime.datetime.now( datetime.timezone.utc )
    goodInstances = [inst for inst in liveInstances if inst['instanceId'] not in badIids ]
    for inst in goodInstances:
        iid = inst['instanceId']
        abbrevIid = iid[0:16]
        startedAtStr = inst['started-at']
        #logger.info( 'good instance %s started at %s', abbrevIid, startedAtStr )
        startedDateTime = universalizeDateTime( dateutil.parser.parse( startedAtStr ) )
        uptimeHrs = (now - startedDateTime).total_seconds() / 3600
        #logger.info( 'uptime %.1f hrs', uptimeHrs )
        if uptimeHrs >= 18:
            wasSigner = iid in savedSigners
            if not wasSigner:
                results = ncsgeth.collectPrimaryAccounts( [inst], configName )
                if results[0]:
                    if not authorizers:
                        authorizers = findAuthorizers( anchorInstances+liveInstances, savedSigners, badIids )
                    logger.info( '%d authorizers', len(authorizers) )
                    iidAccPair = results[0]
                    primaryAccount = iidAccPair['accountAddr']
                    #logger.info( 'primary account: %s', primaryAccount )
                    logger.info( 'authorizing %s account %s', iid[0:16], primaryAccount )
                    authResults = ncsgeth.authorizeSigner( authorizers, configName, primaryAccount, True )
                    logger.info( 'authResults:  %s', authResults )

    terminatedIids = set( terminatedIids )
    stillLive = [inst for inst in liveInstances if inst['instanceId'] not in terminatedIids]
    logger.info( '%d instances are still live', len(stillLive) )
    with open( dataDirPath + '/liveNodes.json','w' ) as outFile:
        json.dump( stillLive, outFile, indent=2 )

    logger.info( 'finished' )
