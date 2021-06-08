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
import re
import subprocess
import sys
import time
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
    errorsByIid = {status['instanceId']: status['status'] for status in stepStatuses if status['status'] }
    logger.info( 'preliminary errorsByIid: %s', errorsByIid )
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

def checkProcesses( liveInstances ):
    logger.info( 'checking %d instance(s)', len(liveInstances) )

    #maybe should weed out some instances
    goodInstances = liveInstances

    cmd = "ps -ef | grep -v grep | grep 'geth' > /dev/null"
    # check for a running geth process on each instance
    stepStatuses = tellInstances.tellInstances( goodInstances, cmd,
        timeLimit=15*60,
        resultsLogFilePath = dataDirPath + '/checkProcesses.jlog',
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
        timeLimit=15*60,
        knownHostsOnly=True
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
            '''
            fileModDateTime = datetime.datetime.fromtimestamp( os.path.getmtime( logFilePath ) )
            fileModDateTime = universalizeDateTime( fileModDateTime )
            logger.info( 'found %s; size %d; %s', logDir,
                os.path.getsize( logFilePath ), fileModDateTime.strftime( '%H:%M:%S' ) 
                )
            '''
            connected = False
            sealed = False
            latestSealLine = None
            with open( logFilePath ) as logFile:
                for line in logFile:
                    line = line.rstrip()
                    if not line:
                        continue
                    if 'ERROR' in line:
                        if 'Ethereum peer removal failed' in line:
                            logger.debug( 'ignoring "peer removal failed" for %s', iidAbbrev )
                            continue
                        logger.info( '%s: %s', iidAbbrev, line )
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
                        logger.info( 'latestSealDateTime: %s', latestSealDateTime.isoformat() )

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
    return nowAuth == shouldAuth


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
    ap.add_argument( '--maxNewAuths', type=int, default=0, help='maximum nmber of new authorized signers (default 0)' )
    args = ap.parse_args()

    logLevel = ncsgeth.parseLogLevel( args.logLevel )
    logger.setLevel(logLevel)
    tellInstances.logger.setLevel( logging.WARNING )  # logLevel
    logger.debug('the logger is configured')

    dataDirPath = args.dataDirPath
    authToken = os.getenv('NCS_AUTH_TOKEN') or 'YourAuthTokenHere'

    configName = args.configName
    farmDirPath = 'ether/%s' % configName

    anchorNodesFilePath = args.anchorNodes

    maxNewAuths = args.maxNewAuths

    if not anchorNodesFilePath:
        logger.error( 'no anchorNodes given')
        sys.exit(1)

    if not dataDirPath:
        logger.info( 'no dataDirPath given, so not checking instance details' )
        sys.exit( 0 )

    # get details of anchor nodes (possibly including non-anchor aws nodes)
    anchorInstances = ncsgeth.loadInstances( anchorNodesFilePath )
    if not False:
        for inst in anchorInstances:
            if 'state' not in inst:
                inst['state'] = 'started'
        #with open( 'anchorInstances.json','w' ) as outFile:
        #    json.dump( anchorInstances, outFile, indent=2 )


    # get details of launched instances from the json file
    instancesFilePath = os.path.join( dataDirPath, 'liveNodes.json' )
    if not os.path.isfile( instancesFilePath ):
        instancesFilePath = os.path.join( dataDirPath, 'recruitLaunched.json' )
    startedInstances = ncsgeth.loadInstances( instancesFilePath )
    logger.info( 'checking %d instance(s) from %s', len(startedInstances), instancesFilePath )
    startedIids = [inst['instanceId'] for inst in startedInstances ]
    #startedIids = [inst['instanceId'] for inst in launchedInstances if inst['state'] == 'started' ]


    savedSignersFilePath = os.path.join( dataDirPath, 'savedSigners.json' )
    historicSigners = {}
    if os.path.isfile( savedSignersFilePath ):
        with open( savedSignersFilePath, 'r') as jsonInFile:
            try:
                historicSigners = json.load(jsonInFile) # a dict of lists, indexed by iid
            except Exception as exc:
                logger.warning( 'could not load savedSigners json (%s) %s', type(exc), exc )
    if not historicSigners:
        logger.info( 'there are no saved signers' )


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

    errorsByIid = checkInstanceClocks( liveInstances, dataDirPath )
    badIids.extend( list( errorsByIid.keys() ) )


    errorsByIid = retrieveLogs( liveInstances, farmDirPath )
    badIids.extend( list( errorsByIid.keys() ) )

    errorsByIid = checkLogs( liveInstances, dataDirPath )
    if errorsByIid:
        badIids.extend( list( errorsByIid.keys() ) )
    goodInstances = [inst for inst in liveInstances if inst['instanceId'] not in badIids ]



    signerInfos = ncsgeth.collectSignerInstances( anchorInstances+goodInstances, args.configName )  # a list
    logger.info( '%d signerInfos: %s', len( signerInfos), signerInfos )
    signersByIid = {signer['instanceId']: signer for signer in signerInfos }
    # conversion code to produce legacy savedSigners
    savedSigners = {}
    for signerInfo in signerInfos:
        savedSigners[ signerInfo['instanceId'] ] = [signerInfo['accountAddr']]
    logger.info( '%d savedSigners: %s', len( savedSigners), savedSigners )
    if not savedSigners:
        logger.info( 'there are NO signers' )


    # can start all miners, if any
    if False:
        if savedSigners:
            authorizers = findAuthorizers( anchorInstances+goodInstances, savedSigners, badIids )
            if authorizers:
                logger.info( 'STARTING ALL %d MINERS', len( authorizers ) )
                ncsgeth.startMiners( authorizers, configName )

    authorizers = []
    terminatedIids = []
    if badIids:
        # deduplicate badIids but leave it as a list
        badIids = list( set( badIids ) )
        # build list of instances capable of voting on authorization
        authorizers = findAuthorizers( anchorInstances+liveInstances, savedSigners, badIids )
        logger.info( '%d authorizers', len(authorizers) )
        # terminate bad instances, deauthorizing any that are also authorized
        for iid in badIids:
            wasSigner = (iid in savedSigners) or (iid in historicSigners)
            #logger.info( 'saved signer? %s', wasSigner )
            if wasSigner:
                logger.info( 'will deauthorize %s', iid[0:16])
                # victim is first account in savedSigners list for this instance
                logger.info( 'sleeping for 90 seconds' )
                time.sleep( 90 )
                victimAccount = historicSigners[iid][0]
                logger.info( 'deauthorizing %s account %s', iid[0:16], victimAccount )
                results = ncsgeth.authorizeSigner( authorizers, configName, victimAccount, False )
                logger.info( 'authorizeSigner returned: %s', results )
                waitForAuth( victimAccount, False, authorizers, configName, timeLimit=15*60 )
            logger.info( 'terminating %s', iid)
            ncs.terminateInstances( authToken, [iid] )
            terminatedIids.append( iid )

    # authorize good instances that have been up for long enough
    #TODO skip this if maxNewAuths <= 0 
    trustedThresh = 12  # hours
    nNewAuths = 0
    sleepAmt = 15
    if badIids:
        sleepAmt = 90  # longer to avoid possible trouble
    authorizers = findAuthorizers( anchorInstances+liveInstances, savedSigners, badIids )
    now = datetime.datetime.now( datetime.timezone.utc )
    for inst in goodInstances:
        # limit the loop to <= maxNewAuths auths per run
        iid = inst['instanceId']
        abbrevIid = iid[0:16]
        startedAtStr = inst['started-at']
        #logger.info( 'good instance %s started at %s', abbrevIid, startedAtStr )
        startedDateTime = universalizeDateTime( dateutil.parser.parse( startedAtStr ) )
        uptimeHrs = (now - startedDateTime).total_seconds() / 3600
        logger.info( 'uptime %.1f hrs', uptimeHrs )
        if nNewAuths >= maxNewAuths:
            break
        if uptimeHrs >= trustedThresh:
            wasSigner = iid in savedSigners
            if not wasSigner:
                results = ncsgeth.collectPrimaryAccounts( [inst], configName )
                if results[0] and results[0].get( 'accountAddr' ):
                    logger.info( '%d authorizers', len(authorizers) )
                    if not authorizers:
                        break
                    iidAccPair = results[0]
                    primaryAccount = iidAccPair['accountAddr']
                    #logger.info( 'primary account: %s', primaryAccount )
                    logger.info( 'sleeping for %.1f seconds', sleepAmt )
                    time.sleep( sleepAmt )
                    sleepAmt = 90  # longer sleep after the first auth
                    logger.info( 'starting miner on %s', iid[0:16] )
                    minerResults = ncsgeth.startMiners( [inst], configName )
                    logger.info( 'authorizing %s account %s', iid[0:16], primaryAccount )
                    authResults = ncsgeth.authorizeSigner( authorizers, configName, primaryAccount, True )
                    #logger.info( 'authResults:  %s', authResults )
                    good = waitForAuth( primaryAccount, True, authorizers, configName, timeLimit=15*60 )
                    if good:
                        authorizers.append( inst )
                        nNewAuths += 1

    terminatedIids = set( terminatedIids )
    stillLive = [inst for inst in liveInstances if inst['instanceId'] not in terminatedIids]
    logger.info( '%d instances are still live', len(stillLive) )
    with open( dataDirPath + '/liveNodes.json','w' ) as outFile:
        json.dump( stillLive, outFile, indent=2 )

    logger.info( 'finished' )
