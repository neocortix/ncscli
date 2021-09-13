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

def truncateVersion( nlVersion ):
    '''drop patch-level part of version number, if any'''
    return '.'.join(nlVersion.split('.')[:-1]) if nlVersion.count('.') > 1 else nlVersion

def checkProcesses( liveInstances ):
    '''check that the expected process is running on each instance'''
    logger.info( 'checking %d instance(s)', len(liveInstances) )

    #maybe should weed out some instances
    goodInstances = liveInstances

    cmd = "ps -ef | grep -v grep | grep 'LoadGeneratorAgent start' > /dev/null"
    #cmd = "free --mega 1>&2 ; ps -ef | grep -v grep | grep 'LoadGeneratorAgent start' > /dev/null"
    # check for a running agent process on each instance
    stepStatuses = tellInstances.tellInstances( goodInstances, cmd,
        timeLimit=30*60,
        knownHostsOnly=True
        )
    #logger.info( 'cmd statuses: %s', stepStatuses )
    errorsByIid = {}
    for status in stepStatuses:
        if status['status']:
            # might like to be more selective here
            #logger.info( 'status: %s', status )
            errorsByIid[ status['instanceId'] ] = status
    logger.debug( 'errorsByIid: %s', errorsByIid )
    return errorsByIid

def retrieveLogs( liveInstances, neoloadVersion ):
    '''
    instancesFilePath = os.path.join( dataDirPath, 'startedAgents.json' )
    startedInstances = []
    # get details of launched instances from the json file
    #TODO should get list of instances with good install, rather than all started instances
    with open( instancesFilePath, 'r') as jsonInFile:
        try:
            startedInstances = json.load(jsonInFile)  # an array
        except Exception as exc:
            logger.warning( 'could not load json (%s) %s', type(exc), exc )
    '''

    #maybe should weed out some instances
    goodInstances = liveInstances
    logger.info( 'retrieving logs for %d instance(s)', len(goodInstances) )

    truncVersion = truncateVersion( neoloadVersion )
    agentLogFilePath = '/root/.neotys/neoload/v%s/logs/*.log' % truncVersion

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

def findForwarders():
    mappings = []
    for proc in psutil.process_iter():
        try:
            procInfo = proc.as_dict(attrs=['pid', 'name', 'cmdline'])
        except psutil.NoSuchProcess:
            continue
        if 'ssh' == procInfo['name']:
            #logger.info( 'procInfo: %s', procInfo )
            cmdLine = procInfo['cmdline']
            #logger.info( 'cmdLine: %s', cmdLine )
            #TODO maybe a better way to identify forwarders
            if '-fNT' in cmdLine:
                mapping = {}
                for arg in cmdLine:
                    # 'neocortix.com' is expected in the hostname of each NCS instance
                    if 'neocortix.com' in arg:
                        host = arg.split('@')[1]
                        #logger.info( 'forwarding to host %s', host )
                        mapping['host'] = host
                    if ':localhost:' in arg:
                        part = arg.split( ':localhost:')[0].split(':')[1]
                        assignedPort = int( part )
                        #logger.info( 'forwarding port %d', assignedPort)
                        mapping['port'] = assignedPort
            if mapping:
                #logger.debug( 'forwarding port %d to %s', mapping['port'], mapping['host'] )
                mappings.append( mapping )
    #logger.info( 'mappings: %s', mappings )
    return mappings

def checkForwarders( liveInstances,  forwarders ):
    badIids = []
    forwardedHosts = set( [fw['host'] for fw in forwarders] )
    #logger.info( 'forwardedHosts: %s', forwardedHosts )

    for inst in liveInstances:
        iid = inst['instanceId']
        instHost = inst['ssh']['host']
        if instHost not in forwardedHosts:
            logger.warning( 'NOT forwarding port for %s', iid[0:8] )
            badIids.append( iid )
    return badIids

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

def queryMongoForLGs( mongoHost ):
    import pymongo
    mongoPort = 27017
    mclient = pymongo.MongoClient(mongoHost, mongoPort)
    dbName = 'neoload-on-premise'
    collName = 'resources-definition'
    db = mclient[dbName]
    coll = db[collName]
    # find loadGenerators in the list of resources
    mFilter = {'t': 'NEOLOAD_LOADGENERATOR_AGENT'}
    resources = list( coll.find( mFilter ) )
    #logger.info( 'found resources: %s', resources )
    logger.info( 'found %d load generators', len(resources) )
    for resource in resources:
        ipSpec = resource.get('a')
        (host, port) = ipSpec.split(':')
        resource['host'] = host
        resource['port'] = int(port)
        logger.debug( '%s %s %d', resource['_id'], resource['host'], resource['port']  )
    '''
    import pandas as pd
    lgDf = pd.DataFrame( resources )
    lgDf.to_csv( 'mongoLGs.csv' )
    '''
    return resources

def queryNlWebForResources( nlWebUrl, nlWebToken ):
    headers = {  "Accept": "application/json", "accountToken": nlWebToken }
    url = nlWebUrl+'/v3/resources/zones'

    logger.info( 'querying: %s', nlWebUrl )
    # set long timeouts for requests.get() as a tuple (connection timeout, read timeout) in seconds
    timeouts = (30, 120)
    try:
        resp = requests.get( url, headers=headers, timeout=timeouts )
    except requests.ConnectionError as exc:
        logger.warning( 'ConnectionError exception (%s) %s', type(exc), exc )
        return None
    except Exception as exc:
        logger.warning( 'Exception (%s) %s', type(exc), exc )
        return None

    if (resp.status_code < 200) or (resp.status_code >= 300):
        logger.warning( 'error code from server (%s) %s', resp.status_code, resp.text )
    else:
        nlWebZones = resp.json()
        logger.debug( 'nlWeb api zones: %s', nlWebZones )
        for zone in nlWebZones:
            logger.info( 'zone id: %s name: "%s"', zone['id'], zone['name'] )
            for controller in zone['controllers']:
                logger.info( '  Controller "%s" %s %s', controller['name'], controller['version'], controller['status'] )
            for lg in zone['loadgenerators']:
                logger.debug( '  LG "%s" %s %s', lg['name'], lg['version'], lg['status'] )
            logger.info( '  %d LGs listed by nlweb in Zone %s', len(zone['loadgenerators']), zone['id'] )
            logger.info( '  %d controllers listed by nlweb in Zone %s', len(zone['controllers']), zone['id'] )


if __name__ == "__main__":
    # configure logger formatting
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)
    logger.setLevel(logging.WARNING)

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@', formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    ap.add_argument( '--dataDirPath', required=True, help='the path to the directory for input and output data' )
    ap.add_argument( '--neoloadVersion', default ='7.10', help='version of neoload to check for' )
    ap.add_argument( '--nlWebUrl', help='the URL of a neoload web server to query' )
    ap.add_argument( '--nlWebToken', help='a token for authorized access to a neoload web server' )
    ap.add_argument( '--logLevel', default ='info', help='verbosity of log (e.g. debug, info, warning, error)' )
    ap.add_argument( '--terminateBad', type=ncs.boolArg, default=False, help='whether to terminate instances found bad' )
    args = ap.parse_args()

    logLevel = parseLogLevel( args.logLevel )
    logger.setLevel(logLevel)
    tellInstances.logger.setLevel( logLevel )
    logger.debug('the logger is configured')

    dataDirPath = args.dataDirPath
    authToken = os.getenv('NCS_AUTH_TOKEN') or 'YourAuthTokenHere'


    if args.nlWebUrl:
        if not args.nlWebToken:
            logger.warning( 'please pass --nlWebToken if you want to query an nlWeb server')
        else:
            url = args.nlWebUrl
            if url == 'SAAS':
                url = 'https://neoload-api.saas.neotys.com'
            queryNlWebForResources( url, args.nlWebToken )
    if False:
        mongoHost = 'yourNLWebMongoHost'
        queryMongoForLGs( mongoHost )

    if not dataDirPath:
        logger.info( 'no dataDirPath given, so not checking instance details' )
        sys.exit( 0 )
    # get details of launched instances from the json file
    instancesFilePath = os.path.join( dataDirPath, 'liveAgents.json' )
    if not os.path.isfile( instancesFilePath ):
        instancesFilePath = os.path.join( dataDirPath, 'startedAgents.json' )
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
    checkProcesses( liveInstances )
    retrieveLogs( liveInstances, args.neoloadVersion )
    #showLogs( dataDirPath )

    nlWebWanted = bool( args.nlWebUrl )
    iidsToTerminate = []
    errorsByIid = checkLogs( liveInstances, dataDirPath, nlWebWanted )
    if errorsByIid:
        badIids = list( errorsByIid.keys() )
        logger.warning( '%d error-logged instance(s)', len( badIids ) )
        if len( badIids ) > 1 and len( badIids ) == len( liveInstances ):
            logger.warning( 'NOT terminating all instances, even though all had errors' )
        else:
            iidsToTerminate.extend( badIids )

    forwarders = findForwarders()
    forwardersByPort = { fw['port']: fw for fw in forwarders }
    for port in sorted( forwardersByPort.keys() ):
        forwarder = forwardersByPort[port]
        logger.debug( 'forwarding port %d to %s', forwarder['port'], forwarder['host'] )

    badIids = checkForwarders( liveInstances,  forwarders )
    if badIids:
        logger.warning( '%d not-forwarded instance(s)', len( badIids ) )
        #ncs.terminateInstances( authToken, badIids )
        iidsToTerminate.extend( badIids )
 
    iidsToTerminate = set( iidsToTerminate )
    terminatedIids = []
    if args.terminateBad and iidsToTerminate:
        logger.info( 'terminating %d bad instances', len(iidsToTerminate) )
        ncs.terminateInstances( authToken, list( iidsToTerminate ) )
        terminatedIids = list( iidsToTerminate )
        toPurge = [inst for inst in startedInstances if inst['instanceId'] in iidsToTerminate]
        purgeHostKeys( toPurge )

    stillLive = [inst for inst in liveInstances if inst['instanceId'] not in terminatedIids]
    logger.info( '%d instances are still live', len(stillLive) )
    with open( dataDirPath + '/liveAgents.json','w' ) as outFile:
        json.dump( stillLive, outFile, indent=2 )

    logger.info( 'finished' )
