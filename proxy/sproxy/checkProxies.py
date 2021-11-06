#!/usr/bin/env python3
"""
check instances with installed proxies; terminate some that appear unusable
"""

# standard library modules
import argparse
import csv
#import datetime
import json
import logging
import os
#import subprocess
import sys
import urllib3
# third-party modules
import psutil
import requests
# neocortix modules
import ncscli.ncs as ncs
import ncscli.tellInstances as tellInstances


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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

def ingestCsv( inFilePath ):
    '''read the csv file; return contents as a list of dicts'''
    rows = []
    with open( inFilePath, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            rows.append( row )
    return rows

def checkProcesses( goodInstances ):
    '''check that the expected process is running on each instance'''
    logger.info( 'checking %d instance(s)', len(goodInstances) )
    cmd = "ps -ef | grep -v grep | grep 'squid' > /dev/null"
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
    logger.info( 'errorsByIid: %s', errorsByIid )
    return errorsByIid

def retrieveLogs( goodInstances ):
    '''download proxy log files from each of the given instances '''
    logger.info( 'retrieving logs for %d instance(s)', len(goodInstances) )

    proxyLogFilePath = '/var/log/squid/*.log'

    stepStatuses = tellInstances.tellInstances( goodInstances,
        download=proxyLogFilePath, downloadDestDir=dataDirPath +'/proxyLogs',
        timeLimit=30*60,
        knownHostsOnly=True
        )
    #logger.info( 'download statuses: %s', stepStatuses )
    # return stepStatuses # or could return errors by iid


def checkLogs( liveInstances, dataDirPath ):
    # prepare to read proxy logs
    logsDirPath = os.path.join( dataDirPath, 'proxyLogs' )
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
        logFilePath = os.path.join( logsDirPath, logDir, 'cache.log' )
        if not os.path.isfile( logFilePath ):
            logger.warning( 'no log file %s', logFilePath )
        elif os.path.getsize( logFilePath ) <= 0:
            logger.warning( 'empty log file "%s"', logFilePath )
        else:
            ready = False
            with open( logFilePath, 'r' ) as logFile:
                for line in logFile:
                    line = line.rstrip()
                    if not line:
                        continue            
                    if 'ERROR' in line or 'FATAL' in line:
                        logger.info( '%s: %s', iidAbbrev, line )
                        theseErrors = errorsByIid.get( iid, [] )
                        theseErrors.append( line )
                        errorsByIid[iid] = theseErrors
                    elif 'Accepting ' in line:
                        ready = True
            if not ready:
                logger.warning( 'instance %s did say "Accepting"', iidAbbrev )
    logger.info( 'found errors for %d instance(s)', len(errorsByIid) )
    #print( 'errorsByIid', errorsByIid  )
    '''
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
    '''
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
            #TODO maybe a better way to identify forwarders
            if '-fNT' in cmdLine:
                logger.debug( 'cmdLine: %s', cmdLine )
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
    logger.debug( 'mappings: %s', mappings )
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

def checkRequests( instances,  forwarders, forwarderHost ):
    targetUrl = 'https://loadtest-target.neocortix.com'
    logger.info( 'checking %d instance(s)', len(forwarders) )
    errorsByIid = {}
    timeouts = (10, 30)
    #urllib3.disable_warnings()
    instancesByHost = {inst['ssh']['host']: inst for inst in instances }
    for fw in forwarders:
        proxyAddr = '%s:%s' % (forwarderHost, fw['port'])
        proxyUrl = 'http://' + proxyAddr
        proxyDict = {'http': proxyUrl, 'https': proxyUrl, 'ftp': proxyUrl }
        iid = instancesByHost[fw['host']]['instanceId']
        logger.debug( 'checking %s; iid: %s', fw, iid )
        try:
            resp = requests.get( targetUrl, proxies=proxyDict, verify=False, timeout=timeouts )
            if resp.status_code in range( 200, 400 ):
                logger.debug( 'good response %d for %s (inst %s)', resp.status_code, proxyAddr, iid[0:8] )
            else:
                logger.warning( 'bad response %d for %s (instance %s)', resp.status_code, proxyAddr, iid )
                errorsByIid[ iid ] = resp.status_code
        except Exception as exc:
            logger.warning( 'exception (%s) for iid %s "%s"', type(exc), iid, exc )
            errorsByIid[ iid ] = {'exception': exc }
    return errorsByIid

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


if __name__ == "__main__":
    # configure logger formatting
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)
    logger.setLevel(logging.WARNING)

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@', formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    ap.add_argument( '--dataDirPath', required=True, help='the path to the directory for input and output data' )
    ap.add_argument( '--logLevel', default ='info', help='verbosity of log (e.g. debug, info, warning, error)' )
    ap.add_argument( '--terminateBad', type=ncs.boolArg, default=False, help='whether to terminate instances found bad' )
    args = ap.parse_args()

    logging.captureWarnings( True )

    logLevel = parseLogLevel( args.logLevel )
    logger.setLevel(logLevel)
    tellInstances.logger.setLevel( logLevel )
    logger.debug('the logger is configured')

    dataDirPath = args.dataDirPath
    authToken = os.getenv('NCS_AUTH_TOKEN') or 'YourAuthTokenHere'


    if not dataDirPath:
        logger.info( 'no dataDirPath given, so not checking instance details' )
        sys.exit( 0 )
    # get details of launched instances from the json file
    instancesFilePath = os.path.join( dataDirPath, 'liveWorkers.json' )
    if not os.path.isfile( instancesFilePath ):
        instancesFilePath = os.path.join( dataDirPath, 'startedWorkers.json' )
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
        logger.warning( 'no proxy instances are still live')
        sys.exit( os.path.basename(__file__) + ' exit: no proxy instances are still live')

    logger.info( '%d live instances', len(liveInstances) )
    #logger.info( 'liveInstances: %s', liveInstances )
    checkProcesses( liveInstances )
    retrieveLogs( liveInstances )

    iidsToTerminate = []
    errorsByIid = checkLogs( liveInstances, dataDirPath )
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

    inFilePath = os.path.join( dataDirPath, 'sshForwarding.csv' )
    try:
        inRows = ingestCsv( inFilePath )
    except Exception as exc:
        logger.warning( 'could not ingestCsv (%s) %s', type(exc), exc )
    #logger.info( 'sshForwarding rows: %s', inRows )
    if len(inRows):
        forwarderHost = inRows[0]['forwarderHost']
        #forwarderHost = inRows[0]['forwarding'].split(':')[0]
    else:
        forwarderHost = 'noForwarderHostFound'

    liveHosts = [inst['ssh']['host'] for inst in liveInstances]

    myForwarders = [fw for fw in forwarders if fw['host'] in liveHosts ]

    # suppress warnings that would occur for untrusted https certs
    logging.getLogger('py.warnings').setLevel( logging.ERROR )

    # check that we can retrieve from a good target url via each proxy
    errorsByIid = checkRequests( liveInstances, myForwarders, forwarderHost )
    #logger.info( 'checkRequests errorsByIid: %s', errorsByIid )
    # set warnings back to normal
    logging.getLogger('py.warnings').setLevel( logging.WARNING )

    proxyListFilePath = os.path.join( dataDirPath, 'proxyAddrs.txt' )
    with open( proxyListFilePath, 'w' ) as proxyListFile:
        for fw in forwarders:
            if fw['host'] in liveHosts:
                print( '%s:%s' % (forwarderHost, fw['port']), file=proxyListFile )
                #logger.info( 'forwarded host %s matched a live instance', fw['host'])
                logger.info( 'forwarding %s:%s', forwarderHost, fw['port'])
            else:
                logger.warning( 'forwarded host %s did not match a live instance', fw['host'])

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
    with open( os.path.join( dataDirPath, 'liveWorkers.json' ),'w' ) as outFile:
        json.dump( stillLive, outFile, indent=2 )

    logger.info( 'finished' )
