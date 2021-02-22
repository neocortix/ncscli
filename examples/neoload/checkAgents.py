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

import psutil
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
    '''
    startedInstances = []
    # get details of launched instances from the json file
    with open( instancesFilePath, 'r') as jsonInFile:
        try:
            startedInstances = json.load(jsonInFile)  # an array
        except Exception as exc:
            logger.warning( 'could not load json (%s) %s', type(exc), exc )
    '''
    logger.info( 'checking %d instance(s)', len(liveInstances) )

    #maybe should weed out some instances
    goodInstances = liveInstances

    cmd = "ps -ef | grep -v grep | grep 'LoadGeneratorAgent start' > /dev/null"
    # check for a running agent process on each instance
    stepStatuses = tellInstances.tellInstances( goodInstances, cmd,
        timeLimit=30*60,
        knownHostsOnly=True
        )
    #logger.info( 'cmd statuses: %s', stepStatuses )


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

    if neoloadVersion == '7.7':
        agentLogFilePath = '/root/.neotys/neoload/v7.7/logs/*.log'
    else:
        agentLogFilePath = '/root/.neotys/neoload/v7.6/logs/*.log'

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

def checkLogs( liveInstances, dataDirPath ):
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


if __name__ == "__main__":
    # configure logger formatting
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)
    logger.setLevel(logging.INFO)
    tellInstances.logger.setLevel( logging.INFO )
    logger.debug('the logger is configured')

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@' )
    ap.add_argument( '--dataDirPath', required=True, help='the path to the directory for input and output data' )
    ap.add_argument( '--neoloadVersion', default ='7.7', help='version of neoload to check for' )
    args = ap.parse_args()

    dataDirPath = args.dataDirPath  # 'data/neoload_2021-02-16_030030'
    authToken = os.getenv('NCS_AUTH_TOKEN') or 'YourAuthTokenHere'




    # get details of launched instances from the json file
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
        try:
            response = ncs.queryNcsSc( 'instances/%s' % iid, authToken, maxRetries=1)
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
            if 'isntanceId' not in inst:
                inst['instanceId'] = iid
            if instState == 'started':
                liveInstances.append( inst )
            else:
                deadInstances.append( inst )
    if not liveInstances:
        logger.warning( 'no agent instances are still live')
        sys.exit( os.path.basename(__file__) + ' exit: no agent instances are still live')

    logger.info( '%d liveInstances', len(liveInstances) )
    #logger.info( 'liveInstances: %s', liveInstances )
    checkProcesses( liveInstances )
    retrieveLogs( liveInstances, args.neoloadVersion )
    #showLogs( dataDirPath )
    errorsByIid = checkLogs( liveInstances, dataDirPath )
    if errorsByIid:
        badIids = list( errorsByIid.keys() )
        logger.warning( 'terminating %d error-logged instance(s)', len( badIids ) )
        ncs.terminateInstances( authToken, badIids )

    forwarders = findForwarders()
    badIids = checkForwarders( liveInstances,  forwarders )
    if badIids:
        logger.warning( 'terminating %d not-forwarded instance(s)', len( badIids ) )
        ncs.terminateInstances( authToken, badIids )
    logger.info( 'finished' )
