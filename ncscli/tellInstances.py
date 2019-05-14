#!/usr/bin/env python3
''' ssh some conmands to several workers'''

# standard library modules
import argparse
import asyncio
from concurrent import futures
import datetime
import json
import logging
import os
import subprocess
import sys
import time

#third-party modules
import asyncssh

#neocortix modules
#from eventTiming import eventTiming  # contents copied below


logger = logging.getLogger(__name__)
resultsLogFile = None

def anyFound( a, b ):
    ''' return true iff any items from iterable a is found in iterable b '''
    for x in a:
        if x in b:
            return True
    return False

def boolArg( v ):
    if v.lower() == 'true':
        return True
    elif v.lower() == 'false':
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

class eventTiming(object):
    '''stores name and beginning and ending of an arbitrary "event"'''
    def __init__(self, eventName, startDateTime=None, endDateTime=None):
        self.eventName = eventName
        self.startDateTime = startDateTime if startDateTime else datetime.datetime.now(datetime.timezone.utc)
        self.endDateTime = endDateTime
    
    def __repr__( self ):
        return str(self.toStrList())

    def finish(self):
        self.endDateTime = datetime.datetime.now(datetime.timezone.utc)

    def duration(self):
        if self.endDateTime:
            return self.endDateTime - self.startDateTime
        else:
            return datetime.timedelta(0)

    def toStrList(self):
        return [self.eventName, 
            self.startDateTime.isoformat(), 
            self.endDateTime.isoformat() if self.endDateTime else None
            ]

def logResult( key, value, instanceId ):
    if resultsLogFile:
        toLog = {key: value, 'instanceId': instanceId,
            'dateTime': datetime.datetime.now(datetime.timezone.utc).isoformat() }
        print( json.dumps( toLog, sort_keys=True ), file=resultsLogFile )

async def run_client(inst, cmd, sshAgent=None, scpSrcFilePath=None, dlDirPath='.', dlFileName=None ):
    #logger.info( 'inst %s', inst)
    sshSpecs = inst['ssh']
    #logger.info( 'iid %s, ssh: %s', inst['instanceId'], inst['ssh'])
    host = sshSpecs['host']
    port = sshSpecs['port']
    user = sshSpecs['user']
    iid = inst['instanceId']
    iidAbbrev = iid[0:16]
    # implement pasword-passing if present in ssh args
    password = sshSpecs.get('password', None )

    try:
        known_hosts = None
        if False:  # 'returnedPubKey' in inst:
            keyStr = inst['returnedPubKey']
            logger.info( 'importing %s', keyStr)
            key = asyncssh.import_public_key( keyStr )
            logger.info( 'imported %s', key.export_public_key() )
            #known_hosts = key # nope
            known_hosts = asyncssh.import_known_hosts(keyStr)
        #sshAgent = os.getenv( 'SSH_AUTH_SOCK' )
        #async with asyncssh.connect(host, port=port, username=user, password=password, known_hosts=None) as conn:
        async with asyncssh.connect(host, port=port, username=user,
            known_hosts=known_hosts, agent_path=sshAgent ) as conn:
            serverHostKey = conn.get_server_host_key()
            #logger.info( 'got serverHostKey (%s) %s', type(serverHostKey), serverHostKey )
            serverPubKey = serverHostKey.export_public_key(format_name='openssh')
            #logger.info( 'serverPubKey (%s) %s', type(serverPubKey), serverPubKey )
            serverPubKeyStr = str(serverPubKey,'utf8')
            #logger.info( 'serverPubKeyStr %s', serverPubKeyStr )
            inst['returnedPubKey'] = serverPubKeyStr

            if scpSrcFilePath:
                logger.info( 'uploading %s to %s', scpSrcFilePath, iidAbbrev )
                await asyncssh.scp( scpSrcFilePath, conn, preserve=True, recurse=True )
                #logger.info( 'uploaded %s to %s', scpSrcFilePath, iidAbbrev )
                logResult( 'upload', scpSrcFilePath, iid )
            proc = None
            # execute cmd on remote, if non-null cmd given
            if cmd:
                # substitute actual instanceId for '<<instanceId>>' in cmd
                cmd = cmd.replace( '<<instanceId>>', iid )
                async with conn.create_process(cmd) as proc:
                    async for line in proc.stdout:
                        logger.info('stdout[%s] %s', iidAbbrev, line.strip() )
                        logResult( 'stdout', line.strip(), iid )

                    async for line in proc.stderr:
                        logger.info('stderr[%s] %s', iidAbbrev, line.strip() )
                        logResult( 'stderr', line.strip(), iid )
                await proc.wait_closed()
                logResult( 'returncode', proc.returncode, iid )
                if proc.returncode is None:
                    logger.warning( 'returncode[%s] NONE', iidAbbrev )
                elif proc.returncode:
                    logger.warning( 'returncode %s for %s', proc.returncode, iidAbbrev )

            if dlFileName:
                destDirPath = '%s/%s' % (dlDirPath, iid)
                logger.info( 'downloading %s from %s to %s',
                    dlFileName, iidAbbrev, destDirPath )
                await asyncssh.scp( (conn, dlFileName), destDirPath, preserve=True, recurse=True )
                #logger.info( 'downloaded from %s to %s', iidAbbrev, destDirPath )
                logResult( 'download', dlFileName, iid )
            if proc:
                return proc.returncode
            else:
                return 0
    except Exception as exc:
        logger.warning( 'got exception (%s) %s', type(exc), exc, exc_info=False )
        logResult( 'exception', {'type': type(exc).__name__, 'msg': str(exc) }, iid )
        return exc
    return 'did we not connect?'

async def run_multiple_clients( instances, cmd, timeLimit=None, sshAgent=None,
    scpSrcFilePath=None,
    dlDirPath='.', dlFileName=None
    ):
    # run cmd on all the given instances
    #logger.info( 'instances %s', instances )

    tasks = (asyncio.wait_for(run_client(inst, cmd, sshAgent=sshAgent, scpSrcFilePath=scpSrcFilePath, dlDirPath=dlDirPath, dlFileName=dlFileName),
        timeout=timeLimit)
        for inst in instances )
    results = await asyncio.gather(*tasks, return_exceptions=True)

    nGood = 0
    nExceptions = 0
    nFailed = 0
    nTimedOut = 0
    nOther = 0
    
    for i, result in enumerate(results, 1):
        inst = instances[i-1]
        iid = inst['instanceId']
        abbrevIid = iid[0:16]

        if isinstance(result, int):
            # the normal case, where each result is a return code from the remote
            if result:
                nFailed += 1
                logger.warning( 'result code %d for %s', result, abbrevIid )
                inst['commandState'] = 'failed'
            else:
                nGood += 1
                #logger.debug( 'result code %d for %s', result, abbrevIid )
                inst['commandState'] = 'good'
        elif isinstance(result, asyncio.TimeoutError):
            nTimedOut += 1
            logger.warning('task timed out for %s', abbrevIid )
            # log it as something different from an exception
            logResult( 'timeout', timeLimit, iid )
            inst['commandState'] = 'timeout'
        elif isinstance(result, ConnectionRefusedError):  # one type of Exception
            nExceptions += 1
            logger.warning('connection refused for %s', abbrevIid )
            inst['commandState'] = 'unreachable'
        elif isinstance(result, Exception):
            nExceptions += 1
            logger.warning('exception (%s) "%s" for %s', type(result), result, abbrevIid )
            inst['commandState'] = 'exception'
        else:
            # unexpected result type
            nOther += 1
            logger.warning('task result for %s was (%s) %s', abbrevIid, type(result), result )
            inst['commandState'] = 'unknown'

    logger.info( '%d good, %d exceptions, %d failed, %d timed out, %d other',
        nGood, nExceptions, nFailed, nTimedOut, nOther )

if __name__ == "__main__":
    # configure logging
    logging.basicConfig(format='%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s', datefmt='%Y/%m/%d %H:%M:%S')
    logger.setLevel(logging.DEBUG)
    logger.debug('the logger is configured')
    asyncssh.set_log_level( logging.WARNING )

    ap = argparse.ArgumentParser( description=__doc__ )
    ap.add_argument('launchedJsonFilePath', default='launched.json')
    ap.add_argument('--command', help='the command to execute')
    ap.add_argument('--download', help='optional fileName to download from all targets')
    ap.add_argument('--downloadDestDir', default='./download', help='dest dir for download (default="./download")')
    ap.add_argument('--jsonOut', help='file path to write updated instance info in json format')
    ap.add_argument('--sshAgent', type=boolArg, default=False, help='whether or not to use ssh agent')
    ap.add_argument('--timeLimit', type=float, help='maximum time (in seconds) to take (default=none (unlimited)')
    ap.add_argument('--upload', help='optional fileName to upload to all targets')
    args = ap.parse_args()
    logger.info( "args: %s", str(args) )
    
    dataDirPath = 'data'
    launchedJsonFilePath = args.launchedJsonFilePath

    timeLimit = args.timeLimit  # seconds

    if args.sshAgent:
        sshAgent = os.getenv( 'SSH_AUTH_SOCK' )
    else:
        sshAgent = None


    startDateTime = datetime.datetime.now( datetime.timezone.utc )
    startTime = time.time()
    eventTimings = []
    starterTiming = eventTiming('startInstaller')


    dateTimeTagFormat = '%Y-%m-%d_%H%M%S'  # cant use iso format dates in filenames because colons
    dateTimeTag = startDateTime.strftime( dateTimeTagFormat )
    logger.debug( 'dateTimeTag is %s', dateTimeTag )

    loadedInstances = None
    with open( launchedJsonFilePath, 'r' ) as jsonFile:
        loadedInstances = json.load(jsonFile)  # a list of dicts

    startedInstances = [inst for inst in loadedInstances if inst['state'] == 'started' ]
    '''
    instancesByIid = {}
    for inst in loadedInstances:
        iid = inst['instanceId']
        instancesByIid[iid] = inst
    remoteHosts = extractSshTargets( startedInstances )
    sessions = {}
    '''
 
    if args.command:
        program = '/bin/bash --login -c "%s"' % args.command
    else:
        program = None
    #program = args.command


    #resultsLogFilePath = os.path.splitext( os.path.basename( __file__ ) )[0] + '_results.log'
    resultsLogFilePath = dataDirPath + '/' \
        + os.path.splitext( os.path.basename( __file__ ) )[0] \
        + '_results_' + dateTimeTag  + '.log'

    resultsLogFile = open( resultsLogFilePath, "w", encoding="utf8" )
    #print( '[]', datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S'),
    #    "args:", str(args), file=resultsLogFile )
    #toLog = {'startDateTime': datetime.datetime.now().isoformat(), 'args': vars(args) }
    #print( json.dumps( toLog ), file=resultsLogFile )
    logResult( 'programArgs', vars(args), '<master>')
    
    installed = set()
    failed = set()

    if args.download:
        # create dirs for downloads
        dlDirPath = args.downloadDestDir
        os.makedirs(dlDirPath, exist_ok=True)
        for inst in startedInstances:
            iid = inst['instanceId']
            os.makedirs(dlDirPath+'/'+iid, exist_ok=True)

    starterTiming.finish()
    eventTimings.append(starterTiming)
    mainTiming = eventTiming('main')
    # the main loop
    asyncio.get_event_loop().run_until_complete(run_multiple_clients( 
        startedInstances, program, scpSrcFilePath=args.upload,
        dlFileName=args.download, dlDirPath=args.downloadDestDir,
        sshAgent=sshAgent,
        timeLimit=timeLimit
        ))

    mainTiming.finish()
    eventTimings.append(mainTiming)

    if args.jsonOut:
        jsonOutFilePath = os.path.expanduser( os.path.expandvars( args.jsonOut ) )
        with open( jsonOutFilePath, 'w') as outFile:
            json.dump( startedInstances, outFile, default=str, indent=2, skipkeys=True )


    elapsed = time.time() - startTime
    logger.info( 'finished; elapsed time %.1f seconds (%.1f minutes)', elapsed, elapsed/60 )

    print('\nTiming Summary (durations in minutes)')
    for ev in eventTimings:
        s1 = ev.startDateTime.strftime( '%H:%M:%S' )
        if ev.endDateTime:
            s2 = ev.endDateTime.strftime( '%H:%M:%S' )
        else:
            s2 = s1
        dur = ev.duration().total_seconds() / 60
        print( s1, s2, '%7.1f' % (dur), ev.eventName )
