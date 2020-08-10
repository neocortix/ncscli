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
import signal
import socket
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
        resultsLogFile.flush()

def sigtermHandler():
    ''' stops the currently running event loop, if any'''
    logger.warning( 'SIGTERM signal received; will try to stop gracefully' )
    terminate()

def terminate():
    ''' stops the currently running event loop, if any'''
    loop = asyncio.get_event_loop()
    if loop.is_running():
        logger.info( 'canceling tasks' )
        for task in asyncio.Task.all_tasks():
            if task is not asyncio.tasks.Task.current_task():
                task.cancel()
        '''
        # not sure if this code ever made sense
        logger.info( 'stopping the eventloop' )
        try:
            loop.stop()
        except Exception as exc:
            logger.info( 'ignoring exception %s', exc )
        '''

async def run_client(inst, cmd, sshAgent=None, scpSrcFilePath=None, dlDirPath='.', 
        dlFileName=None, knownHostsOnly=False ):
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
        if knownHostsOnly:
            known_hosts = os.path.expanduser( '~/.ssh/known_hosts' )
        else:
            known_hosts = None
        if False:  # 'returnedPubKey' in inst:
            keyStr = inst['returnedPubKey']
            logger.info( 'importing %s', keyStr)
            key = asyncssh.import_public_key( keyStr )
            logger.info( 'imported %s', key.export_public_key() )
            #known_hosts = key # nope
            known_hosts = asyncssh.import_known_hosts(keyStr)
        logResult( 'operation', ['connect', host, port], iid )
        #sshAgent = os.getenv( 'SSH_AUTH_SOCK' )
        #async with asyncssh.connect(host, port=port, username=user, password=password, known_hosts=None) as conn:
        async with asyncssh.connect(host, port=port, username=user,
            keepalive_interval=15, keepalive_count_max=4,
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
                logResult( 'operation', ['upload', scpSrcFilePath], iid )
            proc = None
            # execute cmd on remote, if non-null cmd given
            if cmd:
                # substitute actual instanceId for '<<instanceId>>' in cmd
                cmd = cmd.replace( '<<instanceId>>', iid )
                logResult( 'operation', ['command', cmd], iid )
                async with conn.create_process(cmd) as proc:
                    async for line in proc.stdout:
                        logger.info('stdout[%s] %s', iidAbbrev, line.strip() )
                        logResult( 'stdout', line.rstrip(), iid )

                    async for line in proc.stderr:
                        logger.info('stderr[%s] %s', iidAbbrev, line.strip() )
                        logResult( 'stderr', line.rstrip(), iid )
                await proc.wait_closed()
                logResult( 'returncode', proc.returncode, iid )
                if proc.returncode is None:
                    logger.warning( 'returncode[%s] NONE', iidAbbrev )
                #elif proc.returncode:
                #    logger.warning( 'returncode %s for %s', proc.returncode, iidAbbrev )

            if dlFileName:
                destDirPath = '%s/%s' % (dlDirPath, iid)
                logger.info( 'downloading %s from %s to %s',
                    dlFileName, iidAbbrev, destDirPath )
                await asyncssh.scp( (conn, dlFileName), destDirPath, preserve=True, recurse=True )
                #logger.info( 'downloaded from %s to %s', iidAbbrev, destDirPath )
                logResult( 'operation', ['download', dlFileName], iid )
            if proc:
                return proc.returncode
            else:
                return 0
    except Exception as exc:
        logger.warning( 'got exception (%s) %s', type(exc), exc, exc_info=False )
        logResult( 'exception', {'type': type(exc).__name__, 'msg': str(exc) }, iid )
        return exc
    return 'did we not connect?'

async def run_client_simple(inst, cmd, sshAgent=None, scpSrcFilePath=None, dlDirPath='.', dlFileName=None ):
    sshSpecs = inst['ssh']
    host = sshSpecs['host']
    port = sshSpecs['port']
    user = sshSpecs['user']
    iid = inst['instanceId']

    try:
        known_hosts = None
        async with asyncssh.connect(host, port=port, username=user,
            known_hosts=known_hosts, agent_path=sshAgent ) as conn:
            # substitute actual instanceId for '<<instanceId>>' in cmd
            cmd = cmd.replace( '<<instanceId>>', iid )
            async with conn.create_process(cmd) as proc:
                async for line in proc.stdout:
                    print( 'stdout', line.strip(), iid )

                async for line in proc.stderr:
                    print( 'stderr', line.strip(), iid )
            await proc.wait_closed()
            print( 'returncode', proc.returncode, iid )
            if proc:
                return proc.returncode
            else:
                return 0
    except Exception as exc:
        logger.warning( 'got exception (%s) for instance %s; %s', type(exc), iid, exc, exc_info=False )
        logResult( 'exception', {'type': type(exc).__name__, 'msg': str(exc) }, iid )
        return exc
    return 'did we not connect?'

async def run_multiple_clients( instances, cmd, timeLimit=None, sshAgent=None,
    scpSrcFilePath=None,
    dlDirPath='.', dlFileName=None,
    knownHostsOnly=False
    ):
    # run cmd on all the given instances
    #logger.info( 'instances %s', instances )

    tasks = (asyncio.wait_for(run_client(inst, cmd, sshAgent=sshAgent,
                scpSrcFilePath=scpSrcFilePath, dlDirPath=dlDirPath, dlFileName=dlFileName,
                knownHostsOnly=knownHostsOnly),
        timeout=timeLimit)
        for inst in instances )
    results = await asyncio.gather(*tasks, return_exceptions=True)

    nGood = 0
    nExceptions = 0
    nFailed = 0
    nTimedOut = 0
    nOther = 0
    
    statuses = []
    for i, result in enumerate(results, 1):
        inst = instances[i-1]
        iid = inst['instanceId']
        abbrevIid = iid[0:16]

        statuses.append( {'instanceId': iid, 'status': result} )
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
        elif isinstance(result, socket.gaierror):  # another type of Exception
            nExceptions += 1
            logger.warning('gaierror "%s" for %s (%s)', result, abbrevIid, inst['ssh'].get('host') )
            inst['commandState'] = 'gaierror'
        elif isinstance(result, asyncio.CancelledError):  # another type of Exception (sort of)
            nExceptions += 1
            logger.warning('task cancelled for %s', abbrevIid )
            inst['commandState'] = 'cancelled'
        elif isinstance(result, Exception):  # miscellaneous exception
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
    return statuses

def tellInstances( instancesSpec, command=None, resultsLogFilePath=None,
    download=None, downloadDestDir=None,
    jsonOut=None, sshAgent=False, timeLimit=3600, upload=None,
    knownHostsOnly=False, stopOnSigterm=False
    ):
    '''tellInstances to upload, execute, and/or download, things'''
    args = locals().copy()

    dataDirPath = 'data'
    #launchedJsonFilePath = args.launchedJsonFilePath

    #timeLimit = args.timeLimit  # seconds

    if sshAgent:
        sshAgent = os.getenv( 'SSH_AUTH_SOCK' )
    else:
        sshAgent = None


    startDateTime = datetime.datetime.now( datetime.timezone.utc )
    startTime = time.time()
    eventTimings = []
    starterTiming = eventTiming('startInstaller')

    # instancesSpec is a string, use it as a json instances file
    if isinstance( instancesSpec, str ):
        loadedInstances = None
        with open( instancesSpec, 'r' ) as jsonFile:
            loadedInstances = json.load(jsonFile)  # a list of dicts
        startedInstances = [inst for inst in loadedInstances if inst['state'] == 'started' ]
    else:
        # check that the supposed list of instances is iterable
        try:
            _ = iter( instancesSpec )
        except TypeError:
            logger.error( 'the given instances argument is not iterabe')
            return None
        else:
            startedInstances = instancesSpec

    '''
    instancesByIid = {}
    for inst in loadedInstances:
        iid = inst['instanceId']
        instancesByIid[iid] = inst
    remoteHosts = extractSshTargets( startedInstances )
    sessions = {}
    '''
 
    if command:
        program = '/bin/bash --login -c "%s"' % command
    else:
        program = None
    #program = command

    global resultsLogFile
    if resultsLogFilePath:
        resultsLogFile = open( resultsLogFilePath, "w", encoding="utf8" )
    else:
        resultsLogFile = None

    # save args, but avoid saving too much
    argsToSave = args.copy()
    del argsToSave['instancesSpec']
    argsToSave['instanceIds'] = [inst['instanceId'] for inst in startedInstances]
    logResult( 'operation', ['tellInstances', {'args': argsToSave} ], '<master>')
    
    #installed = set()
    #failed = set()

    if download:
        # create dirs for downloads
        dlDirPath = downloadDestDir
        os.makedirs(dlDirPath, exist_ok=True)
        for inst in startedInstances:
            iid = inst['instanceId']
            os.makedirs(dlDirPath+'/'+iid, exist_ok=True)

    starterTiming.finish()
    eventTimings.append(starterTiming)
    mainTiming = eventTiming('main')
    # the main loop
    eventLoop = asyncio.get_event_loop()
    eventLoop.set_debug(True)
    if stopOnSigterm:
        eventLoop.add_signal_handler(signal.SIGTERM, sigtermHandler)
    try:
        statuses = eventLoop.run_until_complete(run_multiple_clients(
            startedInstances, program, scpSrcFilePath=upload,
            dlFileName=download, dlDirPath=downloadDestDir,
            sshAgent=sshAgent,
            timeLimit=timeLimit, knownHostsOnly=knownHostsOnly
            ))
    except Exception as exc:
        logger.warning( 'run_until_complete gave exception (%s) %s', type(exc), exc )
        statuses = []
    #json.dump( statuses, sys.stdout, default=repr, indent=2 )

    if resultsLogFile:
        resultsLogFile.close()

    mainTiming.finish()
    eventTimings.append(mainTiming)

    if jsonOut:
        jsonOutFilePath = os.path.expanduser( os.path.expandvars( jsonOut ) )
        with open( jsonOutFilePath, 'w') as outFile:
            json.dump( startedInstances, outFile, default=str, indent=2, skipkeys=True )


    elapsed = time.time() - startTime
    logger.info( 'finished; elapsed time %.1f seconds (%.1f minutes)', elapsed, elapsed/60 )
    '''
    print('\nTiming Summary (durations in minutes)')
    for ev in eventTimings:
        s1 = ev.startDateTime.strftime( '%H:%M:%S' )
        if ev.endDateTime:
            s2 = ev.endDateTime.strftime( '%H:%M:%S' )
        else:
            s2 = s1
        dur = ev.duration().total_seconds() / 60
        print( s1, s2, '%7.1f' % (dur), ev.eventName )
    '''
    return statuses


if __name__ == "__main__":
    # configure logging
    logging.basicConfig(format='%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s', datefmt='%Y/%m/%d %H:%M:%S')
    logger.setLevel(logging.DEBUG)
    logger.debug('the logger is configured')
    asyncssh.set_log_level( logging.WARNING )
    #logging.getLogger("asyncio").setLevel(logging.DEBUG)

    ap = argparse.ArgumentParser( description=__doc__ )
    ap.add_argument('launchedJsonFilePath', default='launched.json')
    ap.add_argument('--command', help='the command to execute')
    ap.add_argument('--resultsLog', help='file path to write detailed output log in json format')
    ap.add_argument('--download', help='optional fileName to download from all targets')
    ap.add_argument('--downloadDestDir', default='./download', help='dest dir for download (default="./download")')
    ap.add_argument('--jsonOut', help='file path to write updated instance info in json format')
    ap.add_argument('--sshAgent', type=boolArg, default=False, help='whether or not to use ssh agent')
    ap.add_argument('--timeLimit', type=float, help='maximum time (in seconds) to take (default=none (unlimited)')
    ap.add_argument('--upload', help='optional fileName to upload to all targets')
    ap.add_argument('--knownHostsOnly', type=boolArg, default=False, help='whether to use only known_hosts, or just any hosts')
    args = ap.parse_args()
    logger.info( "args: %s", str(args) )
    
    tellInstances( args.launchedJsonFilePath, args.command, args.resultsLog,
        args.download, args.downloadDestDir, args.jsonOut, args.sshAgent,
        args.timeLimit, args.upload, args.knownHostsOnly
        )
    logger.info( 'finished' )
