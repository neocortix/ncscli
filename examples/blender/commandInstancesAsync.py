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
from eventTiming import eventTiming


logger = logging.getLogger(__name__)
resultsLogFile = None

def anyFound( a, b ):
    ''' return true iff any items from iterable a is found in iterable b '''
    for x in a:
        if x in b:
            return True
    return False

def logResult( key, value, instanceId ):
    if resultsLogFile:
        toLog = {key: value, 'instanceId': instanceId,
            'dateTime': datetime.datetime.now(datetime.timezone.utc).isoformat() }
        print( json.dumps( toLog, sort_keys=True ), file=resultsLogFile )

def extractSshTargets( inRecs ):
    targets = []
    
    for details in inRecs:
        iid = details['instanceId']
        #logger.info( 'NCSC Inst details %s', details )
        if details['state'] == 'started':
            if 'ssh' in details:
                target = details['ssh'] # will contain host, port, user, and password
                target['instanceId'] = iid
                targets.append(target)
    return targets

async def run_client(inst, cmd, scpSrcFilePath=None, dlDirPath='.', dlFileName=None ):
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
        #async with asyncssh.connect(host, port=port, username=user, password=password, known_hosts=None) as conn:
        async with asyncssh.connect(host, port=port, username=user, known_hosts=None, agent_path=None) as conn:
            if scpSrcFilePath:
                logger.info( 'uploading %s to %s', scpSrcFilePath, iidAbbrev )
                await asyncssh.scp( scpSrcFilePath, conn, preserve=True, recurse=True )
                #logger.info( 'uploaded %s to %s', scpSrcFilePath, iidAbbrev )
                logResult( 'upload', scpSrcFilePath, iid )
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
            return proc.returncode
    except Exception as exc:
        logger.warning( 'got exception (%s) %s', type(exc), exc, exc_info=False )
        logResult( 'exception', {'type': type(exc).__name__, 'msg': str(exc) }, iid )
        return exc
    return 'did we not connect?'

async def run_multiple_clients( instances, cmd, timeLimit=None,
    scpSrcFilePath=None,
    dlDirPath='.', dlFileName=None
    ):
    # run cmd on all the given instances
    #logger.info( 'instances %s', instances )

    tasks = (asyncio.wait_for(run_client(inst, cmd, scpSrcFilePath=scpSrcFilePath, dlDirPath=dlDirPath, dlFileName=dlFileName),
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
            else:
                nGood += 1
                #logger.debug( 'result code %d for %s', result, abbrevIid )
        elif isinstance(result, asyncio.TimeoutError):
            nTimedOut += 1
            logger.warning('task timed out for %s', abbrevIid )
            # log it as something different from an exception
            logResult( 'timeout', timeLimit, iid )
        elif isinstance(result, ConnectionRefusedError):  # one type of Exception
            nExceptions += 1
            logger.warning('connection refused for %s', abbrevIid )
        elif isinstance(result, Exception):
            nExceptions += 1
            logger.warning('exception (%s) "%s" for %s', type(result), result, abbrevIid )
        else:
            # unexpected result type
            nOther += 1
            logger.warning('task result for %s was (%s) %s', abbrevIid, type(result), result )

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
    ap.add_argument('outJsonFilePath', default='installed.json')
    ap.add_argument('--download', help='optional fileName to download from all targets')
    ap.add_argument('--downloadDestDir', default='./download', help='dest dir for download (default="./download")')
    ap.add_argument('--timeLimit', type=float, help='maximum time (in seconds) to take (default=none (unlimited)')
    ap.add_argument('--upload', help='optional fileName to upload to all targets')
    ap.add_argument('--command', default='uname', help='the command to execute')
    args = ap.parse_args()
    logger.info( "args: %s", str(args) )
    
    dataDirPath = 'data'
    launchedJsonFilePath = args.launchedJsonFilePath

    timeLimit = args.timeLimit  # seconds


    startDateTime = datetime.datetime.now( datetime.timezone.utc )
    startTime = time.time()
    #deadline = startTime + timeLimit

    dateTimeTagFormat = '%Y-%m-%d_%H%M%S'  # cant use iso format dates in filenames because colons
    dateTimeTag = startDateTime.strftime( dateTimeTagFormat )
    logger.debug( 'dateTimeTag is %s', dateTimeTag )

    loadedInstances = None
    with open( launchedJsonFilePath, 'r' ) as jsonFile:
        loadedInstances = json.load(jsonFile)  # a list of dicts

    startedInstances = [inst for inst in loadedInstances if inst['state'] == 'started' ]

    instancesByIid = {}
    for inst in loadedInstances:
        iid = inst['instanceId']
        instancesByIid[iid] = inst

    remoteHosts = extractSshTargets( startedInstances )
 
    sessions = {}
    eventTimings = []

    program = '/bin/bash --login -c "%s"' % args.command
    #program = args.command

    starterTiming = eventTiming('startInstaller')
    starterTiming.finish()
    eventTimings.append(starterTiming)

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

    mainTiming = eventTiming('main')

    if args.download:
        # create dirs for downloads
        dlDirPath = args.downloadDestDir
        os.makedirs(dlDirPath, exist_ok=True)
        for inst in startedInstances:
            iid = inst['instanceId']
            os.makedirs(dlDirPath+'/'+iid, exist_ok=True)



    #cmd = 'hostname'
    asyncio.get_event_loop().run_until_complete(run_multiple_clients( 
        startedInstances, program, scpSrcFilePath=args.upload,
        dlFileName=args.download, dlDirPath=args.downloadDestDir,
        timeLimit=timeLimit
        ))

    mainTiming.finish()
    eventTimings.append(mainTiming)

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
