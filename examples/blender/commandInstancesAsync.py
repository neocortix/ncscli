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


def anyFound( a, b ):
    ''' return true iff any items from iterable a is found in iterable b '''
    for x in a:
        if x in b:
            return True
    return False


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

async def run_client(inst, cmd):
    #logger.info( 'inst %s', inst)
    sshSpecs = inst['ssh']
    logger.info( 'iid %s, ssh: %s', inst['instanceId'], inst['ssh'])
    host = sshSpecs['host']
    port = sshSpecs['port']
    user = sshSpecs['user']
    # implement pasword-passing if present in ssh args
    password = sshSpecs.get('password', None )

    async with asyncssh.connect(host, port=port, username=user, password=password, known_hosts=None) as conn:
        async with conn.create_process(cmd) as proc:
            async for line in proc.stdout:
                print('stdout[%s] %s' % (host, line), end='')
            async for line in proc.stderr:
                print('stderr[%s] %s' % (host, line), end='')
        await proc.wait_closed()
        if proc.returncode is not None:
            print( 'returncode[%s] %d' % (host, proc.returncode) )
        return proc.returncode

    '''
    async with asyncssh.connect(host, port=port, username=user, password=password, known_hosts=None) as conn:
        return await conn.run(command)
    '''

async def run_client_orig(host, command):
    async with asyncssh.connect(host) as conn:
        return await conn.run(command)

async def run_multiple_clients( instances, cmd, timeLimit=30 ):
    # run cmd on all the given instances
    #logger.info( 'instances %s', instances )

    tasks = (asyncio.wait_for(run_client(inst, cmd), timeout=timeLimit)
                 for inst in instances )
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for i, result in enumerate(results, 1):
        if isinstance(result, asyncio.TimeoutError):
            print('Task %d timed out.' % i)
        elif isinstance(result, int):
            # normally, each result is a return code from the remote
            #print('Task %d result: %s' % (i, result))
            inst = instances[i-1]
            iid = inst['instanceId']
            logger.info( 'result code %d for %s', result, iid[0:16] )
        else:
            # unexpected result type
            print('Task %d result: %s' % (i, result))

    '''
    tasks = (run_client(inst, cmd) for inst in instances)
    #tasks = (run_client(host, cmd) for host in hosts)
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for i, result in enumerate(results, 1):
        if isinstance(result, Exception):
            print('Task %d failed: %s' % (i, str(result)))
        elif result.exit_status != 0:
            print('Task %d exited with status %s:' % (i, result.exit_status))
            print(result.stderr, end='')
        else:
            print('Task %d succeeded:' % i)
            print(result.stdout, end='')

        print(45*'-')
    '''


if __name__ == "__main__":
    # configure logging
    logging.basicConfig(format='%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s', datefmt='%Y/%m/%d %H:%M:%S')
    logger.setLevel(logging.DEBUG)
    logger.debug('the logger is configured')
    asyncssh.set_log_level( logging.WARNING )

    ap = argparse.ArgumentParser( description=__doc__ )
    ap.add_argument('launchedJsonFilePath', default='launched.json')
    ap.add_argument('outJsonFilePath', default='installed.json')
    ap.add_argument('--timeLimit', default=0, type=float, help='maximum time (in seconds) to take (default =unlimited (0))')
    ap.add_argument('--command', default='uname', help='the command to execute')
    args = ap.parse_args()
    logger.info( "args: %s", str(args) )
    
    launchedJsonFilePath = args.launchedJsonFilePath

    timeLimit = args.timeLimit  # seconds

    startTime = time.time()
    deadline = startTime + timeLimit
    
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

       
    resultsLogFilePath = os.path.splitext( os.path.basename( __file__ ) )[0] + '_results.log'
    resultsLogFile = open( resultsLogFilePath, "a", encoding="utf8" )
    print( '[]', datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S'),
        "args:", str(args), file=resultsLogFile )
    
    installed = set()
    failed = set()

    mainTiming = eventTiming('main')
    #allFinished = False


    # this agent stuff does not work
    '''
    agent = asyncssh.connect_agent()
    print( dir(agent) )
    print( agent.get_keys() )
    '''

    #cmd = 'hostname'
    asyncio.get_event_loop().run_until_complete(run_multiple_clients( startedInstances, program, timeLimit=timeLimit ))






    mainTiming.finish()
    eventTimings.append(mainTiming)




    elapsed = time.time() - startTime
    logger.info( 'finished; elapsed time %.1f seconds (%.1f minutes)', elapsed, elapsed/60 )

    #with open( args.outJsonFilePath, 'w') as outFile:
    #    json.dump( list(instancesByIid.values()), outFile, default=str, indent=2, skipkeys=True )

    
    print('\nTiming Summary (durations in minutes)')
    for ev in eventTimings:
        s1 = ev.startDateTime.strftime( '%H:%M:%S' )
        if ev.endDateTime:
            s2 = ev.endDateTime.strftime( '%H:%M:%S' )
        else:
            s2 = s1
        dur = ev.duration().total_seconds() / 60
        print( s1, s2, '%7.1f' % (dur), ev.eventName )
