#!/usr/bin/env python3
''' paramiko some conmands to several workers'''

# standard library modules
import argparse
import datetime
from concurrent import futures
import json
import logging
import os
import subprocess
import sys
import time

import paramiko

from eventTiming import eventTiming


logger = logging.getLogger(__name__)

sshDir = os.path.expanduser('~') + '/.ssh/'
#sshDir = 'C:/Users/michael/Documents/MobaXterm/home/.ssh/'
keyFilePath = sshDir + 'id_rsa'
knownHostsPath = sshDir + 'known_hosts'


targetPort = 45976
defaultUser = 'root'

def anyFound( a, b ):
    ''' return true iff any items from iterable a is found in iterable b '''
    for x in a:
        if x in b:
            return True
    return False

def checkConnectivity( target ):
    # create an SSHClient
    with paramiko.SSHClient() as client:
        
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        client.load_system_host_keys()
        client.load_host_keys( knownHostsPath )
        try:
            client.connect(target['host'], target['port'], username=target['user'],
                       key_filename=keyFilePath )
        except Exception as exc:
            logger.warning( 'got exception (%s) %s', type(exc), exc)
            return exc
        
        # do a shell command and print its output
        stdin,stdout,stderr = client.exec_command("hostname")    
        for line in stdout.readlines():
            print( line.strip() )
        # try to get the command's return code
        status = stdout.channel.recv_exit_status()
        if status != 0:
            logger.error( 'host %s returned exit code %d', target['host'], status )
            for line in stderr.readlines():
                print( line.strip() )
        if False:
            # open an sftp client
            with client.open_sftp() as sftp:
                ls = sftp.listdir( '/' )
                print( ls )
        
        return None

def startRemoteCmd( target, cmd ):
    # create an SSHClient
    client = paramiko.SSHClient()
        
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    client.load_system_host_keys()
    client.load_host_keys( knownHostsPath )
    
    try:
        client.connect(target['host'], target['port'], username=target['user'], key_filename=keyFilePath )
    except Exception as exc:
        logger.warning( 'connect() got exception (%s) %s', type(exc), exc)
        return {'client': None }
    
    cmdPrefix = ''
    fullCmd = cmdPrefix + cmd

    # execute a remote command
    try:
        stdin,stdout,stderr = client.exec_command(fullCmd, get_pty=not True)  # , bufsize=800
    except Exception as exc:
        logger.warning( 'exec() got exception (%s) %s', type(exc), exc)
        return {'client': client }
    return {'client': client, 'stdin': stdin, 'stdout': stdout, 'stderr': stderr }

def startRemoteCmd1( args ):
    '''a wrapper for startRemoteCmd that takes its args a s a single dict'''
    return startRemoteCmd( args['target'], args['cmd'] )

    
def startInstaller( targetHost, program, taskIndex ):
    cmd = program

    logger.info( 'executing %s', cmd )
    return startRemoteCmd( targetHost, cmd )

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


if __name__ == "__main__":
    # configure logging
    logging.basicConfig(format='%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s', datefmt='%Y/%m/%d %H:%M:%S')
    logger.setLevel(logging.DEBUG)
    logger.debug('the logger is configured')

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

    instancesByIid = {}
    for inst in loadedInstances:
        iid = inst['instanceId']
        instancesByIid[iid] = inst

    remoteHosts = extractSshTargets( loadedInstances )
 
    sessions = {}
    eventTimings = []

    if False:
        # check connectivity to all remoteHosts
        #logger.info( 'checking connectivity for %s', remoteHosts )
        connTiming = eventTiming('checkConnectivity')
        for remoteHost in remoteHosts:
            logger.info( 'checking connectivity for %s', remoteHost['instanceId'][0:8] )
            checkConnectivity( remoteHost )
        connTiming.finish()
        eventTimings.append(connTiming)

    program = '/bin/bash --login -c "%s"' % args.command
    #program = args.command

    starterTiming = eventTiming('startInstaller')
    threading = False
    # start all the installers
    if threading:
        argList = []
        for remoteHost in remoteHosts:
            argList.append( {'target': remoteHost, 'cmd': program })
        nWorkers = 2
        with futures.ThreadPoolExecutor( max_workers=nWorkers ) as executor:
            parIter = executor.map( startRemoteCmd1, argList )  
            parResultList = list( parIter )
        #print( 'parResultList', parResultList)
        for rh in range( 0, len( remoteHosts ) ):
            remoteHost = remoteHosts[ rh ]
            iid = remoteHost['instanceId']
            #logger.info( 'starting installer on %s', iid[0:8] )
            session = parResultList[rh]
            session['finished'] = 'stdout' not in session
            sessions[ remoteHost['instanceId'] ] = session
            instancesByIid[iid]['commandState'] = 'pending'
        #sys.exit( 'DEBUGGING' )
    else:
        for rh in range( 0, len( remoteHosts ) ):
            remoteHost = remoteHosts[ rh ]
            iid = remoteHost['instanceId']
            logger.info( 'starting installer on %s', iid[0:8] )
            session = startInstaller( remoteHost, program, rh )
            session['finished'] = 'stdout' not in session
            sessions[ remoteHost['instanceId'] ] = session
            instancesByIid[iid]['commandState'] = 'pending'
            if rh == 0:
                time.sleep( 1 )
    starterTiming.finish()
    eventTimings.append(starterTiming)

    # find unreachable instances
    unreachables = []
    for remoteHost in remoteHosts:
        iid = remoteHost['instanceId']
        #logger.info( 'checking installer on %s', remoteHost )
        session = sessions[ iid ]
        if 'stdout' not in session:
            logger.info( '<unreachable> instance %s', iid )
            unreachables.append(remoteHost)
            instancesByIid[iid]['commandState'] = 'unreachable'
    logger.info( 'counted %d unreachable', len(unreachables))
    if len(unreachables) >= len(remoteHosts):
        sys.exit( 'ALL HOSTS UNREACHABLE' )


    #for session in sessions.values():
    #    print (session)
       
    resultsLogFilePath = os.path.splitext( os.path.basename( __file__ ) )[0] + '_results.log'
    resultsLogFile = open( resultsLogFilePath, "a", encoding="utf8" )
    print( '[]', datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S'),
        "args:", str(args), file=resultsLogFile )
    
    installed = set()
    failed = set()

    mainTiming = eventTiming('main')
    allFinished = False
    try:
        while not allFinished:
            if timeLimit and (time.time() >= deadline):
                logger.warning( 'total time exceeded time limit of %d seconds', timeLimit)
                break
            resultsLogFile.flush()
            time.sleep( 2 )
            nGood = 0
            nFinished = 0
            for remoteHost in remoteHosts:
                iid = remoteHost['instanceId']
                #logger.info( 'checking installer on %s', remoteHost )
                session = sessions[ iid ]
                if 'stdout' not in session:
                    continue
                stdout = session['stdout']
                stderr = session['stderr']
                
                hostTag = '[%s]' % remoteHost['instanceId'][0:8]
                # drain the stdout
                while stdout.channel.recv_ready():
                    line = stdout.readline()
                    print( hostTag, line.strip() )
                    #if 'test set accuracy' in line or 'test accuracy' in line:
                    if anyFound( ['warning', 'error'], line.lower() ):
                        print( hostTag, line.strip(), file=resultsLogFile )

                # drain the stderr
                while stderr.channel.recv_ready():
                    line = stderr.readline()
                    print( hostTag, '<stderr>', line.strip() )
                    print( hostTag, '<stderr>', line.strip(), file=resultsLogFile )

                wasFinished = session['finished']
                finished = stdout.channel.exit_status_ready()
                session['finished'] = finished
                if finished and not wasFinished:
                    nFinished += 1
                    for line in stderr.readlines():
                        print( hostTag, '<stderr>', line.strip() )
                        print( hostTag, '<stderr>', line.strip(), file=resultsLogFile )
                    for line in stdout.readlines():
                        print( hostTag, line.strip() )
                        #if 'test set accuracy' in line or 'test accuracy' in line:
                        if anyFound( ['warning', 'error'], line.lower() ):
                            print( hostTag, line.strip(), file=resultsLogFile )
                    status = stdout.channel.recv_exit_status()
                    if status == 0:
                        nGood += 1
                        logger.info( 'host %s GOOD exit code', hostTag )
                        installed.add( iid )
                        instancesByIid[iid]['commandState'] = 'good'
                    if status != 0:
                        logger.error( 'host %s returned exit code %d', hostTag, status )
                        failed.add( iid )
                        instancesByIid[iid]['commandState'] = 'failed'
                        for line in stderr.readlines():
                            # is this redundant (already done) or still necessary?
                            print( hostTag, '<stderr>', line.strip() )
                            print( hostTag, '<stderr>', line.strip(), file=resultsLogFile )
                finished = stdout.channel.exit_status_ready()
                session['finished'] = finished
                #print( [session['finished'] for session in sessions.values()] )
                allFinished = all( [session['finished'] for session in sessions.values()] )
            logger.info( '%d finished, %d failed, %d unreachable, %d total; elapsed time %.1f min',
                len(installed), len(failed), len(unreachables), len(remoteHosts), (time.time()-startTime)/60 )
    except KeyboardInterrupt:
                logger.info( 'caught SIGINT (ctrl-c), skipping ahead' )

    mainTiming.finish()
    eventTimings.append(mainTiming)
    # close the ssh sessions
    for session in sessions.values():
        #print (session)
        if session['client']:
            session['client'].close()

    elapsed = time.time() - startTime
    logger.info( 'finished; elapsed time %.1f seconds (%.1f minutes)', elapsed, elapsed/60 )

    with open( args.outJsonFilePath, 'w') as outFile:
        json.dump( list(instancesByIid.values()), outFile, default=str, indent=2, skipkeys=True )

    if failed:
        for inst in failed:
            logger.warning( 'failed on instance %s', inst )
            
    logger.info( 'counted %d unreachable', len(unreachables))
    for remoteHost in unreachables:
        logger.info( 'unreachable %s %s', remoteHost['instanceId'], remoteHost['host'] )
    
    print('\nTiming Summary (durations in minutes)')
    for ev in eventTimings:
        s1 = ev.startDateTime.strftime( '%H:%M:%S' )
        if ev.endDateTime:
            s2 = ev.endDateTime.strftime( '%H:%M:%S' )
        else:
            s2 = s1
        dur = ev.duration().total_seconds() / 60
        print( s1, s2, '%7.1f' % (dur), ev.eventName )
