#!/usr/bin/env python3
"""
does ping-based telemetry test using NCS instances
"""

# standard library modules
import argparse
import contextlib
import getpass
import json
import logging
import os
import socket
import signal
import subprocess
import sys
import threading
import time
import uuid

# third-party module(s)
import requests

# neocortix modules
try:
    import ncs
except ImportError:
    # set system and python paths for default places, since path seems to be not set properly
    ncscliPath = os.path.expanduser('~/ncscli/ncscli')
    sys.path.append( ncscliPath )
    os.environ["PATH"] += os.pathsep + ncscliPath
    import ncs
import pingFromInstances


logger = logging.getLogger(__name__)


# possible place for globals is this class's attributes
class g_:
    signaled = False

class SigTerm(BaseException):
    pass

def sigtermHandler( sig, frame ):
    g_.signaled = True
    logger.warning( 'SIGTERM received; will try to shut down gracefully' )
    #raise SigTerm()

def sigtermSignaled():
    return g_.signaled


def boolArg( v ):
    '''use with ArgumentParser add_argument for (case-insensitive) boolean arg'''
    if v.lower() == 'true':
        return True
    elif v.lower() == 'false':
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def loadSshPubKey():
    pubKeyFilePath = os.path.expanduser( '~/.ssh/id_rsa.pub' )
    with open( pubKeyFilePath ) as inFile:
        contents = inFile.read()
    return contents

def launchInstances( authToken, launchedJsonFilePath, nInstances, sshClientKeyName,
    filtersJson=None, encryptFiles=True ):
    returnCode = 13
    # call ncs launch via command-line
    #filtersArg = "--filter '" + filtersJson + "'" if filtersJson else " "
    #cmd = 'ncs.py sc --authToken %s launch --count %d %s --sshClientKeyName %s --json > launched.json' % \
    #    (authToken, nInstances, filtersArg, sshClientKeyName )

    cmd = [
        'ncs.py', 'sc', '--authToken', authToken, 'launch',
        '--encryptFiles', str(encryptFiles),
        '--count', str(nInstances), # filtersArg,
        '--sshClientKeyName', sshClientKeyName, '--json'
    ]
    if filtersJson:
        cmd.extend( ['--filter',  filtersJson] )
    #logger.debug( 'cmd: %s', cmd )
    try:
        outFile = open(launchedJsonFilePath,'w' )
        #proc = subprocess.Popen( cmd, shell=True )
        proc = subprocess.Popen( cmd, stdout=outFile )
        while True:
            #logger.debug( 'polling ncs')
            proc.poll() # sets proc.returncode
            if proc.returncode != None:
                break
            if sigtermSignaled():
                logger.info( 'signaling ncs')
                proc.send_signal( signal.SIGTERM )
                try:
                    logger.info( 'waiting ncs')
                    proc.wait(timeout=60)
                    if proc.returncode:
                        logger.warning( 'ncs return code %d', proc.returncode )
                except subprocess.TimeoutExpired:
                    logger.warning( 'ncs launch did not terminate in time' )
            time.sleep( 1 )
        returnCode = proc.returncode
        if outFile:
            outFile.close()
    except Exception as exc: 
        logger.error( 'exception while launching instances (%s) %s', type(exc), exc, exc_info=True )
        returnCode = 99
    return returnCode

def getStartedInstances( instanceJsonFilePath ):
    try:
        with open( instanceJsonFilePath, 'r' ) as jsonFile:
            loadedInstances = json.load(jsonFile)  # a list of dicts
    except Exception as exc:
        logger.warning( 'could not load json (%s) %s', type(exc), exc )
        return []
    startedInstances = [inst for inst in loadedInstances if inst['state'] == 'started' ]
    return startedInstances

def terminateThese( authToken, inRecs ):
    logger.info( 'to terminate %d instances', len(inRecs) )
    iids = [inRec['instanceId'] for inRec in inRecs]
    ncs.terminateInstances( authToken, iids )

def canPing( targetHost, timeLimit=15 ):
    '''ping the target and return true iff successful'''
    # does a ping to the targetHost, just to prove that it can
    proc = subprocess.run( 
        ['ping', str(targetHost), '-c', '1', '-w', str(timeLimit), '-q'],
        stdout=subprocess.DEVNULL
        )
    return proc.returncode == 0



if __name__ == "__main__":
    # configure logger formatting
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)
    ncs.logger.setLevel(logging.INFO)
    pingFromInstances.logger.setLevel(logging.INFO)
    logger.setLevel(logging.INFO)
    logger.debug('the logger is configured')

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@', formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    ap.add_argument( '--authToken', required=True, help='the NCS authorization token to use' )
    ap.add_argument( '--targetHost', help='hostname or ip addr to ping' )
    ap.add_argument( '--filter', help='json to filter instances for launch' )
    ap.add_argument( '--launch', type=boolArg, default=True, help='to launch and terminate instances' )
    ap.add_argument( '--nWorkers', type=int, default=1, help='the # of worker instances to launch (or zero for all available)' )
    ap.add_argument( '--sshAgent', type=boolArg, default=False, help='whether or not to use ssh agent' )
    ap.add_argument( '--sshClientKeyName', help='the name of the uploaded ssh client key to use (default is random)' )
    ap.add_argument( '--testId', help='to identify this test' )
 
    ap.add_argument('--nPings', type=int, default=10, help='# of ping packets to send per instance')
    ap.add_argument('--interval', type=float, default=1, help='time (in seconds) between pings by an instance')
    ap.add_argument('--timeLimit', type=float, default=10, help='maximum time (in seconds) to take per instance' )
    ap.add_argument('--extraTime', type=float, help='extra time (in seconds) for master to wait for results')
    ap.add_argument('--fullDetails', type=boolArg, default=False, help='true for full details, false for summaries only')
    args = ap.parse_args()
    #logger.debug( 'args %s', args )

    signal.signal( signal.SIGTERM, sigtermHandler )

    #logger.info( '--filter arg <%s>', args.filter )

    dataDirPath = 'data'
    wwwDirPath = 'www'
    launchedJsonFilePath = dataDirPath+'/launched.json'
    launchWanted = args.launch


    os.makedirs( dataDirPath, exist_ok=True )

    # check whether the targetHost is available  # TODO make canPing work
    try:
        worked = canPing( args.targetHost )
        if not worked:
            logger.error( 'could not ping target host %s',
                args.targetHost )
            sys.exit(1)
    except Exception as exc:
        logger.warning( 'could not access target host %s',args.targetHost )
        logger.error( 'got exception %s', exc )
        sys.exit(1)

    nWorkersWanted = args.nWorkers
    if launchWanted:
        # overwrite the launchedJson file as empty list, so we won't have problems with stale contents
        with open( launchedJsonFilePath, 'w' ) as outFile:
            json.dump( [], outFile )
    try:
        if launchWanted:
            nAvail = ncs.getAvailableDeviceCount( args.authToken, filtersJson=args.filter )
            if nWorkersWanted > (nAvail + 5):
                logger.error( 'not enough devices available (%d requested)', nWorkersWanted )
                sys.exit(1)
            if nWorkersWanted == 0:
                logger.info( '%d devices available to launch', nAvail )
                nWorkersWanted = nAvail
            if args.sshClientKeyName:
                sshClientKeyName = args.sshClientKeyName
            else:
                keyContents = loadSshPubKey()
                randomPart = str( uuid.uuid4() )[0:13]
                keyContents += ' #' + randomPart
                sshClientKeyName = 'pingtest_%s' % (randomPart)
                respCode = ncs.uploadSshClientKey( args.authToken, sshClientKeyName, keyContents )
                if respCode < 200 or respCode >= 300:
                    logger.warning( 'ncs.uploadSshClientKey returned %s', respCode )
                    sys.exit( 'could not upload SSH client key')

            rc = launchInstances( args.authToken, launchedJsonFilePath, nWorkersWanted, sshClientKeyName, filtersJson=args.filter )
            # delete sshClientKey only if we just uploaded it
            if sshClientKeyName != args.sshClientKeyName:
                logger.info( 'deleting sshClientKey %s', sshClientKeyName)
                ncs.deleteSshClientKey( args.authToken, sshClientKeyName )
            if rc:
                logger.warning( 'launchInstances returned %d', rc )
                sys.exit( 'could not launch instances')
        # find out if any instances were started
        startedInstances = getStartedInstances( launchedJsonFilePath )  # a list of instance dicts
        # do the actual work, unless we shouldn't
        if startedInstances and not sigtermSignaled():
            pingFromInstances.pingFromInstances( launchedJsonFilePath, dataDirPath, wwwDirPath, args.targetHost, 
                args.nPings, args.interval, args.timeLimit, args.extraTime,
                args.fullDetails, args.sshAgent
                )


    except KeyboardInterrupt:
        logger.warning( '(ctrl-c) received, will shutdown gracefully' )
    except SigTerm:
        logger.warning( 'SIGTERM received, will shutdown gracefully' )
    if launchWanted:
        # get instances from json file, to see which ones to terminate
        launchedInstances = []
        with open( launchedJsonFilePath, 'r') as jsonInFile:
            try:
                launchedInstances = json.load(jsonInFile)  # an array
            except Exception as exc:
                logger.warning( 'could not load json (%s) %s', type(exc), exc )
        if len( launchedInstances ):
            jobId = launchedInstances[0].get('job')
            if jobId:
                logger.info( 'calling terminateJobInstances for job "%s"', jobId )
                ncs.terminateJobInstances( args.authToken, jobId )
            else:
                terminateThese( args.authToken, launchedInstances )
            # purgeKnownHosts works well only when known_hosts is not hashed
            cmd='purgeKnownHosts.py %s > /dev/null' % launchedJsonFilePath
            try:
                subprocess.check_call( cmd, shell=True, stderr=subprocess.DEVNULL )
            except Exception as exc:
                logger.error( 'purgeKnownHosts threw exception (%s) %s',type(exc), exc )

    logger.info( 'finished')
    sys.exit(0)
