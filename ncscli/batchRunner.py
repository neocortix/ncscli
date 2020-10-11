#!/usr/bin/env python3
"""
uses instances to compute frames in "batch" mode
"""
# pylint: disable=maybe-no-member

# standard library modules
import argparse
import asyncio
import collections
#import contextlib
from concurrent import futures
import errno
import datetime
#import getpass
import json
import logging
#import math
import os
import re
#import socket
import shutil
import signal
import subprocess
import sys
import threading
import time
import types
import uuid

# third-party module(s)
import requests

# neocortix modules
from . import ncs
from . import jsonToKnownHosts
from . import purgeKnownHosts
from . import tellInstances

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


#global resultsLogFile

# possible place for globals is this class's attributes
class g_:
    signaled = False
    frameDetails = {}
    dataDirPath = None
    frameProcessor = None
    framesToDo = collections.deque()
    nFramesWanted = None
    limitOneFramePerWorker = False
    framesFinished = collections.deque()
    installerLogFile = None
    resultsLogFile = None
    resultsLogFilePath = None
    progressFilePath = None
    deadline = None
    interrupted = False
    workingInstances = collections.deque()
    progressFileLock = threading.Lock()


class frameProcessor(object):
    '''defines details for processing frames of a batch job'''
    # def __init__(self): # probably not needed
    
    def installerCmd( self ):
        return None
        #return 'echo noInstall'

    def frameOutFileName( self, frameNum ):
        return 'frame_%d.out' % (frameNum)

    def frameCmd( self, frameNum ):
        return 'hostname > %s' % (self.frameOutFileName(frameNum))

g_.frameProcessor = frameProcessor()

def getInstallerCmd():
    return g_.frameProcessor.installerCmd()

def getFrameOutFileName( frameNum ):
    return g_.frameProcessor.frameOutFileName( frameNum )

def getFrameCmd( frameNum ):
    return g_.frameProcessor.frameCmd( frameNum )

class SigTerm(BaseException):
    #logger.warning( 'unsupported SigTerm exception created')
    pass

def sigtermHandler( sig, frame ):
    g_.signaled = True
    logger.warning( 'SIGTERM received; will try to shut down gracefully' )
    #raise SigTerm()

def sigtermSignaled():
    return g_.signaled

def sigtermNotSignaled():
    return not sigtermSignaled()

def logResult( key, value, instanceId ):
    if g_.resultsLogFile:
        toLog = {key: value, 'instanceId': instanceId,
            'dateTime': datetime.datetime.now(datetime.timezone.utc).isoformat() }
        print( json.dumps( toLog, sort_keys=True ), file=g_.resultsLogFile )
        g_.resultsLogFile.flush()

def logEvent( eventType, argv, instanceId ):
    if g_.resultsLogFile:
        toLog = {
            'dateTime': datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'instanceId': instanceId, 
            'type': eventType,
            'args': argv
        }
        print( json.dumps( toLog, sort_keys=True ), file=g_.resultsLogFile )
        g_.resultsLogFile.flush()

def logStderr( text, instanceId ):
    logEvent( 'stderr', text, instanceId )

def logStdout( text, instanceId ):
    logEvent( 'stdout', text, instanceId )

def logFrameState( frameNum, state, instanceId, rc=0 ):
    if g_.resultsLogFile:
        toLog = {
            'dateTime': datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'instanceId': instanceId, 
            'type': 'frameState',
            'args': {
                'rc': rc,
                'frameNum': frameNum, 
                'state':state
            }
        }
        '''
        toLog = {'frameNum': frameNum, 'frameState':state,
            'instanceId': instanceId, 'rc': rc,
            'dateTime': datetime.datetime.now(datetime.timezone.utc).isoformat() }
        '''
        print( json.dumps( toLog, sort_keys=True ), file=g_.resultsLogFile )
        g_.resultsLogFile.flush()

def logOperation( op, value, instanceId ):
    if g_.resultsLogFile:
        toLog = {
            'dateTime': datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'instanceId': instanceId,
            'type': 'operation',
            'args': {op: value}
            }
        print( json.dumps( toLog, sort_keys=True ), file=g_.resultsLogFile )
        g_.resultsLogFile.flush()

def logInstallerEvent( key, value, instanceId ):
    logger.debug( 'logging %s', locals() )
    if g_.installerLogFile:
        toLog = {key: value, 'instanceId': instanceId,
            'dateTime': datetime.datetime.now(datetime.timezone.utc).isoformat() }
        print( json.dumps( toLog, sort_keys=True ), file=g_.installerLogFile )
        g_.installerLogFile.flush()

def logInstallerOperation( instanceId, opArgs ):
    # opArgs is a list containing the name of the op and its parameters
    logInstallerEvent( 'operation', opArgs, instanceId )


def boolArg( v ):
    '''use with ArgumentParser add_argument for (case-insensitive) boolean arg'''
    if v.lower() == 'true':
        return True
    elif v.lower() == 'false':
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def loadSshPubKey():
    '''returns the contents of current user public ssh client key'''
    pubKeyFilePath = os.path.expanduser( '~/.ssh/id_rsa.pub' )
    with open( pubKeyFilePath ) as inFile:
        contents = inFile.read()
    return contents

def purgeHostKeys( instanceRecs ):
    '''try to purgeKnownHosts; warn if any exception'''
    logger.debug( 'purgeKnownHosts for %d instances', len(instanceRecs) )
    try:
        purgeKnownHosts.purgeKnownHosts( instanceRecs )
    except Exception as exc: 
        logger.warning( 'exception from purgeKnownHosts (%s) %s', type(exc), exc, exc_info=True )
        return 1
    else:
        return 0

def logLaunches( launchedJsonFilePath, launcherLogFilePath, launchDateTime ):
    ''' append all the launched instance ids to a csv log file with timestamps
        this is intended to enable later cleanup of potentially leaked instances'''
    launchedInstances = []
    # get instances from the launched json file
    with open( launchedJsonFilePath, 'r') as jsonInFile:
        try:
            launchedInstances = json.load(jsonInFile)  # an array
        except Exception as exc:
            logger.warning( 'could not load json (%s) %s', type(exc), exc )
    if launchedInstances:
        try:
            dateTimeStr = launchDateTime.isoformat()
            with open( launcherLogFilePath, 'a') as launcherLogFile:
                for inst in launchedInstances:
                    iid = inst['instanceId']
                    state = inst.get('state', '<unknown>')
                    print( dateTimeStr, iid, state, sep=',', file=launcherLogFile )

        except Exception as exc:
            logger.warning( 'got exception (%s) appending to launcherLogFile %s',
                type(exc), launcherLogFilePath )

def launchInstances( authToken, nInstances, sshClientKeyName, launchedJsonFilepath,
        filtersJson=None, encryptFiles=True ):
    if time.time() >= g_.deadline:
        logger.warning( 'not launching, because global deadline has passed' )
        return 124
    returnCode = 13
    launchDateTime = datetime.datetime.now( datetime.timezone.utc )
    logger.debug( 'launchedJsonFilepath %s', launchedJsonFilepath )
    try:
        with open( launchedJsonFilepath, 'w' ) as launchedJsonFile:
            returnCode = ncs.launchScInstances( authToken, encryptFiles, numReq=int(nInstances),
                sshClientKeyName=sshClientKeyName, jsonFilter=filtersJson,
                okToContinueFunc=sigtermNotSignaled, jsonOutFile=launchedJsonFile )
    except Exception as exc: 
        logger.error( 'exception while launching instances (%s) %s', type(exc), exc, exc_info=True )
        returnCode = 99
    launcherLogFilePath = os.path.join( g_.dataDirPath, 'launchedInstances.csv' )
    logLaunches( launchedJsonFilepath, launcherLogFilePath, launchDateTime )
    return returnCode

def terminateInstances( authToken, instanceIds ):
    '''try to terminate instances; return list of instances terminated (empty if none confirmed)'''
    #instanceIds = [inst['instanceId'] for inst in instanceRecs]   
    logger.debug( 'terminating %d instances', len(instanceIds) )
    terminationLogFilePath = os.path.join( g_.dataDirPath, 'badTerminations.csv' )
    dateTimeStr = datetime.datetime.now( datetime.timezone.utc ).isoformat()
    try:
        ncs.terminateInstances( authToken, instanceIds )
        logger.debug( 'terminateInstances returned' )
    except Exception as exc:
        logger.warning( 'got exception terminating %d instances (%s) %s', 
            len( instanceIds ), type(exc), exc )
        try:
            with open( terminationLogFilePath, 'a' ) as logFile:
                for iid in instanceIds:
                    print( dateTimeStr, iid, sep=',', file=logFile )
        except Exception as exc:
            logger.warning( 'got exception (%s) appending to terminationLogFile %s',
                type(exc), terminationLogFilePath )
        return []  # empty list meaning none may have been terminated
    else:
        return instanceIds

def recruitInstance( launchedJsonFilePath, resultsLogFilePathIgnored ):
    logger.info( 'recruiting 1 instance' )
    nWorkersWanted = 1
    # prepare sshClientKey for launch
    if args.sshClientKeyName:
        sshClientKeyName = args.sshClientKeyName
    else:
        keyContents = loadSshPubKey().strip()
        randomPart = str( uuid.uuid4() )[0:13]
        #keyContents += ' #' + randomPart
        sshClientKeyName = 'batchRunner_%s' % (randomPart)
        respCode = ncs.uploadSshClientKey( args.authToken, sshClientKeyName, keyContents )
        if respCode < 200 or respCode >= 300:
            logger.warning( 'ncs.uploadSshClientKey returned %s', respCode )
            raise Exception( 'could not upload SSH client key')
    #launch
    logOperation( 'launchInstances', 1, '<recruitInstances>' )
    rc = launchInstances( args.authToken, 1,
        sshClientKeyName, launchedJsonFilePath, filtersJson=args.filter,
        encryptFiles = args.encryptFiles
        )
    if rc:
        logger.debug( 'launchInstances returned %d', rc )
    # delete sshClientKey only if we just uploaded it
    if sshClientKeyName != args.sshClientKeyName:
        logger.debug( 'deleting sshClientKey %s', sshClientKeyName)
        ncs.deleteSshClientKey( args.authToken, sshClientKeyName )
    if rc:
        return None
    launchedInstances = []
    # get instances from the launched json file
    with open( launchedJsonFilePath, 'r') as jsonInFile:
        try:
            launchedInstances = json.load(jsonInFile)  # an array
        except Exception as exc:
            logger.warning( 'could not load json (%s) %s', type(exc), exc )
    if len( launchedInstances ) < nWorkersWanted:
        logger.warning( 'could not launch as many instances as wanted (%d vs %d)',
            len( launchedInstances ), nWorkersWanted )
    nonstartedIids = [inst['instanceId'] for inst in launchedInstances if inst['state'] != 'started' ]
    if nonstartedIids:
        logger.warning( 'terminating non-started instances %s', nonstartedIids )
        terminateInstances( args.authToken, nonstartedIids )
    # proceed with instances that were actually started
    startedInstances = [inst for inst in launchedInstances if inst['state'] == 'started' ]
    if len(startedInstances) != 1:
        logger.warning( 'launched %d instances', len(startedInstances) )
        return None

    inst = startedInstances[0]
    iid = inst['instanceId']
    abbrevIid = iid[0:16]
    def trackStderr( proc ):
        for line in proc.stderr:
            print( '<stderr>', abbrevIid, line.strip(), file=sys.stderr )
            logInstallerEvent( 'stderr', line.strip(), iid )

    if sigtermSignaled():
        logger.warning( 'terminating instance because sigtermSignaled %s', iid )
        logOperation( 'terminateFinal', [iid], '<master>' )
        terminateInstances( args.authToken, [iid] )
        return None
    else:
        # add instance to knownHosts
        with open( os.path.expanduser('~/.ssh/known_hosts'), 'a' ) as khFile:
            jsonToKnownHosts.jsonToKnownHosts( startedInstances, khFile )

        deadline = min( g_.deadline, time.time() + args.instTimeLimit )
        # rsync the common input file, if any
        if args.commonInFilePath:
            logFrameState( -1, 'rsyncing', iid, 0 )
            destFileName = os.path.basename( args.commonInFilePath )
            (rc, stderr) = rsyncToRemote( args.commonInFilePath, destFileName, inst, timeLimit=args.instTimeLimit )
            if rc == 0:
                logFrameState( -1, 'rsynced', iid )
            else:
                logStderr( stderr.rstrip(), iid )
                logFrameState( -1, 'rsyncFailed', iid, rc )
                logger.warning( 'rc from rsync was %d', rc )
                terminateInstances( args.authToken, [iid] )
                #g_.workingInstances.remove( iid )
                #saveProgress()
                logger.warning( 'terminating instance because rsync %s', iid )
                terminateInstances( args.authToken, [iid] )
                logOperation( 'terminateBad', [iid], '<recruitInstances>' )
                purgeHostKeys( [inst] )
                return None


        # install something on startedInstance, if required, else just return inst
        installerCmd = getInstallerCmd()
        if not installerCmd:
            return inst
        logger.info( 'installerCmd %s', installerCmd )
        sshSpecs = inst['ssh']
        logInstallerOperation( iid, ['connect', sshSpecs['host'], sshSpecs['port']] )
        with subprocess.Popen(['ssh',
                        '-p', str(sshSpecs['port']),
                        '-o', 'ServerAliveInterval=360',
                        '-o', 'ServerAliveCountMax=3',
                        sshSpecs['user'] + '@' + sshSpecs['host'], installerCmd],
                        encoding='utf8',
                        #stdout=subprocess.PIPE,  # subprocess.PIPE subprocess.DEVNULL
                        stderr=subprocess.PIPE) as proc:
            logInstallerOperation( iid, ['command', installerCmd] )
            stderrThr = threading.Thread(target=trackStderr, args=(proc,))
            stderrThr.start()
            while time.time() < deadline:
                proc.poll() # sets proc.returncode
                if proc.returncode == None:
                    logger.info( 'waiting for install')
                else:
                    if proc.returncode == 0:
                        logger.info( 'installer succeeded on instance %s', abbrevIid )
                    else:
                        logger.warning( 'instance %s gave returnCode %d', abbrevIid, proc.returncode )
                    break
                if sigtermSignaled():
                    break
                if g_.interrupted:
                    break
                if (g_.nFramesWanted - len(g_.framesFinished)) <= 0:
                    break
                time.sleep(30)
            proc.poll()
            returnCode = proc.returncode if proc.returncode != None else 124 # declare timeout if no rc
            if returnCode:
                logger.warning( 'installer returnCode %s', returnCode )
            if returnCode == 124:
                logInstallerEvent( 'timeout', args.instTimeLimit, iid )
            else:
                logInstallerEvent('returncode', returnCode, iid )
            proc.terminate()
            try:
                proc.wait(timeout=5)
                if proc.returncode:
                    logger.warning( 'ssh return code %d', proc.returncode )
            except subprocess.TimeoutExpired:
                logger.warning( 'ssh did not terminate in time' )
            stderrThr.join()
            if returnCode:
                logger.warning( 'terminating instance because installerFailed %s', iid )
                terminateInstances( args.authToken, [iid] )
                logOperation( 'terminateBad', [iid], '<recruitInstances>' )
                purgeHostKeys( [inst] )
                return None
            else:
                return inst
    return None

def triage( statuses ):
    ''' separates good tellInstances statuses from bad ones'''
    goodOnes = []
    badOnes = []

    for status in statuses:
        if isinstance( status['status'], int) and status['status'] == 0:
            goodOnes.append( status['instanceId'])
        else:
            badOnes.append( status )
    return (goodOnes, badOnes)

def recruitInstances( nWorkersWanted, launchedJsonFilePath, launchWanted, resultsLogFilePath ):
    '''launch instances and install prereqs on them;
        terminate those that could not install; return list of good instances'''
    #if not g_.resultsLogFilePath:
    #    resultsLogFilePath = g_.dataDirPath+'/recruitInstances.jlog'
    goodInstances = []
    rc = None
    launchedInstances = None
    sshClientKeyName = args.sshClientKeyName
    try:
        if launchWanted:
            logger.info( 'recruiting %d instances', nWorkersWanted )
            nAvail = ncs.getAvailableDeviceCount( args.authToken, filtersJson=args.filter )
            if nWorkersWanted > (nAvail + 0):
                logger.error( 'not enough devices available (%d requested, %d avail)', nWorkersWanted, nAvail )
                raise ValueError( 'not enough devices available')
            # prepare sshClientKey for launch
            if args.sshClientKeyName:
                sshClientKeyName = args.sshClientKeyName
            else:
                keyContents = loadSshPubKey().strip()
                randomPart = str( uuid.uuid4() )[0:13]
                #keyContents += ' #' + randomPart
                sshClientKeyName = 'batchRunner_%s' % (randomPart)
                respCode = ncs.uploadSshClientKey( args.authToken, sshClientKeyName, keyContents )
                if respCode < 200 or respCode >= 300:
                    logger.warning( 'ncs.uploadSshClientKey returned %s', respCode )
                    return []
            #launch
            #logResult( 'operation', {'launchInstances': nWorkersWanted}, '<recruitInstances>' )
            logOperation( 'launchInstances', nWorkersWanted, '<recruitInstances>' )
            rc = launchInstances( args.authToken, nWorkersWanted,
                sshClientKeyName, launchedJsonFilePath, filtersJson=args.filter,
                encryptFiles = args.encryptFiles
                )
            if rc:
                logger.debug( 'launchInstances returned %d', rc )
            # delete sshClientKey only if we just uploaded it
            if sshClientKeyName != args.sshClientKeyName:
                logger.debug( 'deleting sshClientKey %s', sshClientKeyName)
                time.sleep(10)
                ncs.deleteSshClientKey( args.authToken, sshClientKeyName )
            if rc:
                return []
        launchedInstances = []
        # get instances from the launched json file
        with open( launchedJsonFilePath, 'r') as jsonInFile:
            try:
                launchedInstances = json.load(jsonInFile)  # an array
            except Exception as exc:
                logger.warning( 'could not load json (%s) %s', type(exc), exc )
        if len( launchedInstances ) < nWorkersWanted:
            logger.warning( 'could not launch as many instances as wanted (%d vs %d)',
                len( launchedInstances ), nWorkersWanted )
        nonstartedIids = [inst['instanceId'] for inst in launchedInstances if inst['state'] != 'started' ]
        if nonstartedIids:
            logger.warning( 'terminating non-started instances %s', nonstartedIids )
            terminateInstances( args.authToken, nonstartedIids )
        # proceed with instances that were actually started
        startedInstances = [inst for inst in launchedInstances if inst['state'] == 'started' ]
        # add instances to knownHosts
        with open( os.path.expanduser('~/.ssh/known_hosts'), 'a' ) as khFile:
            jsonToKnownHosts.jsonToKnownHosts( startedInstances, khFile )
        # install something on startedInstances, if required, else just return startedInstances
        if (not getInstallerCmd()) and (not args.commonInFilePath):
            return startedInstances
        if not sigtermSignaled():
            installerCmd = getInstallerCmd()
            logger.info( 'calling tellInstances to install on %d instances', len(startedInstances))
            stepStatuses = tellInstances.tellInstances( startedInstances, installerCmd,
                resultsLogFilePath=resultsLogFilePath,
                download=None, downloadDestDir=None, jsonOut=None, sshAgent=args.sshAgent,
                timeLimit=min(args.instTimeLimit, args.timeLimit), upload=args.commonInFilePath, stopOnSigterm=not True,
                knownHostsOnly=True
                )
            # SHOULD restore our handler because tellInstances may have overridden it
            #signal.signal( signal.SIGTERM, sigtermHandler )
            if not stepStatuses:
                logger.warning( 'no statuses returned from installer')
                startedIids = [inst['instanceId'] for inst in startedInstances]
                logOperation( 'terminateBad', startedIids, '<recruitInstances>' )
                terminateInstances( args.authToken, startedIids )
                return []
            (goodOnes, badOnes) = triage( stepStatuses )
            #stepTiming.finish()
            #eventTimings.append(stepTiming)
            logger.info( '%d good installs, %d bad installs', len(goodOnes), len(badOnes) )
            logger.info( 'stepStatuses %s', stepStatuses )
            goodInstances = [inst for inst in startedInstances if inst['instanceId'] in goodOnes ]
            badIids = []
            for status in badOnes:
                badIids.append( status['instanceId'] )
            if badIids:
                logOperation( 'terminateBad', badIids, '<recruitInstances>' )
                terminateInstances( args.authToken, badIids )
                badInstances = [inst for inst in startedInstances if inst['instanceId'] in badIids ]
                logger.info( 'purging host keys')
                purgeHostKeys( badInstances )
            #if goodInstances:
            #    recycleInstances( goodInstances )
        return goodInstances
    except KeyboardInterrupt:
        logger.warning( 'recruitInstances was interrupted' )
        if rc == 0:
            # read back launchedInstances, if possible and not already done
            if (not launchedInstances) and os.path.isfile( launchedJsonFilePath ):
                with open( launchedJsonFilePath, 'r') as jsonInFile:
                    try:
                        launchedInstances = json.load(jsonInFile)  # an array
                    except Exception as exc:
                        logger.warning( 'could not load json (%s) %s', type(exc), exc )
            # terminate all instances, if any launched
            if launchedInstances:
                jobId = launchedInstances[0].get('job')
                if jobId:
                    ncs.terminateJobInstances( args.authToken, jobId )
        # delete sshClientKey only if we just uploaded it
        if sshClientKeyName != args.sshClientKeyName:
            logger.debug( 'deleting sshClientKey %s', sshClientKeyName)
            ncs.deleteSshClientKey( args.authToken, sshClientKeyName )
        raise


def rsyncToRemote( srcFilePath, destFileName, inst, timeLimit ):
    sshSpecs = inst['ssh']
    host = sshSpecs['host']
    port = sshSpecs['port']
    user = sshSpecs['user']

    srcFilePathFull = os.path.realpath(os.path.abspath( srcFilePath ))
    remote_filename = user + '@' + host + ':~/' + destFileName
    cmd = ' '.join(['rsync -acq', '-e', '"ssh -p %d"' % port, srcFilePathFull, remote_filename])
    logger.info( 'rsyncing to %s', inst['instanceId'] )
    #logger.debug( 'rsyncing %s', cmd )  # would spill the full path
    returnCode = None
    stderr=''
    with subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE )as proc:
        try:
            (stdout, stderr) = proc.communicate( timeout=timeLimit )
            #logger.info( 'stdout %s, stderr %s', stdout, stderr )
            stdout = stdout.decode('utf8')
            stderr = stderr.decode('utf8')
            returnCode = proc.returncode
        except subprocess.TimeoutExpired:
            logger.warning( 'rsync took too long for instance %s', inst['instanceId'] )
            proc.terminate()  # just sends sigterm
            returnCode = 124
            try:
                # give it a chance to respond to sigterm
                proc.wait( timeout=timeLimit/2 )
            except Exception as exc:
                logger.warning( 'rsync exception after timeout (%s) %s', type(exc), exc )
                proc.kill()  # sends sigkill as a last resort
                logger.warning( 'killed rsync for %s', inst['instanceId'] )
        except Exception as exc:
            logger.warning( 'rsync threw exception (%s) %s', type(exc), exc )
            returnCode = -1
    if returnCode:
        logger.warning( 'rsync returnCode %d', returnCode )
    return returnCode, stderr

def scpFromRemote( srcFileName, destFilePath, inst, timeLimit=120 ):
    sshSpecs = inst['ssh']
    host = sshSpecs['host']
    port = sshSpecs['port']
    user = sshSpecs['user']

    destFilePathFull = os.path.realpath(os.path.abspath( destFilePath ))
    cmd = [ 'scp', '-r', '-P', str(port), user+'@'+host+':~/'+srcFileName,
        destFilePathFull
    ]
    logger.debug( 'retrieving from %s', inst['instanceId'] )
    #logger.debug( 'SCPing %s', cmd )  # would spill the full path
    returnCode = None
    stderr=''
    with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE )as proc:
        try:
            (stdout, stderr) = proc.communicate( timeout=timeLimit )
            stdout = stdout.decode('utf8')
            stderr = stderr.decode('utf8')
            returnCode = proc.returncode
        except subprocess.TimeoutExpired:
            proc.kill()
            returnCode = 124
            proc.communicate()  # ignoring any additional outputs
        except Exception as exc:
            logger.warning( 'scp threw exception (%s) %s', type(exc), exc )
            returnCode = -1
    if returnCode:
        logger.warning( 'SCP returnCode %d', returnCode )
    if (returnCode == 1) and ('closed by remote host' in stderr):
        returnCode = 255
    return returnCode, stderr

def saveProgress():
    # lock it to avoid race conditions
    with g_.progressFileLock:
        nFinished = len( g_.framesFinished)
        if not nFinished:
            # kluge: take credit for a fraction of a frame, assuming installaton is finished
            nFinished = 0.1
        nWorkersWorking = len( g_.workingInstances )
        frameDetails = list( g_.frameDetails.values() )
        struc = {
            'nFramesFinished': nFinished,
            'nFramesWanted': g_.nFramesWanted,
            'nWorkersWorking': nWorkersWorking,
            'frameDetails': frameDetails
        }
        with open( g_.progressFilePath, 'w' ) as progressFile:
            json.dump( struc, progressFile )


def renderFramesOnInstance( inst ):
    if g_.interrupted:
        logger.warning( 'exiting because g_.interrupted')
        return 0
    timeLimit = min( args.frameTimeLimit, args.timeLimit )
    rsyncTimeLimit = min( 18000, timeLimit )  # was 240; have used 1800 for big files
    iid = inst['instanceId']
    abbrevIid = iid[0:16]
    g_.workingInstances.append( iid )
    saveProgress()
    logger.info( 'would compute frames on instance %s', abbrevIid )

    '''
    # rsync the common input file, if any
    if args.commonInFilePath:
        logFrameState( -1, 'rsyncing', iid, 0 )
        destFileName = os.path.basename( args.commonInFilePath )
        (rc, stderr) = rsyncToRemote( args.commonInFilePath, destFileName, inst, timeLimit=rsyncTimeLimit )
        if rc == 0:
            logFrameState( -1, 'rsynced', iid )
        else:
            logStderr( stderr.rstrip(), iid )
            logFrameState( -1, 'rsyncFailed', iid, rc )
            logger.warning( 'rc from rsync was %d', rc )
            logOperation( 'terminateFailedWorker', iid, '<master>')
            terminateInstances( args.authToken, [iid] )
            g_.workingInstances.remove( iid )
            purgeHostKeys( [inst] )
            saveProgress()
            return -1  # go no further if we can't rsync to the worker
    '''
    def trackStderr( proc ):
        for line in proc.stderr:
            print( '<stderr>', abbrevIid, line.strip(), file=sys.stderr )
            logStderr( line.rstrip(), iid )

    def trackStdout( proc ):
        nonlocal frameProgress
        for line in proc.stdout:
            #print( '<stdout>', abbrevIid, line.strip(), file=sys.stderr )
            # these progress-tracking details are specific for blender #TODO generalize
            if 'Path Tracing Tile' in line:
                pass
                # yes, this progress-parsing code does work
                pat = r'Path Tracing Tile ([0-9]+)/([0-9]+)'
                match = re.search( pat, line )
                if match:
                    frameProgress = float( match.group(1) ) / float( match.group(2) )
            elif '| Updating ' in line:
                pass
            elif '| Synchronizing object |' in line:
                pass
            elif line.strip():
                print( '<stdout>', abbrevIid, line.strip(), file=sys.stderr )
                logStdout( line.rstrip(), iid )
    nFailures = 0    
    while len( g_.framesFinished) < g_.nFramesWanted:
        if sigtermSignaled():
            break
        if g_.interrupted:
            logger.warning( 'breaking loop because g_.interrupted')
            break
        if time.time() >= g_.deadline:
            logger.warning( 'exiting thread because global deadline has passed' )
            break
        if nFailures >= 3:
            logger.warning( 'exiting thread because instance has encountered %d failures', nFailures )
            logOperation( 'terminateFailedWorker', iid, '<master>')
            terminateInstances( args.authToken, [iid] )
            purgeHostKeys( [inst] )
            break
        #logger.info( '%s would claim a frame; %d done so far', abbrevIid, len( g_.framesFinished) )
        try:
            frameNum = g_.framesToDo.popleft()
        except IndexError:
            #logger.info( 'empty g_.framesToDo' )
            time.sleep(10)
            nUnfinished = g_.nFramesWanted - len(g_.framesFinished)
            nWorkers = len( g_.workingInstances )
            if nWorkers > round( nUnfinished * g_.autoscaleMax ):
                logger.info( 'exiting thread because not many left to do (%d unfinished, %d workers)',
                    nUnfinished, nWorkers )
                logOperation( 'terminateExcessWorker', iid, '<master>')
                g_.workingInstances.remove( iid )
                terminateInstances( args.authToken, [iid] )
                purgeHostKeys( [inst] )
                break
            continue

        frameDetails = { 'frameNum': frameNum, 'elapsedTime': 0, 'progress': 0 }
        frameDetails[ 'lastDateTime' ] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        g_.frameDetails[ frameNum ] = frameDetails

        outFileName = getFrameOutFileName( frameNum )
        returnCode = None
        cmd = getFrameCmd( frameNum )

        logger.debug( 'commanding %s', cmd )
        sshSpecs = inst['ssh']

        curFrameRendered = False
        logFrameState( frameNum, 'starting', iid )
        frameStartDateTime = datetime.datetime.now(datetime.timezone.utc)
        with subprocess.Popen(['ssh',
                            '-p', str(sshSpecs['port']),
                            '-o', 'ServerAliveInterval=360',
                            '-o', 'ServerAliveCountMax=3',
                            sshSpecs['user'] + '@' + sshSpecs['host'], cmd],
                            encoding='utf8',
                            stdout=subprocess.PIPE,  # subprocess.PIPE subprocess.DEVNULL
                            stderr=subprocess.PIPE) as proc:
            frameProgress = 0
            frameProgressReported = 0
            deadline = min( g_.deadline, time.time() + timeLimit )
            stdoutThr = threading.Thread(target=trackStdout, args=(proc,))
            stdoutThr.start()
            stderrThr = threading.Thread(target=trackStderr, args=(proc,))
            stderrThr.start()
            while time.time() < deadline:
                proc.poll() # sets proc.returncode
                if proc.returncode == None:
                    if frameProgress > min( .99, frameProgressReported + .01 ):
                        logger.info( 'frame %d on %s is %.1f %% done', frameNum, abbrevIid, frameProgress*100 )
                        frameProgressReported = frameProgress
                        rightNow = datetime.datetime.now(datetime.timezone.utc)
                        frameDetails[ 'lastDateTime' ] = rightNow.isoformat()
                        frameDetails[ 'elapsedTime' ] = (rightNow - frameStartDateTime).total_seconds()
                        frameDetails[ 'progress' ] = frameProgress
                        saveProgress()
                    if ((deadline - time.time() < timeLimit/2)) and frameProgress < .5:
                        #logger.warning( 'frame %d on %s seems slow', frameNum, abbrevIid )
                        logFrameState( frameNum, 'seemsSlow', iid, frameProgress )
                else:
                    if proc.returncode == 0:
                        logger.info( 'frame %d on %s succeeded', frameNum, abbrevIid )
                        curFrameRendered = True
                    else:
                        logger.warning( 'instance %s gave returnCode %d', abbrevIid, proc.returncode )
                    break
                if sigtermSignaled():
                    break
                if g_.interrupted:
                    logger.info( 'exiting polling loop because interrupted' )
                    break
                time.sleep(10)
            returnCode = proc.returncode if proc.returncode != None else 124
            if returnCode:
                logger.warning( 'computeFailed with rc %d for frame %d on %s', returnCode, frameNum, iid )
                logFrameState( frameNum, 'computeFailed', iid, returnCode )
                frameDetails[ 'progress' ] = 0
                g_.framesToDo.append( frameNum )
                saveProgress()
                time.sleep(10) # maybe we should retire this instance; at least, making it sleep so it is less competitive
            else:
                logFrameState( frameNum, 'computed', iid )
                #g_.framesFinished.append( frameNum )  # too soon

            proc.terminate()
            try:
                proc.wait(timeout=5)
                if proc.returncode:
                    logger.warning( 'ssh return code %d', proc.returncode )
            except subprocess.TimeoutExpired:
                logger.warning( 'ssh did not terminate in time' )
            stdoutThr.join()
            stderrThr.join()
        # may not need this logging here
        if returnCode != 0:
            logger.warning( 'remote returnCode %d for %s', returnCode, abbrevIid )
        if curFrameRendered:
            logFrameState( frameNum, 'retrieving', iid )
            (returnCode, stderr) = scpFromRemote( 
                outFileName, g_.dataDirPath, inst
                #outFileName, os.path.join( g_.dataDirPath, outFileName ), inst
                )
            if returnCode == 0:
                g_.framesFinished.append( frameNum )
                logFrameState( frameNum, 'retrieved', iid )
                logger.debug( 'retrieved frame %d', frameNum )
                logger.info( 'finished %d frames out of %d', len( g_.framesFinished), g_.nFramesWanted )
                rightNow = datetime.datetime.now(datetime.timezone.utc)
                frameDetails[ 'lastDateTime' ] = rightNow.isoformat()
                frameDetails[ 'elapsedTime' ] = (rightNow - frameStartDateTime).total_seconds()
                frameDetails[ 'progress' ] = 1.0
            else:
                g_.framesToDo.append( frameNum )
                logStderr( stderr.rstrip(), iid )
                logFrameState( frameNum, 'retrieveFailed', iid, returnCode )
                frameDetails[ 'progress' ] = 0
            saveProgress()
        if returnCode:
            nFailures += 1
        if g_.limitOneFramePerWorker:
            if len( g_.framesFinished) < g_.nFramesWanted:
                logger.info( 'breaking loop because of limitOneFramePerWorker')
            break
    if iid in g_.workingInstances:
        g_.workingInstances.remove( iid )
        saveProgress()
    return 0

def recruitAndRender():
    '''a threadproc to recruit an instance, compute frames on it, and terminate it'''
    eLoop = asyncio.new_event_loop()
    asyncio.set_event_loop( eLoop )
    
    randomPart = str( uuid.uuid4() )[0:13]
    launchedJsonFilePath = g_.dataDirPath+'/recruitLaunched_' + randomPart + '.json'
    resultsLogFilePath = g_.dataDirPath+'/recruitInstance_' + randomPart + '.jlog'
    try:
        instance = recruitInstance( launchedJsonFilePath, resultsLogFilePath )
    except Exception as exc:
        logger.info( 'got exception from recruitInstance (%s) %s', type(exc), exc )
        return -13
    if not instance:
        logger.warning( 'no good instance from recruit')
        return -14
    else:
        renderFramesOnInstance( instance )
        iid = instance['instanceId']
        logOperation( 'terminateFinal', [iid], '<master>' )
        terminateInstances( args.authToken, [iid] )
        purgeHostKeys( [instance] )

def checkForInstances():
    '''a threadproc to check whether we have enough instances running and maybe launch more'''
    threads = []
    while len(g_.framesFinished) < g_.nFramesWanted and sigtermNotSignaled() and time.time()< g_.deadline:
        if g_.interrupted:
            logger.warning( 'breaking loop because g_.interrupted')
            break

        nUnfinished = g_.nFramesWanted - len(g_.framesFinished)
        nWorkers = len( g_.workingInstances )
        if nWorkers < round(nUnfinished * g_.autoscaleMin):
            nAvail = ncs.getAvailableDeviceCount( args.authToken, filtersJson=args.filter )
            if nAvail >= 2:
                logger.info( 'starting thread because not enough workers (%d unfinished, %d workers)',
                    nUnfinished, nWorkers )
                rendererThread = threading.Thread( target=recruitAndRender, name='recruitAndRender' )
                threads.append( rendererThread )
                rendererThread.start()

        time.sleep( 20 )
    logger.info( 'waiting for worker threads to finish')
    for thread in threads:
        thread.join( timeout = args.instTimeLimit + args.frameTimeLimit )
        if thread.is_alive():
            logger.warning( 'thread %s did not exit', thread.name )
    logger.info( 'finished')

def runBatch( **kwargs ):
    ncs.logger.setLevel( logger.level )
    if 'authToken' not in kwargs:
        logger.error( 'authToken is required' )
        return 1
    ap = createArgumentParser()
    dfltArgs = ap.parse_args( ['--authToken=abcd'])
    argDict = vars( dfltArgs )
    argDict.update( kwargs )
    batchArgs = argparse.Namespace( **argDict )
    #batchArgs = types.SimpleNamespace( **argDict )  # another way, but not iterable (no "in")

    global args
    args = batchArgs
    #logger.debug('args: %s', args)
    g_.dataDirPath = args.outDataDir
    logger.info( 'args.outDataDir: %s', args.outDataDir )
    os.makedirs( g_.dataDirPath, exist_ok=True )

    signal.signal( signal.SIGTERM, sigtermHandler )
    myPid = os.getpid()
    logger.debug('procID: %s', myPid)

    g_.progressFilePath = g_.dataDirPath + '/progress.json'
    settingsJsonFilePath = g_.dataDirPath + '/settings.json'
    installerLogFilePath = g_.dataDirPath + '/recruitInstances.jlog'
    resultsLogFilePath = g_.dataDirPath+'/'+ \
        os.path.splitext( os.path.basename( __file__ ) )[0] + '_results.jlog'
    if resultsLogFilePath:
        g_.resultsLogFile = open( resultsLogFilePath, "w", encoding="utf8" )
    else:
        g_.resultsLogFile = None
    argsToSave = vars(args).copy()
    del argsToSave['authToken']
    #del argsToSave['frameProcessor']
    argsToSave.pop('frameProcessor', None)
    logOperation( 'starting', argsToSave, '<master>')

    if 'frameProcessor' in batchArgs:
        g_.frameProcessor = batchArgs.frameProcessor
    else:
        logger.error( 'no frameProcessor given' )
        return 1

    if args.frameTimeLimit > args.timeLimit:
        logger.warning('given frameTimeLimit (%d) > given job timeLimit; using %d for both',
            args.frameTimeLimit, args.timeLimit )

    if args.commonInFilePath and not os.path.isfile( args.commonInFilePath ) and not os.path.isdir( args.commonInFilePath ):
        logger.error( 'file not found: %s', args.commonInFilePath )
        return 1

    if args.endFrame < args.startFrame:
        logger.error( 'specified endFrame (%d) is less than startFrame (%d)', args.endFrame, args.startFrame )
        return 1


    startTime = time.time()
    g_.deadline = startTime + args.timeLimit

    #extensions = {'PNG': 'png', 'OPEN_EXR': 'exr'}

    # check that they have a public key for ssh
    try:
        loadSshPubKey()
    except FileNotFoundError as exc:
        logger.error( 'you do not have an ssh public key in ~/.ssh/id_rsa.pub')
        logger.info( ' to fix this, on command line, run ssh-keygen -t rsa' )
        logger.info( 'exception details (%s) %s', type(exc), exc )
        return 1
    except Exception as exc:
        logger.error( 'there was a problem reading your ssh public key')
        logger.info( 'exception was (%s) %s', type(exc), exc )
        return 1

    # validate the authToken
    resp = ncs.queryNcsSc( 'instances', args.authToken )
    if resp['statusCode'] == 403:
        logger.error( 'the given authToken was not accepted' )
        return 1
    g_.limitOneFramePerWorker = args.limitOneFramePerWorker
    g_.autoscaleMax = 1
    if not args.nWorkers:
        # check consistency of autoscale settings
        if args.autoscaleMax <= 0:
            logger.error( 'bad autoscaleMax' )
            return 1
        elif args.autoscaleMin < 0:
            logger.error( 'bad autoscaleMin' )
            return 1
        elif args.autoscaleInit < 0:
            logger.error( 'bad autoscaleInit' )
            return 1
        elif args.autoscaleMax <  args.autoscaleMin:
            logger.error( 'conflicting autoscaleMin and autoscaleMax' )
            return 1
        else:
            logger.debug( 'autoscale settings, init: %.2f, min: %.2f, max: %.2f',
                args.autoscaleInit, args.autoscaleMin, args.autoscaleMax )
            g_.autoscaleInit = args.autoscaleInit
            g_.autoscaleMin = args.autoscaleMin
            g_.autoscaleMax = args.autoscaleMax

    if not args.nWorkers:
        # regular case, where we pick a suitably large number to launch, based on # of frames
        nAvail = round( ncs.getAvailableDeviceCount( args.authToken, filtersJson=args.filter ) * .9 )
        logger.debug( 'args.filter: %s', args.filter )
        logger.info( '%d filtered devices available', nAvail )
        nFrames = len( range(args.startFrame, args.endFrame+1, args.frameStep ) )
        nToRecruit = min( nAvail, round( nFrames * g_.autoscaleInit ) )
        logger.debug( 'recruiting up to %d instances', nToRecruit )
    elif args.nWorkers > 0:
        # an override for advanced users, specifying exactly how many instances to launch
        nToRecruit = args.nWorkers
    else:
        msg = 'invalid nWorkers arg (%d, should be >= 0, or omitted)' % args.nWorkers
        logger.error( msg )
        return 1
    onTheFlyWanted = (args.nWorkers==0)
    checkerThread = None
    goodInstances = None
    try:
        if args.launch:
            goodInstances= recruitInstances( nToRecruit, g_.dataDirPath+'/recruitLaunched.json', True, installerLogFilePath )
        else:
            goodInstances = recruitInstances( nToRecruit, g_.dataDirPath+'/survivingInstances.json', False, installerLogFilePath )
        g_.installerLogFile = open( installerLogFilePath, 'a' )

        g_.framesToDo.extend( range(args.startFrame, args.endFrame+1, args.frameStep ) )
        g_.nFramesWanted = len( g_.framesToDo )
        logger.debug( 'g_.framesToDo %s', g_.framesToDo )

        settingsToSave = argsToSave.copy()
        with open( settingsJsonFilePath, 'w' ) as settingsFile:
            json.dump( settingsToSave, settingsFile )

        if not len(goodInstances):
            logger.error( 'no good instances were recruited')
        else:
            saveProgress()
            logOperation( 'parallelRender',
                {'commonInFilePath': args.commonInFilePath, 'nInstances': len(goodInstances),
                    'nFramesReq': g_.nFramesWanted },
                '<master>' )
            with futures.ThreadPoolExecutor( max_workers=len(goodInstances) ) as executor:
                parIter = executor.map( renderFramesOnInstance, goodInstances )
                if onTheFlyWanted:
                    checkerThread = threading.Thread( target=checkForInstances, name='checkForInstances' )
                    checkerThread.start()
                #parResultList = list( parIter )
                try:
                    for x in parIter:
                        time.sleep( .1 )
                except KeyboardInterrupt:
                    logger.warning( 'interrupted 1, setting flag')
                    g_.interrupted = True
                    raise
            logger.debug( 'finished initial thread pool')
            if onTheFlyWanted:
                # wait until it is time to exit
                while len(g_.framesFinished) < g_.nFramesWanted and sigtermNotSignaled() and time.time()< g_.deadline:
                    logger.info( 'waiting for frames to finish')
                    time.sleep( 10 )
    except KeyboardInterrupt:
        logger.warning( 'interrupted, setting flag')
        g_.interrupted = True


    if args.launch:
        if not goodInstances:
            logger.info( 'no good instances to terminate')
        else:
            logger.info( 'terminating %d instances', len( goodInstances) )
            iids = [inst['instanceId'] for inst in goodInstances]
            logOperation( 'terminateFinal', iids, '<master>' )
            terminateInstances( args.authToken, iids )
            purgeHostKeys( goodInstances )
    else:
        with open( g_.dataDirPath + '/survivingInstances.json','w' ) as outFile:
            json.dump( list(goodInstances), outFile )

    nFramesFinished = len(g_.framesFinished)

    # this is where post-batch aggregation would occur (formerly video encoding)

    if checkerThread:
        checkerThread.join( args.instTimeLimit + args.frameTimeLimit )  # could consider deadline
        if checkerThread.is_alive():
            logger.warning( 'checkerThread did not exit' )
    if g_.interrupted:
        raise KeyboardInterrupt

    elapsed = time.time() - startTime
    logger.info( 'computed %d frames out of %d', nFramesFinished, g_.nFramesWanted )
    logger.info( 'finished; elapsed time %.1f seconds (%.1f minutes)', elapsed, elapsed/60 )
    logOperation( 'finished',
        {'nInstancesRecruited': len(goodInstances),
            'nFramesFinished': nFramesFinished
        },
        '<master>'
        )
    if nFramesFinished > 0:
        return 0
    else:
        return 1

def createArgumentParser():
    ap = argparse.ArgumentParser( description=__doc__,
        fromfile_prefix_chars='@', formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    ap.add_argument( '--commonInFilePath', help='a file to upload initially to all instances' )
    ap.add_argument( '--authToken', required=True, help='the NCS authorization token to use (required)' )
    ap.add_argument( '--outDataDir', help='output data darectory', default='./aniData/' )
    ap.add_argument( '--encryptFiles', type=boolArg, default=True, help='whether to encrypt files on launched instances' )
    ap.add_argument( '--filter', help='json to filter instances for launch' )
    ap.add_argument( '--frameTimeLimit', type=int, default=8*60*60, help='amount of time (in seconds) allowed for each frame' )
    ap.add_argument( '--instTimeLimit', type=int, default=900, help='amount of time (in seconds) installer is allowed to take on instances' )
    ap.add_argument( '--batchId', help='to help identify this batch in a process list or log' )
    ap.add_argument( '--launch', type=boolArg, default=True, help='to launch and terminate instances' )
    ap.add_argument( '--sshAgent', type=boolArg, default=False, help='whether or not to use ssh agent' )
    ap.add_argument( '--sshClientKeyName', help='the name of the uploaded ssh client key to use (default is random)' )
    ap.add_argument( '--nWorkers', type=int, help='to override the # of worker instances (default=0 for automatic)',
        default=0 )
    ap.add_argument( '--limitOneFramePerWorker', type=boolArg, help='prevent any worker from doing multiple frames',
        default=False )
    ap.add_argument( '--autoscaleInit', type=float, help='multiple (instances per frame) to launch initially',
        default=1 )
    ap.add_argument( '--autoscaleMax', type=float, help='maximum multiple (instances per frame) to keep active',
        default=1 )
    ap.add_argument( '--autoscaleMin', type=float, help='minimum multiple (instances per frame) to keep active',
        default=1 )
    ap.add_argument( '--timeLimit', type=int, help='time limit (in seconds) for the whole job',
        default=24*60*60 )
    ap.add_argument( '--startFrame', type=int, help='the first frame number to compute',
        default=1 )
    ap.add_argument( '--endFrame', type=int, help='the last frame number to compute',
        default=1 )
    ap.add_argument( '--frameStep', type=int, help='the frame number increment',
        default=1 )
    return ap

if __name__ == "__main__":
    # configure logger formatting
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)
    ncs.logger.setLevel(logging.INFO)
    logger.setLevel(logging.INFO)
    tellInstances.logger.setLevel(logging.INFO)
    logger.debug('the logger is configured')

    ap = createArgumentParser()
    mainArgs = ap.parse_args()
    #logger.debug('args: %s', mainArgs)

    rc = runBatch( **vars( mainArgs ) )
    sys.exit( rc )
