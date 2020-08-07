#!/usr/bin/env python3
"""
animates using distributed blender rendering (assigning whole frames to instances)
"""

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
import jsonToKnownHosts
#import runDistributedBlender
import tellInstances

logger = logging.getLogger(__name__)


global resultsLogFile

# possible place for globals is this class's attributes
class g_:
    signaled = False
    frameDetails = {}
    installerLogFile = None
g_deadline = None
g_workingInstances = collections.deque()
g_progressFileLock = threading.Lock()

g_framesToDo = collections.deque()
g_nFramesWanted = None  # total number to do; used as stopping criterion
#g_framesToDoLock = threading.Lock()
g_framesFinished = collections.deque()

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
    if resultsLogFile:
        toLog = {key: value, 'instanceId': instanceId,
            'dateTime': datetime.datetime.now(datetime.timezone.utc).isoformat() }
        print( json.dumps( toLog, sort_keys=True ), file=resultsLogFile )
        resultsLogFile.flush()

def logEvent( eventType, argv, instanceId ):
    if resultsLogFile:
        toLog = {
            'dateTime': datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'instanceId': instanceId, 
            'type': eventType,
            'args': argv
        }
        print( json.dumps( toLog, sort_keys=True ), file=resultsLogFile )
        resultsLogFile.flush()

def logStderr( text, instanceId ):
    logEvent( 'stderr', text, instanceId )

def logStdout( text, instanceId ):
    logEvent( 'stdout', text, instanceId )

def logFrameState( frameNum, state, instanceId, rc=0 ):
    if resultsLogFile:
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
        print( json.dumps( toLog, sort_keys=True ), file=resultsLogFile )
        resultsLogFile.flush()

def logOperation( op, value, instanceId ):
    if resultsLogFile:
        toLog = {
            'dateTime': datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'instanceId': instanceId,
            'type': 'operation',
            'args': {op: value}
            }
        print( json.dumps( toLog, sort_keys=True ), file=resultsLogFile )
        resultsLogFile.flush()

def logInstallerEvent( key, value, instanceId ):
    logger.info( 'logging %s', locals() )
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

def launchInstances( authToken, nInstances, sshClientKeyName, launchedJsonFilepath,
        filtersJson=None, encryptFiles=True ):
    returnCode = 13
    logger.info( 'launchedJsonFilepath %s', launchedJsonFilepath )
    try:
        with open( launchedJsonFilepath, 'w' ) as launchedJsonFile:
            returnCode = ncs.launchScInstances( authToken, encryptFiles, numReq=nInstances,
                sshClientKeyName=sshClientKeyName, jsonFilter=filtersJson,
                okToContinueFunc=sigtermNotSignaled, jsonOutFile=launchedJsonFile )
    except Exception as exc: 
        logger.error( 'exception while launching instances (%s) %s', type(exc), exc, exc_info=True )
        returnCode = 99
    return returnCode

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
        sshClientKeyName = 'bfr_%s' % (randomPart)
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
        logger.info( 'deleting sshClientKey %s', sshClientKeyName)
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
        ncs.terminateInstances( args.authToken, nonstartedIids )
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
        ncs.terminateInstances( args.authToken, [iid] )
        return None
    else:
        # add instance to knownHosts
        with open( os.path.expanduser('~/.ssh/known_hosts'), 'a' ) as khFile:
            jsonToKnownHosts.jsonToKnownHosts( startedInstances, khFile )
        # install blender on startedInstance
        installerCmd = 'sudo apt-get -qq update && sudo apt-get -qq -y install blender > /dev/null'
        logger.info( 'installerCmd %s', installerCmd )
        sshSpecs = inst['ssh']
        deadline = min( g_deadline, time.time() + args.instTimeLimit )
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
                ncs.terminateInstances( args.authToken, [iid] )
                logOperation( 'terminateBad', [iid], '<recruitInstances>' )
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

def recruitInstances( nWorkersWanted, launchedJsonFilePath, launchWanted, resultsLogFilePath='' ):
    '''launch instances and install blender on them;
        terminate those that could not install; return list of good instances'''
    logger.info( 'recruiting up to %d instances', nWorkersWanted )
    if not resultsLogFilePath:
        resultsLogFilePath = dataDirPath+'/recruitInstances.jlog'
    goodInstances = []
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
            sshClientKeyName = 'bfr_%s' % (randomPart)
            respCode = ncs.uploadSshClientKey( args.authToken, sshClientKeyName, keyContents )
            if respCode < 200 or respCode >= 300:
                logger.warning( 'ncs.uploadSshClientKey returned %s', respCode )
                sys.exit( 'could not upload SSH client key')
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
            logger.info( 'deleting sshClientKey %s', sshClientKeyName)
            ncs.deleteSshClientKey( args.authToken, sshClientKeyName )
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
        ncs.terminateInstances( args.authToken, nonstartedIids )
    # proceed with instances that were actually started
    startedInstances = [inst for inst in launchedInstances if inst['state'] == 'started' ]
    # add instances to knownHosts
    with open( os.path.expanduser('~/.ssh/known_hosts'), 'a' ) as khFile:
        jsonToKnownHosts.jsonToKnownHosts( startedInstances, khFile )
    # install blender on startedInstances
    if not sigtermSignaled():
        installerCmd = 'sudo apt-get -qq update && sudo apt-get -qq -y install blender > /dev/null'
        logger.info( 'calling tellInstances to install on %d instances', len(startedInstances))
        stepStatuses = tellInstances.tellInstances( startedInstances, installerCmd,
            resultsLogFilePath=resultsLogFilePath,
            download=None, downloadDestDir=None, jsonOut=None, sshAgent=args.sshAgent,
            timeLimit=min(args.instTimeLimit, args.timeLimit), upload=None, stopOnSigterm=not True,
            knownHostsOnly=True
            )
        # SHOULD restore our handler because tellInstances may have overridden it
        #signal.signal( signal.SIGTERM, sigtermHandler )
        if not stepStatuses:
            logger.warning( 'no statuses returned from installer')
            startedIids = [inst['instanceId'] for inst in startedInstances]
            logOperation( 'terminateBad', startedIids, '<recruitInstances>' )
            ncs.terminateInstances( args.authToken, startedIids )
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
            ncs.terminateInstances( args.authToken, badIids )
        #if goodInstances:
        #    recycleInstances( goodInstances )
    return goodInstances

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
            proc.kill()  #TODO need another timeout here
            returnCode = 124
            proc.communicate()  # ignoring any additional outputs
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
    cmd = [ 'scp', '-P', str(port), user+'@'+host+':~/'+srcFileName,
        destFilePathFull
    ]
    logger.info( 'SCPing from %s', inst['instanceId'] )
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
    with g_progressFileLock:
        nFinished = len( g_framesFinished)
        if not nFinished:
            # kluge: take credit for a fraction of a frame, assuming installaton is finished
            nFinished = 0.1
        nWorkersWorking = len( g_workingInstances )
        frameDetails = list( g_.frameDetails.values() )
        struc = {
            'nFramesFinished': nFinished,
            'nFramesWanted': g_nFramesWanted,
            'nWorkersWorking': nWorkersWorking,
            'frameDetails': frameDetails
        }
        with open( progressFilePath, 'w' ) as progressFile:
            json.dump( struc, progressFile )


def renderFramesOnInstance( inst ):
    timeLimit = min( args.frameTimeLimit, args.timeLimit )
    rsyncTimeLimit = min( 18000, timeLimit )  # was 240; have used 1800 for big files
    iid = inst['instanceId']
    abbrevIid = iid[0:16]
    g_workingInstances.append( iid )
    saveProgress()
    logger.info( 'would render frames on instance %s', abbrevIid )

    # rsync the blend file, with standardized dest file name
    logFrameState( -1, 'rsyncing', iid, 0 )
    blendFileName = 'render.blend'  # was SpinningCube_002_c.blend 'cube0c.blend'
    (rc, stderr) = rsyncToRemote( args.blendFilePath, blendFileName, inst, timeLimit=rsyncTimeLimit )
    if rc == 0:
        logFrameState( -1, 'rsynced', iid )
    else:
        logStderr( stderr.rstrip(), iid )
        logFrameState( -1, 'rsyncFailed', iid, rc )
        logger.warning( 'rc from rsync was %d', rc )
        logOperation( 'terminateFailedWorker', iid, '<master>')
        ncs.terminateInstances( args.authToken, [iid] )
        g_workingInstances.remove( iid )
        saveProgress()
        return -1  # go no further if we can't rsync to the worker

    def trackStderr( proc ):
        for line in proc.stderr:
            print( '<stderr>', abbrevIid, line.strip(), file=sys.stderr )
            logStderr( line.rstrip(), iid )

    def trackStdout( proc ):
        nonlocal frameProgress
        for line in proc.stdout:
            #print( '<stdout>', abbrevIid, line.strip(), file=sys.stderr )
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
    while len( g_framesFinished) < g_nFramesWanted:
        if sigtermSignaled():
            break
        if time.time() >= g_deadline:
            logger.warning( 'exiting thread because global deadline has passed' )
            break

        if nFailures >= 3:
            logger.warning( 'exiting thread because instance has encountered %d failures', nFailures )
            logOperation( 'terminateFailedWorker', iid, '<master>')
            ncs.terminateInstances( args.authToken, [iid] )
            break
        #logger.info( '%s would claim a frame; %d done so far', abbrevIid, len( g_framesFinished) )
        try:
            frameNum = g_framesToDo.popleft()
        except IndexError:
            #logger.info( 'empty g_framesToDo' )
            time.sleep(10)
            overageFactor = 3
            nUnfinished = g_nFramesWanted - len(g_framesFinished)
            nWorkers = len( g_workingInstances )
            if nWorkers > (nUnfinished * overageFactor):
                logger.warning( 'exiting thread because not many left to do (%d unfinished, %d workers)',
                    nUnfinished, nWorkers )
                logOperation( 'terminateExcessWorker', iid, '<master>')
                g_workingInstances.remove( iid )
                ncs.terminateInstances( args.authToken, [iid] )
                break
            continue

        frameDetails = { 'frameNum': frameNum, 'elapsedTime': 0, 'progress': 0 }
        frameDetails[ 'lastDateTime' ] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        g_.frameDetails[ frameNum ] = frameDetails

        outFileName = g_outFilePattern.replace( '######', '%06d' % frameNum )
        returnCode = None
        pyExpr = ''
        if args.width > 0 and args.height > 0:
            pyExpr = '--python-expr "import bpy; scene=bpy.context.scene; '\
                'scene.render.resolution_x=%d; scene.render.resolution_y=%d; '\
                'scene.render.resolution_percentage=100"' % (args.width, args.height)
        #cmd = 'blender -b -noaudio --version'
        cmd = 'blender -b -noaudio --enable-autoexec %s %s -o %s --render-format %s -f %d' % \
            (blendFileName, pyExpr, g_outFilePattern, args.frameFileType, frameNum)

        logger.info( 'commanding %s', cmd )
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
            deadline = min( g_deadline, time.time() + timeLimit )
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
                        logger.warning( 'frame %d on %s seems slow', frameNum, abbrevIid )
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
                time.sleep(10)
            returnCode = proc.returncode if proc.returncode != None else 124
            if returnCode:
                logger.warning( 'renderFailed for frame %d on %s', frameNum, iid )
                logFrameState( frameNum, 'renderFailed', iid, returnCode )
                frameDetails[ 'progress' ] = 0
                g_framesToDo.append( frameNum )
                saveProgress()
                time.sleep(10) # maybe we should retire this instance; at least, making it sleep so it is less competitive
            else:
                logFrameState( frameNum, 'rendered', iid )
                #g_framesFinished.append( frameNum )  # too soon

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
            logger.warning( 'blender returnCode %d for %s', returnCode, abbrevIid )
        if curFrameRendered:
            logFrameState( frameNum, 'retrieving', iid )
            (returnCode, stderr) = scpFromRemote( 
                outFileName, os.path.join( dataDirPath, outFileName ), inst
                )
            if returnCode == 0:
                logFrameState( frameNum, 'retrieved', iid )
                logger.info( 'retrieved frame %d', frameNum )
                logger.info( 'finished %d frames out of %d', len( g_framesFinished), g_nFramesWanted )
                g_framesFinished.append( frameNum )
                rightNow = datetime.datetime.now(datetime.timezone.utc)
                frameDetails[ 'lastDateTime' ] = rightNow.isoformat()
                frameDetails[ 'elapsedTime' ] = (rightNow - frameStartDateTime).total_seconds()
                frameDetails[ 'progress' ] = 1.0
            else:
                logStderr( stderr.rstrip(), iid )
                logFrameState( frameNum, 'retrieveFailed', iid, returnCode )
                frameDetails[ 'progress' ] = 0
                g_framesToDo.append( frameNum )
            saveProgress()
        if returnCode:
            nFailures += 1
    if iid in g_workingInstances:
        g_workingInstances.remove( iid )
        saveProgress()
    return 0

def recruitAndRender():
    '''a threadproc to recruit an instance,render frames on it, and terminate it'''
    eLoop = asyncio.new_event_loop()
    asyncio.set_event_loop( eLoop )
    
    randomPart = str( uuid.uuid4() )[0:13]
    launchedJsonFilePath = dataDirPath+'/recruitLaunched_' + randomPart + '.json'
    resultsLogFilePath = dataDirPath+'/recruitInstance_' + randomPart + '.jlog'
    try:
        instance = recruitInstance( launchedJsonFilePath, resultsLogFilePath )
        #instances = recruitInstances( 1, launchedJsonFilePath, True, resultsLogFilePath )
    except Exception as exc:
        logger.info( 'got exception from recruitInstance (%s) %s', type(exc), exc )
        return -13
    if not instance:
        logger.warning( 'no good instance from recruit')
        return -14
    else:
        renderFramesOnInstance( instance )
        #return renderFramesOnInstance( instances[0] )
        iid = instance['instanceId']
        logOperation( 'terminateFinal', [iid], '<master>' )
        ncs.terminateInstances( args.authToken, [iid] )

def checkForInstances():
    '''a threadproc to check whether we have enough instances running and maybe launch more'''
    while len(g_framesFinished) < g_nFramesWanted and sigtermNotSignaled() and time.time()< g_deadline:
        overageFactor = 2
        nUnfinished = g_nFramesWanted - len(g_framesFinished)
        nWorkers = len( g_workingInstances )
        if nWorkers < (nUnfinished * overageFactor):
            nAvail = ncs.getAvailableDeviceCount( args.authToken, filtersJson=args.filter )
            if nAvail >= 2:
                logger.warning( 'starting thread because not enough workers (%d unfinished, %d workers)',
                    nUnfinished, nWorkers )
                rendererThread = threading.Thread( target=recruitAndRender, name='recruitAndRender' )
                rendererThread.start()

        time.sleep( 60 )
    logger.info( 'finished')

def encodeTo264( destDirPath, destFileName, frameRate, kbps=30000,
    frameFileType='png', startFrame=0 ):
    kbpsParam = str(kbps)+'k'
    cmd = [ 'ffmpeg', '-y', '-framerate', str(frameRate),
        '-start_number', str(startFrame),
        '-i', destDirPath + '/rendered_frame_%%06d.%s'%(frameFileType),
        '-c:v', 'libx264', '-preset', 'fast', '-pix_fmt', 'yuv420p', 
        '-b:v', kbpsParam,
        os.path.join( destDirPath, destFileName )
    ]
    try:
        subprocess.check_call( cmd,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
    except Exception as exc:
        logger.warning( 'ffmpeg call threw exception (%s) %s',type(exc), exc )

if __name__ == "__main__":
    # configure logger formatting
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)
    ncs.logger.setLevel(logging.INFO)
    #runDistributedBlender.logger.setLevel(logging.INFO)
    logger.setLevel(logging.INFO)
    tellInstances.logger.setLevel(logging.INFO)
    logger.debug('the logger is configured')

    ap = argparse.ArgumentParser( description=__doc__,
        fromfile_prefix_chars='@', formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    ap.add_argument( 'blendFilePath', help='the .blend file to render' )
    ap.add_argument( '--authToken', required=True, help='the NCS authorization token to use (required)' )
    ap.add_argument( '--dataDir', help='output data darectory', default='./aniData/' )
    ap.add_argument( '--encryptFiles', type=boolArg, default=True, help='whether to encrypt files on launched instances' )
    ap.add_argument( '--filter', help='json to filter instances for launch' )
    ap.add_argument( '--frameRate', type=int, default=24, help='the frame rate (frames per second) for video output' )
    ap.add_argument( '--frameTimeLimit', type=int, default=8*60*60, help='amount of time (in seconds) allowed for each frame' )
    ap.add_argument( '--instTimeLimit', type=int, default=900, help='amount of time (in seconds) installer is allowed to take on instances' )
    ap.add_argument( '--jobId', help='to identify this job in log' )
    ap.add_argument( '--launch', type=boolArg, default=True, help='to launch and terminate instances' )
    #ap.add_argument( '--nWorkers', type=int, default=1, help='the # of worker instances to launch (or zero for all available)' )
    ap.add_argument( '--origBlendFilePath', help='for logging, set this if different from blendFilePath' )
    ap.add_argument( '--sshAgent', type=boolArg, default=False, help='whether or not to use ssh agent' )
    ap.add_argument( '--sshClientKeyName', help='the name of the uploaded ssh client key to use (default is random)' )
    ap.add_argument( '--nWorkers', type=int, help='to override the # of worker instances (default=0 for automatic)',
        default=0 )
    ap.add_argument( '--timeLimit', type=int, help='time limit (in seconds) for the whole job',
        default=24*60*60 )
    #ap.add_argument( '--useCompositor', type=boolArg, default=True, help='whether or not to use blender compositor' )
    ap.add_argument( '--width', type=int, help='the width (in pixels) of the output (0 for .blend file default)',
        default=0 )
    ap.add_argument( '--height', type=int, help='the height (in pixels) of the output (0 for .blend file default)',
        default=0 )
    ap.add_argument( '--frameFileType', choices=['PNG', 'OPEN_EXR'], help='the type of frame output file',
        default='PNG' )
    ap.add_argument( '--startFrame', type=int, help='the first frame number to render',
        default=1 )
    ap.add_argument( '--endFrame', type=int, help='the last frame number to render',
        default=1 )
    ap.add_argument( '--frameStep', type=int, help='the frame number increment',
        default=1 )
    args = ap.parse_args()
    #logger.debug('args: %s', args)

    if not os.path.isfile( args.blendFilePath ):
        sys.exit( 'file not found: '+args.blendFilePath )
    if not args.origBlendFilePath:
        args.origBlendFilePath = args.blendFilePath

    dataDirPath = args.dataDir
    os.makedirs( dataDirPath, exist_ok=True )

    signal.signal( signal.SIGTERM, sigtermHandler )
    myPid = os.getpid()
    logger.info('procID: %s', myPid)
    logger.info('jobID: %s', args.jobId)

    if args.frameTimeLimit > args.timeLimit:
        logger.warning('given frameTimeLimit (%d) > given job timeLimit; using %d for both',
            args.frameTimeLimit, args.timeLimit )


    progressFilePath = dataDirPath + '/progress.json'
    settingsJsonFilePath = dataDirPath + '/settings.json'
    installerLogFilePath = dataDirPath + '/recruitInstances.jlog'
    resultsLogFilePath = dataDirPath+'/'+ \
        os.path.splitext( os.path.basename( __file__ ) )[0] + '_results.jlog'
    if resultsLogFilePath:
        resultsLogFile = open( resultsLogFilePath, "w", encoding="utf8" )
    else:
        resultsLogFile = None
    argsToSave = vars(args).copy()
    del argsToSave['authToken']
    logOperation( 'starting', argsToSave, '<master>')

    startTime = time.time()
    g_deadline = startTime + args.timeLimit

    extensions = {'PNG': 'png', 'OPEN_EXR': 'exr'}
    g_outFilePattern = 'rendered_frame_######.%s'%(extensions[args.frameFileType])


    if not args.nWorkers:
        # regular case, where we pick a suitably large number to launch, based on # of frames
        nAvail = ncs.getAvailableDeviceCount( args.authToken, filtersJson=args.filter )
        nFrames = len( range(args.startFrame, args.endFrame+1, args.frameStep ) )
        nToRecruit = min( nAvail, nFrames * 3 )  # SHOULD be * 3
    elif args.nWorkers > 0:
        # an override for advanced users, specifying exactly how many instances to launch
        nToRecruit = args.nWorkers
    elif args.nWorkers == -1:
        # traditional test-user override, to launch as many instances as possible
        nToRecruit = ncs.getAvailableDeviceCount( args.authToken, filtersJson=args.filter )
    else:
        msg = 'invalid nWorkers arg (%d, should be >= -1)' % args.nWorkers
        logger.warning( msg )
        sys.exit( msg )
    onTheFlyWanted = (args.nWorkers==0)

    if args.launch:
        goodInstances= recruitInstances( nToRecruit, dataDirPath+'/recruitLaunched.json', True )
    else:
        goodInstances = recruitInstances( nToRecruit, dataDirPath+'/survivingInstances.json', False )

    g_.installerLogFile = open( installerLogFilePath, 'a' )

    if args.nWorkers == -1:
        # for testing, do 3 frames for every well-installed instance
        g_framesToDo = collections.deque( range( 0, len(goodInstances) * 3) )
    else:
        g_framesToDo.extend( range(args.startFrame, args.endFrame+1, args.frameStep ) )
    logger.info( 'g_framesToDo %s', g_framesToDo )

    settingsToSave = argsToSave.copy()
    settingsToSave['outVideoFileName'] = 'rendered_preview.mp4'
    with open( settingsJsonFilePath, 'w' ) as settingsFile:
        json.dump( settingsToSave, settingsFile )

    if not len(goodInstances):
        logger.error( 'no good instances were recruited')
    else:
        g_nFramesWanted = len( g_framesToDo )
        saveProgress()
        logOperation( 'parallelRender',
            {'blendFilePath': args.blendFilePath, 'nInstances': len(goodInstances),
                'origBlendFilePath': args.origBlendFilePath,
                'nFramesReq': g_nFramesWanted },
            '<master>' )
        with futures.ThreadPoolExecutor( max_workers=len(goodInstances) ) as executor:
            parIter = executor.map( renderFramesOnInstance, goodInstances )
            if onTheFlyWanted:
                checkerThread = threading.Thread( target=checkForInstances, name='checkForInstances' )
                checkerThread.start()
            parResultList = list( parIter )
        logger.info( 'finished initial thread pool')
        # wait until it is time to exit
        while len(g_framesFinished) < g_nFramesWanted and sigtermNotSignaled() and time.time()< g_deadline:
            logger.info( 'waiting for frames to finish')
            time.sleep( 10 )


    if args.launch:
        if not goodInstances:
            logger.info( 'no good instances to terminate')
        else:
            logger.info( 'terminating %d instances', len( goodInstances) )
            iids = [inst['instanceId'] for inst in goodInstances]
            logOperation( 'terminateFinal', iids, '<master>' )
            ncs.terminateInstances( args.authToken, iids )
    else:
        with open( dataDirPath + '/survivingInstances.json','w' ) as outFile:
            json.dump( list(goodInstances), outFile )

    nFramesFinished = len(g_framesFinished)
    if nFramesFinished:
        encodeTo264( dataDirPath, settingsToSave['outVideoFileName'], 
            args.frameRate, startFrame=args.startFrame,
            frameFileType=extensions[args.frameFileType] )

    elapsed = time.time() - startTime
    logger.info( 'rendered %d frames using %d "good" instances', len(g_framesFinished), len(goodInstances) )
    logger.info( 'finished; elapsed time %.1f seconds (%.1f minutes)', elapsed, elapsed/60 )
    logOperation( 'finished',
        {'nInstancesRecruited': len(goodInstances),
            'nFramesFinished': nFramesFinished
        },
        '<master>'
        )
    if nFramesFinished == g_nFramesWanted:
        sys.exit()
    else:
        sys.exit( 1 )
