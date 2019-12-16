#!/usr/bin/env python3
"""
anaimates using distributed blender rendering (assigning whole frames to instances)
"""

# standard library modules
import argparse
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
import runDistributedBlender
import tellInstances

logger = logging.getLogger(__name__)


global resultsLogFile

# possible place for globals is this class's attributes
class g_:
    signaled = False
g_releasedInstances = collections.deque()
g_releasedInstancesLock = threading.Lock()

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


def logResult( key, value, instanceId ):
    if resultsLogFile:
        toLog = {key: value, 'instanceId': instanceId,
            'dateTime': datetime.datetime.now(datetime.timezone.utc).isoformat() }
        print( json.dumps( toLog, sort_keys=True ), file=resultsLogFile )
        resultsLogFile.flush()

def logFrameState( frameNum, state, instanceId, rc=0 ):
    if resultsLogFile:
        toLog = {'frameNum': frameNum, 'frameState':state,
            'instanceId': instanceId, 'rc': rc,
            'dateTime': datetime.datetime.now(datetime.timezone.utc).isoformat() }
        print( json.dumps( toLog, sort_keys=True ), file=resultsLogFile )
        resultsLogFile.flush()

def logOperation( op, value, instanceId ):
    logResult( 'operation', {op: value}, instanceId )

def boolArg( v ):
    '''use with ArgumentParser add_argument for (case-insensitive) boolean arg'''
    if v.lower() == 'true':
        return True
    elif v.lower() == 'false':
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def scriptDirPath():
    '''returns the absolute path to the directory containing this script'''
    return os.path.dirname(os.path.realpath(__file__))

def allocateInstances( nWorkersWanted, launchedJsonFilePath ):
    # see if there are enough released ones to reuse
    toReuse = []
    with g_releasedInstancesLock:
        if len( g_releasedInstances ) >= nWorkersWanted:
            for _ in range( 0, nWorkersWanted ):
                toReuse.append( g_releasedInstances.popleft() )
    if toReuse:
        logger.info( 'REUSING instances')
        with open( launchedJsonFilePath,'w' ) as outFile:
            json.dump( toReuse, outFile )
        return toReuse

    nAvail = ncs.getAvailableDeviceCount( args.authToken, filtersJson=args.filter )
    if nWorkersWanted > (nAvail + 0):
        logger.error( 'not enough devices available (%d requested)', nWorkersWanted )
        raise ValueError( 'not enough devices available')
    if args.sshClientKeyName:
        sshClientKeyName = args.sshClientKeyName
    else:
        keyContents = runDistributedBlender.loadSshPubKey()
        randomPart = str( uuid.uuid4() )[0:13]
        keyContents += ' #' + randomPart
        sshClientKeyName = 'bfr_%s' % (randomPart)
        respCode = ncs.uploadSshClientKey( args.authToken, sshClientKeyName, keyContents )
        if respCode < 200 or respCode >= 300:
            logger.warning( 'ncs.uploadSshClientKey returned %s', respCode )
            sys.exit( 'could not upload SSH client key')
    logResult( 'launchInstances', nWorkersWanted, 'allocateInstances' )
    rc = runDistributedBlender.launchInstances( args.authToken, nWorkersWanted,
        sshClientKeyName, launchedJsonFilePath, filtersJson=args.filter )
    if rc:
        logger.debug( 'launchInstances returned %d', rc )
    # delete sshClientKey only if we just uploaded it
    if sshClientKeyName != args.sshClientKeyName:
        logger.info( 'deleting sshClientKey %s', sshClientKeyName)
        ncs.deleteSshClientKey( args.authToken, sshClientKeyName )
 
    # get instances from the launched json file
    launchedInstances = []
    with open( launchedJsonFilePath, 'r') as jsonInFile:
        try:
            launchedInstances = json.load(jsonInFile)  # an array
        except Exception as exc:
            logger.warning( 'could not load json (%s) %s', type(exc), exc )
    if len( launchedInstances ) < nWorkersWanted:
        logger.warning( 'could not launch as many instances as wanted (%d vs %d)',
            len( launchedInstances ), nWorkersWanted )

    if True:  #launchWanted:
        with open( os.path.expanduser('~/.ssh/known_hosts'), 'a' ) as khFile:
            jsonToKnownHosts.jsonToKnownHosts( launchedInstances, khFile )
    return launchedInstances

def recycleInstances( instances ):
    iids = [inst['instanceId'] for inst in instances]
    logOperation( 'recycleInstances', iids, '<master>' )
    with g_releasedInstancesLock:
        g_releasedInstances.extend( instances )

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

def recruitInstances( nWorkersWanted, launchedJsonFilePath, launchWanted ):
    '''fancy function to preallocate instances with good blender installation'''
    logger.info( 'recruiting up to %d instances', nWorkersWanted )
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
            keyContents = runDistributedBlender.loadSshPubKey()
            randomPart = str( uuid.uuid4() )[0:13]
            keyContents += ' #' + randomPart
            sshClientKeyName = 'bfr_%s' % (randomPart)
            respCode = ncs.uploadSshClientKey( args.authToken, sshClientKeyName, keyContents )
            if respCode < 200 or respCode >= 300:
                logger.warning( 'ncs.uploadSshClientKey returned %s', respCode )
                sys.exit( 'could not upload SSH client key')
        #launch
        logResult( 'operation', {'launchInstances': nWorkersWanted}, '<recruitInstances>' )
        rc = runDistributedBlender.launchInstances( args.authToken, nWorkersWanted,
            sshClientKeyName, launchedJsonFilePath, filtersJson=args.filter )
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
            resultsLogFilePath=dataDirPath+'/recruitInstances.jlog',
            download=None, downloadDestDir=None, jsonOut=None, sshAgent=args.sshAgent,
            timeLimit=min(args.instTimeLimit, args.timeLimit), upload=None,
            knownHostsOnly=True
            )
        # restore our handler because tellInstances may have overridden it
        signal.signal( signal.SIGTERM, sigtermHandler )
        if not stepStatuses:
            logger.warning( 'no statuses returned from installer')
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
            logResult( 'operation', {'terminateBad': badIids}, '<recruitInstances>' )
            ncs.terminateInstances( args.authToken, badIids )
        if goodInstances:
            recycleInstances( goodInstances )
    return goodInstances

def rsyncToRemote( srcFilePath, destFileName, inst, timeLimit=60 ):
    sshSpecs = inst['ssh']
    host = sshSpecs['host']
    port = sshSpecs['port']
    user = sshSpecs['user']

    srcFilePathFull = os.path.realpath(os.path.abspath( srcFilePath ))
    remote_filename = user + '@' + host + ':~/' + destFileName
    cmd = ' '.join(['rsync -acq', '-e', '"ssh -p %d"' % port, srcFilePathFull, remote_filename])
    logger.info( 'rsyncing %s', cmd )
    returnCode = None
    
    with subprocess.Popen(cmd, shell=True, \
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) as proc:
        returnCode = proc.wait( timeout=timeLimit )
    if returnCode:
        logger.warning( 'rsync returnCode %d', returnCode )
    return returnCode

def scpFromRemote( srcFileName, destFilePath, inst, timeLimit=60 ):
    sshSpecs = inst['ssh']
    host = sshSpecs['host']
    port = sshSpecs['port']
    user = sshSpecs['user']

    destFilePathFull = os.path.realpath(os.path.abspath( destFilePath ))
    cmd = [ 'scp', '-P', str(port), user+'@'+host+':~/'+srcFileName,
        destFilePathFull
    ]
    logger.info( 'SCPing %s', cmd )
    returnCode = None
    
    with subprocess.Popen(cmd, shell=False, \
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) as proc:
        returnCode = proc.wait( timeout=timeLimit )
    if returnCode:
        logger.warning( 'SCP returnCode %d', returnCode )
    return returnCode

def renderFramesOnInstance( inst, timeLimit=1500 ):
    iid = inst['instanceId']
    abbrevIid = iid[0:16]
    logger.info( 'would render frames on instance %s', abbrevIid )

    # args.blendFilePath should be used
    blendFileName = 'render.blend'  # was SpinningCube_002_c.blend 'cube0c.blend'
    rc = rsyncToRemote( args.blendFilePath, blendFileName, inst, timeLimit=240 )
    if rc != 0:
        logger.warning( 'rc from rsync was %d', rc )
        return -1
    def trackStderr( proc ):
        for line in proc.stderr:
            print( '<stderr>', abbrevIid, line.strip(), file=sys.stderr )

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
                    #if (frameProgress < 0.1) or (frameProgress > 0.9):
                    #    logger.info( '%s is %.1f %% done', abbrevIid, frameProgress*100 )
            elif '| Updating ' in line:
                pass
            elif '| Synchronizing object |' in line:
                pass
            elif line.strip():
                print( '<stdout>', abbrevIid, line.strip(), file=sys.stderr )

    #seed = 0
    #fileExt = 'png'
    while len( g_framesFinished) < g_nFramesWanted:
        logger.info( '%s would claim a frame; %d done so far', abbrevIid, len( g_framesFinished) )
        try:
            frameNum = g_framesToDo.popleft()
        except IndexError:
            #logger.info( 'empty g_framesToDo' )
            time.sleep(5)
            continue
        #outFilePattern = 'rendered_frame_######_seed_%d.%s'%(args.seed,fileExt)
        outFileName = g_outFilePattern.replace( '######', '%06d' % frameNum )
        returnCode = None
        #cmd = 'blender -b -noaudio --version'
        cmd = 'blender -b -noaudio %s -o %s -f %d' % \
            (blendFileName, g_outFilePattern, frameNum)
        logger.info( 'commanding %s', cmd )
        sshSpecs = inst['ssh']

        curFrameRendered = False
        logFrameState( frameNum, 'starting', iid )
        with subprocess.Popen(['ssh',
                            '-p', str(sshSpecs['port']),
                            '-o', 'ServerAliveInterval=360',
                            '-o', 'ServerAliveCountMax=1',
                            sshSpecs['user'] + '@' + sshSpecs['host'], cmd],
                            encoding='utf8',
                            stdout=subprocess.PIPE,  # subprocess.PIPE subprocess.DEVNULL
                            stderr=subprocess.PIPE) as proc:
            frameProgress = 0
            deadline = time.time() + timeLimit
            stdoutThr = threading.Thread(target=trackStdout, args=(proc,))
            stdoutThr.start()
            stderrThr = threading.Thread(target=trackStderr, args=(proc,))
            stderrThr.start()
            while time.time() < deadline:
                proc.poll() # sets proc.returncode
                if proc.returncode == None:
                    if (frameProgress < 0.01) or (frameProgress > 0.9):
                        logger.info( '%s is %.1f %% done', abbrevIid, frameProgress*100 )
                    if ((deadline - time.time() < timeLimit/2)) and frameProgress < .5:
                        logger.warning( '%s SEEMS SLOW for frame %d', abbrevIid, frameNum )
                        logFrameState( frameNum, 'seemsSlow', iid, frameProgress )
                else:
                    if proc.returncode == 0:
                        logger.info( 'remote %s succeeded on frame %d', abbrevIid, frameNum )
                        curFrameRendered = True
                    else:
                        logger.warning( 'remote %s gave returnCode %d', abbrevIid, proc.returncode )
                    break
                time.sleep(1)
            returnCode = proc.returncode if proc.returncode != None else 124
            if returnCode:
                logger.warning( 'unfinishedFrame %d, %s', frameNum, iid )
                #logResult( 'unfinishedFrame', [frameNum, returnCode], iid )
                logFrameState( frameNum, 'renderFailed', iid, returnCode )
                g_framesToDo.append( frameNum )
                time.sleep(10) # maybe we should retire this instance; at least, making it sleep so it is less competitive
            else:
                logFrameState( frameNum, 'rendered', iid )
                #logResult( 'finishedFrame', frameNum, iid )
                g_framesFinished.append( frameNum )

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
        else:
            logger.info( 'ok' )
        if curFrameRendered:
            returnCode = scpFromRemote( outFileName, os.path.join( dataDirPath, outFileName ), inst )
            if returnCode == 0:
                logFrameState( frameNum, 'retrieved', iid )
            else:
                logFrameState( frameNum, 'retrieveFailed', iid, returnCode )
    return 0


if __name__ == "__main__":
    # configure logger formatting
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)
    ncs.logger.setLevel(logging.INFO)
    runDistributedBlender.logger.setLevel(logging.INFO)
    logger.setLevel(logging.INFO)
    tellInstances.logger.setLevel(logging.INFO)
    logger.debug('the logger is configured')

    ap = argparse.ArgumentParser( description=__doc__,
        fromfile_prefix_chars='@', formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    ap.add_argument( 'blendFilePath', help='the .blend file to render' )
    ap.add_argument( '--authToken', required=True, help='the NCS authorization token to use' )
    ap.add_argument( '--dataDir', help='output data darectory', default='./aniData/' )
    ap.add_argument( '--filter', help='json to filter instances for launch' )
    ap.add_argument( '--instTimeLimit', type=int, default=900, help='amount of time (in seconds) installer is allowed to take on instances' )
    ap.add_argument( '--jobId', help='to identify this job' )
    ap.add_argument( '--launch', type=boolArg, default=True, help='to launch and terminate instances' )
    ap.add_argument( '--nWorkers', type=int, default=1, help='the # of worker instances to launch (or zero for all available)' )
    ap.add_argument( '--sshAgent', type=boolArg, default=False, help='whether or not to use ssh agent' )
    ap.add_argument( '--sshClientKeyName', help='the name of the uploaded ssh client key to use (default is random)' )
    ap.add_argument( '--nParFrames', type=int, help='how many frames to render in parallel',
        default=30 )
    ap.add_argument( '--timeLimit', type=int, help='time limit (in seconds) for the whole job',
        default=24*60*60 )
    ap.add_argument( '--useCompositor', type=boolArg, default=True, help='whether or not to use blender compositor' )
    # dtr-specific args
    ap.add_argument( '--width', type=int, help='the width (in pixels) of the output',
        default=960 )
    ap.add_argument( '--height', type=int, help='the height (in pixels) of the output',
        default=540 )
    ap.add_argument( '--blocks_user', type=int, help='the number of blocks to partition the image (or zero for "auto"',
        default=0 )
    ap.add_argument( '--fileType', choices=['PNG', 'OPEN_EXR'], help='the type of output file',
        default='PNG' )
    ap.add_argument( '--startFrame', type=int, help='the first frame number to render',
        default=0 )
    ap.add_argument( '--endFrame', type=int, help='the last frame number to render',
        default=1 )
    ap.add_argument( '--frameStep', type=int, help='the frame number increment',
        default=1 )
    ap.add_argument( '--seed', type=int, help='the blender cycles noise seed',
        default=0 )
    args = ap.parse_args()
    #logger.debug('args: %s', args)

    if not os.path.isfile( args.blendFilePath ):
        sys.exit( 'file not found: '+args.blendFilePath )
    #dateTimeTag = datetime.datetime.now().strftime( '%Y-%m-%d_%H%M%S' )
    dataDirPath = args.dataDir
    os.makedirs( dataDirPath, exist_ok=True )

    signal.signal( signal.SIGTERM, sigtermHandler )
    myPid = os.getpid()
    logger.info('procID: %s', myPid)

    resultsLogFilePath = dataDirPath+'/'+ \
        os.path.splitext( os.path.basename( __file__ ) )[0] + '_results.jlog'
    if resultsLogFilePath:
        resultsLogFile = open( resultsLogFilePath, "w", encoding="utf8" )
    else:
        resultsLogFile = None
    logResult( 'operation', 'starting', '<master>')

    startTime = time.time()
    extensions = {'PNG': 'png', 'OPEN_EXR': 'exr'}
    g_outFilePattern = 'rendered_frame_######_seed_%d.%s'%(args.seed,extensions[args.fileType])

    g_framesToDo.extend( range(args.startFrame, args.endFrame+1, args.frameStep ) )
    g_nFramesWanted = len( g_framesToDo )
    logger.info( 'g_framesToDo %s', g_framesToDo )

    nParFrames = args.nParFrames  # 30
    nToRecruit = min( args.endFrame+1-args.startFrame, nParFrames)*args.nWorkers
    nToRecruit = int( nToRecruit * 2 )
    if args.launch:
        goodInstances= recruitInstances( nToRecruit, dataDirPath+'/recruitLaunched.json', True )
    else:
        goodInstances = recruitInstances( nToRecruit, dataDirPath+'/survivingInstances.json', False )

    logOperation( 'parallelRender', len(goodInstances), '<master>' )
    with futures.ThreadPoolExecutor( max_workers=len(goodInstances) ) as executor:
        parIter = executor.map( renderFramesOnInstance, goodInstances )
        parResultList = list( parIter )

    '''
    if False:
        for frameNum in range( args.startFrame, args.endFrame+1, args.frameStep ):
            renderFrame( frameNum )
    else:
        logger.info( 'sleeping for some seconds')
        time.sleep( 30 )
        frameNums = list(range( args.startFrame, args.endFrame+1, args.frameStep ))
        # main loop to follow up and re-do frames that fail
        while len( frameNums ) > 0:
            logResult( 'parallelRender', len(frameNums), '<master>' )
            with futures.ThreadPoolExecutor( max_workers=nParFrames ) as executor:
                parIter = executor.map( renderFrame, frameNums )
                parResultList = list( parIter )

            logResult( 'progress', 'map() returned', '<master>' )
            #break  # remove this
            failedFrameNums = []
            for (index, result) in enumerate( parResultList ):
                fn = frameNums[ index ]
                if result:  # tup[1]:
                    logger.warning( 'frame # %d got result %s', fn, result )
                    failedFrameNums.append( fn )
            frameNums = failedFrameNums
            logResult( 'progress', 'end of loop', '<master>' )
    iids = ncs.listNcsScInstances( args.authToken )
    logger.info( 'surviving iids (%d) %s', len(iids), iids)
    '''

    if False:  # args.launch:
        logger.info( 'terminating %d instances', len( g_releasedInstances) )
        runDistributedBlender.terminateThese( args.authToken, g_releasedInstances )
    else:
        with open( dataDirPath + '/survivingInstances.json','w' ) as outFile:
            json.dump( list(g_releasedInstances), outFile )

    elapsed = time.time() - startTime
    logger.info( 'finished; elapsed time %.1f seconds (%.1f minutes)', elapsed, elapsed/60 )


