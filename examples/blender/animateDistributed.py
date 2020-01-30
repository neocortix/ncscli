#!/usr/bin/env python3
"""
anaimates using distributed blender rendering
"""

# standard library modules
import argparse
import collections
import contextlib
from concurrent import futures
import errno
import datetime
import getpass
import json
import logging
import math
import os
import socket
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

g_releasedInstances = collections.deque()
g_releasedInstancesLock = threading.Lock()

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
    logResult( 'recycleInstances', iids, '<master>' )
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

def recruitInstances( nWorkersWanted, launchedJsonFilePath ):
    '''fancy function to preallocate instances with good blender installation'''
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
    logResult( 'launchInstances', nWorkersWanted, 'recruitInstances' )
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
            logResult( 'recruitInstances would terminate bad instances', badIids, '<master>' )
            ncs.terminateInstances( args.authToken, badIids )
        if goodInstances:
            recycleInstances( goodInstances )


def demuxResults( inFilePath ):
    '''deinterleave jlog items into separate lists for each instance'''
    byInstance = {}
    badOnes = set()
    topLevelKeys = collections.Counter()
    # demux by instance
    with open( inFilePath, 'rb' ) as inFile:
        for line in inFile:
            decoded = json.loads( line )
            for key in decoded:
                topLevelKeys[ key ] += 1
            iid = decoded.get( 'instanceId', '<unknown>')
            have = byInstance.get( iid, [] )
            have.append( decoded )
            byInstance[iid] = have
            if 'returncode' in decoded:
                rc = decoded['returncode']
                if rc:
                    logger.info( 'returncode %d for %s', rc, iid )
                    badOnes.add( iid )
            if 'exception' in decoded:
                logger.info( 'exception %s for %s', decoded['exception'], iid )
                badOnes.add( iid )
            if 'timeout' in decoded:
                logger.info( 'timeout %s for %s', decoded['timeout'], iid )
                badOnes.add( iid )
    return byInstance, badOnes

def renderFrame( frameNum ):
    frameDirPath = os.path.join( dataDirPath, 'frame_%06d' % frameNum )
    frameFileName = frameFilePattern.replace( '######', '%06d' % frameNum )
    installerFilePath = os.path.join(frameDirPath, 'data', 'runDistributedBlender.py.jlog' )

    logResult( 'frameStart', frameNum, frameNum )
    os.makedirs( frameDirPath+'/data', exist_ok=True )

    instances = allocateInstances( args.nWorkers, frameDirPath+'/data/launched.json')
    try:
        cmd = [
            scriptDirPath()+'/runDistributedBlender.py',
            os.path.realpath( args.blendFilePath ),
            '--launch', False,
            '--authToken', args.authToken,
            '--blocks_user', args.blocks_user,
            '--nWorkers', args.nWorkers,
            '--filter', args.filter,
            '--width', args.width,
            '--height', args.height,
            '--seed', args.seed,
            '--timeLimit', args.timeLimit,
            '--useCompositor', args.useCompositor,
            '--frame', frameNum
        ]
        cmd = [ str( arg ) for arg in cmd ]
        logger.info( 'frame %d, %s', frameNum, frameDirPath )
        logger.info( 'cmd %s', cmd )

        if True:
            try:
                retCode = subprocess.call( cmd, cwd=frameDirPath,
                    stdout=sys.stdout, stderr=sys.stderr
                    )
            except Exception as exc:
                logger.warning( 'runDistributedBlender call threw exception (%s) %s',type(exc), exc )
                logResult( 'exception', str(exc), frameNum )
                return exc
            else:
                logger.info( 'RC from runDistributed: %d', retCode )
        if retCode:
            logResult( 'retCode', retCode, frameNum )
            #recycleInstances( instances )
            return retCode
        frameFilePath = os.path.join( frameDirPath, 'data', frameFileName)
        if not os.path.isfile( frameFilePath ):
            logResult( 'retCode', errno.ENOENT, frameNum )
            return FileNotFoundError( errno.ENOENT, 'could not render frame', frameFileName )
        outFilePath = os.path.join(dataDirPath, frameFileName )
        logger.info( 'moving %s to %s', frameFilePath, dataDirPath )
        try:
            if os.path.isfile( outFilePath ):
                os.remove( outFilePath )
            shutil.move( os.path.join( frameDirPath, 'data', frameFileName), dataDirPath )
        except Exception as exc:
            logger.warning( 'trouble moving %s (%s) %s', frameFileName, type(exc), exc )
            logResult( 'exception', str(exc), frameNum )
            return exc
    finally:
        (byInstance, badSet) = demuxResults( installerFilePath )
        if badSet:
            logger.warning( 'instances not well installed: %s', badSet )
            badIids = []
            goodInstances = []
            # remove bad instances from the list of instances to recycle
            for inst in instances:
                iid = inst['instanceId']
                if iid in badSet:
                    badIids.append( iid )
                else:
                    goodInstances.append( inst )
            # terminate any bad instances
            if badIids:
                logResult( 'renderFrame would terminate bad instances', badIids, frameNum )
                ncs.terminateInstances( args.authToken, badIids )
                instances = goodInstances
        # recycle the (hopefully non-bad) instances
        recycleInstances( instances )

    logResult( 'frameEnd', frameNum, frameNum )
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
        default=1 )
    ap.add_argument( '--endFrame', type=int, help='the last frame number to render',
        default=1 )
    ap.add_argument( '--frameStep', type=int, help='the frame number increment',
        default=1 )
    ap.add_argument( '--seed', type=int, help='the blender cycles noise seed',
        default=0 )
    args = ap.parse_args()
    #logger.debug('args: %s', args)

    dataDirPath = './aniData'
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
    frameFilePattern = 'rendered_frame_######_seed_%d.%s'%(args.seed,extensions[args.fileType])

    if False:
        for frameNum in range( args.startFrame, args.endFrame+1, args.frameStep ):
            renderFrame( frameNum )
    else:
        nParFrames = args.nParFrames  # 30
        nToRecruit = min( args.endFrame+1-args.startFrame, nParFrames)*args.nWorkers
        nToRecruit = int( nToRecruit * 1.5 )
        #nToRecruit = 18  # min( nFrames, nParFrames)*args.nWorkers
        recruitInstances( nToRecruit, dataDirPath+'/recruitLaunched.json' )
        logger.info( 'sleeping for some seconds')
        time.sleep( 90 )
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

    logger.info( 'terminating %d instances', len( g_releasedInstances) )
    runDistributedBlender.terminateThese( args.authToken, g_releasedInstances )
    elapsed = time.time() - startTime
    logger.info( 'finished; elapsed time %.1f seconds (%.1f minutes)', elapsed, elapsed/60 )


