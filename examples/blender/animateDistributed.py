#!/usr/bin/env python3
"""
anaimates using distributed blender rendering
"""

# standard library modules
import argparse
import contextlib
from concurrent import futures
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


logger = logging.getLogger(__name__)


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

def renderFrame( frameNum ):
    frameDirPath = os.path.join( dataDirPath, 'frame_%06d' % frameNum )
    frameFileName = frameFilePattern.replace( '######', '%06d' % frameNum )

    os.makedirs( frameDirPath+'/data', exist_ok=True )

    cmd = [
        scriptDirPath()+'/runDistributedBlender.py',
        os.path.realpath( args.blendFilePath ),
        '--authToken', args.authToken,
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
            return exc
        else:
            logger.info( 'RC from runDistributed: %d', retCode )
    if retCode:
        return retCode
    frameFilePath = os.path.join( frameDirPath, 'data', frameFileName)
    if not os.path.isfile( frameFilePath ):
        return FileNotFoundError( errno.ENOENT, 'could not render frame', frameFileName )
    outFilePath = os.path.join(dataDirPath, frameFileName )
    logger.info( 'moving %s to %s', frameFilePath, dataDirPath )
    try:
        if os.path.isfile( outFilePath ):
            os.remove( outFilePath )
        shutil.move( os.path.join( frameDirPath, 'data', frameFileName), dataDirPath )
    except Exception as exc:
        logger.warning( 'trouble moving %s (%s) %s', frameFileName, type(exc), exc )
        return exc
    return 0

if __name__ == "__main__":
    # configure logger formatting
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)
    ncs.logger.setLevel(logging.INFO)
    logger.setLevel(logging.INFO)
    #tellInstances.logger.setLevel(logging.INFO)
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

    signal.signal( signal.SIGTERM, sigtermHandler )
    myPid = os.getpid()
    logger.info('procID: %s', myPid)

    startTime = time.time()
    extensions = {'PNG': 'png', 'OPEN_EXR': 'exr'}
    frameFilePattern = 'rendered_frame_######_seed_%d.%s'%(args.seed,extensions[args.fileType])

    dataDirPath = './aniData'
    if False:
        for frameNum in range( args.startFrame, args.endFrame+1, args.frameStep ):
            renderFrame( frameNum )
    else:
        nWorkers = 12
        frameNums = list(range( args.startFrame, args.endFrame+1, args.frameStep ))
        # main loop
        with futures.ThreadPoolExecutor( max_workers=nWorkers ) as executor:
            parIter = executor.map( renderFrame, frameNums )
            parResultList = list( parIter )

        failedFrameNums = []
        for tup in enumerate( parResultList ):
            fn = frameNums[ tup[0] ]
            if tup[1]:
                logger.warning( 'frame # %d got result %s', fn, tup[1] )
                failedFrameNums.append( fn )
        frameNums = failedFrameNums.copy()

        # same as the main loop
        with futures.ThreadPoolExecutor( max_workers=nWorkers ) as executor:
            parIter = executor.map( renderFrame, frameNums )
            parResultList = list( parIter )

        failedFrameNums = []
        for tup in enumerate( parResultList ):
            fn = frameNums[ tup[0] ]
            if tup[1]:
                logger.warning( 'retried frame # %d got result %s', fn, tup[1] )
                failedFrameNums.append( fn )
 
    elapsed = time.time() - startTime
    logger.info( 'finished; elapsed time %.1f seconds (%.1f minutes)', elapsed, elapsed/60 )


