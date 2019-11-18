#!/usr/bin/env python3
"""
anaimates using distributed blender rendering
"""

# standard library modules
import argparse
import contextlib
from concurrent import futures
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
import pandas as pd
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

def checkFrame( frameNum ):
    info = { 'frameNum': frameNum, 'state': 'unstarted' }
    frameDirPath = os.path.join( dataDirPath, 'frame_%06d' % frameNum )
    frameFileName = frameFilePattern.replace( '######', '%06d' % frameNum )
    launchedJsonFilePath = os.path.join(frameDirPath, 'data', 'launched.json' )
    installerFilePath = os.path.join(frameDirPath, 'data', 'runDistributedBlender.py.jlog' )
    frameFilePath = os.path.join(dataDirPath, frameFileName )

    if not os.path.isdir( frameDirPath ):
        logger.error( '%06d, %s not found', frameNum, frameDirPath )
        #info['state'] = 'noData'
        return info

    if os.path.isfile( launchedJsonFilePath ):
        info['state'] = 'launched'
        launchedDateTime = datetime.datetime.fromtimestamp( os.path.getmtime( launchedJsonFilePath ) )
        info[ 'launchedDateTime' ] = launchedDateTime
        #logger.info( '%06d, launched %s', frameNum, launchedDateTime.strftime( '%Y-%m-%d_%H%M%S' ) )
    if os.path.isfile( installerFilePath ):
        info['state'] = 'installing'
        installedDateTime = datetime.datetime.fromtimestamp( os.path.getmtime( installerFilePath ) )
        info[ 'installedDateTime' ] = installedDateTime

    if os.path.isfile( frameFilePath ):
        info['state'] = 'finished'
        info[ 'finishedDateTime' ] = datetime.datetime.fromtimestamp( os.path.getmtime( frameFilePath ) )
    else:
        #info['state'] = 'unfinished'
        logger.warning( '%06d, %s not found', frameNum, frameFilePath )
        return info

    return info

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
    #ap.add_argument( 'blendFilePath', help='the .blend file to render' )
    ap.add_argument( '--authToken', required=False, help='the NCS authorization token to use' )
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

    startTime = time.time()
    extensions = {'PNG': 'png', 'OPEN_EXR': 'exr'}
    frameFilePattern = 'rendered_frame_######_seed_%d.%s'%(args.seed,extensions[args.fileType])

    dataDirPath = './aniData'
    frameInfos = []
    for frameNum in range( args.startFrame, args.endFrame+1, args.frameStep ):
        frameInfo = checkFrame( frameNum )
        if frameInfo['state'] != 'finished':
            logger.info( '%s', frameInfo )
        frameInfos.append( frameInfo )
    framesTable = pd.DataFrame( frameInfos )
    framesTable.to_csv( dataDirPath+'/frameSummaries.csv', index=False)

    if not len(framesTable):
        sys.exit( 'no frames in framesTable')
    #logger.info( '%s', framesTable.info() )
    framesTable['launchedDateTime'] = pd.to_datetime( framesTable.launchedDateTime )
    framesTable['finishedDateTime'] = pd.to_datetime( framesTable.finishedDateTime )

    finished = framesTable[ ~pd.isnull(framesTable.finishedDateTime) ]
    justStarted = framesTable[ pd.isnull(framesTable.finishedDateTime) & ~pd.isnull(framesTable.launchedDateTime) ]

    print( len(finished), 'frames finished' )
    print( len(justStarted), 'frames launched but not finished' )
    print( 'earliest start', finished.launchedDateTime.min() )
    print( 'latest finish', finished.finishedDateTime.max() )
    overallDur = finished.finishedDateTime.max() - finished.launchedDateTime.min()
    print( 'overall duration %.2f minutes (%.2f hours)' % (overallDur.total_seconds() / 60, overallDur.total_seconds() / 3600) )
    timePerFrame = (finished.finishedDateTime.max() - finished.launchedDateTime.min()) / len(finished)
    #print( 'time per frame', timePerFrame, type(timePerFrame) )
    print( 'time per frame %d seconds (%.2f minutes)' % \
        (timePerFrame.total_seconds(), timePerFrame.total_seconds()/60 ) )



    #elapsed = time.time() - startTime
    #logger.info( 'finished; elapsed time %.1f seconds (%.1f minutes)', elapsed, elapsed/60 )


