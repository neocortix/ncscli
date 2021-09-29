#!/usr/bin/env python3
'''runs k6 loadtest on a new batch of ncs SC instances'''
import argparse
import datetime
import json
import logging
import math
import os
import subprocess
import sys
# third-party modules
import requests
import ncscli.batchRunner as batchRunner


class k6FrameProcessor(batchRunner.frameProcessor):
    '''defines details for using k6 for a simplistic load test'''
    workerDirPath = None
    scriptFilePath = None

    def installerCmd( self ):
        cmd = 'curl -s -S -L --output k6.tar.gz https://github.com/grafana/k6/releases/download/v%s/k6-v%s-linux-arm64.tar.gz' \
            % (k6Version, k6Version)
        cmd += ' && tar -zxf k6.tar.gz'
        cmd += ' && mv k6-v%s-linux-arm64/k6 .' % (k6Version)
        return cmd

    def frameOutFileName( self, frameNum ):
        return 'worker_%03d.*' % frameNum
        #return 'worker_%03d.csv' % frameNum

    def frameCmd( self, frameNum ):
        cmd = 'cd %s && ~/k6 run -q %s --out csv=~/worker_%03d.csv > ~/worker_%03d.log 2>&1' % (
            (self.workerDirPath, self.scriptFilePath, frameNum, frameNum )
        )
        return cmd

def getSupportedVersions():
    minK6Version = '0.32.0'  # '0.32.0' is the first version where k6 has arm64 binary release

    resp = requests.get( 'https://api.github.com/repos/grafana/k6/releases' )
    respJson = resp.json()
    availableVersions = [rel['name'].lstrip('v') for rel in respJson]
    logger.debug( 'availableVersions: %s', availableVersions )
    supportedVersions = [vers for vers in availableVersions if vers >= minK6Version ]
    return supportedVersions


# configure logger formatting
#logging.basicConfig()
logger = logging.getLogger(__name__)
logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
logDateFmt = '%Y/%m/%d %H:%M:%S'
formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
logging.basicConfig(format=logFmt, datefmt=logDateFmt)
logger.setLevel(logging.INFO)
#batchRunner.logger.setLevel(logging.DEBUG)  # for more verbosity

ap = argparse.ArgumentParser( description=__doc__,
    fromfile_prefix_chars='@', formatter_class=argparse.ArgumentDefaultsHelpFormatter )
ap.add_argument( '--authToken', help='the NCS authorization token to use (or none, to use NCS_AUTH_TOKEN env var' )
ap.add_argument( '--outDataDir', required=False, help='a path to the output data dir for this run (required)' )
ap.add_argument( '--filter', help='json to filter instances for launch',
    default = '{ "regions": ["usa", "india"], "dar": "==100", "dpr": ">=48", "ram": ">=3800000000", "storage": ">=2000000000" }'
    )
ap.add_argument( '--k6Version', default='0.34.1', help='the version of k6 to run on workers' )
ap.add_argument( '--planDuration', type=float, default=600, help='the expected duration of the test plan, in seconds' )
ap.add_argument( '--workerDir', help='the directory to upload to workers',
    default='simpleWorker'
    )
ap.add_argument( '--scriptFile', required=True, help='the k6 test script (required)' )
ap.add_argument( '--nWorkers', type=int, default=6, help='the number of Load-generating workers' )
ap.add_argument( '--supportedVersions', action='store_true', help='to list supported versions and exit' )
args = ap.parse_args()

supportedVersions = getSupportedVersions()
logger.debug( 'supportedVersions: %s', supportedVersions )
if args.supportedVersions:
    print( json.dumps( supportedVersions ) )
    sys.exit( 0 )

k6Version = args.k6Version
if k6Version not in supportedVersions:
    logger.error( 'k6 version %s is not supported', k6Version )
    logger.info( 'these versions are supported: %s', supportedVersions )
    sys.exit( 1 )

workerDirPath = args.workerDir.rstrip( '/' )  # trailing slash could cause problems with rsync
if workerDirPath:
    if not os.path.isdir( workerDirPath ):
        logger.error( 'the workerDirPath "%s" is not a directory', workerDirPath )
        sys.exit( 1 )
    k6FrameProcessor.workerDirPath = workerDirPath
else:
    logger.error( 'this version requires a workerDirPath' )
    sys.exit( 1 )
logger.debug( 'workerDirPath: %s', workerDirPath )

scriptFilePath = args.scriptFile
scriptFullerPath = os.path.join( workerDirPath, scriptFilePath )
if not os.path.isfile( scriptFullerPath ):
    logger.error( 'the script file "%s" was not found in %s', scriptFilePath, workerDirPath )
    sys.exit( 1 )
k6FrameProcessor.scriptFilePath = scriptFilePath
logger.info( 'using script "%s"', scriptFilePath )

# compute frameTimeLimit based on planDuration, unless it is not positive, which is an error
planDuration = args.planDuration
if planDuration <= 0:
    logger.error( 'please supply a positive planDuration (not %d)', planDuration )
    sys.exit( 1 )
frameTimeLimit = max( round( planDuration * 1.5 ), planDuration+8*60 ) # some slop beyond the planned duration

nFrames = args.nWorkers
nWorkers = math.ceil(nFrames*1.5) if nFrames <=10 \
    else round( max( nFrames*1.12, nFrames + 5 * math.log10( nFrames ) ) )


outDataDir = args.outDataDir
if not outDataDir:
    dateTimeTag = datetime.datetime.now().strftime( '%Y-%m-%d_%H%M%S' )
    outDataDir = 'data/k6_' + dateTimeTag

#sys.exit( 'DEBUGGING' )
#if not os.path.isfile( workerDirPath + '/k6.tar.gz' ):
#    logger.error( 'the compressed k6 binary was not found, you may need to build and compress it' )
#    sys.exit( 1)
try:
    rc = batchRunner.runBatch(
        frameProcessor = k6FrameProcessor(),
        commonInFilePath = workerDirPath,
        authToken = os.getenv('NCS_AUTH_TOKEN') or 'YourAuthTokenHere',
        encryptFiles=False,
        timeLimit = frameTimeLimit + 40*60,
        instTimeLimit = 7*60,
        frameTimeLimit = frameTimeLimit,
        filter = args.filter,
        outDataDir = outDataDir,
        startFrame = 1,
        endFrame = nFrames,
        nWorkers = nWorkers,
        limitOneFramePerWorker = True,
        autoscaleMax = 1
    )
    if os.path.isfile( outDataDir +'/recruitLaunched.json' ):
        rc2 = subprocess.call( [sys.executable, 'plotK6Output.py', '--dataDirPath', outDataDir],
            stdout=subprocess.DEVNULL )
        if rc2:
            logger.warning( 'plotK6Output.py exited with returnCode %d', rc2 )

    sys.exit( rc )
except KeyboardInterrupt:
    logger.warning( 'an interuption occurred')
