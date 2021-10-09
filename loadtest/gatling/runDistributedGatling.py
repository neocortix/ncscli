#!/usr/bin/env python3
'''runs Gatling on a new batch of instances'''
import argparse
import datetime
import logging
import math
import os
import subprocess
import sys

import ncscli.batchRunner as batchRunner

logger = logging.getLogger(__name__)


def scriptDirPath():
    '''returns the absolute path to the directory containing this script'''
    return os.path.dirname(os.path.realpath(__file__))

def removeSuffix( x, suff ):
    return x.rsplit(suff, 1)[0]

class gatlingFrameProcessor(batchRunner.frameProcessor):
    '''defines details for using gatling for a load test'''
    workerDirPath = None
    scriptFilePath = None

    def installerCmd( self ):
        useScript = False
        if useScript:
            cmd = '%s/install.sh %s' % (workerDirPath, gatlingVersion)
        else:
            cmd = 'sudo apt-get -qq update > /dev/null && sudo apt-get -qq install -y unzip > /dev/null'
            cmd += ' && curl -s -S -L -O https://repo1.maven.org/maven2/io/gatling/highcharts/gatling-charts-highcharts-bundle/%s/gatling-charts-highcharts-bundle-%s-bundle.zip' \
                % (gatlingVersion, gatlingVersion)
            cmd += ' && unzip -q gatling-charts-highcharts-bundle-%s-bundle.zip' % gatlingVersion
        return cmd

    def frameOutFileName( self, frameNum ):
        return 'gatlingResults_%03d' % frameNum

    def frameCmd( self, frameNum ):
        dottedPath = self.scriptFilePath.replace( '/', '.' )
        simulationClass = removeSuffix( dottedPath, '.scala' )
        #simulationClass = 'ncsSim'
        cmd = 'JAVA_OPTS="-Dgatling.ssl.useOpenSsl=false -Dgatling.data.console.light=true" ~/gatling-charts-highcharts-bundle-%s/bin/gatling.sh -nr --simulation %s -sf ~/%s -rf ~/gatlingResults_%03d' % (
            gatlingVersion, simulationClass, self.workerDirPath, frameNum
        )
        return cmd


# configure logger formatting
#logging.basicConfig()
logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
logDateFmt = '%Y/%m/%d %H:%M:%S'
formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
logging.basicConfig(format=logFmt, datefmt=logDateFmt)
logger.setLevel(logging.INFO)
#batchRunner.logger.setLevel(logging.DEBUG)  # for more verbosity

ap = argparse.ArgumentParser( description=__doc__,
    fromfile_prefix_chars='@', formatter_class=argparse.ArgumentDefaultsHelpFormatter )
ap.add_argument( '--authToken', help='the NCS authorization token to use (or none, to use NCS_AUTH_TOKEN env var' )
ap.add_argument( '--outDataDir', help='a path to the output data dir for this run' )
ap.add_argument( '--filter', help='json to filter instances for launch',
    default = '{ "regions": ["usa", "india"], "dar": "==100", "dpr": ">=48", "ram": ">=3800000000", "storage": ">=2000000000" }'
    )
ap.add_argument( '--scriptFile', required=True, help='the Gatling test script relative file path (required)' )
ap.add_argument( '--planDuration', type=float, default=600, help='the expected duration of the test plan, in seconds' )
ap.add_argument( '--workerDir', help='the directory to upload to workers',
    default='gatlingWorker'
    )
ap.add_argument( '--nWorkers', type=int, default=6, help='the number of Load-generating workers' )
# for analysis and plotting
#ap.add_argument( '--rampStepDuration', type=float, default=60, help='duration of ramp step, in seconds' )
#ap.add_argument( '--SLODuration', type=float, default=240, help='SLO duration, in seconds' )
#ap.add_argument( '--SLOResponseTimeMax', type=float, default=2.5, help='SLO RT threshold, in seconds' )
# environmental
ap.add_argument( '--gatlingVersion', help='the version of gatling to run', default='3.6.1' )
ap.add_argument( '--gatlingBinPath', help='path to the local gatling.sh for aggregating results' )
args = ap.parse_args()

workerDirPath = args.workerDir.rstrip( '/' )  # trailing slash could cause problems with rsync
if workerDirPath:
    if not os.path.isdir( workerDirPath ):
        logger.error( 'the workerDirPath "%s" is not a directory', workerDirPath )
        sys.exit( 1 )
    gatlingFrameProcessor.workerDirPath = workerDirPath
else:
    logger.error( 'this version requires a workerDirPath' )
    sys.exit( 1 )
logger.debug( 'workerDirPath: %s', workerDirPath )

gatlingVersion = args.gatlingVersion

scriptFilePath = args.scriptFile.replace( '\\', '/' )
scriptFullerPath = os.path.join( workerDirPath, scriptFilePath )
if not os.path.isfile( scriptFullerPath ):
    logger.error( 'the script file "%s" was not found in %s', scriptFilePath, workerDirPath )
    sys.exit( 1 )
gatlingFrameProcessor.scriptFilePath = scriptFilePath
logger.debug( 'using script "%s"', scriptFilePath )

gatlingBinPath = args.gatlingBinPath
if not gatlingBinPath:
    gatlingBinPath = scriptDirPath()+('/gatling-charts-highcharts-bundle-%s/bin/gatling.sh' % gatlingVersion)

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
    outDataDir = 'data/gatling_' + dateTimeTag


try:
    rc = batchRunner.runBatch(
        frameProcessor = gatlingFrameProcessor(),
        commonInFilePath = workerDirPath,
        pushDeviceLocs=True,
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
        autoscaleMax = 1,
    )
    if (rc == 0) and os.path.isfile( outDataDir +'/recruitLaunched.json' ):
        # plot output (requires matplotlib)
        rc2 = subprocess.call( [sys.executable, scriptDirPath()+'/plotGatlingOutput.py',
                '--dataDirPath', outDataDir],
            stdout=subprocess.DEVNULL )
        if rc2:
            logger.warning( 'plotGatlingOutput.py exited with returnCode %d', rc2 )
        # report aggregated output (requires local gatling)
        
        if os.path.isfile( gatlingBinPath ):
            rc2 = subprocess.call( [sys.executable, scriptDirPath()+'/reportGatlingOutput.py', 
                    '--dataDirPath', outDataDir, '--gatlingBinPath', gatlingBinPath],
                stdout=subprocess.DEVNULL )
            if rc2:
                logger.warning( 'reportGatlingOutput.py exited with returnCode %d', rc2 )

    sys.exit( rc )
except KeyboardInterrupt:
    logger.warning( 'an interuption occurred')
