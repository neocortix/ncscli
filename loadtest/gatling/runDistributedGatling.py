#!/usr/bin/env python3
'''runs Gatling on a new batch of instances'''
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
# neocortix modules
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
    simulationClass = None

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
        simulationClass = None
        if self.simulationClass:
            dottedClass = self.simulationClass.replace( '/', '.' )
            simulationClass = dottedClass
        elif self.scriptFilePath:
            dottedPath = self.scriptFilePath.replace( '/', '.' )
            simulationClass = removeSuffix( dottedPath, '.scala' )
        if not simulationClass:
            raise ValueError( 'no Gatling simulation class was specified' )

        resourcesDirPath = self.workerDirPath + '/resources'
        if not os.path.isdir( resourcesDirPath ):
            resourcesDirPath = self.workerDirPath
        cmd = 'JAVA_OPTS="-Dgatling.ssl.useOpenSsl=false -Dgatling.data.console.light=true" ~/gatling-charts-highcharts-bundle-%s/bin/gatling.sh -nr --simulation %s -sf ~/%s -rsf %s -rf ~/gatlingResults_%03d' % (
            gatlingVersion, simulationClass, self.workerDirPath, resourcesDirPath, frameNum
        )
        return cmd

def getSupportedVersions():
    minGVersion = '3.4.0'  # '3.1.2' is the first 3.x version from 2021, but versions below 3.4.0 don't work well

    import packaging.version
    pvp = packaging.version.parse  # an alias for the long function name
    minPv = pvp( minGVersion )

    resp = requests.get( 'https://api.github.com/repos/gatling/gatling/tags' )
    #resp = requests.get( 'https://api.github.com/repos/gatling/gatling/releases' ) # has not worked
    respJson = resp.json()
    availableVersions = [rel['name'].lstrip('v') for rel in respJson]
    logger.debug( 'availableVersions: %s', availableVersions )
    supportedVersions = [vers for vers in availableVersions if pvp(vers) >= minPv and 'M' not in vers ]
    return supportedVersions


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
    default = '{ "regions": ["usa", "india"], "dar": ">= 99", "dpr": ">=48", "ram": ">=3800000000", "storage": ">=2000000000" }'
    )
ap.add_argument( '--scriptFile', required=False, help='the Gatling test script relative file path' )
ap.add_argument( '--simulationClass', help='the name of the gatling simulation class (if different from script file name)' )
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
ap.add_argument( '--supportedVersions', action='store_true', help='to list supported versions and exit' )
ap.add_argument( '--gatlingBinPath', help='path to the local gatling.sh for aggregating results' )
args = ap.parse_args()

supportedVersions = getSupportedVersions()
logger.debug( 'supportedVersions: %s', supportedVersions )
if args.supportedVersions:
    print( json.dumps( supportedVersions ) )
    sys.exit( 0 )

gatlingVersion = args.gatlingVersion
if gatlingVersion not in supportedVersions:
    logger.error( 'Gatling version %s is not supported', gatlingVersion )
    logger.info( 'these versions are supported: %s', supportedVersions )
    sys.exit( 1 )

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


if not args.scriptFile and not args.simulationClass:
    logger.error( 'please pass --scriptFile or --simulationClass' )
    sys.exit(1)

if args.scriptFile:
    scriptFilePath = args.scriptFile.replace( '\\', '/' )
    scriptFullerPath = os.path.join( workerDirPath, scriptFilePath )
    if not os.path.isfile( scriptFullerPath ):
        logger.error( 'the script file "%s" was not found in %s', scriptFilePath, workerDirPath )
        sys.exit( 1 )
    gatlingFrameProcessor.scriptFilePath = scriptFilePath
    logger.debug( 'using scriptFile "%s"', scriptFilePath )

if args.simulationClass:
    gatlingFrameProcessor.simulationClass = args.simulationClass
    logger.debug( 'using simulationClass "%s"', args.simulationClass )

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
