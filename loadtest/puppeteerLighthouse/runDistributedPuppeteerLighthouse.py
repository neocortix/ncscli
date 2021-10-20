#!/usr/bin/env python3
'''runs puppeter and lighthouse on a new batch of ncs SC instances'''
import argparse
import datetime
import glob
import json
import logging
import math
import os
import shutil
import subprocess
import sys
import tarfile
# neocortix modules
import ncscli.batchRunner as batchRunner


logger = logging.getLogger(__name__)

def scriptDirPath():
    '''returns the absolute path to the directory containing this script'''
    return os.path.dirname(os.path.realpath(__file__))

def plotInstanceMap( inFilePath, outFilePath ):
    rc = 2
    # this works on linux but maybe not on windows
    plotterBinPath = shutil.which( 'plotInstanceMap.py' )
    if not plotterBinPath:
        if os.path.isfile( '../ncscli/plotInstanceMap.py' ):
            plotterBinPath = '../ncscli/plotInstanceMap.py'
    if plotterBinPath:
        rc = subprocess.call( [plotterBinPath,
            inFilePath, outFilePath
            ],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL )
        if rc:
            logger.warning( 'plotInstanceMap exited with returnCode %d', rc )
    return rc


class FrameProcessor(batchRunner.frameProcessor):
    '''defines details for using Puppeteer and Lighthouse on a device'''
    workerDirPath = None
    scriptFilePath = None

    def installerCmd( self ):
        cmd = 'apt-get -qq update'  # for default debian install
        #cmd = 'curl -fsSL https://deb.nodesource.com/setup_lts.x | bash -'  # for LTS version
        #cmd = 'curl -fsSL https://deb.nodesource.com/setup_12.x | bash -'  # for old but supported version

        cmd += ' && apt-get install -y nodejs npm'
        cmd += ' && apt-get -qq update > /dev/null && apt-get install -y chromium && ln -s chromium /usr/bin/chromium-browser && PUPPETEER_SKIP_DOWNLOAD=yes npm install --quiet -g puppeteer && npm install --quiet -g lighthouse@6.5.0'
        return cmd
        #return 'apt update && apt install -y chromium nodejs npm && ln -s chromium /usr/bin/chromium-browser && PUPPETEER_SKIP_DOWNLOAD=yes npm install -g puppeteer && npm install -g lighthouse@6.5.0'

    def frameOutFileName( self, frameNum ):
        return 'puppeteerOut_%03d.tar.gz' % frameNum

    def frameCmd( self, frameNum ):
        outDirName = 'puppeteerOut'
        cmd = 'cd %s' % self.workerDirPath
        cmd += ' && mkdir %s' % outDirName
        cmd += ' && export NODE_PATH=/usr/local/lib/node_modules && export PATH=$PATH:/usr/local/bin && node --unhandled-rejections=strict %s %d %s' % (
            self.scriptFilePath, frameNum, args.targetUrl
        )
        outDirNumbered = 'puppeteerOut_%03d' % frameNum
        cmd += ' && mv %s %s' % (outDirName, outDirNumbered )
        cmd += ' && tar -zcf ~/%s %s' % (
            self.frameOutFileName( frameNum ), outDirNumbered
        )
        return cmd

def untarResults( outDataDir ):
    tarFilePaths = glob.glob( outDataDir+'/puppeteerOut_*.tar.gz' )
    for tarFilePath in tarFilePaths:
        with tarfile.open( tarFilePath, 'r' ) as tarFile:
            try:
                tarFile.extractall( path=outDataDir )
            except Exception as exc:
                logger.warning( 'could not untar %s; %s', tarFilePath, exc )


if __name__ == "__main__":
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
    ap.add_argument( '--outDataDir', required=False, help='a path to the output data dir for this run (required)' )
    ap.add_argument( '--filter', help='json to filter instances for launch',
        default = '{ "regions": ["usa", "india"], "dar": ">= 99", "dpr": ">=48", "ram": ">=3800000000", "storage": ">=2000000000" }'
        )
    #ap.add_argument( '--nodeVersion', default='x.x', help='the version of nodejs to run on workers' )
    ap.add_argument( '--planDuration', type=float, default=600, help='the expected duration of the test plan, in seconds' )
    ap.add_argument( '--workerDir', help='the directory to upload to workers',
        default='simpleWorker'
        )
    ap.add_argument( '--scriptFile', help='the nodejs test script (required)' )
    ap.add_argument( '--targetUrl', help='a URL to test against' )
    ap.add_argument( '--nWorkers', type=int, default=6, help='the number of Load-generating workers' )
    #ap.add_argument( '--supportedVersions', action='store_true', help='to list supported versions and exit' )
    ap.add_argument( '--cookie' )
    args = ap.parse_args()

    workerDirPath = args.workerDir.rstrip( '/' )  # trailing slash could cause problems with rsync
    if workerDirPath:
        if not os.path.isdir( workerDirPath ):
            logger.error( 'the workerDirPath "%s" is not a directory', workerDirPath )
            sys.exit( 1 )
        FrameProcessor.workerDirPath = workerDirPath
    else:
        logger.error( 'this version requires a workerDirPath' )
        sys.exit( 1 )
    logger.debug( 'workerDirPath: %s', workerDirPath )

    scriptFilePath = args.scriptFile
    if not scriptFilePath:
        logger.error( 'no --scriptFile was given' )
        sys.exit( 1 )
    if not args.targetUrl:
        logger.error( 'no --targetUrl was given' )
        sys.exit( 1 )

    scriptFullerPath = os.path.join( workerDirPath, scriptFilePath )
    if not os.path.isfile( scriptFullerPath ):
        logger.error( 'the script file "%s" was not found in %s', scriptFilePath, workerDirPath )
        sys.exit( 1 )
    FrameProcessor.scriptFilePath = scriptFilePath
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
        outDataDir = 'data/puppeteer_' + dateTimeTag

    try:
        rc = batchRunner.runBatch(
            frameProcessor = FrameProcessor(),
            commonInFilePath = workerDirPath,
            authToken = os.getenv( 'NCS_AUTH_TOKEN' ) or 'YourAuthTokenHere',
            cookie = args.cookie,
            encryptFiles=False,
            timeLimit = frameTimeLimit + 40*60,
            instTimeLimit = 21*60,
            frameTimeLimit = frameTimeLimit,
            filter = args.filter,
            outDataDir = outDataDir,
            startFrame = 1,
            endFrame = nFrames,
            nWorkers = nWorkers,
            limitOneFramePerWorker = True,
            autoscaleMax = 1
        )
        if rc==0 and os.path.isfile( outDataDir +'/recruitLaunched.json' ):
            untarResults( outDataDir )
            rc2_ = plotInstanceMap( os.path.join( outDataDir, 'recruitLaunched.json' ),
                os.path.join( outDataDir, 'worldMap.png' )
                )
            rc2_ = plotInstanceMap( os.path.join( outDataDir, 'recruitLaunched.json' ),
                os.path.join( outDataDir, 'worldMap.svg' )
                )
        sys.exit( rc )
    except KeyboardInterrupt:
        logger.warning( 'an interuption occurred')
