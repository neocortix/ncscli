#!/usr/bin/env python3
import datetime
import logging
import os
import subprocess
import sys

import ncscli.batchRunner as batchRunner


class gatlingFrameProcessor(batchRunner.frameProcessor):
    '''defines details for using gatling for a simplistic load test'''

    def installerCmd( self ):
        return 'gatlingWorker/installGatling.sh %s' % gatlingVersion

    def frameOutFileName( self, frameNum ):
        return 'gatlingResults_%03d' % frameNum

    def frameCmd( self, frameNum ):
        # substitute your own gatling simulation class, and put the scala file in the gatlingWorker dir
        # -or- modify the provided gatlingWorker/ncsSim.scala file to change details of the test
        simulationClass = 'neocortix.ncsSim'
        cmd = 'JAVA_OPTS="-Dgatling.ssl.useOpenSsl=false -Dgatling.data.console.light=true" ~/gatling-charts-highcharts-bundle-%s/bin/gatling.sh -nr --simulation %s -sf ~/gatlingWorker -rf ~/gatlingResults_%03d' % (
            gatlingVersion, simulationClass, frameNum
        )
        return cmd


# configure logger formatting
#logging.basicConfig()
logger = logging.getLogger(__name__)
logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
logDateFmt = '%Y/%m/%d %H:%M:%S'
formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
logging.basicConfig(format=logFmt, datefmt=logDateFmt)
#batchRunner.logger.setLevel(logging.DEBUG)  # for more verbosity

gatlingVersion = '3.6.1'

dateTimeTag = datetime.datetime.now().strftime( '%Y-%m-%d_%H%M%S' )
outDataDir = 'data/gatling_' + dateTimeTag

try:
    rc = batchRunner.runBatch(
        frameProcessor = gatlingFrameProcessor(),
        commonInFilePath = 'gatlingWorker',
        pushDeviceLocs=True,
        authToken = os.getenv('NCS_AUTH_TOKEN') or 'YourAuthTokenHere',
        encryptFiles=False,
        timeLimit = 89*60,
        instTimeLimit = 12*60,
        frameTimeLimit = 15*60,
        filter = '{ "regions": ["usa", "india"], "dar": ">= 99", "dpr": ">=48", "ram": ">=3800000000", "storage": ">=2000000000" }',
        outDataDir = outDataDir,
        limitOneFramePerWorker = True,
        autoscaleMax = 2,
        startFrame = 1,
        endFrame = 6,
        nWorkers = 10
    )
    if os.path.isfile( outDataDir +'/recruitLaunched.json' ):
        # plot output (requires matplotlib)
        rc2 = subprocess.call( [sys.executable, 'plotGatlingOutput.py', '--dataDirPath', outDataDir],
            stdout=subprocess.DEVNULL )
        if rc2:
            logger.warning( 'plotGatlingOutput.py exited with returnCode %d', rc2 )
        # report aggregated output (requires gatling)
        gatlingBinPath = 'gatling-charts-highcharts-bundle-%s/bin/gatling.sh' % (gatlingVersion)
        if os.path.isfile( gatlingBinPath ):
            rc2 = subprocess.call( [sys.executable, 'reportGatlingOutput.py',
                    '--dataDirPath', outDataDir, '--gatlingBinPath', gatlingBinPath],
                stdout=subprocess.DEVNULL )
            if rc2:
                logger.warning( 'reportGatlingOutput.py exited with returnCode %d', rc2 )

    sys.exit( rc )
except KeyboardInterrupt:
    logger.warning( 'an interuption occurred')
