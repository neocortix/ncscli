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
        return 'gatlingWorker/install.sh'

    def frameOutFileName( self, frameNum ):
        return 'gatlingResults_%03d/' % frameNum

    def frameCmd( self, frameNum ):
        # you can modify the gatlingWorker/ncsSim.scala file to change details of the test
        cmd = '~/gatling-charts-highcharts-bundle-3.4.0/bin/gatling.sh -nr --simulation neocortix.ncsSim -sf ~/gatlingWorker -rf ~/gatlingResults_%03d' % (
            frameNum
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

dateTimeTag = datetime.datetime.now().strftime( '%Y-%m-%d_%H%M%S' )
outDataDir = 'data/gatling_' + dateTimeTag

try:
    rc = batchRunner.runBatch(
        frameProcessor = gatlingFrameProcessor(),
        commonInFilePath = 'gatlingWorker',
        authToken = os.getenv('NCS_AUTH_TOKEN') or 'YourAuthTokenHere',
        encryptFiles=False,
        timeLimit = 80*60,
        instTimeLimit = 12*60,
        frameTimeLimit = 12*60,
        filter = '{"dpr": ">=48","ram:":">=2800000000","app-version": ">=2.1.11"}',
        outDataDir = outDataDir,
        limitOneFramePerWorker = True,
        autoscaleMax = 2,
        startFrame = 1,
        endFrame = 6,
        nWorkers = 10
    )
    if os.path.isfile( outDataDir +'/recruitLaunched.json' ):
        # plot output (requires matplotlib)
        rc2 = subprocess.call( ['./plotGatlingOutput.py', '--dataDirPath', outDataDir],
            stdout=subprocess.DEVNULL )
        if rc2:
            logger.warning( 'plotGatlingOutput.py exited with returnCode %d', rc2 )
        # report aggregated output (requires gatling)
        gatlingBinPath = 'gatling-3.4.0/bin/gatling.sh'
        if os.path.isfile( gatlingBinPath ):
            rc2 = subprocess.call( ['./reportGatlingOutput.py', '--dataDirPath', outDataDir],
                stdout=subprocess.DEVNULL )
            if rc2:
                logger.warning( 'reportGatlingOutput.py exited with returnCode %d', rc2 )

    sys.exit( rc )
except KeyboardInterrupt:
    logger.warning( 'an interuption occurred')
