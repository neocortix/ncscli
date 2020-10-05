#!/usr/bin/env python3
import datetime
import logging
import os
import subprocess
import sys

import ncscli.batchRunner as batchRunner


class k6FrameProcessor(batchRunner.frameProcessor):
    '''defines details for using k6 for a simplistic load test'''

    def frameOutFileName( self, frameNum ):
        return 'worker_%03d.*' % frameNum
        #return 'worker_%03d.csv' % frameNum

    def frameCmd( self, frameNum ):
        usersPerWorker = 6  # number of simulated users per worker instance
        duration = 90  # number of seconds to run the test
        cmd = 'cd k6Worker && ./k6 run -q script.js --vus %d --duration %.1fs --out csv=~/worker_%03d.csv --out json=~/worker_%03d.json' % (
            usersPerWorker, duration, frameNum, frameNum
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
outDataDir = 'data/k6_' + dateTimeTag

if not os.path.isfile( 'k6Worker/k6' ):
    logger.error( 'the k6 Arm-64 binary was not found, you may need to build it using "go get"' )
    sys.exit( 1)
try:
    rc = batchRunner.runBatch(
        frameProcessor = k6FrameProcessor(),
        commonInFilePath = 'k6Worker',
        authToken = os.getenv('NCS_AUTH_TOKEN') or 'YourAuthTokenHere',
        encryptFiles=False,
        timeLimit = 80*60,
        instTimeLimit = 8*60,
        frameTimeLimit = 10*60,
        filter = '{"dpr": ">=48","ram:":">=2800000000","app-version": ">=2.1.11"}',
        outDataDir = outDataDir,
        startFrame = 1,
        endFrame = 6,
        nWorkers = 10,
        limitOneFramePerWorker = True,
        autoscaleMax = 2
    )
    if os.path.isfile( outDataDir +'/recruitLaunched.json' ):
        rc2 = subprocess.call( ['./plotK6Output.py', '--dataDirPath', outDataDir],
            stdout=subprocess.DEVNULL )
        if rc2:
            logger.warning( 'plotK6Output.py exited with returnCode %d', rc2 )

    sys.exit( rc )
except KeyboardInterrupt:
    logger.warning( 'an interuption occurred')
