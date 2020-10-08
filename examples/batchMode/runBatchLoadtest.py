#!/usr/bin/env python3
import datetime
import logging
import os
import subprocess
import sys

import ncscli.batchRunner as batchRunner


class locustFrameProcessor(batchRunner.frameProcessor):
    '''defines details for using Locust for a simplistic load test'''

    def installerCmd( self ):
        return 'curl -L https://github.com/locustio/locust/archive/0.12.2.tar.gz > locust.tar.gz && tar -xf locust.tar.gz && mv locust-0.12.2/ locust'

    def frameOutFileName( self, frameNum ):
        return 'worker_%03d_*.csv' % frameNum
        #return 'worker_%03d_requests.csv' % frameNum

    def frameCmd( self, frameNum ):
        usersPerWorker = 6  # number of simulated users per worker instance
        rampUpRate = 1  # number of simulated users to spawn per second (can be fractional)
        duration = 60  # number of seconds to run the test (must be integer)
        csvSpec = '--csv ~/worker_%03d' % frameNum
        cmd = 'cd locustWorker && python3 -u ./runLocustWorker.py --host=https://loadtest-target.neocortix.com %s --only-summary --exit-code-on-error 0 --no-web -c %d -r %f --run-time %d' % (
            csvSpec, usersPerWorker, rampUpRate, duration
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
outDataDir = 'data/loadtest_' + dateTimeTag

try:
    rc = batchRunner.runBatch(
        frameProcessor = locustFrameProcessor(),
        commonInFilePath = 'locustWorker',
        authToken = os.getenv('NCS_AUTH_TOKEN') or 'YourAuthTokenHere',
        encryptFiles=False,
        timeLimit = 14*60,
        frameTimeLimit = 240,
        filter = '{"dpr": ">=48","ram:":">=2800000000","app-version": ">=2.1.11"}',
        outDataDir = outDataDir,
        startFrame = 1,
        endFrame = 5,
        nWorkers = 6,
        limitOneFramePerWorker = True,
        autoscaleMax = 2
    )
    if os.path.isfile( outDataDir +'/recruitLaunched.json' ):
        rc2 = subprocess.call( ['./plotLocustOutput.py', '--dataDirPath', outDataDir],
            stdout=subprocess.DEVNULL )
        if rc2:
            logger.warning( 'plotLocustOutput.py exited with returnCode %d', rc2 )

    sys.exit( rc )
except KeyboardInterrupt:
    logger.warning( 'an interuption occurred')
