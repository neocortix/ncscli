#!/usr/bin/env python3
import datetime
import logging
import sys

import ncscli.batchRunner as batchRunner


class locustFrameProcessor(batchRunner.frameProcessor):
    '''defines details for using Locust for a simplistic load test'''

    def installerCmd( self ):
        return 'curl -L https://github.com/locustio/locust/archive/0.12.2.tar.gz > locust.tar.gz && tar -xf locust.tar.gz && mv locust-0.12.2/ locust'

    def frameOutFileName( self, frameNum ):
        return 'worker_%03d_requests.csv' % frameNum

    def frameCmd( self, frameNum ):
        usersPerWorker = 20  # number of simulated users per worker instance
        rampUpRate = 5  # number of simulated users to spawn per second
        duration = 60  # number of seconds to run the test
        csvSpec = '--csv ~/worker_%03d' % frameNum
        cmd = 'cd locustWorker && python3 -u ./runLocustWorker.py --host=https://loadtest-target.neocortix.com %s --exit-code-on-error 0 --no-web -c %d -r %d --run-time %d' % (
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
        commonInFilePath = 'locustWorker/',
        authToken = 'YourAuthTokenHere',
        encryptFiles=False,
        timeLimit = 14*60,
        frameTimeLimit = 240,
        filter = '{"dpr": ">=24"}',
        outDataDir = outDataDir,
        startFrame = 1,
        endFrame = 5,
        nWorkers = 6,
        autoscaleMax = 2
    )

    sys.exit( rc )
except KeyboardInterrupt:
    logger.warning( 'an interuption occurred')
