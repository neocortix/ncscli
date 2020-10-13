#!/usr/bin/env python3
import datetime
import logging
import os
import subprocess
import sys

import ncscli.batchRunner as batchRunner


class k6FrameProcessor(batchRunner.frameProcessor):
    '''defines details for using k6 for a simplistic load test'''

    def installerCmd( self ):
        return 'tar -vzxf k6Worker/k6.tar.gz; ls -al k6Worker'

    def frameOutFileName( self, frameNum ):
        return 'worker_%03d.*' % frameNum
        #return 'worker_%03d.csv' % frameNum

    def frameCmd( self, frameNum ):
        cmd = 'cd k6Worker && ./k6 run -q script.js --out csv=~/worker_%03d.csv' % (
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
outDataDir = 'data/k6_' + dateTimeTag

if not os.path.isfile( 'k6Worker/k6.tar.gz' ):
    logger.error( 'the compressed k6 binary was not found, you may need to build and compress it' )
    sys.exit( 1)
try:
    rc = batchRunner.runBatch(
        frameProcessor = k6FrameProcessor(),
        commonInFilePath = 'k6Worker',
        authToken = os.getenv('NCS_AUTH_TOKEN') or 'YourAuthTokenHere',
        encryptFiles=False,
        timeLimit = 80*60,
        instTimeLimit = 7*60,
        frameTimeLimit = 14*60,
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
