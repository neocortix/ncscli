#!/usr/bin/env python3
import datetime
import logging
import os
import subprocess
import sys

import ncscli.batchRunner as batchRunner


class taurusFrameProcessor(batchRunner.frameProcessor):
    '''defines details for using taurus for a simplistic load test'''

    def installerCmd( self ):
        cmd = 'sudo apt-get -qq update'
        cmd += ' && sudo apt-get -qq install build-essential'
        cmd += ' && sudo apt-get -qq install python3-dev'
        cmd += ' && python3 -m pip install --user numpy==1.19.4 bzt'
        cmd += ' && cd taurusWorker'
        cmd += ' && patch -b -p4 ../.local/lib/python3.7/site-packages/bzt/modules/monitoring.py < monitoring.patch'
        cmd += ' && bzt warmup.yml'
        return cmd

    def frameOutFileName( self, frameNum ):
        return 'artifacts_%03d' % frameNum

    def frameCmd( self, frameNum ):
        configFileName = 'test.yml'  # substitute your own file name here (must be in taurusWorker dir)
        cmd = 'cd taurusWorker && ~/.local/bin/bzt -o settings.artifacts-dir=~/artifacts_%03d %s' % (
            frameNum, configFileName
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
outDataDir = 'data/taurus_' + dateTimeTag

try:
    rc = batchRunner.runBatch(
        frameProcessor = taurusFrameProcessor(),
        commonInFilePath = 'taurusWorker',
        authToken = os.getenv('NCS_AUTH_TOKEN') or 'YourAuthTokenHere',
        encryptFiles=False,
        timeLimit = 80*60,
        instTimeLimit = 15*60,
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
        rc2 = subprocess.call( ['./plotTaurusOutput.py', '--dataDirPath', outDataDir],
            stdout=subprocess.DEVNULL )
        if rc2:
            logger.warning( 'plotTaurusOutput.py exited with returnCode %d', rc2 )

    sys.exit( rc )
except KeyboardInterrupt:
    logger.warning( 'an interuption occurred')
