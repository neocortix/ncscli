#!/usr/bin/env python3
import datetime
import logging
import os
import subprocess
import sys

import ncscli.batchRunner as batchRunner


class taurusFrameProcessor(batchRunner.frameProcessor):
    '''defines details for using taurus for a simplistic load test'''
    # disable not-fully-working code for using preinstalled jmeter and installing plugins
    usePreinstalled = False

    def installerCmd( self ):
        cmd = 'sudo apt-get -qq update'
        cmd += ' && sudo apt-get -qq -y install build-essential > /dev/null'
        cmd += ' && sudo apt-get -qq -y install python3-dev > /dev/null'
        cmd += ' && python3 -m pip install --user --quiet numpy>=1.19.4 bzt>=1.15.4'
        cmd += ' && cd taurusWorker'
        if self.usePreinstalled:
            cmd += ' && cp -p PluginsManagerCMD.sh /opt/apache-jmeter/bin'
            cmd += ' && cp -p cmdrunner-*.jar /opt/apache-jmeter/lib'
            cmd += ' && cp -p jmeter-plugins-manager-*.jar /opt/apache-jmeter/lib/ext'
            cmd += ' && bzt -o modules.jmeter.path=/opt/apache-jmeter/bin/jmeter.sh -o modules.jmeter.version=5.4.1 warmup.yml'
        else:
            cmd += ' && bzt warmup.yml'
        return cmd

    def frameOutFileName( self, frameNum ):
        return 'artifacts_%03d' % frameNum

    def frameCmd( self, frameNum ):
        configFileName = 'test.yml'  # substitute your own file name here (must be in taurusWorker dir)
        if self.usePreinstalled:
            cmd = 'cd taurusWorker && ~/.local/bin/bzt -o modules.jmeter.path=/opt/apache-jmeter/bin/jmeter.sh -o modules.jmeter.version=5.4.1 -o settings.artifacts-dir=~/artifacts_%03d %s' % (
                frameNum, configFileName
            )
        else:
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
        filter = '{ "regions": ["usa", "india"], "dar": "==100", "dpr": ">=48", "ram": ">=3800000000", "storage": ">=2000000000" }',
        outDataDir = outDataDir,
        startFrame = 1,
        endFrame = 6,
        nWorkers = 10,
        limitOneFramePerWorker = True,
        autoscaleMax = 2
    )
    if os.path.isfile( outDataDir +'/recruitLaunched.json' ):
        rc2 = subprocess.call( [sys.executable, 'plotTaurusOutput.py', '--dataDirPath', outDataDir],
            stdout=subprocess.DEVNULL )
        if rc2:
            logger.warning( 'plotTaurusOutput.py exited with returnCode %d', rc2 )

    sys.exit( rc )
except KeyboardInterrupt:
    logger.warning( 'an interuption occurred')
