#!/usr/bin/env python3
import datetime
import glob
import logging
import os
import subprocess
import sys
import tarfile

import ncscli.batchRunner as batchRunner


class PuppeteerLighthouseFrameProcessor(batchRunner.frameProcessor):
    '''defines details for using Puppeteer and Lighthouse to analyze a web page from multiple devices'''

    def installerCmd( self ):
        return 'apt update && apt install -y chromium nodejs npm && ln -s chromium /usr/bin/chromium-browser && PUPPETEER_SKIP_DOWNLOAD=yes npm install -g puppeteer && npm install -g lighthouse'

    PuppeteerFilePath = 'Puppeteer.js'

    def frameOutFileName( self, frameNum ):
        return 'Puppeteer_results_%03d.tar.gz' % frameNum

    def frameCmd( self, frameNum ):
        cmd = 'date && export NODE_PATH=/usr/local/lib/node_modules && export PATH=$PATH:/usr/local/bin && node %s && lighthouse https://www.google.com --no-enable-error-reporting --chrome-flags="--headless --no-sandbox" --emulated-form-factor=none --throttling-method=provided && mv google.png google_%03d.png && mv *google*.html google_%03d.html && tar -zcvf Puppeteer_results_%03d.tar.gz google*' % (
            self.PuppeteerFilePath, frameNum, frameNum, frameNum
        )
        return cmd

def untarResults( outDataDir ):
    tarFilePaths = glob.glob( outDataDir+'/Puppeteer_results_*.tar.gz' )
    for tarFilePath in tarFilePaths:
        with tarfile.open( tarFilePath, 'r' ) as tarFile:
            tarFile.extractall( path=outDataDir )


# configure logger formatting
#logging.basicConfig()
logger = logging.getLogger(__name__)
logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
logDateFmt = '%Y/%m/%d %H:%M:%S'
formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
logging.basicConfig(format=logFmt, datefmt=logDateFmt)
#batchRunner.logger.setLevel(logging.DEBUG)  # for more verbosity

dateTimeTag = datetime.datetime.now().strftime( '%Y-%m-%d_%H%M%S' )
outDataDir = 'data/puppeteer_' + dateTimeTag

try:
    rc = batchRunner.runBatch(
        frameProcessor = PuppeteerLighthouseFrameProcessor(),
        commonInFilePath = PuppeteerLighthouseFrameProcessor.PuppeteerFilePath,
        authToken = os.getenv( 'NCS_AUTH_TOKEN' ) or 'YourAuthTokenHere',
        encryptFiles=False,
        timeLimit = 80*60,
        instTimeLimit = 24*60,
        frameTimeLimit = 600,
        filter = '{"dpr": ">=48","ram:":">=2800000000","app-version": ">=2.1.11"}',
        outDataDir = outDataDir,
        startFrame = 1,
        endFrame = 5,
        nWorkers = 10,
        limitOneFramePerWorker = True,
        autoscaleMax = 2
    )
    if os.path.isfile( outDataDir +'/recruitLaunched.json' ):
        untarResults( outDataDir )
        rc2 = subprocess.call( ['./processPuppeteerOutput.py', '--dataDirPath', outDataDir],
            stdout=subprocess.DEVNULL )
        if rc2:
            logger.warning( 'processPuppeteerOutput exited with returnCode %d', rc2 )
    sys.exit( rc )
except KeyboardInterrupt:
    logger.warning( 'an interuption occurred')
