#!/usr/bin/env python3
import datetime
import logging
import os
import subprocess
import sys

import ncscli.batchRunner as batchRunner


class JMeterFrameProcessor(batchRunner.frameProcessor):
    '''defines details for using JMeter for a simplistic load test'''

    def installerCmd( self ):
        cmd = 'curl -s -S -L https://mirrors.sonic.net/apache/jmeter/binaries/apache-jmeter-5.3.tgz > apache-jmeter-5.3.tgz && tar zxf apache-jmeter-5.3.tgz'
        # alternativey, could use https://mirror.olnevhost.net/pub/apache/... or https://downloads.apache.org/...
        #cmd += ' && curl -s -S -L https://github.com/dynatrace-oss/jmeter-dynatrace-plugin/releases/download/v1.3.snapshot/jmeter-dynatrace-plugin-1.3-SNAPSHOT.jar > jmeter-dynatrace-plugin-1.3-SNAPSHOT.jar'
        cmd += ' && curl -s -S -L https://ltmaster-1.neocortix.com/dynatrace/jmeter-dynatrace-plugin-1.3-ncs.jar > jmeter-dynatrace-plugin-1.3-ncs.jar'
        cmd += ' && cp -p jmeter-dynatrace-plugin-1.3-ncs.jar apache-jmeter-5.3/lib/ext'
        return cmd

    JMeterFilePath = 'TestPlan_dynatrace.jmx'
    #JMeterFilePath = 'TestPlan_dynatrace_RampLong.jmx'

    def frameOutFileName( self, frameNum ):
        return 'TestPlan_results_%03d.csv' % frameNum
        #return 'jmeter_%03d.log' % frameNum

    def frameCmd( self, frameNum ):
        cmd = 'date && apache-jmeter-5.3/bin/jmeter -n -t %s -l TestPlan_results_%03d.csv -D httpclient4.time_to_live=20000 -D httpclient.reset_state_on_thread_group_iteration=true' % (
            self.JMeterFilePath, frameNum
        )
        cmd += ' && cp -p jmeter.log jmeter_%03d.log' % frameNum
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
outDataDir = 'data/dynatrace_' + dateTimeTag

try:
    rc = batchRunner.runBatch(
        frameProcessor = JMeterFrameProcessor(),
        commonInFilePath = JMeterFrameProcessor.JMeterFilePath,
        authToken = os.getenv( 'NCS_AUTH_TOKEN' ) or 'YourAuthTokenHere',
        encryptFiles=False,
        timeLimit = 80*60,
        instTimeLimit = 12*60,
        frameTimeLimit = 30*60,
        filter = '{"dpr": ">=48", "ram":">=2800000000", "app-version": ">=2.1.11"}',
        outDataDir = outDataDir,
        startFrame = 1,
        endFrame = 6,
        nWorkers = 10,
        limitOneFramePerWorker = True,
        autoscaleMax = 2
    )
    if (rc == 0) and os.path.isfile( outDataDir +'/recruitLaunched.json' ):
        rc2 = subprocess.call( [sys.executable, 'plotJMeterOutput.py', '--dataDirPath', outDataDir],
            stdout=subprocess.DEVNULL )
        if rc2:
            logger.warning( 'plotJMeterOutput exited with returnCode %d', rc2 )

    sys.exit( rc )
except KeyboardInterrupt:
    logger.warning( 'an interuption occurred')
