#!/usr/bin/env python3
import datetime
import logging
import os
import subprocess
import sys

import ncscli.batchRunner as batchRunner


jmeterVersion = '5.4.1'  # 5.3 and 5.4.1 have been tested, others may work as well

class JMeterFrameProcessor(batchRunner.frameProcessor):
    '''defines details for using JMeter for a simplistic load test'''

    workerDirPath = 'jmeterWorker'
    #JMeterFilePath = workerDirPath+'/TestPlan.jmx'
    JMeterFilePath = workerDirPath+'/JImageUpload.jmx'
    JVM_ARGS ='-Xms30m -Xmx212m -XX:MaxMetaspaceSize=64m -Dnashorn.args=--no-deprecation-warning'

    def installerCmd( self ):
        cmd = 'free --mega -t 1>&2'  # to show amount of free ram
        cmd += ' && cp -p %s/*.jar /opt/apache-jmeter/lib/ext' % self.workerDirPath  # for plugins
        cmd += ' && JVM_ARGS="%s" /opt/apache-jmeter/bin/jmeter.sh --version' % self.JVM_ARGS

        # tougher pretest
        pretestFilePath = self.workerDirPath+'/pretest.jmx'
        if os.path.isfile( pretestFilePath ):
            cmd += ' && /opt/apache-jmeter/bin/jmeter -n -t %s -l jmeterOut/pretest_results.csv -D httpclient4.time_to_live=1 -D httpclient.reset_state_on_thread_group_iteration=true' % (
                pretestFilePath
            )
        return cmd


    def frameOutFileName( self, frameNum ):
        return 'jmeterOut_%03d' % frameNum
        #return 'TestPlan_results_%03d.csv' % frameNum

    def frameCmd( self, frameNum ):
        cmd = 'mkdir -p jmeterOut && JVM_ARGS="%s" /opt/apache-jmeter/bin/jmeter.sh -n -t %s -l jmeterOut/TestPlan_results.csv -D httpclient4.time_to_live=1 -D httpclient.reset_state_on_thread_group_iteration=true' % (
            self.JVM_ARGS, self.JMeterFilePath
        )
        cmd += ' && mv jmeterOut %s' % (self.frameOutFileName( frameNum ))
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
outDataDir = 'data/imageUpload_' + dateTimeTag

try:
    rc = batchRunner.runBatch(
        frameProcessor = JMeterFrameProcessor(),
        commonInFilePath = JMeterFrameProcessor.workerDirPath,
        authToken = os.getenv( 'NCS_AUTH_TOKEN' ) or 'YourAuthTokenHere',
        encryptFiles=False,
        timeLimit = 60*60,
        instTimeLimit = 12*60,
        frameTimeLimit = 14*60,
        filter = '{ "regions": ["usa", "india"], "dar": "==100", "dpr": ">=48", "ram": ">=3800000000", "storage": ">=2000000000" }',
        #filter = '{ "regions": ["usa", "india"], "dar": "==100", "dpr": ">=48", "ram:": ">=5800000000", "app-version": ">=2.1.11" }',
        outDataDir = outDataDir,
        startFrame = 1,
        endFrame = 6,
        nWorkers = 10,
        limitOneFramePerWorker = True,
        autoscaleMax = 2
    )
    if (rc == 0) and os.path.isfile( outDataDir +'/recruitLaunched.json' ):
        rampStepDuration = 60
        SLODuration = 240
        SLOResponseTimeMax = 2.5

        rc2 = subprocess.call( [sys.executable, 'plotJMeterOutput.py',
            '--dataDirPath', outDataDir,
            '--rampStepDuration', str(rampStepDuration), '--SLODuration', str(SLODuration),
            '--SLOResponseTimeMax', str(SLOResponseTimeMax)
            ],
            stdout=subprocess.DEVNULL )
        if rc2:
            logger.warning( 'plotJMeterOutput exited with returnCode %d', rc2 )
 
        jtlFileName = 'VRT.jtl'  # make this match output file name from the .jmx (or empty if none)
        if jtlFileName:
            nameParts = os.path.splitext(jtlFileName)
            mergedJtlFileName = nameParts[0]+'_merged_' + dateTimeTag + nameParts[1]
            rc2 = subprocess.call( [sys.executable, 'mergeBatchOutput.py',
                '--dataDirPath', outDataDir,
                '--csvPat', 'jmeterOut_%%03d/%s' % jtlFileName,
                '--mergedCsv', mergedJtlFileName
                ], stdout=subprocess.DEVNULL
                )
            if rc2:
                logger.warning( 'mergeBatchOutput.py exited with returnCode %d', rc2 )
            else:
                jmeterBinFilePath = 'apache-jmeter-%s/bin/jmeter.sh' % jmeterVersion
                if not os.path.isfile( jmeterBinFilePath ):
                    logger.info( 'no jmeter installed for producing reports (%s)', jmeterBinFilePath )
                else:
                    rcx = subprocess.call( [jmeterBinFilePath,
                        '-g', os.path.join( outDataDir, mergedJtlFileName ),
                        '-o', os.path.join( outDataDir, 'htmlReport' )
                        ], stderr=subprocess.DEVNULL
                    )
                    if rcx:
                        logger.warning( 'jmeter reporting exited with returnCode %d', rcx )
    sys.exit( rc )
except KeyboardInterrupt:
    logger.warning( 'an interuption occurred')
