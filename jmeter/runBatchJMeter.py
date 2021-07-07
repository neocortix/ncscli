#!/usr/bin/env python3
import argparse
import datetime
import logging
import os
import subprocess
import sys

import ncscli.batchRunner as batchRunner
import jmxTool  # assumed to be in the same dir as this script


logger = logging.getLogger(__name__)


class JMeterFrameProcessor(batchRunner.frameProcessor):
    '''defines details for using JMeter for a simplistic load test'''

    def installerCmd( self ):
        cmd = 'free --mega -t 1>&2'
        cmd += ' && JVM_ARGS="%s" /opt/apache-jmeter/bin/jmeter.sh --version' % self.JVM_ARGS
        return cmd
        # could install an alternative version of jmeter, if the preinstalled version is not wanted
        '''
        jmeterVersion = '5.4.1'  # 5.3 and 5.4.1 have been tested, others may work as well
        cmd = 'curl -s -S -L https://mirrors.sonic.net/apache/jmeter/binaries/apache-jmeter-%s.tgz > apache-jmeter.tgz' % jmeterVersion
        cmd += ' && tar zxf apache-jmeter.tgz'
        return cmd
        # alternatively, could use https://mirror.olnevhost.net/pub/apache/... or https://downloads.apache.org/...
        '''
    JMeterFilePath = 'TestPlan.jmx'
    #JMeterFilePath = 'TestPlan_RampLong.jmx'
    #JMeterFilePath = 'TestPlan_RampLong_MoreSlow.jmx'
    #JMeterFilePath = 'TestPlan_RampLong_LessSlow.jmx'
    JVM_ARGS ='-Xms20m -Xmx212m -XX:MaxMetaspaceSize=64m -Dnashorn.args=--no-deprecation-warning'


    def frameOutFileName( self, frameNum ):
        return 'TestPlan_results_%03d.csv' % frameNum

    def frameCmd( self, frameNum ):
        cmd = 'JVM_ARGS="%s" /opt/apache-jmeter/bin/jmeter.sh -n -t %s -l TestPlan_results_%03d.csv -D httpclient4.time_to_live=1 -D httpclient.reset_state_on_thread_group_iteration=true' % (
            self.JVM_ARGS, self.JMeterFilePath, frameNum
        )
        return cmd


# configure logger formatting
#logging.basicConfig()
logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
logDateFmt = '%Y/%m/%d %H:%M:%S'
formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
logging.basicConfig(format=logFmt, datefmt=logDateFmt)
logger.setLevel(logging.INFO)
batchRunner.logger.setLevel(logging.INFO)  # for more verbosity

# this variable can be overriden by --nInstances
nInstances = 6

ap = argparse.ArgumentParser( description=__doc__,
    fromfile_prefix_chars='@', formatter_class=argparse.ArgumentDefaultsHelpFormatter )
ap.add_argument( '--authToken', help='the NCS authorization token to use' )
ap.add_argument( '--dataDirPrefix', default='data/jmeter_', help='partial prefix for the output data dir' )
ap.add_argument( '--filter', help='json to filter instances for launch',
    default ='{"regions": ["usa"], "dpr": ">=48", "dar": "==100", "ram":">=3800000000"}'
    )
ap.add_argument( '--jmxFile', help='the JMeter test plan file path',
    default=JMeterFrameProcessor.JMeterFilePath
    )
ap.add_argument( '--nInstances', type=int, default=nInstances, help='the number of Load-generating instances' )
# for analysis and plotting
ap.add_argument( '--rampStepDuration', type=float, default=60, help='duration, in seconds, of ramp step' )
ap.add_argument( '--SLODuration', type=float, default=240, help='SLO duration, in seconds' )
ap.add_argument( '--SLOResponseTimeMax', type=float, default=2.5, help='SLO RT threshold, in seconds' )
args = ap.parse_args()

jmxFilePath = args.jmxFile
if not os.path.isfile( jmxFilePath ):
    logger.error( 'the jmx file "%s" was not found', jmxFilePath )
    sys.exit( 1 )
logger.debug( 'using test plan "%s"', jmxFilePath )

jmxTree = jmxTool.parseJmxFile( jmxFilePath )
jmxDur = jmxTool.getDuration( jmxTree )
logger.debug( 'jmxDur: %s seconds', jmxDur )

frameTimeLimit = max( round( jmxDur * 1.25 ), jmxDur+6*60 ) # some slop beyond the planned duration

JMeterFrameProcessor.JMeterFilePath = jmxFilePath

nFrames = args.nInstances
nWorkersToLaunch = round( nFrames * 1.5 )

dateTimeTag = datetime.datetime.now().strftime( '%Y-%m-%d_%H%M%S' )
outDataDir = args.dataDirPrefix + dateTimeTag

perfMonHost = None
#perfMonHost = 'loadtest-target.neocortix.com'


try:
    monProc = None
    if perfMonHost:
        perfmonOutFilePath = os.path.join( outDataDir, 'perfmonOut.csv' )
        monProc = subprocess.Popen( [sys.executable, 'perfmonClient.py',
            '--outFilePath', perfmonOutFilePath, '--pmHost', perfMonHost,
            '--pmPort', str(4444)
            ])
    rc = batchRunner.runBatch(
        frameProcessor = JMeterFrameProcessor(),
        commonInFilePath = JMeterFrameProcessor.JMeterFilePath,
        authToken = args.authToken or os.getenv( 'NCS_AUTH_TOKEN' ) or 'YourAuthTokenHere',
        encryptFiles=False,
        timeLimit = frameTimeLimit + 40*60,
        instTimeLimit = 6*60,
        frameTimeLimit = frameTimeLimit,
        filter = args.filter,
        outDataDir = outDataDir,
        startFrame = 1,
        endFrame = nFrames,
        nWorkers = nWorkersToLaunch,
        limitOneFramePerWorker = True,
        autoscaleMax = 2
    )
    if monProc:
        monProc.terminate()
    if (rc == 0) and os.path.isfile( outDataDir +'/recruitLaunched.json' ):
        rampStepDuration = args.rampStepDuration
        SLODuration = args.SLODuration
        SLOResponseTimeMax = args.SLOResponseTimeMax

        rc2 = subprocess.call( [sys.executable, 'plotJMeterOutput.py',
            '--dataDirPath', outDataDir,
            '--rampStepDuration', str(rampStepDuration), '--SLODuration', str(SLODuration),
            '--SLOResponseTimeMax', str(SLOResponseTimeMax)
            ],
            stdout=subprocess.DEVNULL )
        if rc2:
            logger.warning( 'plotJMeterOutput exited with returnCode %d', rc2 )
    sys.exit( rc )
except KeyboardInterrupt:
    logger.warning( 'an interuption occurred')
