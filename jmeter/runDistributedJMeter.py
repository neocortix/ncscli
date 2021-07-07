#!/usr/bin/env python3
'''launches instances and runs JMeter on them'''
import argparse
import datetime
import glob
import logging
import os
import shutil
import subprocess
import sys

import ncscli.batchRunner as batchRunner
import jmxTool  # assumed to be in the same dir as this script


logger = logging.getLogger(__name__)


def scriptDirPath():
    '''returns the absolute path to the directory containing this script'''
    return os.path.dirname(os.path.realpath(__file__))

class JMeterFrameProcessor(batchRunner.frameProcessor):
    '''defines details for using JMeter for a simplistic load test'''

    workerDirPath = 'jmeterWorker'
    #JMeterFilePath = workerDirPath+'/TestPlan.jmx'
    JMeterFilePath = workerDirPath+'/XXX.jmx'
    JVM_ARGS ='-Xms30m -Xmx212m -XX:MaxMetaspaceSize=64m -Dnashorn.args=--no-deprecation-warning'

    def installerCmd( self ):
        cmd = 'free --mega -t 1>&2'  # to show amount of free ram
        if glob.glob( os.path.join( self.workerDirPath, '*.jar' ) ):
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
logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
logDateFmt = '%Y/%m/%d %H:%M:%S'
formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
logging.basicConfig(format=logFmt, datefmt=logDateFmt)
logger.setLevel(logging.INFO)
#batchRunner.logger.setLevel(logging.DEBUG)  # for more verbosity

ap = argparse.ArgumentParser( description=__doc__,
    fromfile_prefix_chars='@', formatter_class=argparse.ArgumentDefaultsHelpFormatter )
ap.add_argument( '--authToken', help='the NCS authorization token to use (or none, to use NCS_AUTH_TOKEN env var' )
ap.add_argument( '--outDataDir', default='data', help='a path to the output data dir for this run' )
ap.add_argument( '--filter', help='json to filter instances for launch',
    default = '{ "regions": ["usa", "india"], "dar": "==100", "dpr": ">=48", "ram": ">=3800000000", "storage": ">=2000000000" }'
    )
ap.add_argument( '--jmxFile', required=True, help='the JMeter test plan file path (required)' )
ap.add_argument( '--jtlFile', help='the file name of the jtl file produced by the test plan (if any)' )
ap.add_argument( '--workerDir', help='the directory to upload to workers',
    default='jmeterWorker'
    )
ap.add_argument( '--nWorkers', type=int, default=6, help='the number of Load-generating workers' )
# for analysis and plotting
ap.add_argument( '--rampStepDuration', type=float, default=60, help='duration, in seconds, of ramp step' )
ap.add_argument( '--SLODuration', type=float, default=240, help='SLO duration, in seconds' )
ap.add_argument( '--SLOResponseTimeMax', type=float, default=2.5, help='SLO RT threshold, in seconds' )
# environmental
ap.add_argument( '--jmeterBinPath', help='path to the local jmeter.sh for generating html report' )
args = ap.parse_args()


workerDirPath = args.workerDir.rstrip( '/' )  # trailing slash could cause problems with rsync
if workerDirPath:
    if not os.path.isdir( workerDirPath ):
        logger.error( 'the workerDirPath "%s" is not a directory', workerDirPath )
        sys.exit( 1 )
    JMeterFrameProcessor.workerDirPath = workerDirPath
else:
    logger.error( 'this version requires a workerDirPath' )
    sys.exit( 1 )
logger.info( 'workerDirPath: %s', workerDirPath )

jmxFilePath = args.jmxFile
if not os.path.isfile( jmxFilePath ):
    logger.error( 'the jmx file "%s" was not found', jmxFilePath )
    sys.exit( 1 )
logger.debug( 'using test plan "%s"', jmxFilePath )

if workerDirPath and jmxFilePath.find( workerDirPath+'/' ) != 0:
    logger.error( 'the jmx file "%s" is not in the workerDirPath "%s"', jmxFilePath, workerDirPath )
    sys.exit( 1 )

jmeterBinPath = args.jmeterBinPath
if not jmeterBinPath:
    jmeterVersion = '5.4.1'  # 5.3 and 5.4.1 have been tested, others may work as well
    jmeterBinPath = scriptDirPath()+'/apache-jmeter-%s/bin/jmeter.sh' % jmeterVersion

jmxTree = jmxTool.parseJmxFile( jmxFilePath )
jmxDur = jmxTool.getDuration( jmxTree )
logger.debug( 'jmxDur: %s seconds', jmxDur )

frameTimeLimit = max( round( jmxDur * 1.25 ), jmxDur+6*60 ) # some slop beyond the planned duration

JMeterFrameProcessor.JMeterFilePath = jmxFilePath

nFrames = args.nWorkers
nWorkers = round( nFrames * 1.5 )

dateTimeTag = datetime.datetime.now().strftime( '%Y-%m-%d_%H%M%S' )
outDataDir = args.outDataDir

try:
    rc = batchRunner.runBatch(
        frameProcessor = JMeterFrameProcessor(),
        commonInFilePath = JMeterFrameProcessor.workerDirPath,
        authToken = args.authToken or os.getenv( 'NCS_AUTH_TOKEN' ) or 'YourAuthTokenHere',
        encryptFiles=False,
        timeLimit = frameTimeLimit + 40*60,
        instTimeLimit = 6*60,
        frameTimeLimit = frameTimeLimit,
        filter = args.filter,
        #filter = '{ "regions": ["usa", "india"], "dar": "==100", "dpr": ">=48", "ram": ">=3800000000", "storage": ">=2000000000" }',
        #filter = '{ "regions": ["usa", "india"], "dar": "==100", "dpr": ">=48", "ram": ">=2800000000", "app-version": ">=2.1.11" }',
        outDataDir = outDataDir,
        startFrame = 1,
        endFrame = nFrames,
        nWorkers = nWorkers,
        limitOneFramePerWorker = True,
        autoscaleMax = 2
    )
    if (rc == 0) and os.path.isfile( outDataDir +'/recruitLaunched.json' ):
        rampStepDuration = args.rampStepDuration
        SLODuration = args.SLODuration
        SLOResponseTimeMax = args.SLOResponseTimeMax

        rc2 = subprocess.call( [sys.executable, scriptDirPath()+'/plotJMeterOutput.py',
            '--dataDirPath', outDataDir,
            '--rampStepDuration', str(rampStepDuration), '--SLODuration', str(SLODuration),
            '--SLOResponseTimeMax', str(SLOResponseTimeMax)
            ],
            stdout=subprocess.DEVNULL )
        if rc2:
            logger.warning( 'plotJMeterOutput exited with returnCode %d', rc2 )
 
        jtlFileName = args.jtlFile  # make this match output file name from the .jmx (or empty if none)
        if jtlFileName:
            nameParts = os.path.splitext(jtlFileName)
            mergedJtlFileName = nameParts[0]+'_merged_' + dateTimeTag + nameParts[1]
            rc2 = subprocess.call( [sys.executable, scriptDirPath()+'/mergeBatchOutput.py',
                '--dataDirPath', outDataDir,
                '--csvPat', 'jmeterOut_%%03d/%s' % jtlFileName,
                '--mergedCsv', mergedJtlFileName
                ], stdout=subprocess.DEVNULL
                )
            if rc2:
                logger.warning( 'mergeBatchOutput.py exited with returnCode %d', rc2 )
            else:
                if not os.path.isfile( jmeterBinPath ):
                    logger.info( 'no jmeter installed for producing reports (%s)', jmeterBinPath )
                else:
                    rcx = subprocess.call( [jmeterBinPath,
                        '-g', os.path.join( outDataDir, mergedJtlFileName ),
                        '-o', os.path.join( outDataDir, 'htmlReport' )
                        ], stderr=subprocess.DEVNULL
                    )
                    try:
                        shutil.move( 'jmeter.log', os.path.join( outDataDir, 'genHtml.log') )
                    except Exception as exc:
                        logger.warning( 'could not move the jmeter.log file (%s) %s', type(exc), exc )
                    if rcx:
                        logger.warning( 'jmeter reporting exited with returnCode %d', rcx )
    sys.exit( rc )
except KeyboardInterrupt:
    logger.warning( 'an interuption occurred')
