#!/usr/bin/env python3
'''pytest-compatible tests for runDistributedJMeter examples'''
import datetime
import glob
import json
import os
import re
import sys
import subprocess
# third-party modules
import pytest
# neocortix modules
try:
    import jmxTool
except ImportError:
    # set python path for default place, since path seems to be not set adequately
    jmxModulePath = os.path.expanduser('../jmeter')
    sys.path.append( jmxModulePath )
    import jmxTool



def check_batchRunner_results( jlogFilePath ):
    with open( jlogFilePath, 'r') as inFile:
        for line in inFile:
            pass
        lastLine = line
    decoded = json.loads( lastLine )

    assert decoded.get( 'type' ) == 'operation', 'not operation'
    assert 'args' in decoded, 'no args'
    assert 'finished' in decoded['args'], 'not finished'
    assert 'nFramesFinished' in decoded['args']['finished'], 'no nFramesFinished'
    nFramesFinished = decoded['args']['finished']['nFramesFinished']
    assert nFramesFinished > 0, 'no frames finished'
    print( 'nFramesFinished', nFramesFinished )

def check_runDistributed_example( workerDir, jmxFile, planDuration,
    jtlFilePath=None, nWorkers=6
    ):
    binPath = '../jmeter/runDistributedJMeter.py'
    dateTimeTag = datetime.datetime.now().strftime( '%Y-%m-%d_%H%M%S' )
    outDataDir  = 'data/' + workerDir + '_' + dateTimeTag
    workerDirPath = workerDir
    cmd = [
        binPath, '--workerDir', workerDirPath, '--jmxFile', jmxFile,
        '--planDuration', str(planDuration), '--outDataDir', outDataDir,
        '--nWorkers', str(nWorkers)
    ]
    proc = subprocess.run( cmd, stderr=subprocess.PIPE )
    rc = proc.returncode
    if rc:
        print( proc.stderr.decode('utf-8'), file=sys.stderr )
    assert rc==0, 'runDistributedJMeter.py returned non-zero rc'
    stderr = proc.stderr.decode('utf-8')
    assert 'args.outDataDir' in stderr, 'no args.outDataDir in stderr'
    outDataDir = re.search( r'outDataDir: (.*)\s', stderr ).group(1)
    assert outDataDir, 'regex for outDataDir did not work'
    resultsFilePath = outDataDir + '/batchRunner_results.jlog'
    assert os.path.isfile(resultsFilePath) , 'no batchRunner_results.jlog file'
    check_batchRunner_results( resultsFilePath )

    if not jtlFilePath:
        jtlFilePath = 'TestPlan_results.csv'
    # check that at least 1 jtl file exists
    outFilePaths = glob.glob( os.path.join( outDataDir, 'jmeterOut_*', jtlFilePath ) )
    assert len(outFilePaths), 'no jtl output files'
    if len(outFilePaths) < nWorkers:
        pytest.xfail( 'runDistributedJMeter.py produced fewer jtl files than nWorkers' )

def findJtlInJmx( jtlFilePath, jmxFilePath ):
    import jmxTool
    # parse the jmx file so we can find jtl file references
    jmxTree = jmxTool.parseJmxFile( jmxFilePath )
    planJtlFiles = jmxTool.findJtlFileNames( jmxTree )
    return jtlFilePath in planJtlFiles

def check_JtlInJmx( jtlFilePath, jmxFilePath ):
    wasFound = findJtlInJmx( jtlFilePath, jmxFilePath )
    assert wasFound, 'could not find %s in %s' % (jtlFilePath, jmxFilePath)

def test_authToken():
    assert os.getenv('NCS_AUTH_TOKEN'), 'env var NCS_AUTH_TOKEN not found'

def test_imports():
    import ncscli
    import ncscli.batchRunner
    import jmxTool

def test_path():
    rc = subprocess.call( 'hash ncs.py', shell=True )
    assert rc==0, 'ncs.py was not found in the PATH'

def test_installLocalJMeter():
    rc = subprocess.call( 'hash javac', shell=True )
    if rc != 0:
        pytest.xfail( 'JDK not found (for local JMeter)' )
    rc = subprocess.call( './installJMeter5.4.1.sh', shell=True )
    assert rc==0, 'could not install local JMeter'


def test_jtl_lessSlowWorker():
    check_JtlInJmx( '/home/lloyd/jmeter/TestPlan_025_results.csv',
        'lessSlowWorker/TestPlan_RampLong_LessSlow.jmx' )

def test_jtl_moreSlowWorker():
    check_JtlInJmx( '/home/lloyd/jmeter/TestPlan_025_results.csv',
        'moreSlowWorker/TestPlan_RampLong_MoreSlow.jmx' )

def test_jtl_petstoreWorker():
    check_JtlInJmx( 'jmeterOut/VRT.jtl',
        'petstoreWorker/JPetstore_JMeter5.4.1.jmx' )

def test_jtl_rampWorker():
    check_JtlInJmx( '/home/lloyd/jmeter/TestPlan_025_results.csv',
        'rampWorker/TestPlan_RampLong.jmx' )

def test_jtl_simpleWorker():
    check_JtlInJmx( 'jmeterOut/TestPlan.jtl', 'simpleWorker/TestPlan.jmx' )

def test_jtl_uploaderWorker():
    check_JtlInJmx( 'jmeterOut/VRT.jtl',
        'uploaderWorker/JImageUpload.jmx' )


def test_lessSlowWorker():
    check_runDistributed_example( 'lessSlowWorker', 'TestPlan_RampLong_LessSlow.jmx', 600 )

def test_minimalWorker():
    check_runDistributed_example( 'minimalWorker', 'minimal.jmx', 90 )

def test_moreSlowWorker():
    check_runDistributed_example( 'moreSlowWorker', 'TestPlan_RampLong_MoreSlow.jmx', 600 )

def test_petstoreWorker():
    check_runDistributed_example( 'petstoreWorker', 'JPetstore_JMeter5.4.1.jmx', 
        600, jtlFilePath='VRT.jtl' )

def test_rampWorker():
    check_runDistributed_example( 'rampWorker', 'TestPlan_RampLong.jmx', 600 )

def test_simpleWorker():
    check_runDistributed_example( 'simpleWorker', 'TestPlan.jmx', 90 )

def test_uploaderWorker():
    check_runDistributed_example( 'uploaderWorker', 'JImageUpload.jmx', 
        600, jtlFilePath='VRT.jtl' )

