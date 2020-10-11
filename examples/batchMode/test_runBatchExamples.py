#!/usr/bin/env python3
'''pytest-compatible tests for runBatch examples'''
import glob
import json
import os
import re
import sys
import subprocess

import pytest


def check_batchRunner_results( jlogFilePath ):
    with open( jlogFilePath, 'r') as inFile:
        for line in inFile:
            pass
        lastLine = line
    #print( 'the last line was', lastLine )
    decoded = json.loads( lastLine )
    #print( 'decoded last line:', decoded )

    assert decoded.get( 'type' ) == 'operation', 'not operation'
    assert 'args' in decoded, 'no args'
    assert 'finished' in decoded['args'], 'not finished'
    assert 'nFramesFinished' in decoded['args']['finished'], 'no nFramesFinished'
    nFramesFinished = decoded['args']['finished']['nFramesFinished']
    assert nFramesFinished > 0, 'no frames finished'
    print( 'nFramesFinished', nFramesFinished )

def check_batchRunner_example( exampleName, frameFilePattern=None ):
    binPath = './' + exampleName + '.py'
    proc = subprocess.run( [binPath], stderr=subprocess.PIPE )
    rc = proc.returncode
    if rc:
        print( proc.stderr.decode('utf-8'), file=sys.stderr )
    assert rc==0, exampleName+' returned non-zero rc'
    stderr = proc.stderr.decode('utf-8')
    assert 'args.outDataDir' in stderr, 'no args.outDataDir in stderr'
    outDataDir = re.search( r'outDataDir: (.*)\s', stderr ).group(1)
    assert outDataDir, 'regex for outDataDir did not work'
    resultsFilePath = outDataDir + '/batchRunner_results.jlog'
    assert os.path.isfile(resultsFilePath) , 'no batchRunner_results.jlog file'
    check_batchRunner_results( resultsFilePath )

    if frameFilePattern:
        # check that at least 1 final output file exists
        outFilePaths = glob.glob( os.path.join( outDataDir, frameFilePattern ) )
        assert len(outFilePaths), 'no frame output files'


def test_authToken():
    assert os.getenv('NCS_AUTH_TOKEN'), 'env var NCS_AUTH_TOKEN not found'

def test_imports():
    import ncscli
    import ncscli.batchRunner

def test_path():
    rc = subprocess.call( 'hash ncs.py', shell=True )
    assert rc==0, 'ncs.py was not found in the PATH'

def test_runBatchBinary():
    # check if the built ARM executable already exists
    if not os.path.isfile('helloFrame_aarch64'):
        # check if the C compiler is available
        rc = subprocess.call( 'hash aarch64-linux-gnu-gcc', shell=True )
        if rc != 0:
            pytest.xfail( 'C compiler not found (for building helloFrame_aarch64)' )
        # build the ARM executable
        rc = subprocess.call( ['aarch64-linux-gnu-gcc', '-o', 'helloFrame_aarch64', 'helloFrame.c'] )
        assert rc==0, 'could not build helloFrameGo_aarch64'

    check_batchRunner_example( 'runBatchBinary', 'frame_*.out' )

def test_runBatchBinaryGo():
    # check if the built ARM executable already exists
    if not os.path.isfile('helloFrameGo_aarch64'):
        # check if the go compiler is available
        rc = subprocess.call( 'hash go', shell=True )
        if rc != 0:
            pytest.xfail( '"go" compiler not found (for building helloFrameGo_aarch64)' )
        # build the ARM executable
        rc = subprocess.call( 'GOARCH=arm64 go build -o helloFrameGo_aarch64 helloFrameGo.go', shell=True )
        assert rc==0, 'could not build helloFrame_aarch64'

    check_batchRunner_example( 'runBatchBinaryGo', 'frame_*.out' )

def test_runBatchBlender():
    check_batchRunner_example( 'runBatchBlender', 'rendered_frame_*.png' )

def test_runBatchGatling():
    check_batchRunner_example( 'runBatchGatling', 'gatlingResults_*' )

def test_runBatchJMeter():
    check_batchRunner_example( 'runBatchJMeter', 'TestPlan_results_*.csv' )

def test_runBatchK6():
    # check if the built ARM executable already exists
    if not os.path.isfile('k6Worker/k6'):
        # check if the go compiler is available
        rc = subprocess.call( 'hash go', shell=True )
        if rc != 0:
            pytest.xfail( '"go" compiler not found (for building k6Worker/k6)' )
        # build the ARM executable
        rc = subprocess.call( 'GOARCH=arm64 GOPATH=$PWD/go go get github.com/loadimpact/k6', shell=True )
        assert rc==0, 'could not build k6 for Arm64'
        # copy ARM executable to k6worker dir
        rc = subprocess.call( 'cp -p go/bin/linux_arm64/k6 k6Worker', shell=True )
        assert rc==0, 'could not copy ARM executable to k6worker dir'

    check_batchRunner_example( 'runBatchK6', 'worker_*.csv' )

def test_runBatchLoadtest():
    check_batchRunner_example( 'runBatchLoadtest', 'worker_*.csv' )

def test_runBatchPing():
    check_batchRunner_example( 'runBatchPing', 'frame_*.out' )

def test_runBatchPuppeteerLighthouse():
    check_batchRunner_example( 'runBatchPuppeteerLighthouse', 'Puppeteer_results_*.tar.gz' )

def test_runBatchPython():
    check_batchRunner_example( 'runBatchPython', 'sine_*.png' )
