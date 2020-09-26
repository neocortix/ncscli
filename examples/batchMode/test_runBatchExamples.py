#!/usr/bin/env python3
'''pytest-compatible tests for runBatch examples'''
import glob
import json
import os
import re
import subprocess

def check_batchRunner_results( jlogFilePath ):
    with open( jlogFilePath, 'r') as inFile:
        for line in inFile:
            pass
        lastLine = line
    #print( 'the last line was', lastLine )
    decoded = json.loads( lastLine )
    print( 'decoded last line:', decoded )

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

def test_runBatchBinary():
    if not os.path.isfile('helloFrame_aarch64'):
        rc = subprocess.call( ['aarch64-linux-gnu-gcc', '-o', 'helloFrame_aarch64', 'helloFrame.c'] )
        assert rc==0, 'could not build helloFrameGo_aarch64'

    check_batchRunner_example( 'runBatchBinary', 'frame_*.out' )

def test_runBatchBinaryGo():
    if not os.path.isfile('helloFrameGo_aarch64'):
        rc = subprocess.call( 'GOARCH=arm64 go build -o helloFrameGo_aarch64 helloFrameGo.go', shell=True )
        assert rc==0, 'could not build helloFrame_aarch64'

    check_batchRunner_example( 'runBatchBinaryGo', 'frame_*.out' )

def test_runBatchBlender():
    check_batchRunner_example( 'runBatchBlender', 'rendered_frame_*.png' )

def test_runBatchPing():
    check_batchRunner_example( 'runBatchPing', 'frame_*.out' )
    


def no_test_runBatchLoadtest():
    rc = subprocess.call( ['./runBatchLoadtest.py'] )
    assert rc == 0
