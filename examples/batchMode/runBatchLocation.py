#!/usr/bin/env python3
import datetime
import logging
import os
import sys
 
import ncscli.batchRunner as batchRunner
 
class pythonFrameProcessor(batchRunner.frameProcessor):
    '''defines details for using python in a simple batch job'''
    workerScriptPath = 'helloLocation.py'
 
    def frameOutFileName( self, frameNum ):
        return 'frame_%d.out' % (frameNum)
 
    def frameCmd( self, frameNum ):
        pythonFileName = os.path.basename( self.workerScriptPath )
        cmd = 'python3 %s %d | tee %s' % \
            (pythonFileName, frameNum, self.frameOutFileName(frameNum) )
        return cmd
 
if __name__ == "__main__":
    # configure logger
    logging.basicConfig()
    dateTimeTag = datetime.datetime.now().strftime( '%Y-%m-%d_%H%M%S' )
    outDataDirPath = 'data/python_' + dateTimeTag
 
    rc = batchRunner.runBatch(
        frameProcessor = pythonFrameProcessor(),
        commonInFilePath = pythonFrameProcessor.workerScriptPath,
        authToken = os.getenv( 'NCS_AUTH_TOKEN' ) or 'YourAuthTokenHere',
        timeLimit = 1200,
        instTimeLimit = 300,
        frameTimeLimit = 30,
        filter = '{"dpr": ">=24"}',
        outDataDir = outDataDirPath,
        encryptFiles = False,
        startFrame = 1,
        endFrame = 10
    )
    sys.exit( rc )
