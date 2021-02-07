#!/usr/bin/env python3
import datetime
import logging
import os
import sys
 
import ncscli.batchRunner as batchRunner
 
class javascriptFrameProcessor(batchRunner.frameProcessor):
    '''defines details for using javascript in a simple batch job'''
 
    def installerCmd( self ):
        return 'sudo apt-get -qq update && sudo apt-get -qq -y install nodejs > /dev/null' 
   
    workerScriptPath = 'helloFrame.js'
 
    def frameOutFileName( self, frameNum ):
        return 'frame_%d.out' % (frameNum)
 
    def frameCmd( self, frameNum ):
        jsFileName = os.path.basename( self.workerScriptPath )
        cmd = 'node %s %d > %s' % \
            (jsFileName, frameNum, self.frameOutFileName(frameNum))
        return cmd
 
if __name__ == "__main__":
    # configure logger
    logging.basicConfig()
    dateTimeTag = datetime.datetime.now().strftime( '%Y-%m-%d_%H%M%S' )
    outDataDirPath = 'data/javascript_' + dateTimeTag

 
    rc = batchRunner.runBatch(
        frameProcessor = javascriptFrameProcessor(),
        commonInFilePath = javascriptFrameProcessor.workerScriptPath,
        authToken = os.getenv( 'NCS_AUTH_TOKEN' ) or 'YourAuthTokenHere',
        timeLimit = 1200,
        instTimeLimit = 600,
        frameTimeLimit = 300,
        filter = '{"dpr": ">=24"}',
        outDataDir = outDataDirPath,
        encryptFiles = False,
        autoscaleMin=0.8,
        autoscaleMax=1.5,
        startFrame = 1,
        endFrame = 5
    )
    sys.exit( rc )
