#!/usr/bin/env python3
import datetime
import logging
import os
import sys
 
import ncscli.batchRunner as batchRunner
 
class pythonFrameProcessor(batchRunner.frameProcessor):
    '''defines details for using python in a simple batch job'''
 
    def installerCmd( self ):
        return 'sudo apt-get -qq update && sudo apt-get -qq -y install python3-matplotlib > /dev/null' 
   
    workerScriptPath = 'plotFrame.py'
    frameFileType = 'png'
    outFilePattern = 'sine_######.%s'%(frameFileType)
 
    def frameOutFileName( self, frameNum ):
        outFileName = self.outFilePattern.replace( '######', '%06d' % frameNum )
        # outFileName = './sine*.png'
        return outFileName
 
    def frameCmd( self, frameNum ):
        pythonFileName = os.path.basename( self.workerScriptPath )
        cmd = 'python3 %s %d' % \
            (pythonFileName, frameNum)
        return cmd
 
if __name__ == "__main__":
    # configure logger
    logger = logging.getLogger(__name__)
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)

    dateTimeTag = datetime.datetime.now().strftime( '%Y-%m-%d_%H%M%S' )
    outDataDirPath = 'data/python_' + dateTimeTag

 
    rc = batchRunner.runBatch(
        frameProcessor = pythonFrameProcessor(),
        commonInFilePath = pythonFrameProcessor.workerScriptPath,
        authToken = os.getenv( 'NCS_AUTH_TOKEN' ) or 'YourAuthTokenHere',
        timeLimit = 1200,
        instTimeLimit = 450,
        frameTimeLimit = 300,
        filter = '{"dpr": ">=24"}',
        outDataDir = outDataDirPath,
        encryptFiles = False,
        autoscaleMin=0.8,
        autoscaleMax=1.5,
        startFrame = 1,
        endFrame = 9
    )
    sys.exit( rc )
