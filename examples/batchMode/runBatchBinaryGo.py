#!/usr/bin/env python3
import datetime
import logging
import os
import sys
 
import ncscli.batchRunner as batchRunner
 
class binaryFrameProcessor(batchRunner.frameProcessor):
    '''defines details for using a binary executable in a simple batch job'''
 
    def installerCmd( self ):
        return None

    workerBinFilePath = 'helloFrameGo_aarch64'  # binary to copy to instance
 
    def frameOutFileName( self, frameNum ):
        return 'frame_%d.out' % (frameNum)

    def frameCmd( self, frameNum ):
        workerBinFileName = os.path.basename( self.workerBinFilePath )
        cmd = './%s %d > %s' % \
            (workerBinFileName, frameNum, self.frameOutFileName(frameNum))
        return cmd
 
if __name__ == "__main__":
    # configure logger formatting
    #logging.basicConfig()
    logger = logging.getLogger(__name__)
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)

    dateTimeTag = datetime.datetime.now().strftime( '%Y-%m-%d_%H%M%S' )
    outDataDirPath = 'data/binary_' + dateTimeTag

    rc = batchRunner.runBatch(
        frameProcessor = binaryFrameProcessor(),
        commonInFilePath = binaryFrameProcessor.workerBinFilePath,
        filter = '{"cpu-arch": "aarch64", "dpr": ">=24"}',
        authToken = os.getenv('NCS_AUTH_TOKEN') or 'YourAuthTokenHere',
        timeLimit = 480,
        instTimeLimit = 120,
        frameTimeLimit = 120,
        outDataDir = outDataDirPath,
        encryptFiles = False,
        startFrame = 1,
        endFrame = 6
    )
    sys.exit( rc )
