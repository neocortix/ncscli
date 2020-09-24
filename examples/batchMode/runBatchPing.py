#!/usr/bin/env python3
import datetime
import logging
import os
import sys

import ncscli.batchRunner as batchRunner


class pingFrameProcessor(batchRunner.frameProcessor):
    '''defines details for using ping in a simplistic batch job'''

    def frameOutFileName( self, frameNum ):
        return 'frame_%d.out' % (frameNum)

    def frameCmd( self, frameNum ):
        targetHost = 'neocortix.com'
        nPings = 3
        timeLimit = 60
        interval = 5
        pingCmd = 'ping %s -U -D -c %s -w %f -i %f > %s' \
            % (targetHost, nPings, timeLimit,  interval, self.frameOutFileName(frameNum) )
        return pingCmd


# configure logger formatting
logging.basicConfig()

dateTimeTag = datetime.datetime.now().strftime( '%Y-%m-%d_%H%M%S' )
outDataDir = 'data/ping_' + dateTimeTag

rc = batchRunner.runBatch(
    frameProcessor = pingFrameProcessor(),
    authToken = os.getenv('NCS_AUTH_TOKEN') or 'YourAuthTokenHere',
    timeLimit = 720,  # seconds
    frameTimeLimit = 120,
    outDataDir = outDataDir,
    autoscaleMax = 1.5,
    startFrame = 1,
    endFrame = 3
 )

sys.exit( rc )
