#!/usr/bin/env python3
import datetime
import logging
import os
import sys
 
import ncscli.batchRunner as batchRunner
 
class blenderFrameProcessor(batchRunner.frameProcessor):
    '''defines details for using blender in a simple batch job'''
 
    def installerCmd( self ):
        return 'sudo apt-get -qq update && sudo apt-get -qq -y install -t buster-backports blender > /dev/null'
   
    blendFilePath = 'SpinningCube.blend'
    frameFileType = 'png'
    outFilePattern = 'rendered_frame_######.%s'%(frameFileType)
 
    def frameOutFileName( self, frameNum ):
        outFileName = self.outFilePattern.replace( '######', '%06d' % frameNum )
        return outFileName
 
    def frameCmd( self, frameNum ):
        blendFileName = os.path.basename( self.blendFilePath )
        cmd = 'blender -b -noaudio --enable-autoexec %s -o %s --render-format %s -f %d' % \
            (blendFileName, self.outFilePattern, self.frameFileType.upper(), frameNum)
        return cmd
 
if __name__ == "__main__":
    # configure logger formatting
    #logging.basicConfig() # could just do this
    logger = logging.getLogger(__name__)
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)
    logger.setLevel(logging.INFO)
    #batchRunner.logger.setLevel(logging.DEBUG)  # for more verbosity

    dateTimeTag = datetime.datetime.now().strftime( '%Y-%m-%d_%H%M%S' )
    outDataDirPath = 'data/spin_' + dateTimeTag

 
    rc = batchRunner.runBatch(
        frameProcessor = blenderFrameProcessor(),
        commonInFilePath = blenderFrameProcessor.blendFilePath,
        authToken = os.getenv( 'NCS_AUTH_TOKEN' ) or 'YourAuthTokenHere',
        timeLimit = 4*3600,
        instTimeLimit = 1200,
        frameTimeLimit = 2100,
        autoscaleInit = 2,
        autoscaleMin = 1.5,
        autoscaleMax = 3,
        filter = '{"dpr": ">=48","ram:":">=2800000000","app-version": ">=2.1.11"}',
        outDataDir = outDataDirPath,
        encryptFiles = False,
        startFrame = 0,
        endFrame = 5
    )
    # this part is "extra credit" if you want to encode the output as video (and ffmpeg is installed)
    if rc == 0:
        import subprocess
        def encodeTo264( destDirPath, destFileName, frameRate, kbps=30000,
                frameFileType='png', startFrame=0 ):
            '''encode frames to an h.264 video; only works if you have ffmpeg installed'''
            kbpsParam = str(kbps)+'k'
            cmd = [ 'ffmpeg', '-y', '-framerate', str(frameRate),
                '-start_number', str(startFrame),
                '-i', destDirPath + '/rendered_frame_%%06d.%s'%(frameFileType),
                '-c:v', 'libx264', '-preset', 'fast', '-pix_fmt', 'yuv420p', 
                '-b:v', kbpsParam,
                os.path.join( destDirPath, destFileName )
            ]
            try:
                subprocess.check_call( cmd,
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
                    )
            except Exception as exc:
                logger.warning( 'ffmpeg call threw exception (%s) %s',type(exc), exc )

        if subprocess.call( ['which', 'ffmpeg'], stdout=subprocess.DEVNULL ) == 0:
            encodeTo264( outDataDirPath, 'rendered.mp4', 30, startFrame=1 )

    sys.exit( rc )
