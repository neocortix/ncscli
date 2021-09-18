#!/usr/bin/env python3
import datetime
import logging
import os
import sys

import ncscli.batchRunner as batchRunner


class dlProcessor(batchRunner.frameProcessor):
    '''defines details for doiong downloads in a simplistic batch job'''

    def frameOutFileName( self, frameNum ):
        return 'frame_%d.out' % (frameNum)

    def frameCmd( self, frameNum ):
        cmd = 'curl -L -s -S --remote-time -o %s %s' % (self.frameOutFileName(frameNum), urlList[frameNum] )
        return cmd

if __name__ == '__main__':
    # configure logger formatting
    logger = logging.getLogger(__name__)
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)
    #batchRunner.logger.setLevel(logging.DEBUG)  # for more verbosity
    logger.setLevel(logging.INFO)

    dateTimeTag = datetime.datetime.now().strftime( '%Y-%m-%d_%H%M%S' )
    outDataDir = 'data/download_' + dateTimeTag

    urlFilePath = 'dlUrlList.txt'
    #filtersJson = '{ "regions": ["russia-ukraine-belarus"], "dpr": "<60", "dar": "<100" }'
    filtersJson = '{ "regions": ["russia-ukraine-belarus"], "storage": ">=4000000000" }'

    with open( urlFilePath, 'r' ) as urlFile:
        urlList = urlFile.readlines()
    # strip out surrounding whitespace and empty rows
    urlList = [x.strip() for x in urlList if x.strip()]
    # strip out lines beginning with '#'
    urlList = [x for x in urlList if not x.startswith( '#' )]
    # replace spaces with '%20'
    urlList = [x.replace( ' ', '%20' ) for x in urlList]
    logger.debug( 'urlList: %s', urlList )

    if not urlList:
        logger.warning('no urls in the url file')
        sys.exit(1)

    processor = dlProcessor()

    os.makedirs( outDataDir, exist_ok=True )
    indexFilePath = os.path.join( outDataDir, 'index.csv' )
    with open( indexFilePath, 'w' ) as indexFile:
        print( 'fileName', 'url', sep=',', file=indexFile )
        for ii, url in enumerate( urlList ):
            fileName = processor.frameOutFileName( ii )
            #print( 'file', fileName, 'url', url )
            print( fileName, url, sep=',', file=indexFile )
    #sys.exit( 'DEBUGGING' )
    rc = batchRunner.runBatch(
        frameProcessor = processor,
        authToken = os.getenv('NCS_AUTH_TOKEN') or 'YourAuthTokenHere',
        filter = filtersJson,
        timeLimit = 30 * 60,  # seconds
        frameTimeLimit = 300,
        outDataDir = outDataDir,
        autoscaleInit = 1.25,
        autoscaleMax = 3.0,
        startFrame = 0,
        endFrame = len(urlList) - 1
    )

    sys.exit( rc )
