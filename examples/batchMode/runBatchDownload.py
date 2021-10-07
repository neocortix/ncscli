#!/usr/bin/env python3
''' launches instances to download resources from the web and return their contents '''
import argparse
import datetime
import logging
import os
import sys
import urllib

import ncscli.batchRunner as batchRunner


def checkUrl( url ):
    if not url:
        return None
    parsed = urllib.parse.urlparse( url )
    if parsed.scheme and parsed.netloc:
        return url
    logger.info( 'invalid url given "%s"', url )
    return None


class dlProcessor(batchRunner.frameProcessor):
    '''defines details for doiong downloads in a simplistic batch job'''

    def frameOutFileName( self, frameNum ):
        return 'frame_%d.out' % (frameNum)

    def frameCmd( self, frameNum ):
        cmd = 'rm -f frame_*.out'
        cmd += '&& curl -L -s -S --remote-time -o %s %s' % (self.frameOutFileName(frameNum), urlList[frameNum] )
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

    ap = argparse.ArgumentParser( description=__doc__,
        fromfile_prefix_chars='@' )
    ap.add_argument( '--authToken',
        help='the NCS authorization token to use (default empty, to use NCS_AUTH_TOKEN env var' )
    ap.add_argument( '--filter', help='json to filter instances for launch (default: %(default)s)',
        default = '{ "storage": ">=4000000000", "dar": ">=95" }' )
    ap.add_argument( '--outDataDir',
        help='a path to the output data dir for this run (default: empty for a new timestamped dir)' )
    ap.add_argument( '--timeLimit', type=float, default=30*60,
        help='amount of time (in seconds) allowed for the whole job (default: %(default)s)' )
    ap.add_argument( '--unitTimeLimit', type=float, default=5*60,
        help='amount of time (in seconds) allowed for each download (default: %(default)s)' )
    ap.add_argument( '--urlListFile', help='a path to a text file containing urls to download from (default: %(default)s)',
        default='dlUrlList.txt' )
    ap.add_argument( '--nWorkers', type=int, help='the # of worker instances to launch (or 0 for autoscale) (default: 0)',
        default=0 )
    args = ap.parse_args()


    outDataDir = args.outDataDir
    if not outDataDir:
        dateTimeTag = datetime.datetime.now().strftime( '%Y-%m-%d_%H%M%S' )
        outDataDir = 'data/download_' + dateTimeTag

    urlFilePath = args.urlListFile  # 'dlUrlList.txt'
    if not os.path.isfile( urlFilePath ):
        logger.error( 'could not find the given --urlListFile "%s"', urlFilePath )
        sys.exit( 1 )

    with open( urlFilePath, 'r' ) as urlFile:
        urlList = urlFile.readlines()
    # strip out surrounding whitespace and empty rows
    urlList = [x.strip() for x in urlList if x.strip()]
    # strip out lines beginning with '#'
    urlList = [x for x in urlList if not x.startswith( '#' )]
    origUrlList = urlList
    # strip out invalid urls
    urlList = [x for x in urlList if checkUrl( x )]
    # replace spaces with '%20'
    urlList = [x.replace( ' ', '%20' ) for x in urlList]
    logger.debug( 'urlList: %s', urlList )
    if len( urlList ) < len( origUrlList):
        logger.warning( '%d of the given %d urls were not valid', 
            len(origUrlList) - len(urlList), len(origUrlList)
            )

    if not urlList:
        logger.warning('no valid urls in the url file')
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
        authToken = args.authToken or os.getenv('NCS_AUTH_TOKEN'),
        filter = args.filter,
        timeLimit = args.timeLimit,
        frameTimeLimit = args.unitTimeLimit,
        outDataDir = outDataDir,
        nWorkers = args.nWorkers,
        autoscaleInit = 1.25,
        autoscaleMax = 3.0,
        startFrame = 0,
        endFrame = len(urlList) - 1
    )

    sys.exit( rc )
