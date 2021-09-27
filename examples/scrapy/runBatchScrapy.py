#!/usr/bin/env python3
''' runs scrapy on a new batch of instances '''
import argparse
import datetime
import logging
import os
import subprocess
import sys
# third-party modules
#import scrapy
# neocortix modules
import ncscli.batchRunner as batchRunner


class dlProcessor(batchRunner.frameProcessor):
    '''defines details for running scrapy in a batch job'''

    def installerCmd( self ):
        cmd = 'python3 -m pip install --user scrapy'
        return cmd

    def frameOutFileName( self, frameNum ):
        spiderName = spiderNames[frameNum]
        return '%s_out.%s' % (spiderName, args.outFileFormat)

    def frameCmd( self, frameNum ):
        spiderName = spiderNames[frameNum]
        cmd = 'rm -f *_out.' + args.outFileFormat
        cmd += ' && cd %s && ~/.local/bin/scrapy crawl %s --loglevel INFO -o ../%s' % \
            (self.scrapyProjPath, spiderName, self.frameOutFileName(frameNum) )
        #cmd += ' && cd %s && ~/.local/bin/scrapy crawl %s && cp -p %s ~/' % \
        #    (self.scrapyProjPath, spiderName, self.frameOutFileName(frameNum) )
        return cmd

def getProjSpiders( projDir ):
    if not scrapyInstalled:
        return None
    spiders = []
    proc = subprocess.run( ['scrapy', 'list'], cwd=projDir, stdout=subprocess.PIPE )
    stdout = proc.stdout.decode('utf8')
    spiders = stdout.split()
    return spiders

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
    ap.add_argument( '--outFileFormat', choices=['csv', 'jl', 'json', 'xml'], help='the format of output file',
        default='csv' )
    ap.add_argument( '--timeLimit', type=float, default=30*60,
        help='amount of time (in seconds) allowed for the whole job (default: %(default)s)' )
    ap.add_argument( '--unitTimeLimit', type=float, default=5*60,
        help='amount of time (in seconds) allowed for each spider (default: %(default)s)' )
    ap.add_argument( '--scrapyProj', help='the name of the scrapy project directory',
        default='newsProject' )
    ap.add_argument( '--spiders', nargs='*', help='list of spiders to run (from the given project) (default: run all)' )
    ap.add_argument( '--nWorkers', type=int, help='the # of worker instances to launch (default: 0 for autoscale)',
        default=0 )
    args = ap.parse_args()


    scrapyProjPath = args.scrapyProj
    scrapyProjPath = scrapyProjPath.strip()  # no surrounding spaces wanted
    scrapyProjPath = scrapyProjPath.rstrip( '/' )  # trailing slash could cause problems with rsync

    if not scrapyProjPath:
        logger.error('no scrapy project given')
        sys.exit(1)
    if not os.path.isdir( scrapyProjPath ):
        logger.error('the given scrapy project is not a directory name')
        sys.exit(1)
    dlProcessor.scrapyProjPath = scrapyProjPath

    outDataDir = args.outDataDir
    if not outDataDir:
        dateTimeTag = datetime.datetime.now().strftime( '%Y-%m-%d_%H%M%S' )
        outDataDir = ('data/%s_' % scrapyProjPath)  + dateTimeTag


    scrapyInstalled = subprocess.call( 'hash scrapy', shell=True ) == 0
    if not scrapyInstalled and not args.spiders:
        print( 'please pass --spiders with a list of the names of spiders you want to run')
        sys.exit( 1 )
    spiderNames = args.spiders
    if scrapyInstalled:
        availSpiders = getProjSpiders( scrapyProjPath )
        if spiderNames:
            someNotFound = False
            for x in spiderNames:
                if x not in availSpiders:
                    someNotFound = True
                    logger.error( 'spider "%s" not found in project "%s:', x, scrapyProjPath )
            if someNotFound:
                logger.info( 'available spiders: %s', availSpiders )
                sys.exit( 1 )
        else:
            spiderNames = availSpiders
    logger.info( 'spiderNames: %s',  spiderNames)

    if not spiderNames:
        logger.error( 'no spiders to run' )
        sys.exit( 1 )

    nSpiders = len( spiderNames )
    nWorkers = args.nWorkers if args.nWorkers else 0
    
    authToken = args.authToken or os.getenv('NCS_AUTH_TOKEN')
    if not authToken:
        logger.error( 'please provide --authToken or set env var NCS_AUTH_TOKEN')
        sys.exit( 1 )

    #sys.exit( 'DEBUGGING' )

    rc = batchRunner.runBatch(
        frameProcessor = dlProcessor(),
        commonInFilePath = dlProcessor.scrapyProjPath,
        authToken = authToken,
        filter = args.filter,
        timeLimit = args.timeLimit,
        frameTimeLimit = args.unitTimeLimit,
        outDataDir = outDataDir,
        nWorkers = nWorkers,
        autoscaleInit = 1.33,
        autoscaleMin = 1.33,
        autoscaleMax = 2.0,
        startFrame = 0,
        endFrame = nSpiders-1,
    )

    sys.exit( rc )
