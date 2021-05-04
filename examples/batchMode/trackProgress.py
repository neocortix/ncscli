#!/usr/bin/env python3
"""
track progress of a batchRunner run
"""
# standard library modules
import argparse
import datetime
import collections
import json
import logging
import os
import sys
import time
# third-party modules
import dateutil.parser
import enlighten

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def datetimeIsAware( dt ):
    if not dt: return None
    return (dt.tzinfo is not None) and (dt.tzinfo.utcoffset( dt ) is not None)

def universalizeDateTime( dt ):
    if not dt: return None
    if datetimeIsAware( dt ):
        #return dt
        return dt.astimezone(datetime.timezone.utc)
    return dt.replace( tzinfo=datetime.timezone.utc )


def readJLog( inFilePath ):
    '''read JLog file, return list of decoded objects'''
    recs = []
    topLevelKeys = collections.Counter()  # for debugging
    # demux by instance
    with open( inFilePath, 'rb' ) as inFile:
        for line in inFile:
            decoded = json.loads( line )
            if isinstance( decoded, dict ):
                for key in decoded:
                    topLevelKeys[ key ] += 1
            recs.append( decoded )
    logger.debug( 'topLevelKeys: %s', topLevelKeys )
    return recs

def parseBatchRunnerLog( jlogFilePath ):
    installerLog = readJLog(jlogFilePath)
    logger.debug( 'found %d jlog lines', len(installerLog) )

    startDateTime = None
    for line in installerLog:
        if line['type'] == 'operation' and 'starting' in line['args']:
            startingArgs = line['args']['starting']
            logger.debug( 'batchRunner starting: %s', startingArgs )
            startDateTime = dateutil.parser.parse( line['dateTime'] )
    if startingArgs:
        return {'startingArgs': startingArgs, 'startDateTime': startDateTime }
    else:
        return {}

def parseInstallerLog( jlogFilePath ):
    installerLog = readJLog(jlogFilePath)
    logger.info( 'found %d jlog lines', len(installerLog) )

    nInstances = 0
    nInstDone = 0
    nInstErr = 0
    for line in installerLog:
        if 'operation' in line and 'tellInstances' in line['operation']:
            tellInstancesOp = line['operation'][1]
            logger.debug( 'tellInstances: %s', tellInstancesOp )

            iids = tellInstancesOp['args']['instanceIds']
            logger.debug( 'iids: %s', iids )
            nInstances = len(iids)
            logger.debug( 'installing on %d instances', nInstances )

            timeLimit = tellInstancesOp['args']['timeLimit']
            logger.debug( 'timeLimit %d', timeLimit )
        elif 'returncode' in line:
            logger.debug( '%s', line )
            nInstDone += 1
            if line['returncode'] != 0:
                nInstErr += 1
            logger.debug( '%d done (%d errors) by %s', nInstDone, nInstErr, line['dateTime'] )
    return {'nInstances': nInstances, 'nInstDone': nInstDone, 'nInstErr': nInstErr  }


if __name__ == "__main__":
    # configure logger formatting
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)
    logging.captureWarnings(True)

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@', formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    ap.add_argument( '--inFilePath', help='the path to the for input data (defaut is stdin)' )
    args = ap.parse_args()

    if args.inFilePath:
        inFilePath = args.inFilePath
        if os.path.isfile( inFilePath ):
            inFile = open( inFilePath, 'r')
            logger.info( 'tracking progress in file %s', os.path.realpath(args.inFilePath)  )
    else:
        inFile = sys.stdin
    sleepy = not not args.inFilePath  # will do extra sleep when reading from a log file

    manager = None
    statusbar = None
    pbar = None
    phase = 'Initializing'
    if True:
        manager = enlighten.get_manager()
        title = manager.counter(total=0,
            desc='Neocortix Cloud Services BatchRunner',
            bar_format='{desc}')
        statusbar = manager.status_bar( 'statusbar', min_delta=0.033 )
        statusbar.update( phase, force=True )
        if sleepy:
            time.sleep( 2 )

    nInstances = 0
    nInstDone = 0
    nFailed = 0
    frameTimeLimit = 0
    elapsed = 0
    outDataDir = None
    batchRunnerJLogPath = None
    throughputFilePath = None
    throughputFile = None
    brResults = {}
    for line in inFile:
        logger.debug( '%s', line)
        if throughputFile:
            throughputFile.write( line )
            throughputFile.flush()
        if sleepy:
            time.sleep( .033 )
        phaseChange = False
        if 'runBatch args.outDataDir' in line:
            outDataDir = line.split('args.outDataDir: ')[1].strip()
            logger.debug( 'outDataDir: %s', outDataDir )
            batchRunnerJLogPath = os.path.join( outDataDir, 'batchRunner_results.jlog' )
            logger.debug( 'batchRunnerJLogPath: %s', batchRunnerJLogPath )
            throughputFilePath = os.path.join( outDataDir, 'trackedStderr.log' )
            if not os.path.isfile( throughputFilePath ):
                logger.info( 'Log File Path: %s', throughputFilePath )
                throughputFile = open( throughputFilePath, 'w' )
                throughputFile.write( line )
        elif 'recruitInstances recruiting' in line:
            numPart = line.split('recruiting ')[1].split(' instances' )[0]
            nRecruiting = int( numPart )
            phase = 'Launching %d instances' % nRecruiting
            phaseChange = True
        elif 'launchScInstances allocated' in line:
            numPart = line.split('allocated ')[1].split(' instances' )[0]
            nAllocated = int( numPart )
            phase = 'Allocated %d instances' % nAllocated
            phaseChange = True
        elif 'instance(s) launched so far' in line:
            numPart = line.split('launchScInstances ')[1].split(' instance(s)')[0]
            nLaunched = int( numPart )
            phase = 'Launched %d instances' % nLaunched
            phaseChange = True
        elif 'recruitInstances calling tellInstances to install on' in line:
            numPart = line.split('to install on ')[1].split(' instances' )[0]
            nInstInstances = int( numPart )
            phase = 'Installing on %d instances' % nInstInstances
            phaseChange = True
        elif 'good installs' in line:
            phase = 'Running'
            phaseChange = True
            if pbar and frameTimeLimit > 0:
                pbar.total = int( min( elapsed + frameTimeLimit, timeLimit ) )
        elif 'renderFramesOnInstance finished' in line:
            nInstDone += 1
        elif 'renderFramesOnInstance computeFailed' in line:
            nFailed += 1
            nInstDone += 1
        elif 'runBatch terminating' in line or 'runBatch computed' in line:
            if 'computed 0 frames' in line:
                phase = 'error: zero frames computed; see %s' % throughputFilePath
                phaseChange = True
            elif phase != 'Tidying up':
                phase = 'Tidying up'
                phaseChange = True
        elif 'plotting data' in line:
            phase = 'Analyzing and plotting results'
            phaseChange = True

        if batchRunnerJLogPath:
            if not brResults and os.path.isfile( batchRunnerJLogPath ):
                logger.debug( 'would read %s', batchRunnerJLogPath )
                brResults = parseBatchRunnerLog( batchRunnerJLogPath )
                startingArgs = brResults.get( 'startingArgs' )
                if startingArgs:
                    timeLimit = startingArgs['timeLimit']
                    frameTimeLimit = startingArgs['frameTimeLimit']
                    instTimeLimit = startingArgs['instTimeLimit']
                startDateTime = brResults.get( 'startDateTime' )
                logger.debug( 'startDateTime: %s', startDateTime )
            if timeLimit > 0 and not pbar:
                pbar = manager.counter( total=timeLimit, desc='Progress:', color='blue' )

        lineDateTime = None
        # extract the dateTime stamp from the line
        parts = line.split()
        if len( parts[0] ) >= 19:
            # in this case, it hopefully is an iso-like date stamp without embedded spaces
            dateTimePart =  parts[0]
        else:
            # in this case, it hopefully is human-friendly dateTime stamp with 1 embedded space
            dateTimePart = ' '.join( parts[0:2] )
        try:
            lineDateTime = dateutil.parser.parse( dateTimePart )
            lineDateTime = universalizeDateTime( lineDateTime )
        except Exception:
            # ignore any failure to parse dateTime
            pass
        if lineDateTime:
            logger.debug( 'lineDateTime: %s', lineDateTime )
            if startDateTime:
                elapsed = (lineDateTime - startDateTime).total_seconds()
                logger.debug( 'elapsed: %.1f', elapsed )
                intElapsed = int(elapsed)
                if pbar and intElapsed > pbar.count:
                    pbar.update( intElapsed - pbar.count, force=True )

        if statusbar and phaseChange:
            msg = line[25:80]
            logger.debug( 'updating %s', msg)
            statusbar.update( phase, force=True )
            if sleepy:
                time.sleep( 1 )  # delete this
    if throughputFile:
        throughputFile.close()
    if sleepy:
        time.sleep( 1 )
