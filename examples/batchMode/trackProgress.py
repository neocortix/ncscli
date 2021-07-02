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
import threading
import time
# third-party modules
import dateutil.parser
import enlighten

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # WARNING


class g_:
    interrupted = False


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

    startingArgs = None
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

def clocker():
    thr = threading.current_thread()
    while not thr.stopRequested:
        if g_.interrupted:
            break
        time.sleep( .5 )
        if not thr.stopRequested:
            if statusbar and phase:
                statusbar.update( demo=phase, force=True )
            if pbar and phase == 'Running on Instances':
                if startDateTime:
                    lineDateTime = datetime.datetime.now( datetime.timezone.utc )
                    elapsed = (lineDateTime - startDateTime).total_seconds()
                    logger.debug( 'elapsed: %.1f, startDateTime: %s', elapsed, startDateTime )
                    intElapsed = int(elapsed)
                    constrainedElapsed = min( intElapsed, pbar.total * 98 / 100 )
                    if constrainedElapsed > pbar.count and constrainedElapsed <= pbar.total:
                        pbar.update( constrainedElapsed - pbar.count, force=True )


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
            logger.debug( 'tracking progress in file %s', os.path.realpath(args.inFilePath)  )
    else:
        inFile = sys.stdin
    sleepy = bool( args.inFilePath )  # will do extra sleep when reading from a log file
    realTimeWanted = not args.inFilePath  # will use real time when not reading from a log file

    phase = 'Initializing'
    # initialize the enlighten manager and bars
    manager = enlighten.get_manager()
    title = manager.counter(total=0,
        desc=' Neocortix Cloud Services BatchRunner',
        bar_format='{desc}'
        )

    pbar = manager.counter(total=100, count=1, desc=' Progress:',
        bar_format='{desc}{desc_pad}{bar}|{percentage:3.0f}% ',
        color='blue', series = [' ','▌','█']
        )
    statusbar = manager.status_bar(status_format='   Status: {demo}{fill}{fill}{elapsed} ',
                                demo='', autorefresh=True, min_delta=0.033
                                )
    dataBar = manager.status_bar()
    bottomBar = manager.counter(total=1, bar_format='' )

    statusbar.update( demo=phase, force=True )
    if sleepy:
        time.sleep( 2 )

    clockerWanted = True
    clockThread = None
    if clockerWanted:
        # start a clock thread to maintain a ticking clock effect
        clockThread = threading.Thread( target=clocker )
        clockThread.stopRequested = False
        clockThread.start()

    try:
        # prepare to loop for every input line
        nInstances = 0
        nInstDone = 0
        nFailed = 0
        frameTimeLimit = 0
        timeLimit = 0
        estTotTime = 0
        elapsed = 0
        hasRunSome = False
        finalMsg = None
        outDataDir = None
        batchRunnerJLogPath = None
        throughputFilePath = None
        throughputFile = None
        brResults = {}
        lastUpdateTime = datetime.datetime.fromtimestamp(0, tz=datetime.timezone.utc)
        if realTimeWanted:
            startDateTime = datetime.datetime.now( datetime.timezone.utc )
        else:
            startDateTime = None
        for line in inFile:
            try:
                logger.debug( '%s', line)
                if throughputFile:
                    throughputFile.write( line )
                    throughputFile.flush()
                if sleepy:
                    time.sleep( .033 )
                phaseChange = False
                if ' ERROR ' in line or 'SyntaxError:' in line:
                    print( line.strip(), file=sys.stderr )
                if 'runBatch args.outDataDir' in line:
                    outDataDir = line.split('args.outDataDir: ')[1].strip()
                    logger.debug( 'outDataDir: %s', outDataDir )
                    if dataBar:
                        dataBar.update( '   Output: %s' % outDataDir, force=True )
                    batchRunnerJLogPath = os.path.join( outDataDir, 'batchRunner_results.jlog' )
                    logger.debug( 'batchRunnerJLogPath: %s', batchRunnerJLogPath )
                    throughputFilePath = os.path.join( outDataDir, 'trackedStderr.log' )
                    if not os.path.isfile( throughputFilePath ):
                        logger.debug( 'log file path: %s', throughputFilePath )
                        if not os.path.isdir( outDataDir ):
                            logger.debug( 'creating dir: %s', outDataDir )
                            os.makedirs( outDataDir, exist_ok=True )
                        throughputFile = open( throughputFilePath, 'w' )
                        throughputFile.write( line )
                elif 'recruitInstances recruiting' in line:
                    numPart = line.split('recruiting ')[1].split(' instances' )[0]
                    nRecruiting = int( numPart )
                    phase = 'Launching %d instances' % nRecruiting
                    phaseChange = True
                elif 'launchScInstances allocated' in line and not hasRunSome:
                    numPart = line.split('allocated ')[1].split(' instances' )[0]
                    nAllocated = int( numPart )
                    phase = 'Allocated %d instances' % nAllocated
                    phaseChange = True
                elif 'instance(s) launched so far' in line and not hasRunSome:
                    numPart = line.split('launchScInstances ')[1].split(' instance(s)')[0]
                    nLaunched = int( numPart )
                    phase = 'Launched %d instances' % nLaunched
                    phaseChange = True
                elif 'recruitInstances calling tellInstances to install on' in line:
                    numPart = line.split('to install on ')[1].split(' instances' )[0]
                    nInstInstances = int( numPart )
                    phase = 'Installing on %d instances' % nInstInstances
                    phaseChange = True
                elif 'good installs' in line or 'would compute frames' in line:
                    hasRunSome = True
                    phase = 'Running on Instances'
                    phaseChange = True
                    # could re-estimate total time sith code like this (but be more careful)
                    #if pbar and frameTimeLimit > 0:
                    #    pbar.total = int( min( elapsed + frameTimeLimit, estTotTime ) )
                elif 'renderFramesOnInstance finished' in line:
                    nInstDone += 1
                elif 'renderFramesOnInstance computeFailed' in line:
                    nFailed += 1
                    nInstDone += 1
                elif 'runBatch terminating' in line:
                    phase = 'Tidying up'
                    phaseChange = True
                elif 'runBatch computed' in line:
                    if 'computed 0 frames' in line:
                        finalMsg = 'ERROR. Zero frames computed; see %s' % throughputFilePath
                        phase = finalMsg
                        phaseChange = True
                    else:
                        msg = line.split('runBatch ')[1].strip()
                        msg = msg.replace( 'computed', 'Completed', 1 )
                        finalMsg = 'SUCCESS. %s' % msg
                        logger.debug( 'found success' )
                        phase = finalMsg
                        phaseChange = True
                elif 'plotting data' in line:
                    phase = 'Analyzing and plotting results'
                    phaseChange = True
                #elif '<stdout>' in line:
                #    logger.debug( '%s %s', lineDateTime, '<stdout>' )

                if batchRunnerJLogPath:
                    if not brResults and os.path.isfile( batchRunnerJLogPath ):
                        logger.debug( 'would read %s', batchRunnerJLogPath )
                        brResults = parseBatchRunnerLog( batchRunnerJLogPath )
                        startingArgs = brResults.get( 'startingArgs' )
                        if startingArgs:
                            timeLimit = startingArgs['timeLimit']
                            frameTimeLimit = startingArgs['frameTimeLimit']
                            instTimeLimit = startingArgs['instTimeLimit']
                            estTotTime = int( min(timeLimit, instTimeLimit*1.7 + frameTimeLimit) )
                            if pbar and estTotTime > 0:
                                logger.debug( 'pbar.total=estTotTime: %d', estTotTime )
                                pbar.total=estTotTime
                                fakeElapsed = int( round( estTotTime/100 ))
                                if fakeElapsed > pbar.count and fakeElapsed <= pbar.total:
                                    pbar.update( fakeElapsed - pbar.count, force=True )
                        if not realTimeWanted:
                            startDateTime = brResults.get( 'startDateTime' )
                        logger.debug( 'startDateTime: %s', startDateTime )

                if realTimeWanted:
                    lineDateTime = datetime.datetime.now( datetime.timezone.utc )
                else:
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
                if pbar and lineDateTime:
                    logger.debug( 'lineDateTime: %s', lineDateTime )
                    if startDateTime:
                        elapsed = (lineDateTime - startDateTime).total_seconds()
                        logger.debug( 'elapsed: %.1f, startDateTime: %s', elapsed, startDateTime )
                        intElapsed = int(elapsed)
                        constrainedElapsed = min( intElapsed, pbar.total * 98 / 100 )
                        if constrainedElapsed > pbar.count and constrainedElapsed <= pbar.total:
                            pbar.update( constrainedElapsed - pbar.count, force=True )

                if statusbar:
                    beenAWhile = lineDateTime and \
                        lineDateTime > lastUpdateTime + datetime.timedelta(seconds=1)
                    if phaseChange or beenAWhile:
                        if 'SUCCESS' in phase:
                            logger.debug( 'displaying success' )
                        statusbar.update( demo=phase, force=True )
                        lastUpdateTime = datetime.datetime.now( datetime.timezone.utc )
                        if sleepy:
                            time.sleep( 1 )
            except Exception as exc:
                logger.warning( 'got exception (%s) %s', type(exc), exc, exc_info=False )
        if statusbar:
            if finalMsg:
                statusbar.update( demo=finalMsg, force=True )
        if pbar:
            logger.debug( 'finishing pbar' )
            # top it up to 100%, while avoiding negative increment
            increment = max( 0, (pbar.total-pbar.count) )
            pbar.update( increment, force=True )
            #pbar.clear()  # could clear it instead
            if sleepy:
                time.sleep( 1 )
    finally:
        if clockThread:
            clockThread.stopRequested = True
            clockThread.join( timeout=60 )
            if clockThread.is_alive():
                logger.warning( 'the clock thread did not stop')
        if manager:
            manager.stop()
        if throughputFile:
            throughputFile.close()
