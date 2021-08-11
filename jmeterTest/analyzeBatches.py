#!/usr/bin/env python3
"""
does diagnostic analysis of batchMode batches
"""
# standard library modules
import argparse
import collections
import csv
import json
import logging
#import math
import os
import sys
#import warnings


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def boolArg( v ):
    '''use with ArgumentParser add_argument for (case-insensitive) boolean arg'''
    if v.lower() == 'true':
        return True
    elif v.lower() == 'false':
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def readJLog( inFilePath ):
    '''read JLog file, return list of decoded objects'''
    recs = []
    topLevelKeys = collections.Counter()  # for debugging
    # read and decode each line as json
    try:
        with open( inFilePath, 'rb' ) as inFile:
            for line in inFile:
                try:
                    decoded = json.loads( line )
                except Exception as exc:
                    logger.warning( 'exception decoding json (%s) %s', type(exc), exc )
                if isinstance( decoded, dict ):
                    for key in decoded:
                        topLevelKeys[ key ] += 1
                recs.append( decoded )
        logger.debug( 'topLevelKeys: %s', topLevelKeys )
    except Exception as exc:
        logger.warning( 'excption reading file (%s) %s', type(exc), exc )
    return recs

def findOperation( opCode, entries ):
    for entry in entries:
        if entry['type'] == 'operation' and opCode in entry['args']:
            return entry
    return None

def findFrameStarts( entries ):
    frames = []
    for entry in entries:
        if entry['type'] == 'frameState' and entry['args']['state'] == 'starting':
            frames.append( entry )
    return frames

def findFailedFrames( entries ):
    failedFrames = []
    for entry in entries:
        if entry['type'] == 'frameState':
            if 'rc' not in entry['args']:
                logger.warning( 'no rc in frameState entry %s', entry)
                continue
            frameArgs = entry['args']
            if frameArgs['rc']:
                failedFrames.append( entry )
    return failedFrames

def findStderrsForInstance( iid, entries ):
    stderrs = []
    for entry in entries:
        if entry['type'] == 'stderr' and entry['instanceId'] == iid:
            stderrs.append( entry )
    return stderrs

def findStdoutsForInstance( iid, entries ):
    stdouts = []
    for entry in entries:
        if entry['type'] == 'stdout' and entry['instanceId'] == iid:
            stdouts.append( entry )
    return stdouts

def findFrameStart( iid, frameNum, entries ):
    for entry in entries:
        if entry['type'] == 'frameState' and entry['instanceId'] == iid:
            entryArgs = entry['args']
            if entryArgs['state']=='starting' and entryArgs['frameNum'] == frameNum :
                return entry
    return None


def ingestCsv( inFilePath ):
    '''read the csv file; return contents as a list of dicts'''
    rows = []
    with open( inFilePath, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            rows.append( row )
    return rows


if __name__ == "__main__":
    # configure logger formatting
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)
    logging.captureWarnings(True)
    logger.setLevel(logging.WARNING)  # for more verbosity

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@', formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    ap.add_argument( '--dataDirPath', required=True, help='the path to to directory for input and output data' )
    ap.add_argument( '--batchA', help='the name of the first batch dir to analyze' )
    ap.add_argument( '--batchB', help='the name of the last batch dir to analyze' )
    args = ap.parse_args()

    dataDir = args.dataDirPath

    if not args.batchB:
        args.batchB = args.batchA

    batchNames = []
    dirContents = sorted( os.listdir( dataDir ) )
    #logger.info( 'dirContents (%d): %s', len(dirContents), sorted(dirContents) )
    for innerDir in dirContents:
        innerPath = os.path.join( dataDir, innerDir )
        if os.path.isdir( innerPath ) and innerDir >= args.batchA and innerDir <= args.batchB:
            batchNames.append( innerDir )
    logger.info( 'analyzing %d batches: %s', len(batchNames), batchNames )

    nAnalyzed = 0
    nPerfect = 0
    nImperfect = 0
    nUnfinished = 0
    failedStates = collections.Counter()
    allDevsCounter = collections.Counter()
    failedDevsCounter = collections.Counter()
    countryCounter = collections.Counter()
    instancesByDevId = {}

    for batchName in batchNames:
        batchDirPath = os.path.join( args.dataDirPath, batchName )
        logger.debug( 'analyzing data in directory %s', os.path.realpath(batchDirPath)  )

        batchJlogFilePath = batchDirPath + "/batchRunner_results.jlog"
        launchedJsonFilePath = batchDirPath + "/recruitLaunched.json"
        recruiterJlogFilePath = batchDirPath + "/recruitInstances.jlog"

        installerEntry = None
        if os.path.isfile( recruiterJlogFilePath ):
            recruiterResults = readJLog( recruiterJlogFilePath )
            if not recruiterResults:
                logger.warning( 'no entries in %s', recruiterJlogFilePath )
            recruiterEntry = recruiterResults[0]
            if 'operation' not in recruiterEntry:
                logger.info( 'installer did not run')
            else:
                installerEntry = recruiterEntry
                #logger.info( 'installerOp %s', installerEntry['operation'] )

        # load details of launched instances
        instancesByIid = {}  # for just this batch
        if installerEntry:
            with open( launchedJsonFilePath, 'r') as jsonInFile:
                try:
                    launchedInstances = json.load(jsonInFile)  # an array
                    instancesByIid = { inst['instanceId']: inst for inst in launchedInstances }
                except Exception as exc:
                    logger.warning( 'could not load json (%s) %s', type(exc), exc )

        brResults = readJLog( batchJlogFilePath )
        if brResults:
            nAnalyzed += 1
            #logger.info( 'last decoded: %s', brResults[-1] )
            startingOp = findOperation( 'starting', brResults )
            #logger.info( 'startingOp: %s', startingOp )
            startingArgs = startingOp['args'].get( 'starting' )
            logger.debug( 'startingArgs: %s', startingArgs )
            batchStartDateStr = startingOp['dateTime']
            nFramesReq = startingArgs['endFrame'] + 1 - startingArgs['startFrame']

            finishedOp = findOperation( 'finished', brResults )
            if not finishedOp or not finishedOp['args']:
                logger.info( 'batch %s is not finished', batchName )
                nUnfinished += 1
                continue
            finishedArgs = finishedOp['args'].get( 'finished' )
            nFramesFinished = finishedArgs['nFramesFinished']
            if nFramesFinished == nFramesReq:
                nPerfect += 1
            
            # update the global instancesByDevId, possibly overwriting an earlier records
            for inst in launchedInstances:
                devId = inst.get( 'device-id', 0 )
                if devId:
                    instancesByDevId[ devId ] = inst

            #logger.info( 'batch %s completed: %d out of %d', batchName, nFramesFinished, nFramesReq )
            print()
            print( 'BATCH %s completed %d out of %d' % (batchName, nFramesFinished, nFramesReq) )
            print( 'using filter %s' % (startingArgs['filter']) )

            # scan installer (recruiter) log
            for recruiterResult in recruiterResults:
                rc = recruiterResult.get( 'returncode' )
                if rc:
                    failedStates[ 'installer-' + str(rc) ] += 1
                    print( 'installer RC', rc, 'for inst', recruiterResult['instanceId'] )
                rc = recruiterResult.get( 'timeout' )
                if rc:
                    failedStates[ 'installer-124' ] += 1
                    print( 'installer TIMEOUT', rc, 'for inst', recruiterResult['instanceId'] )
                ex = recruiterResult.get( 'exception' )
                if ex:
                    failedStates[ 'installer-exc' ] += 1
                    print( 'installer EXCEPTION', ex, 'for inst', recruiterResult['instanceId'] )
                sigKill = 'SIGKILL' in recruiterResult.get( 'stdout', '' ) or 'SIGILL' in recruiterResult.get( 'stdout', '' )
                if sigKill:
                    print( 'installer SIGKILL for inst', recruiterResult['instanceId'] )
                onp = 'Operation not permitted' in recruiterResult.get( 'stdout', '' )
                if onp:
                    print( 'installer ONP for inst', recruiterResult['instanceId'] )

            frameStarts = findFrameStarts( brResults )
            logger.info( 'found %d frameStarts', len(frameStarts) )
            for entry in frameStarts:
                iid = entry['instanceId']
                inst = instancesByIid.get( iid, {} )
                devId = inst.get( 'device-id', 0 )
                allDevsCounter[ devId ] += 1
                country = inst.get('device-location', {}).get( 'country', '' )
                countryCounter[ country ] += 1

            failedFrames = findFailedFrames( brResults )
            logger.debug( 'failedFrames: %s', failedFrames )
            # eliminate ones with negative frame numbers (which are for uploads and pre-checks)
            failedFrames = [frame for frame in failedFrames if frame['args']['frameNum']>=0]
            if failedFrames:
                nImperfect += 1
            for failedEntry in failedFrames:
                iid = failedEntry['instanceId']
                abbrevIid = iid[0:8]
                failedArgs = failedEntry['args']
                frameNum = failedArgs['frameNum']
                instHasONP = False
                if frameNum >= 0:
                    devId = dpr = totRam = 0
                    inst = instancesByIid.get( iid, {} )
                    if inst:
                        ramSpecs = inst.get( 'ram', {} )
                        totRam = ramSpecs.get('total', 0 )
                        devId = inst.get( 'device-id', 0 )
                        dpr = round( inst.get( 'dpr', 0 ) )
                    failedDevsCounter[ devId ] += 1
                    failKey = failedArgs['state'] + '-' + str(failedArgs['rc'])
                    failedStates[ failKey ] += 1
                    frameStartEntry = findFrameStart( iid, frameNum, brResults )
                    logger.debug( 'frameStartEntry: %s', frameStartEntry )
                    print( '%s %s on %s; devId %d, dpr %d, tot ram %d' %
                        (frameStartEntry['dateTime'][0:23], 
                            frameStartEntry['args']['state'], iid, devId, dpr, totRam )
                         )

                    stderrs = findStderrsForInstance( iid, brResults )
                    logger.debug( 'stderrs: %s', stderrs )
                    for stderr in stderrs:
                        #print( stderr )
                        if 'Nashorn engine is planned to be removed' not in stderr['args']:
                            print( '%s %s %s' % (stderr['dateTime'][0:23], abbrevIid, stderr['args']) )
                    stdouts = findStdoutsForInstance( iid, brResults )
                    #logger.debug( 'stdouts: %s', stdouts )
                    for stdout in stdouts:
                        if 'Operation not permitted' in stdout['args']:
                            if not instHasONP:
                                instHasONP = True
                                failedStates[ 'ONP' ] += 1
                                print( '%s %s %s' % (stdout['dateTime'][0:23], abbrevIid, stdout['args']) )
                        #TODO other types of errors


                    logger.debug( 'failure: %s', failedEntry )
                    logger.debug( '%s %s %s RC %d', 
                        failedEntry['dateTime'][0:23], abbrevIid, failedArgs['state'], failedArgs['rc']
                        )
                    print( '%s %s %s RC %d' %
                        (failedEntry['dateTime'][0:23], abbrevIid, failedArgs['state'], failedArgs['rc'])
                     )
                    #TODO save data for this failed instance
    print()
    print( '%d failed device(s)' % len(failedDevsCounter) )
    for x, count in failedDevsCounter.most_common():
        #print( '%s: %d' % (x, count) )
        errRate = 100 * count / allDevsCounter[x]
        inst = instancesByDevId.get( x, {} )
        dpr = round( inst.get( 'dpr', 0 ) )
        locInfo = inst.get('device-location', {})
        countryCode = locInfo.get( 'country-code' )
        locality = locInfo.get( 'locality' ) + ', ' + locInfo.get( 'area' )
        ramSpecs = inst.get( 'ram', {} )
        totRam = ramSpecs.get('total', 0 )
        print( 'dev %s in %s had %2d failure(s) in %2d attempt(s) %4.1f%% failure rate; dpr %d, ram %d (%s)' %
            (x, countryCode, count, allDevsCounter[x], errRate, dpr, totRam, locality) 
            )
    print()
    print( len(allDevsCounter), 'devices tested' )
    print( allDevsCounter )
    if not True:
        for x, count in allDevsCounter.most_common():
            #print( '%s: %d' % (x, count) )
            errRate = 100 * failedDevsCounter[x] / count
            inst = instancesByDevId.get( x, {} )
            dpr = round( inst.get( 'dpr', 0 ) )
            locInfo = inst.get('device-location', {})
            countryCode = locInfo.get( 'country-code' )
            locality = locInfo.get( 'locality' ) + ', ' + locInfo.get( 'area' )
            ramSpecs = inst.get( 'ram', {} )
            totRam = ramSpecs.get('total', 0 )
            print( 'dev %s in %s had %2d failure(s) in %2d attempt(s) %4.1f%% failure rate; dpr %d, ram %d (%s)' %
                (x, countryCode, failedDevsCounter[x], allDevsCounter[x], errRate, dpr, totRam, locality) 
                )
    print()
    print( '%d batches were analyzed ' % nAnalyzed)
    print( '%d batches were perfect (n out of n instances succeeded)' % nPerfect)
    print( '%d batch(es) had at least 1 failure' % nImperfect)
    if nUnfinished:
        print( '%d batch(es) unfinished (interrupted or still running)' % nUnfinished)
    for state, count in failedStates.items():
        print( '%s: %d' % (state, count) )
    print()
    print( 'countryCounter', countryCounter )
    print()
