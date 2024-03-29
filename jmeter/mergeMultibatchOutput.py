#!/usr/bin/env python3
"""
merge csv files produced by multi-batch batchRunner examples
"""
# standard library modules
import argparse
import csv
import glob
import json
import logging
import math
import os
import sys
#import warnings
# third-party modules
import numpy as np


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

def extractFrameInfo( inFilePath ):
    '''extract frame numbers and instance ids from a batchRunner jlog file''' 
    instanceList = []
    with open( inFilePath, 'rb' ) as inFile:
        for line in inFile:
            decoded = json.loads( line )
            # print( 'decoded', decoded ) # just for debugging, would be verbose
            # iid = decoded.get( 'instanceId', '<unknown>')
            if 'args' in decoded:
                # print( decoded['args'] )
                if type(decoded['args']) is dict and 'state' in decoded['args'].keys():
                    if decoded['args']['state'] == 'retrieved':
                        # print("%s  %s" % (decoded['args']['frameNum'],decoded['instanceId']))
                        #instanceList.append([decoded['args']['frameNum'],decoded['instanceId']])
                        instanceList.append(
                            {'frameNum': decoded['args']['frameNum'],
                                'instanceId': decoded['instanceId']}
                            )
    return instanceList

def ingestCsv( inFilePath ):
    '''read the csv file; return contents as a list of dicts'''
    rows = []
    with open( inFilePath, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            rows.append( row )
    return rows

def findTimeStampBounds():
    # uses global iidByFrame, outputDir, and others
    outBounds = []
    for frameNum in iidByFrame:
        inFilePath = batchDirPath + "/" + (resultsCsvPat % frameNum )
        logger.debug( 'reading %s', inFilePath )
        try:
            rows = ingestCsv( inFilePath )
        except Exception as exc:
            logger.warning( 'could not ingestCsv (%s) %s', type(exc), exc )
            continue
        if not rows:
            logger.info( 'no rows in %s', inFilePath )
            continue
        logger.debug( 'read %d rows from %s', len(rows), inFilePath )
        timeStamps = [float(row[args.tsField]) for row in rows]
        minTimeStamp = min(timeStamps)
        maxTimeStamp = max(timeStamps)
        outBounds.append({ 'min': minTimeStamp, 'max': maxTimeStamp })
    return outBounds


if __name__ == "__main__":
    # configure logger formatting
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)
    logging.captureWarnings(True)
    #logger.setLevel(logging.DEBUG)  # for more verbosity

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@', formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    ap.add_argument( '--dataDirPath', required=True, help='the path to to directory for input and output data' )
    ap.add_argument( '--csvPat', default='worker_%03d_*.csv', help='%%-based pattern for worker result csv file names' )
    ap.add_argument( '--mergedCsv', default='workers_merged.csv', help='file name for merged results csv file' )
    ap.add_argument( '--tsField', default='timeStamp', help='the name of the time stamp field in incoming csv files' )
    ap.add_argument( '--timeDiv', type=float, default=1000, help='timeStamp divisor (1000 for incoming ms; 1 for incoming seconds)' )
    ap.add_argument( '--multibatch', type=boolArg, help='pass True for multiple batches, false for a single batch' )
    ap.add_argument( '--augment', type=boolArg, help='pass True if you want additional columns' )
    args = ap.parse_args()

    logger.info( 'merging data in directory %s', os.path.realpath(args.dataDirPath)  )

    outputDir = args.dataDirPath
    #launchedJsonFilePath = outputDir + "/recruitLaunched.json"

    mergedCsvFileName = args.mergedCsv
    resultsCsvPat = args.csvPat
    tsDivisor = args.timeDiv
    if tsDivisor <= 0:
        sys.exit( 'error: please pass a --timeDiv greater than 0')

    '''
    launchedInstances = []
    with open( launchedJsonFilePath, 'r') as jsonInFile:
        try:
            launchedInstances = json.load(jsonInFile)  # an array
        except Exception as exc:
            logger.warning( 'could not load json (%s) %s', type(exc), exc )
    instancesByIid = { inst['instanceId']: inst for inst in launchedInstances }
    '''
    if args.multibatch:
        batchDirPaths = glob.glob( os.path.join( outputDir, 'batch_*_*' ) )
    else:
        batchDirPaths = [outputDir]
    logger.info( 'batchDirs: %s', batchDirPaths )

    totRowsRead = 0
    fieldNames = None
    writer = None
    outFilePath = outputDir + '/' + mergedCsvFileName
    with open( outFilePath, 'w', newline='') as outfile:
        for batchDirPath in batchDirPaths:
            jlogFilePath = batchDirPath + "/batchRunner_results.jlog"
            if not os.path.isfile( jlogFilePath ):
                logger.warning( 'did not find %s in %s', 'batchRunner_results.jlog', batchDirPath )
                continue
            completedFrames = extractFrameInfo(jlogFilePath)
            logger.debug( 'found %d frames', len(completedFrames) )
            if not completedFrames:
                continue  # move on to next batch
            iidByFrame = { frame['frameNum']: frame['instanceId'] for frame in completedFrames }
            logger.debug( 'iidByFrame: %s', iidByFrame )
            frameNums = [int(frame['frameNum']) for frame in completedFrames]
            maxFrameNum = max( frameNums )
            #print( 'maxFrameNum', maxFrameNum )

            timeStampBounds = findTimeStampBounds()
            logger.debug( 'timeStampBounds %s', timeStampBounds )
            if not timeStampBounds:
                logger.warning( 'no timestamps found in any files')
                sys.exit( 1 )
            minTimeStamps = [bounds['min'] for bounds in timeStampBounds]
            maxTimeStamps = [bounds['max'] for bounds in timeStampBounds]

            minMinTimeStamp = int( min( minTimeStamps ) ) if minTimeStamps else 0
            maxMaxTimeStamp = max( maxTimeStamps ) if maxTimeStamps else minMinTimeStamp + 1
            logger.debug( 'minMinTimeStamp %d', minMinTimeStamp )

            effDurs = [(bounds['max']-minMinTimeStamp)/args.timeDiv for bounds in timeStampBounds]
            logger.debug( 'effDurs %s', effDurs )

            maxSeconds = int( math.ceil( max(effDurs) ) )

            extraFields = ['relTime', 'instanceId'] if args.augment else []
            allThreadsCounter = np.zeros( [maxSeconds+1, maxFrameNum], dtype=np.int64 )
            grpThreadsCounter = np.zeros( [maxSeconds+1, maxFrameNum], dtype=np.int64 )
            outRows = []
            for frameNum in iidByFrame:
                inFilePath = batchDirPath + "/" + (resultsCsvPat % frameNum )
                iid = iidByFrame[ frameNum ]
                logger.debug( 'reading %s', inFilePath )
                try:
                    rows = ingestCsv( inFilePath )
                except Exception as exc:
                    logger.warning( 'could not ingestCsv (%s) %s', type(exc), exc )
                    continue
                if not rows:
                    logger.info( 'no rows in %s', inFilePath )
                    continue
                logger.debug( 'read %d rows from %s', len(rows), inFilePath )
                totRowsRead += len(rows)
                timeStamps = [float(row[args.tsField]) for row in rows]
                minTimeStamp = min(timeStamps)
                if minTimeStamp > minMinTimeStamp + 60000:
                    logger.debug( 'frame %d started late', frameNum )
                if not fieldNames:
                    fieldNames = list( rows[0].keys() ) + extraFields
                    logger.debug( 'columns:  %s', fieldNames )
                if not writer:
                    if not fieldNames:
                        logger.warning( 'no fieldNames')
                        continue
                    writer = csv.DictWriter(outfile, fieldnames=fieldNames)
                    writer.writeheader()
                for row in rows:
                    outRow = row
                    relTime = (float(row[args.tsField])-minMinTimeStamp) / tsDivisor
                    roundedTs = min( maxSeconds, round( relTime / 10 ) * 10 )
                    allThreadsCounter[ roundedTs, frameNum-1 ] = int( row['allThreads'] )
                    grpThreadsCounter[ roundedTs, frameNum-1 ] = int( row['grpThreads'] )
                    if args.augment:
                        outRow['relTime'] = round( relTime, 4 )
                        outRow['instanceId'] = iid
                    #outRow['threadName'] = outRow['threadName'] + '-' + str( frameNum )
                    outRows.append( outRow )
                    #writer.writerow( outRow )
            for row in outRows:
                outRow = row
                relTime = (float(row[args.tsField])-minMinTimeStamp) / tsDivisor
                roundedTs = min( maxSeconds, round( relTime / 10 ) * 10 )
                nThreads = allThreadsCounter[ roundedTs, :].sum()
                outRow['allThreads'] = nThreads
                nThreads = grpThreadsCounter[ roundedTs, :].sum()
                outRow['grpThreads'] = nThreads
                writer.writerow( outRow )
        logger.debug( 'totRowsRead: %d', totRowsRead )
