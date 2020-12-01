#!/usr/bin/env python3
"""
merge csv files produced by batchRunner examples, inserting worker-relative time stamps
"""
# standard library modules
import argparse
import csv
import json
import logging
#import math
import os
import sys
#import warnings


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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


if __name__ == "__main__":
    # configure logger formatting
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)
    logging.captureWarnings(True)

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@', formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    ap.add_argument( '--dataDirPath', required=True, help='the path to to directory for input and output data' )
    ap.add_argument( '--csvPat', default='worker_%03d_*.csv', help='%%-based pattern for worker result csv file names' )
    ap.add_argument( '--mergedCsv', default='workers_merged.csv', help='file name for merged results csv file' )
    ap.add_argument( '--tsField', default='timeStamp', help='the name of the time stamp field in incoming csv files' )
    ap.add_argument( '--timeDiv', type=float, default=1000, help='timeStamp divisor (1000 for incoming ms; 1 for incoming seconds)' )
    args = ap.parse_args()

    logger.info( 'merging data in directory %s', os.path.realpath(args.dataDirPath)  )

    outputDir = args.dataDirPath
    launchedJsonFilePath = outputDir + "/recruitLaunched.json"
    jlogFilePath = outputDir + "/batchRunner_results.jlog"

    mergedCsvFileName = args.mergedCsv
    resultsCsvPat = args.csvPat
    tsDivisor = args.timeDiv
    if tsDivisor <= 0:
        sys.exit( 'error: please pass a --timeDiv greater than 0')

    launchedInstances = []
    with open( launchedJsonFilePath, 'r') as jsonInFile:
        try:
            launchedInstances = json.load(jsonInFile)  # an array
        except Exception as exc:
            logger.warning( 'could not load json (%s) %s', type(exc), exc )
    instancesByIid = { inst['instanceId']: inst for inst in launchedInstances }

    completedFrames = extractFrameInfo(jlogFilePath)
    logger.info( 'found %d frames', len(completedFrames) )
    iidByFrame = { frame['frameNum']: frame['instanceId'] for frame in completedFrames }
    logger.debug( 'iidByFrame: %s', iidByFrame )

    extraFields = ['relTime', 'instanceId']
    outFilePath = outputDir + '/' + mergedCsvFileName
    totRowsRead = 0
    with open( outFilePath, 'w', newline='') as outfile:
        fieldNames = None
        writer = None
        for frameNum in iidByFrame:
            inFilePath = outputDir + "/" + (resultsCsvPat % frameNum )
            iid = iidByFrame[ frameNum ]
            logger.debug( 'reading %s', inFilePath )
            rows = ingestCsv( inFilePath )
            if not rows:
                logger.info( 'no rows in %s', inFilePath )
                continue
            logger.info( 'read %d rows from %s', len(rows), inFilePath )
            totRowsRead += len(rows)
            timeStamps = [float(row[args.tsField]) for row in rows]
            minTimeStamp = min(timeStamps)
            if not fieldNames:
                fieldNames = list( rows[0].keys() ) + extraFields
                logger.info( 'columns:  %s', fieldNames )
            if not writer:
                if not fieldNames:
                    logger.warning( 'no fieldNames')
                    continue
                writer = csv.DictWriter(outfile, fieldnames=fieldNames)
                writer.writeheader()
            for row in rows:
                outRow = row
                relTime = (float(row['timeStamp'])-minTimeStamp) / tsDivisor
                outRow['relTime'] = round( relTime, 4 )
                outRow['instanceId'] = iid
                writer.writerow( outRow )
    logger.info( 'totRowsRead: %d', totRowsRead )