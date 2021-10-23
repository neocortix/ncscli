#!/usr/bin/env python3
"""
maps and merges data from lighthouse reports for a batch, incorporating some device info
"""
# standard library modules
import argparse
import csv
import json
import logging
#import math
import os
#import sys
# neocortix modules
import ncscli.plotInstanceMap as plotInstanceMap


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

def ingestJson( inFilePath ):
    '''read the json file; return contents'''
    contents = None
    with open( inFilePath, encoding='utf8' ) as infile:
        contents = json.load( infile )
    return contents

def mergeLighthouseOutput(  outputDir, mergedCsvFileName, reportFilePat ):
    launchedJsonFilePath = outputDir + "/recruitLaunched.json"
    jlogFilePath = outputDir + "/batchRunner_results.jlog"

    #mergedCsvFileName = args.mergedCsv
    #reportFilePat = args.reportFilePattern

    launchedInstances = []
    with open( launchedJsonFilePath, 'r') as jsonInFile:
        try:
            launchedInstances = json.load(jsonInFile)  # an array
        except Exception as exc:
            logger.warning( 'could not load json (%s) %s', type(exc), exc )
    instancesByIid = { inst['instanceId']: inst for inst in launchedInstances }

    completedFrames = extractFrameInfo(jlogFilePath)
    logger.debug( 'found %d frames', len(completedFrames) )
    iidByFrame = { frame['frameNum']: frame['instanceId'] for frame in completedFrames }
    logger.debug( 'iidByFrame: %s', iidByFrame )
    frameNums = [int(frame['frameNum']) for frame in completedFrames]
    maxFrameNum = max( frameNums )
    #print( 'maxFrameNum', maxFrameNum )

    outFilePath = outputDir + '/' + mergedCsvFileName
    catsWanted = ['performance', 'accessibility', 'best-practices', 'pwa', 'seo']
    fieldNames = ['frameNum'] + catsWanted + ['country', 'area', 'locality', 'finalUrl', 'instanceId']
    outRows = []
    with open( outFilePath, 'w', newline='') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldNames)
        writer.writeheader()
        for frameNum in iidByFrame:
            outRow = {}
            inFilePath = outputDir + "/" + (reportFilePat % frameNum )
            iid = iidByFrame[ frameNum ]
            logger.debug( 'reading %s', inFilePath )
            try:
                lhr = ingestJson( inFilePath )
            except Exception as exc:
                logger.warning( 'could not ingestJson (%s) %s', type(exc), exc )
                continue
            if not lhr:
                logger.info( 'no contents in %s', inFilePath )
                continue
            logger.debug( 'lhr keys: %s', lhr.keys() )
            logger.debug( 'lhr category keys: %s', lhr['categories'].keys() )
            for cat, info in lhr['categories'].items():
                catId = info['id']
                logger.debug( 'id: %s, score: %s, title: %s', info['id'], info['score'], info['title'])
                #outRow = { 'instanceId': iid, 'id': info['id'], 'score': info['score'], 'title': info['title'],  }
                if catId in catsWanted:
                    outRow[catId] = info['score']
            if outRow:
                inst = instancesByIid.get( iid, {} )
                locInfo = inst.get('device-location', {})
                countryCode = locInfo.get( 'country-code' )
                locality = locInfo.get( 'locality' )
                area = locInfo.get( 'area' )
                outRow['country'] = countryCode
                outRow['area'] = area
                outRow['locality'] = locality
                outRow['frameNum'] = frameNum
                outRow['instanceId'] = iid
                outRow['finalUrl'] = lhr['finalUrl']
                writer.writerow( outRow )

def plotGoodInstances( outputDir ):
    launchedJsonFilePath = outputDir + "/recruitLaunched.json"
    jlogFilePath = outputDir + "/batchRunner_results.jlog"

    launchedInstances = []
    with open( launchedJsonFilePath, 'r') as jsonInFile:
        try:
            launchedInstances = json.load(jsonInFile)  # an array
        except Exception as exc:
            logger.warning( 'could not load json (%s) %s', type(exc), exc )

    completedFrames = extractFrameInfo(jlogFilePath)
    logger.debug( 'found %d frames', len(completedFrames) )

    goodIids = set([ frame['instanceId'] for frame in completedFrames ])
    goodInstances = [inst for inst in launchedInstances if inst['instanceId'] in goodIids ]

    plotInstanceMap.plotInstanceMap( goodInstances, outputDir + "/worldMap.png" )
    plotInstanceMap.plotInstanceMap( goodInstances, outputDir + "/worldMap.svg" )


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
    ap.add_argument( '--reportFilePattern', default='puppeteerOut_%03d/lighthouse.report.json', help='%%-based pattern for worker report json file names' )
    ap.add_argument( '--mergedCsv', default='lighthouse_scores.csv', help='file name for merged results csv file' )
    args = ap.parse_args()

    logger.info( 'merging data in directory %s', os.path.realpath(args.dataDirPath)  )
    mergeLighthouseOutput( args.dataDirPath, args.mergedCsv, args.reportFilePattern )
    logger.debug( 'plotting instance map' )
    plotGoodInstances( args.dataDirPath )
