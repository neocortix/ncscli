#!/usr/bin/env python3
"""
produces aggregate reports from runBatchGatling using gatling
"""
# standard library modules
import argparse
import glob
import logging
import os
import re
import shutil
import subprocess
import sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


if __name__ == "__main__":
    # configure logger formatting
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)


    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@', formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    ap.add_argument( '--dataDirPath', required=True, help='the path to to directory for input and output data' )
    args = ap.parse_args()

    gatlingBinPath = 'gatling-3.4.0/bin/gatling.sh'
    if not os.path.isfile( gatlingBinPath ):
        sys.exit( 'ERROR the gatling binary was not found at ' +  gatlingBinPath )

    logger.info( 'collecting data in directory %s', os.path.realpath(args.dataDirPath)  )

    # search for simultion.log files in subdirectories
    resultFilePaths = []
    workerDirs = glob.glob( os.path.join( args.dataDirPath, 'gatlingResults_*' ) )
    for workerDir in workerDirs:
        dirContents = os.listdir( workerDir )
        if dirContents:
            innerPath = os.path.join( workerDir, dirContents[0] )
            if os.path.isdir( innerPath ):
                filePath = os.path.join( innerPath, 'simulation.log' )
                if os.path.isfile( filePath ):
                    resultFilePaths.append( filePath )

    if not resultFilePaths:
        sys.exit( 'ERROR no simulation.log files were found' )

    glogsDirPath = os.path.join( args.dataDirPath, 'gatlingResults_aggregated' )
    os.makedirs( glogsDirPath, exist_ok=True )

    # copy the simulation.log files (which contain timings for every request)
    nCopied = 0
    pat = r'gatlingResults_([^/]*)'  # for extracting frameNum
    copiedFilePaths = []
    for inFilePath in resultFilePaths:
        match = re.search( pat, inFilePath ).group(1)
        if match:
            frameNum = int( match )
            outFilePath = os.path.join( glogsDirPath, 'simulation_'+ match + '.log' )
            shutil.copyfile( inFilePath, outFilePath )
            copiedFilePaths.append( outFilePath )
            nCopied +=1
    logger.debug( 'copied %d log files to %s', nCopied, glogsDirPath )
    if nCopied:
        # run gatling in reports-only mode on the aggregated log files
        cmd =[gatlingBinPath, '--reports-only', 'gatlingResults_aggregated', '-rf', args.dataDirPath]
        rc = subprocess.call( cmd, shell=False, stdin=subprocess.DEVNULL )
        if rc:
            logger.warning( 'gatling exited with return code %d', rc)
        else:
            logger.info( 'gatling reports are in %s', args.dataDirPath+'/gatlingResults_aggregated' )
        for filePath in copiedFilePaths:
            os.remove( filePath )
        sys.exit( rc )
    else:
        sys.exit( 'ERROR could not copy any simulation.log files' )
