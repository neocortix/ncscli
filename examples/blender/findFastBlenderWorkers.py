#!/usr/bin/env python3
"""
produces dtr configuration entries based on ncs json instance descriptions
"""
# standard library modules
import argparse
import json
import logging
import os
import subprocess
import sys

# third-party modules
sys.path.append( os.path.expanduser('~/dtr'))

# Neocortix modules
#import devicePerformance


logger = logging.getLogger(__name__)


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s', datefmt='%Y/%m/%d %H:%M:%S')
    logger.setLevel(logging.INFO)
    logger.debug( 'the logger is configured' )

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@' )
    #ap.add_argument('launchedJsonFilePath', default='launched.json')
    #ap.add_argument('outJsonFilePath', default='installed.json')
    args = ap.parse_args()
    #logger.info( 'args %s', args )

    dataDirPath = os.path.expanduser('~/dtr/data')
    dtrDirPath = os.path.expanduser('~/dtr')

    settingsActualFilePath = dtrDirPath+'/user_settings.conf'
    settingsManualFilePath = dtrDirPath+'/user_settings_manual.conf'
    bmFilePath = dtrDirPath+'/benchmark_cache.p.json'
    bmOutFilePath = dataDirPath+'/dtrmarks.csv'
    #workersOutFilePath = dtrDirPath+'/fastNodes.txt'

    maxReps = 10
    bmThreshold = 90
    minWorkersToKeep = 48

    with open( settingsManualFilePath, 'r' ) as settingsFile:
        settings = settingsFile.read()
    with open( bmOutFilePath, 'w' ) as bmOutFile:
        # run dtr up to maxReps times
        for rep in range( 0, maxReps ):
            rc = subprocess.call( ['./dtr.py', '--flush', '--benchmarkOnly'] )
            if rc:
                sys.exit( 'dtr returned %d' % (rc) )

            goodWorkers = []
            anyBad = False
            with open( bmFilePath, 'r' ) as jsonFile:
                bmOuter = json.load(jsonFile)  # a list of dicts
                bmList = bmOuter[1]
                for bm in bmList:
                    node = bm[0]
                    mTime = bm[1]
                    goodness = mTime <= bmThreshold
                    logger.info( '%s %s %s', node, mTime, goodness )
                    print( node, mTime, sep=',', file=bmOutFile )
                    if goodness:
                        goodWorkers.append( node )
                    else:
                        anyBad = True
                bmOutFile.flush()
            logger.info( 'found %d fast worker nodes', len(goodWorkers) )
            with open( settingsActualFilePath, 'w' ) as workersOutFile:
                workersOutFile.write( settings )
                for worker in goodWorkers:
                    print( 'node = root@%s' % (worker), file=workersOutFile )
                workersOutFile.flush()
            if len( goodWorkers ) <= minWorkersToKeep:
                logger.info( 'quitting because n goodWorkers (%d) reached minimum (%d)',
                    len( goodWorkers ), minWorkersToKeep
                    )
                break
            if not anyBad:
                logger.info( 'no bad workers in this rep' )
                break
