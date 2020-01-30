#!/usr/bin/env python3
"""
uses dtr to benchmark all the nodes in user_settings, looking for fast ones
"""
# standard library modules
import argparse
import json
import logging
import os
import re
import subprocess
import sys

# third-party modules
sys.path.append( os.path.expanduser('~/dtr'))  # should not need
import pandas as pd

# Neocortix modules
#import devicePerformance


logger = logging.getLogger(__name__)


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s', datefmt='%Y/%m/%d %H:%M:%S')
    logger.setLevel(logging.INFO)
    logger.debug( 'the logger is configured' )

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@' )
    ap.add_argument('--minWorkersToKeep', type=int, default=0)
    ap.add_argument('--numToKeep', type=int, default=sys.maxsize)
    #ap.add_argument('outJsonFilePath', default='installed.json')
    args = ap.parse_args()
    #logger.info( 'args %s', args )

    #dataDirPath = '.'
    dataDirPath = os.path.abspath('./data')
    #dataDirPath = os.path.expanduser('~/dtr/data')
    dtrDirPath = os.path.expanduser('~/dtr')

    dtrBinPath = dtrDirPath + '/dtr.py'
    settingsActualFilePath = dataDirPath+'/user_settings.conf'
    #settingsManualFilePath = dataDirPath+'/user_settings_manual.conf'
    bmFilePath = dataDirPath+'/benchmark_cache.p.json'
    bmOutFilePath = dataDirPath+'/dtrmarks.csv'
    #workersOutFilePath = dtrDirPath+'/fastNodes.txt'

    maxReps = 3
    bmThreshold = 90
    #minWorkersToKeep = 36
    #numToKeep = 40

    #with open( settingsManualFilePath, 'r' ) as settingsFile:
    #    settings = settingsFile.read()
    settings = ''
    with open( settingsActualFilePath, 'r' ) as settingsFile:
        for line in settingsFile:
            if not re.search(r'^\s*node\s*=', line ):
                settings += line
    print( 'settings' )
    print( settings )
    #sys.exit()

    totTimes = {}
    reps = {}
    with open( bmOutFilePath, 'w' ) as bmOutFile:
        print( 'node,time', file=bmOutFile )
        # run dtr up to maxReps times
        for rep in range( 0, maxReps ):
            logger.info( 'benchmarking pass %d', rep+1 )
            rc = subprocess.call( [dtrBinPath, '--flush', '--benchmarkOnly'],
                cwd=dataDirPath )
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
                    totTimes[ node ] = totTimes.get(node, 0) + mTime
                    reps[ node ] = reps.get(node, 0) + 1
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
            if len( goodWorkers ) <= args.minWorkersToKeep:
                logger.info( 'quitting because n goodWorkers (%d) reached minimum (%d)',
                    len( goodWorkers ), args.minWorkersToKeep
                    )
                break
            #if not anyBad:
            #    logger.info( 'no bad workers in this rep' )
            #    break
            logger.info( '%d good after %d reps', len( goodWorkers ), rep+1 )
        # done main loop
        survivors = []
        for node in goodWorkers:
            survivors.append( {'node': node, 'totTime': totTimes[node], 'reps': reps[node] } )
        survivingDf = pd.DataFrame( survivors )
        sorted = survivingDf.sort_values( 'totTime' )
        #print( sorted )
        numToKeep = min( args.numToKeep, len(sorted) )
        print( 'selected', numToKeep )
        print( sorted.iloc[0:numToKeep] )
        # save the final list as settings file
        with open( settingsActualFilePath, 'w' ) as workersOutFile:
            workersOutFile.write( settings )
            for index, row in sorted.iloc[0:numToKeep].iterrows():
                print( 'node = root@%s' % (row.node), file=workersOutFile )
            workersOutFile.flush()



