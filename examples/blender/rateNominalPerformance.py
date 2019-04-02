#!/usr/bin/env python3
"""
produces dtr configuration entries based on ncs json instance descriptions
"""
# standard library modules
import argparse
import json
import logging
import sys

# Neocortix modules
import devicePerformance


logger = logging.getLogger(__name__)


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s', datefmt='%Y/%m/%d %H:%M:%S')
    logger.setLevel(logging.INFO)
    logger.debug( 'the logger is configured' )

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@' )
    ap.add_argument('launchedJsonFilePath', default='launched.json')
    ap.add_argument('outJsonFilePath', default='installed.json')
    args = ap.parse_args()
    #logger.info( 'args %s', args )

    performanceCutoff = 37


    loadedInstances = None
    with open( args.launchedJsonFilePath, 'r' ) as jsonFile:
        loadedInstances = json.load(jsonFile)  # a list of dicts

    goodInstances = []
    for inst in loadedInstances:
        iid = inst['instanceId']
        #logger.info( 'NCSC Inst details %s', inst )
        # cpuarch:      string like "aarch64" or "armv7l"
        # cpunumcores:  int
        # cpuspeeds:    list of floats of length cpunumcores, each representing a clock frequency in GHz
        # cpufamily:    list of strings of length cpunumcores
        cpuarch = inst['cpu']['arch']
        cpunumcores = len( inst['cpu']['cores'])
        cpuspeeds = []
        cpufamily = []
        for core in inst['cpu']['cores']:
            cpuspeeds.append( core['freq'] / 1e9)
            cpufamily.append( core['family'] )
        
        dpr = devicePerformance.devicePerformanceRating( cpuarch, cpunumcores, cpuspeeds, cpufamily )
        print( 'device', inst['device-id'], 'dpr', dpr )
        inst['dpr'] = dpr
        if dpr >= performanceCutoff:
            goodInstances.append( inst )
    logger.info( 'chose %d instances with %d+ dpr', len(goodInstances), performanceCutoff )
    with open( args.outJsonFilePath, 'w') as outFile:
        json.dump( goodInstances, outFile, default=str, indent=2, skipkeys=True )

