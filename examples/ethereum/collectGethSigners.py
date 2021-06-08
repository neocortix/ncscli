#!/usr/bin/env python3
"""
collect authorized and propsed geth signing nodes
"""

# standard library modules
import argparse
from concurrent import futures
import datetime
import json
import logging
import os
import re
import subprocess
import sys
import threading
import time
# third-party modules
import psutil
import requests
# neocortix modules
#import ncscli.ncs as ncs
import ncscli.tellInstances as tellInstances
import ncsgeth  # assumed to be in the same directory as this script

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


if __name__ == "__main__":
    # configure logger formatting
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)
    logger.setLevel(logging.WARNING)

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@' )
    #ap.add_argument( '--dataDirPath', help='the path to the directory for input and output data' )
    ap.add_argument( '--invFile', help='the path an ansible inventory file in json form' )
    ap.add_argument( '--ncsInstances', help='the path an ncs instances file' )
    ap.add_argument( '--configName', required=True, help='the name of the geth configuration' )
    ap.add_argument( '--logLevel', default ='info', help='verbosity of log (e.g. debug, info, warning, error)' )
    args = ap.parse_args()

    logLevel = ncsgeth.parseLogLevel( args.logLevel )
    logger.setLevel(logLevel)
    tellInstances.logger.setLevel( logLevel )
    logger.debug('the logger is configured')

    invFilePath = args.invFile
    if not invFilePath:
        logger.error( 'no --invFile given' )
        sys.exit( 1 )
    # get details of launched instances from the json file
    if not os.path.isfile( invFilePath ):
        logger.error( 'file "%s"', invFilePath )
        sys.exit( 1 )
    instances = ncsgeth.loadInstances( invFilePath )
    logger.info( 'inventory instances: %s', instances)

    savedSignersFilePath = None
    if args.ncsInstances:
        savedSignersFilePath = os.path.dirname( args.ncsInstances ) + '/savedSigners.json'
        ncsInstances = ncsgeth.loadInstances( args.ncsInstances )
        if ncsInstances:
           instances.extend( ncsInstances )

    logger.info( 'calling collectSignerInstances')
    signerInfos = ncsgeth.collectSignerInstances( instances, args.configName )
    logger.info( '%d signerInfos: %s', len( signerInfos), signerInfos )
    logger.info( 'done collectSignerInstances')

    if instances:
        instancesByIid = {inst['instanceId']: inst for inst in instances }
        propsFound = ncsgeth.collectProposals( instances, args.configName )
        propSummary = propsFound['summary']
        proposees = set( propSummary.keys() )
        logger.debug( '%d proposees: %s', len(proposees), proposees )
        logger.info( 'propSummary: %s', propSummary )

        signers = ncsgeth.collectAuthSigners( instances, args.configName )
        logger.info( '%d authSigners: %s', len(signers), signers )

        allSigners = proposees.union( signers )
        logger.debug( '%d allSigners: %s', len(allSigners), allSigners )

        nonauth = proposees - signers
        logger.info( '%d unauth: %s', len(nonauth), nonauth )

        # get primary accounts from each instance
        instanceAccountPairs = ncsgeth.collectPrimaryAccounts( instances, args.configName )
        instancesByAccount = {pair['accountAddr']: instancesByIid[pair['instanceId']]
            for pair in instanceAccountPairs if pair.get( 'accountAddr' )
            }
        logger.info( '%d instanceAccountPairs: %s', len(instanceAccountPairs), instanceAccountPairs)

        if allSigners:
            logger.info( 'iterating allSigners')
            for signerId in allSigners:
                if signerId not in instancesByAccount:
                    logger.debug( 'no instance for signer %s', signerId )
                    continue
                inst = instancesByAccount[signerId]
                iid = inst.get('instanceId')
                if 'ssh' not in inst:
                    logger.warning( 'no ssh info for instance %s', iid )
                    continue
                if 'host' not in inst['ssh']:
                    logger.warning( 'no ssh host for instance %s', iid )
                    continue
                logger.debug( '%s: %s', signerId, inst['ssh']['host'] )
                print( iid, signerId, signerId in signers, sep=',', file = sys.stdout )

            if savedSignersFilePath:
                savedSigners = {}
                if os.path.isfile( savedSignersFilePath ):
                    with open( savedSignersFilePath, 'r') as jsonInFile:
                        try:
                            savedSigners = json.load(jsonInFile) # a dict of lists, indexed by iid
                        except Exception as exc:
                            logger.warning( 'could not load savedSigners json (%s) %s', type(exc), exc )
                for signerId in allSigners:
                    if signerId not in instancesByAccount:
                        continue
                    inst = instancesByAccount[signerId]
                    iid = inst.get('instanceId')
                    if iid:
                        savedSigners[ iid ] = [signerId]

                with open( savedSignersFilePath,'w' ) as outFile:
                    json.dump( savedSigners, outFile, indent=2 )
    logger.info( 'finished' )
