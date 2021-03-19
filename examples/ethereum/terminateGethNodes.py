#!/usr/bin/env python3
"""
terminates instances and purges them from known_hosts
"""
# standard library modules
import argparse
import json
import logging
import os
import subprocess
#import sys
# neocortix modules
import ncscli.ncs as ncs

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)
    logger.setLevel(logging.INFO)
    ncs.logger.setLevel(logging.INFO)
    logger.debug( 'the logger is configured' )

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@' )
    ap.add_argument( 'inFilePath', help='file path of json instance descriptions' )
    ap.add_argument( '--authToken', help='the NCS authorization token to use (default uses env var)' )
    args = ap.parse_args()

    # use authToken env var if none given as arg
    authToken = args.authToken or os.getenv('NCS_AUTH_TOKEN')

    inFilePath = args.inFilePath
    if os.path.isdir( inFilePath ):
        inFilePath = os.path.join( inFilePath, 'recruitLaunched.json' )
        logger.info( 'a directory path was given; reading from %s', inFilePath )
    with open( inFilePath ) as inFile:
        instances = json.load( inFile )
        if instances:
            jobId = instances[0]['job']
            # terminate only if there's an authtoken  
            if authToken:
                ncs.terminateJobInstances( authToken, jobId )
            else:
                logger.info( 'no authToken given, so not terminating')
            ncs.purgeKnownHosts( instances )
    logger.info( 'finished' )
