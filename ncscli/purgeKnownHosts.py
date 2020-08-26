#!/usr/bin/env python3
"""
purges instance ssh info from local user's known_hosts file
"""
# standard library modules
import argparse
import json
import logging
import subprocess
#import sys

logger = logging.getLogger(__name__)


def purgeKnownHost( host, port ):
    # purge an entry from known_hosts
    cmd = 'ssh-keygen -q -R [%s]:%s > /dev/null 2> /dev/null' % (host, port )
    #logger.debug( 'cmd: %s', cmd )
    retCode = subprocess.call( cmd, shell=True )
    if retCode != 0:
        logger.error( 'returnd error code %s', retCode )

def purgeKnownHosts( inRecs ):
    for inRec in inRecs:
        if 'ssh' in inRec:
            host = inRec['ssh'].get('host')
            port = inRec['ssh'].get('port')
            if host and port:
                purgeKnownHost( host, port )

if __name__ == "__main__":
    logging.basicConfig()
    logger.setLevel(logging.INFO)
    logger.debug( 'the logger is configured' )

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@' )
    ap.add_argument( 'inFilePath', help='file path of json instance descriptions' )
    args = ap.parse_args()

    inFilePath = args.inFilePath
    with open( inFilePath ) as inFile:
        inRecs = json.load( inFile )
        for inRec in inRecs:
            if 'ssh' in inRec:
                host = inRec['ssh']['host']
                port = inRec['ssh']['port']
                purgeKnownHost( host, port )
                #break
