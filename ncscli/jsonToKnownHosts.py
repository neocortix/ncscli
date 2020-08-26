#!/usr/bin/env python3
"""
converts key info from an instances.json format to known_hosts format
"""
# standard library modules
import argparse
import json
import logging
import socket
import sys

logger = logging.getLogger(__name__)


def jsonToKnownHosts( instances, outFile ):
    outLines = []
    for inRec in instances:
        details = inRec
        if 'commandState' in details and details['commandState'] != 'good':
            continue
        if details['state'] == 'started':
            if 'ssh' in details:
                host = details['ssh']['host']
                port = details['ssh']['port']
                ecdsaKey = details['ssh']['host-keys']['ecdsa']
                try:
                    ipAddr = socket.gethostbyname( host )
                except Exception as exc:
                    logger.warning( 'exception (%s) for host %s', type(exc), host )
                else:
                    outLine = "[%s]:%s,[%s]:%s %s" % (
                        host, port, ipAddr, port, ecdsaKey
                        )
                    outLines.append( outLine )
    for outLine in sorted( outLines):
        print( outLine, file=outFile )

if __name__ == "__main__":
    logging.basicConfig()
    logger.setLevel(logging.INFO)
    logger.debug( 'the logger is configured' )

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@' )
    args = ap.parse_args()
    #logger.info( 'args %s', args )

    inRecs = json.load(sys.stdin)
    jsonToKnownHosts( inRecs, sys.stdout )
