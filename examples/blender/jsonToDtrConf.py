#!/usr/bin/env python3
"""
produces dtr configuration entries based on ncs json instance descriptions
"""
# standard library modules
import argparse
import json
import logging
import sys

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logging.basicConfig()
    logger.setLevel(logging.INFO)
    logger.debug( 'the logger is configured' )

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@' )
    args = ap.parse_args()
    #logger.info( 'args %s', args )

    inRecs = json.load(sys.stdin)

    outLines = []
    for inRec in inRecs:
        details = inRec
        iid = details['instanceId']
        #logger.info( 'NCSC Inst details %s', details )
        if 'commandState' in details and details['commandState'] != 'good':
            continue
        if details['state'] == 'started':
            if 'ssh' in details:
                host = details['ssh']['host']
                port = details['ssh']['port']
                user = details['ssh']['user']
                outLine = "node = %s@%s:%s" % (
                        user, host, port
                )
                #print( outLine)
                outLines.append( outLine )
                #print( "node = root@%s:%s" % (
                #        host, port
                #    ))
    for outLine in sorted( outLines):
        print( outLine )
