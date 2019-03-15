#!/usr/bin/env python3
"""
terminates AWS EC2 instances, dooming them to deletion
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

    for inRec in inRecs:
        details = inRec
        iid = details['instanceId']
        #logger.info( 'NCSC Inst details %s', details )
        if details['state'] == 'started':
            if 'ssh' not in details:
                host = 'none'
                port = 0
                print( '#', end='' )  # comment out this line in output
            else:
                host = details['ssh']['host']
                port = details['ssh']['port']
            print( "phone_%s ansible_python_interpreter=/usr/bin/python3 ansible_user=root ansible_ssh_host=%s ansible_port=%s" % (
                iid,
                host,
                port
                #, details['ssh']['password'])
            ))

