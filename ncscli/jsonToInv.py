#!/usr/bin/env python3
"""
outputs instance information as an ansible-compativle inventory file
"""
# standard library modules
import argparse
import json
import logging
import sys

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s', datefmt='%Y/%m/%d %H:%M:%S')
    logger.setLevel(logging.INFO)
    logger.debug( 'the logger is configured' )

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@' )
    args = ap.parse_args()
    #logger.info( 'args %s', args )

    try:
        inRecs = json.load(sys.stdin)
    except Exception as exc:
        sys.exit( 'could not decode input as json (%s)' % (exc) )

    for inRec in inRecs:
        details = inRec
        if 'instanceId' not in details:
            logger.error( 'no "instanceId" field found in %s', details)
            continue
        iid = details['instanceId']
        #logger.info( 'NCSC Inst details %s', details )
        if 'state' not in details:
            logger.error( 'no "state" field found for instance %s', iid)
        if details.get('state') == 'started':
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

